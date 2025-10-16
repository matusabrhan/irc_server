use crate::{
    dispatch::context::Context,
    manager::{session::SessionManager, user::UserManager},
};
use irc_proto::{connection::Connection, message::Message};
use log::{debug, info, warn};
use std::{io, net::SocketAddr, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::sleep,
};

pub struct Server {
    session_manager: SessionManager,
    user_manager: UserManager,
    listener_handle: JoinHandle<io::Result<()>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl Server {
    fn new() -> Self {
        Self {
            session_manager: SessionManager::new(),
            user_manager: UserManager::new(),
            listener_handle: tokio::spawn(async { Ok(()) }),
            shutdown_tx: broadcast::channel(1).0,
        }
    }

    pub async fn start(address: SocketAddr) -> Result<Self, ()> {
        let mut server = Self::new();
        let session_mgr = server.session_manager.clone();
        let user_mgr = server.user_manager.clone();
        let mut shutdown = server.subscribe_shutdown();

        server.listener_handle = tokio::spawn(async move {
            let listener = TcpListener::bind(address).await?;
            loop {
                tokio::select! {
                    Ok((stream, addr)) = listener.accept() => {
                        debug!("connection from {:}", addr);

                        let (write, read) = Server::spawn_rw_task(stream, shutdown.resubscribe());
                        let session_id = session_mgr.create_session(write).await;
                        let user_id = user_mgr.create_user().await;
                        let ctx = Context::new(session_id, user_id, session_mgr.clone(), user_mgr.clone());
                        let exit_sess_mgr = session_mgr.clone();
                        let exit_user_mgr = user_mgr.clone();
                        tokio::spawn(async move {
                            Server::run_session(read, ctx).await;
                            exit_user_mgr.delete_user(user_id).await;
                            exit_sess_mgr.delete_session(session_id).await;
                        });
                    }
                    _ = shutdown.recv() => break,
                }
            }
            Ok(())
        });

        info!("Server started at {:}", address);
        Ok(server)
    }

    async fn run_session(mut read: mpsc::UnboundedReceiver<Message>, mut ctx: Context) {
        let registered_ctx = loop {
            match read.recv().await {
                Some(msg) => {
                    info!("msg: {:?}", msg);
                    if ctx.handle(msg).await.is_err() {
                        return;
                    }
                    ctx = match ctx.register().await {
                        Ok(registered_ctx) => break registered_ctx,
                        Err(unregistered_ctx) => unregistered_ctx,
                    };
                }
                None => {
                    return;
                }
            }
        };
        while let Some(msg) = read.recv().await {
            info!("msg: {:?}", msg);
            if registered_ctx.handle(msg).await.is_err() {
                break;
            }
        }
    }

    fn spawn_rw_task(
        stream: TcpStream,
        mut shutdown: broadcast::Receiver<()>,
    ) -> (
        mpsc::UnboundedSender<Message>,
        mpsc::UnboundedReceiver<Message>,
    ) {
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Message>();
        let (read_tx, read_rx) = mpsc::unbounded_channel::<Message>();

        let mut conn = Connection::new(stream);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = conn.read() => {
                        match msg {
                            Ok(msg) => {
                                if let Err(_) = read_tx.send(msg) {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    msg = write_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if let Err(_) = conn.write(msg).await {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = shutdown.recv() => break,
                }
            }
            write_rx.close();
            warn!("rw task exited");
        });
        (write_tx, read_rx)
    }

    pub fn subscribe_shutdown(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    pub async fn shutdown(&self) {
        // TODO: await listener_handle ?
        info!("Server shutting down");
        let mut remaining = self.shutdown_tx.send(());
        while remaining.is_ok() {
            sleep(Duration::from_millis(100)).await;
            remaining = self.shutdown_tx.send(());
        }
    }
}
