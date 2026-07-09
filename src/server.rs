use crate::{
    dispatch::context::Context,
    manager::{channel::ChannelManager, session::SessionManager, user::UserManager},
};
use irc_proto::{connection::Connection, message::Message};
use log::{debug, info};
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
    channel_manager: ChannelManager,
    listener_handle: JoinHandle<io::Result<()>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl Server {
    fn new() -> Self {
        Self {
            session_manager: SessionManager::new(),
            user_manager: UserManager::new(),
            channel_manager: ChannelManager::new(),
            listener_handle: tokio::spawn(async { Ok(()) }),
            shutdown_tx: broadcast::channel(1).0,
        }
    }

    pub async fn start(address: SocketAddr) -> Result<Self, ()> {
        let mut server = Self::new();
        let session_mgr = server.session_manager.clone();
        let user_mgr = server.user_manager.clone();
        let channel_mgr = server.channel_manager.clone();
        let mut shutdown = server.subscribe_shutdown();

        server.listener_handle = tokio::spawn(async move {
            let listener = TcpListener::bind(address).await?;
            loop {
                tokio::select! {
                    Ok((stream, addr)) = listener.accept() => {
                        debug!("connection from {:}", addr);

                        let (write, read) = Server::spawn_connection_task(stream, shutdown.resubscribe());
                        let session_id = session_mgr.create_session(write).await;
                        let user_id = user_mgr.create_user(session_id).await;
                        let ctx = Context::new(session_id, user_id, session_mgr.clone(), user_mgr.clone(), channel_mgr.clone());
                        let exit_sess_mgr = session_mgr.clone();
                        let exit_user_mgr = user_mgr.clone();
                        tokio::spawn(async move {
                            let _exit_status = Server::run_session(read, ctx).await;
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

    async fn run_session(
        mut read: mpsc::UnboundedReceiver<Message>,
        mut ctx: Context,
    ) -> Result<(), ()> {
        let ctx = loop {
            match read.recv().await {
                Some(msg) => {
                    info!("user: {:?}", msg);
                    ctx.handle(msg).await?;
                    ctx = match ctx.register().await {
                        Ok(registered_ctx) => break registered_ctx,
                        Err(unregistered_ctx) => unregistered_ctx,
                    };
                }
                None => return Ok(()),
            }
        };

        ctx.welcome().await?;
        while let Some(msg) = read.recv().await {
            info!("{:?}: {:?}", ctx.get_nickname().await, msg);
            ctx.handle(msg).await?
        }
        Ok(())
    }

    fn spawn_connection_task(
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
