use crate::ipc_bus::{ServerBus, ServerMessage};
use crate::{manager::Manager, session::Session};
use log::{debug, info};
use std::collections::HashMap;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpStream;
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle, time::sleep};

pub struct Server {
    handle: JoinHandle<()>,
    cancel: broadcast::Sender<()>,
}

struct ServerContext {
    ids: Vec<usize>,
    sessions: HashMap<usize, Session>,
}

impl Server {
    pub async fn start(address: SocketAddr) -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);

        let handle = tokio::spawn(async move {
            let listener = TcpListener::bind(address)
                .await
                .expect("could not start server");
            let (mut server_bus_local, server_bus_manager) = ServerBus::new_duplex();
            let manager = Manager::start(server_bus_manager);
            let mut ctx = ServerContext::new();

            loop {
                tokio::select! {
                    Ok((stream, addr)) = listener.accept() => {
                        ctx.handle_new_session(stream, addr, &manager, &server_bus_local);
                    }

                    Some(msg) = server_bus_local.recv() => {
                        ctx.handle_server_msg(msg, &manager, &server_bus_local).await;
                    },

                    _ = cancel_rx.recv() => break,
                }
            }
            manager.stop().await;
            for session in ctx.sessions.values() {
                session.stop().await;
            }
        });

        info!("Server listening on {:}", address.to_string());
        Self {
            handle,
            cancel: cancel_tx,
        }
    }

    pub async fn shutdown(&self) {
        info!("Server shutting down");
        while self.cancel.send(()).is_ok() {
            sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }
}

impl ServerContext {
    fn new() -> Self {
        Self {
            ids: (1..256).collect(),
            sessions: HashMap::new(),
        }
    }

    fn handle_new_session(
        &mut self,
        stream: TcpStream,
        address: SocketAddr,
        manager: &Manager,
        server_bus: &ServerBus,
    ) {
        match self.ids.pop() {
            Some(id) => {
                let (session, request_sender) =
                    Session::start(stream, id, manager.new_request_sender());
                self.sessions.insert(id, session);
                let _ = server_bus.send(ServerMessage::RegisterSession(id, request_sender));
                debug!("opened session from {:} with id {:}", address, id)
            }
            None => {}
        }
    }

    async fn handle_server_msg(
        &mut self,
        msg: ServerMessage,
        manager: &Manager,
        server_bus: &ServerBus,
    ) {
        match msg {
            ServerMessage::CloseSession(id) => {
                if let Some(session) = self.sessions.remove(&id) {
                    self.ids.push(id);
                    session.stop().await;
                    debug!("closed session with id {:}", id)
                }
            }

            ServerMessage::RegisterSession(..) => {
                unreachable!();
            }
        }
    }
}
