use crate::ipc_bus::{ServerBus, ServerMessage};
use crate::{manager::Manager, session::Session};
use log::{debug, info};
use std::collections::HashMap;
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle, time::sleep};

pub struct Server {
    handle: JoinHandle<()>,
    cancel: broadcast::Sender<()>,
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

            //TODO: server context
            let mut ids: Vec<usize> = (1..256).collect();
            let mut sessions: HashMap<usize, Session> = HashMap::new();

            loop {
                tokio::select! {
                    Ok((stream, addr)) = listener.accept() => {
                        debug!("connection from {:}", addr);

                        match ids.pop() {
                            Some(id) => {
                                let (session, request_sender) = Session::start(stream, id, manager.new_request_sender());
                                sessions.insert(id, session);
                                let _ = server_bus_local.send(ServerMessage::RegisterSession(id, request_sender));
                            }
                            None => {}
                        }
                    }

                    Some(msg) = server_bus_local.recv() => {
                        match msg {
                            ServerMessage::CloseSession(id) => {
                                if let Some(session) = sessions.remove(&id) {
                                    ids.push(id);
                                    session.stop().await
                                }
                            }

                            ServerMessage::RegisterSession(..) => {
                                unreachable!();
                            }

                        }
                    },

                    _ = cancel_rx.recv() => break,
                }
            }
            manager.stop().await;
            for session in sessions.values() {
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
