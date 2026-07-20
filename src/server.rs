use crate::{manager::Manager, session::Session};
use log::{debug, info};
use std::collections::HashMap;
use std::{net::SocketAddr, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle, time::sleep};

pub struct ServerEndpoint {
    pub tx: mpsc::UnboundedSender<ServerMessage>,
    pub rx: mpsc::UnboundedReceiver<ServerMessage>,
}

pub struct RpcMessage<T, TRes> {
    pub request: T,
    pub reply: oneshot::Sender<TRes>,
}

impl<T> RpcMessage<T, Result<(), ()>> {
    pub fn new(
        contents: T,
    ) -> (
        RpcMessage<T, Result<(), ()>>,
        oneshot::Receiver<Result<(), ()>>,
    ) {
        let (tx, rx) = oneshot::channel::<Result<(), ()>>();
        (
            RpcMessage {
                request: contents,
                reply: tx,
            },
            rx,
        )
    }
}

pub struct Request<T> {
    pub id: usize,
    pub msg: T,
}

impl<T> Request<T> {
    pub fn new(id: usize, msg: T) -> Self {
        Self { id, msg }
    }
}

pub enum ServerMessage {
    RegisterSessionRequest(usize, mpsc::UnboundedSender<ManagerMessage>),
}

pub enum ManagerMessage {}

pub enum SessionMessage {
    RegisterNickname(RpcMessage<Request<String>, Result<(), ()>>),
}

impl ServerEndpoint {
    fn new_multicast() -> (Self, Self) {
        let (tx1, rx2) = mpsc::unbounded_channel::<ServerMessage>();
        let (tx2, rx1) = mpsc::unbounded_channel::<ServerMessage>();

        (Self { tx: tx1, rx: rx1 }, Self { tx: tx2, rx: rx2 })
    }
}

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
            let (server_endpoint1, mut server_endpoint2) = ServerEndpoint::new_multicast();
            let manager = Manager::start(server_endpoint1);

            let mut ids: Vec<usize> = (1..256).collect();
            let mut sessions: HashMap<usize, Session> = HashMap::new();

            loop {
                tokio::select! {
                    Ok((stream, addr)) = listener.accept() => {
                        debug!("connection from {:}", addr);

                        match ids.pop() {
                            Some(id) => {
                                // let (endpoint, tx) = SessionEndpoint::new(manager.new_request_sender());
                                let (session, request_sender) = Session::start(stream, id, manager.new_request_sender());
                                sessions.insert(id, session);
                                server_endpoint2.tx.send(ServerMessage::RegisterSessionRequest(id, request_sender));
                            }
                            None => {}
                        }
                    }

                    Some(msg) = server_endpoint2.rx.recv() => {},

                    _ = cancel_rx.recv() => break,
                }
            }
            manager.stop();
        });

        Self {
            handle,
            cancel: cancel_tx,
        }
    }

    pub async fn shutdown(&self) {
        // TODO: await listener_handle ?
        info!("Server shutting down");
        let mut remaining = self.cancel.send(());
        while remaining.is_ok() {
            sleep(Duration::from_millis(100)).await;
            remaining = self.cancel.send(());
        }
    }
}
