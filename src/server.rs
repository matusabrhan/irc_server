use crate::{manager::Manager, session::Session};
use irc_proto::message::Message;
use log::{debug, info};
use std::collections::HashMap;
use std::{net::SocketAddr, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle, time::sleep};

pub struct ServerEndpoint {
    tx: mpsc::UnboundedSender<ServerMessage>,
    rx: mpsc::UnboundedReceiver<ServerMessage>,
}

impl ServerEndpoint {
    pub fn new_multicast() -> (Self, Self) {
        let (tx1, rx2) = mpsc::unbounded_channel::<ServerMessage>();
        let (tx2, rx1) = mpsc::unbounded_channel::<ServerMessage>();

        (Self { tx: tx1, rx: rx1 }, Self { tx: tx2, rx: rx2 })
    }

    pub async fn recv(&mut self) -> Option<ServerMessage> {
        self.rx.recv().await
    }

    fn send(&self, msg: ServerMessage) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.tx.send(msg)
    }
}

pub struct RpcMessage<T, TRes> {
    pub request: T,
    pub reply: oneshot::Sender<TRes>,
}

impl<TReq, TRes> RpcMessage<TReq, TRes> {
    pub fn new(contents: TReq) -> (RpcMessage<TReq, TRes>, oneshot::Receiver<TRes>) {
        let (tx, rx) = oneshot::channel::<TRes>();
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
    RegisterSession(usize, mpsc::UnboundedSender<ManagerMessage>),
    CloseSession(usize),
}

pub enum ManagerMessage {
    PrivateMessage(Message),
}

pub enum SessionMessage {
    RegisterNickname(RpcMessage<Request<String>, Result<(), ()>>),
    PrivateMessage(Request<(Vec<String>, Message)>),
    JoinChannels(RpcMessage<Request<(Vec<String>, Option<Vec<String>>)>, Vec<String>>),
    Quit(Request<()>),
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
                                let (session, request_sender) = Session::start(stream, id, manager.new_request_sender());
                                sessions.insert(id, session);
                                let _ = server_endpoint2.send(ServerMessage::RegisterSession(id, request_sender));
                            }
                            None => {}
                        }
                    }

                    Some(msg) = server_endpoint2.rx.recv() => {
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
