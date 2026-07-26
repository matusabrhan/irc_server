use std::collections::HashMap;

use irc_proto::message::Message;
use tokio::sync::{mpsc, oneshot};

pub struct ServerBus {
    server_tx: mpsc::UnboundedSender<ServerMessage>,
    server_rx: mpsc::UnboundedReceiver<ServerMessage>,
}

pub struct ManagerBus {
    request_receiver: mpsc::UnboundedReceiver<SessionMessage>,
    _request_sender: mpsc::UnboundedSender<SessionMessage>,
    response_sender_map: HashMap<usize, mpsc::UnboundedSender<ManagerMessage>>,

    server_tx: mpsc::UnboundedSender<ServerMessage>,
}

pub struct SessionBus {
    request_receiver: mpsc::UnboundedReceiver<ManagerMessage>,
    response_sender: mpsc::UnboundedSender<SessionMessage>,
}

impl ServerBus {
    pub fn new_duplex() -> (Self, Self) {
        let (tx1, rx2) = mpsc::unbounded_channel::<ServerMessage>();
        let (tx2, rx1) = mpsc::unbounded_channel::<ServerMessage>();

        (
            Self {
                server_tx: tx1,
                server_rx: rx1,
            },
            Self {
                server_tx: tx2,
                server_rx: rx2,
            },
        )
    }

    pub async fn recv(&mut self) -> Option<ServerMessage> {
        self.server_rx.recv().await
    }

    pub fn send(&self, msg: ServerMessage) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.server_tx.send(msg)
    }
}

impl ManagerBus {
    pub fn new(server_bus: ServerBus) -> (Self, mpsc::UnboundedReceiver<ServerMessage>) {
        let (_request_sender, request_receiver) = mpsc::unbounded_channel::<SessionMessage>();

        (
            Self {
                request_receiver,
                _request_sender,
                response_sender_map: HashMap::new(),
                server_tx: server_bus.server_tx,
            },
            server_bus.server_rx,
        )
    }

    pub fn new_session_sender(&self) -> mpsc::UnboundedSender<SessionMessage> {
        self._request_sender.clone()
    }

    pub fn add_sender(&mut self, id: usize, sender: mpsc::UnboundedSender<ManagerMessage>) {
        self.response_sender_map.insert(id, sender);
    }

    pub fn remove_sender(&mut self, id: &usize) {
        self.response_sender_map.remove(id);
    }

    pub fn session_send(
        &self,
        id: usize,
        msg: ManagerMessage,
    ) -> Result<(), mpsc::error::SendError<ManagerMessage>> {
        self.response_sender_map
            .get(&id)
            .expect("id not found")
            .send(msg)
    }

    pub async fn session_recv(&mut self) -> Option<SessionMessage> {
        self.request_receiver.recv().await
    }

    pub fn server_send(
        &self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.server_tx.send(msg)
    }
}

impl SessionBus {
    pub fn new(
        response_sender: mpsc::UnboundedSender<SessionMessage>,
    ) -> (Self, mpsc::UnboundedSender<ManagerMessage>) {
        let (request_sender, request_receiver) = mpsc::unbounded_channel::<ManagerMessage>();

        (
            Self {
                request_receiver,
                response_sender,
            },
            request_sender,
        )
    }

    pub async fn recv(&mut self) -> Option<ManagerMessage> {
        self.request_receiver.recv().await
    }

    pub fn send(&self, msg: SessionMessage) -> Result<(), mpsc::error::SendError<SessionMessage>> {
        self.response_sender.send(msg)
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

pub struct RpcMessage<TReq, TRes> {
    pub request: TReq,
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
