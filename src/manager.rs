use std::{
    collections::{HashMap, HashSet}, time::Duration
};

use irc_proto::message::Message;
use log::{debug};
use tokio::{
    net::TcpStream, sync::{broadcast, mpsc, oneshot}, task::JoinHandle, time
};

use crate::session::{ManagerToSessionMsg, Session, SessionId};

pub struct Event<T> {
    id: SessionId,
    content: T,
}

impl<T> Event<T> {
    pub fn new(id: SessionId, content: T) -> Self {
        Self { id, content }
    }

    pub fn id(&self) -> &SessionId {
        &self.id
    }

    pub fn content(&self) -> &T {
        &self.content
    }
}

pub struct Request<TReq, TRes> {
    id: SessionId,
    content: TReq,
    reply: oneshot::Sender<TRes>,
}

impl<TReq, TRes> Request<TReq, TRes> {
    pub fn new(id: SessionId, contents: TReq) -> (Request<TReq, TRes>, oneshot::Receiver<TRes>) {
        let (tx, rx) = oneshot::channel::<TRes>();
        (
            Request {
                id,
                content: contents,
                reply: tx,
            },
            rx,
        )
    }

    pub fn id(&self) -> &SessionId {
        &self.id
    }

    pub fn content(&self) -> &TReq {
        &self.content
    }

    pub fn reply(self, msg: TRes) -> Result<(), ()> {
        self.reply.send(msg).map_err(|_| ())
    }
}

pub enum ServerToManagerMsg {
    OpenSession(TcpStream),
}

struct ServerToManagerReceiver(mpsc::UnboundedReceiver<ServerToManagerMsg>);
pub struct ServerToManagerSender(pub mpsc::UnboundedSender<ServerToManagerMsg>);

pub enum SessionToManagerMsg {
    RegisterNickname(Request<String, Result<(), ()>>),
    PrivateMessage(Event<PrivateMessageInfo>),
    JoinChannels(Request<JoinChannelsInfo, Vec<String>>),
    Quit(Event<()>),
}

pub struct PrivateMessageInfo {
    pub targets: Vec<String>,
    pub msg: Message,
}

pub struct JoinChannelsInfo {
    pub names: Vec<String>,
    pub passwords: Option<Vec<String>>
}

struct SessionToManagerReceiver(mpsc::UnboundedReceiver<SessionToManagerMsg>);
pub struct SessionToManagerSender(pub mpsc::UnboundedSender<SessionToManagerMsg>);

pub struct Manager {
    handle: JoinHandle<()>,
    server_to_manager: ServerToManagerSender,
    cancel: broadcast::Sender<()>,
}

struct ManagerContext {
    sessions_to_manager: SessionToManagerSender,
    session_ids: Vec<SessionId>,
    sessions: HashMap<SessionId, Session>,
    nicknames: HashMap<SessionId, String>,
    channels: HashMap<String, HashSet<SessionId>>,
}

impl Manager {
    pub fn start() -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let (server_to_manager_tx, mut server_to_manager_rx) = Self::server_to_manager_channel();
        let (sessions_to_manager_tx, mut sessions_to_manager_rx) = Self::sessions_to_manager_channel();

        let handle = tokio::spawn(async move {
            let mut ctx = ManagerContext::new(sessions_to_manager_tx);
            loop {
                tokio::select! {
                    Some(msg) = sessions_to_manager_rx.0.recv() => {
                        ctx.handle_session_msg(msg).await;
                    }

                    Some(msg) = server_to_manager_rx.0.recv() => {
                         ctx.handle_server_msg(msg)
                    }

                    _ = cancel_rx.recv() => break,
                }
            }
        });

        Self {
            handle,
            server_to_manager: server_to_manager_tx,
            cancel: cancel_tx,
        }
    }

    pub async fn stop(&self) {
        while self.cancel.send(()).is_ok() {
            time::sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }

    pub fn get_server_to_manager_sender(&self) -> ServerToManagerSender {
        ServerToManagerSender(self.server_to_manager.0.clone())
    }

    fn server_to_manager_channel() -> (ServerToManagerSender, ServerToManagerReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ServerToManagerSender(tx), ServerToManagerReceiver(rx))
    }

    fn sessions_to_manager_channel() -> (SessionToManagerSender, SessionToManagerReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        (SessionToManagerSender(tx), SessionToManagerReceiver(rx))
    }
}

impl ManagerContext {
    fn new(sessions_to_manager: SessionToManagerSender) -> Self {
        Self {
            sessions_to_manager,
            session_ids: (1..256).map(SessionId).collect(),
            sessions: HashMap::new(),
            nicknames: HashMap::new(),
            channels: HashMap::new(),
        }
    }

    fn find_nickname_id(&self, nickname: &str) -> Option<SessionId> {
        self.nicknames
            .iter()
            .find(|(_, v)| *v == nickname)
            .map(|(k, _)| *k)
    }

    fn get_sessions_to_manager_sender(&self) -> SessionToManagerSender {
        SessionToManagerSender(self.sessions_to_manager.0.clone())
    }

    fn handle_server_msg(&mut self, msg: ServerToManagerMsg) {
        match msg {
            ServerToManagerMsg::OpenSession(stream) => {
                if let Some(id) = self.session_ids.pop() {
                    let address = stream.peer_addr().unwrap();
                    let session =
                        Session::start(stream, id, self.get_sessions_to_manager_sender());
                    self.sessions.insert(id, session);
                    debug!("opened session from {:} with id {:}", address, id)
                }
            }
        }
    }

    async fn handle_session_msg(&mut self, msg: SessionToManagerMsg) {
        match msg {
            SessionToManagerMsg::RegisterNickname(request) => {
                match self.find_nickname_id(request.content()) {
                    Some(_) => {
                        let _ = request.reply(Result::Err(()));
                    }
                    None => {
                        self.nicknames
                            .insert(*request.id(), request.content().to_string());
                        let _ = request.reply(Result::Ok(()));
                    }
                }
            }

            SessionToManagerMsg::PrivateMessage(event) => {
                let source = self
                    .nicknames
                    .get(event.id())
                    .expect("nick must be known when sending private message");
                for target in &event.content().targets {
                    if target == source {
                        continue;
                    }

                    match self.find_nickname_id(target) {
                        Some(target_id) => {
                            if let Some(session) = self.sessions.get(&target_id) {
                                session.send(ManagerToSessionMsg::PrivateMessage(event.content().msg.clone()),);
                            }
                        }
                        None => {
                            if let Some(channel_member_ids) = self.channels.get(target) {
                                for member_id in channel_member_ids {
                                    if let Some(session) = self.sessions.get(member_id) {
                                        session.send(ManagerToSessionMsg::PrivateMessage(event.content().msg.clone()),);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            SessionToManagerMsg::JoinChannels(request) => {
                // TODO: handle channel passwords

                let mut joined_channels: Vec<String> = Vec::new();
                for channel_name in &request.content().names {
                    joined_channels.push(channel_name.clone());
                    self.channels
                        .entry(channel_name.clone())
                        .or_default()
                        .insert(*request.id());
                }
                let _ = request.reply.send(joined_channels);
            }

            SessionToManagerMsg::Quit(request) => {
                self.nicknames.remove(request.id());
                for channel in self.channels.values_mut() {
                    channel.remove(request.id());
                }
                if let Some(session) = self.sessions.remove(request.id()) {
                    self.session_ids.push(*request.id());
                    session.stop().await;
                    debug!("closed session with id {:}", request.id())
                }
            }
        }
    }
}
