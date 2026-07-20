use crate::{
    config::CONFIG,
    server::{ManagerMessage, Request, RpcMessage, SessionMessage},
    transport::Transport,
};
use irc_proto::message::{Command, Message, Source};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

#[derive(Default)]
struct RegistrationState(u8);

impl RegistrationState {
    const PASS: RegistrationState = RegistrationState(0b001);
    const NICK: RegistrationState = RegistrationState(0b010);
    const USER: RegistrationState = RegistrationState(0b100);
    const ALL: RegistrationState = RegistrationState(0b111);

    fn set(&mut self, state: RegistrationState, enable: bool) {
        match enable {
            true => self.0 |= state.0,
            false => self.0 ^= state.0,
        }
    }

    fn check(&self, state: RegistrationState) -> bool {
        self.0 & state.0 == state.0
    }
}

pub struct SessionEndpoint<TReq, TRes> {
    request_receiver: mpsc::UnboundedReceiver<TReq>,
    response_sender: mpsc::UnboundedSender<TRes>,
}

impl SessionEndpoint<ManagerMessage, SessionMessage> {
    fn new(
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

#[derive(Default)]
struct SessionContext {
    id: usize,
    nickname: String,
    username: String,
    realname: String,
    registration: RegistrationState,
}

pub struct Session {
    handle: JoinHandle<()>,
    cancel: broadcast::Sender<()>,
}

impl Session {
    pub fn start(
        stream: TcpStream,
        id: usize,
        // mut endpoint: SessionEndpoint<ManagerMessage, SessionMessage>,
        request_sender: mpsc::UnboundedSender<SessionMessage>,
    ) -> (Self, mpsc::UnboundedSender<ManagerMessage>) {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let (mut endpoint, tx) = SessionEndpoint::new(request_sender);

        let handle = tokio::spawn(async move {
            let mut transport = Transport::start(stream);
            let mut ctx = SessionContext::new(id);

            loop {
                tokio::select! {
                    Some(msg) = transport.recv() => {
                        if ctx.handle_client_msg(msg, &transport, &endpoint).await.is_err() { break }
                    }

                    Some(msg) = endpoint.recv() => {
                        ctx.handle_manager_msg( msg, &transport, &endpoint);
                    }

                    _ = cancel_rx.recv() => break,
                }
            }
            transport.stop();
        });

        (
            Self {
                handle,
                cancel: cancel_tx,
            },
            tx,
        )
    }

    pub fn stop(&self) {
        self.cancel.send(());
    }
}

impl SessionContext {
    fn new(id: usize) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    fn send_welcome(&self, transport: &Transport) -> Result<(), ()> {
        transport
            .send(
                Message::default()
                    .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                    .with_command(Command::RPL_WELCOME {
                        text: format!(
                            "Welcome to the {} Network, {}",
                            CONFIG.network_name.clone(),
                            self.nickname
                        ),
                    }),
            )
            .map_err(|_| ())?;

        transport
            .send(
                Message::default()
                    .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                    .with_command(Command::RPL_YOURHOST {
                        //"<client> :Your host is <servername>, running version <version>"
                        text: format!(
                            "Your host is {}, running version {}",
                            CONFIG.server.name.clone(),
                            CONFIG.server.version.clone(),
                        ),
                    }),
            )
            .map_err(|_| ())?;
        transport
            .send(
                Message::default()
                    .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                    .with_command(Command::RPL_CREATED {
                        // "<client> :This server was created <datetime>"
                        text: format!("This server was created {:?}", CONFIG.server.time),
                    }),
            )
            .map_err(|_| ())?;
        transport
        .send(
            Message::default()
                .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                .with_command(Command::RPL_MYINFO{
                    // TODO:
                    // "<client> <servername> <version> <available user modes> <available channel modes> [<channel modes with a parameter>]"
                    text: format!("{} {} <available user modes> <available channel modes> [<channel modes with a parameter>]",
                        CONFIG.server.name, CONFIG.server.version),
                }),
        )
        .map_err(|_| ())?;

        Ok(())
    }

    async fn handle_client_msg(
        &mut self,
        msg: Message,
        transport: &Transport,
        endpoint: &SessionEndpoint<ManagerMessage, SessionMessage>,
    ) -> Result<(), ()> {
        match msg.command() {
            Command::PING { token } => transport
                .send(
                    Message::default()
                        .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                        .with_command(Command::PONG {
                            server: Some(CONFIG.server.name.clone()),
                            token: token.to_string(),
                        }),
                )
                .map_err(|_| ()),

            Command::PASS { password } => {
                if self.registration.check(RegistrationState::ALL) {
                    return Ok(());
                }
                if CONFIG.server.password != *password {
                    transport
                        .send(
                            Message::default()
                                .with_source(
                                    Source::default().with_name(CONFIG.server.name.clone()),
                                )
                                .with_command(Command::ERR_PASSWDMISMATCH {
                                    client: String::new(),
                                }),
                        )
                        .map_err(|_| ())?
                }
                self.registration.set(RegistrationState::PASS, true);
                Ok(())
            }

            Command::USER { user, realname, .. } => {
                if self.registration.check(RegistrationState::ALL) {
                    return Ok(());
                }
                self.username = user.to_string();
                self.realname = realname.to_string();
                self.registration.set(RegistrationState::USER, true);
                if self.registration.check(RegistrationState::ALL) {
                    self.send_welcome(transport);
                }
                Ok(())
            }

            Command::NICK { nickname } => {
                let (rpc_msg, rx) = RpcMessage::new(Request::new(self.id, nickname.clone()));
                endpoint
                    .send(SessionMessage::RegisterNickname(rpc_msg))
                    .map_err(|_| ())?;
                if rx.await.is_err() {
                    transport
                        .send(
                            Message::default()
                                .with_source(
                                    Source::default().with_name(CONFIG.server.name.clone()),
                                )
                                .with_command(Command::ERR_NICKNAMEINUSE {
                                    client: String::new(),
                                    nick: String::new(),
                                }),
                        )
                        .map_err(|_| ())?;
                }
                self.nickname = nickname.clone();
                self.registration.set(RegistrationState::NICK, true);
                if self.registration.check(RegistrationState::ALL) {
                    self.send_welcome(transport);
                }
                Ok(())
            }

            _ => Ok(()),
        }
    }

    fn handle_manager_msg(
        &mut self,
        msg: ManagerMessage,
        transport: &Transport,
        endpoint: &SessionEndpoint<ManagerMessage, SessionMessage>,
    ) {
    }
}
