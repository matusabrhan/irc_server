use log::debug;
use std::time::Duration;

use crate::{
    config::CONFIG,
    ipc_bus::{ManagerMessage, Request, RpcMessage, SessionBus, SessionId, SessionMessage},
    transport::Transport,
};
use irc_proto::message::{Command, Message, Source};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{self, Instant, Interval, interval},
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

struct SessionContext {
    id: SessionId,
    nickname: String,
    username: String,
    realname: String,
    registration: RegistrationState,
    last_pong: Instant,
    interval: time::Interval
}

pub struct Session {
    handle: JoinHandle<()>,
    cancel: broadcast::Sender<()>,
}

impl Session {
    pub fn start(
        stream: TcpStream,
        id: SessionId,
        request_sender: mpsc::UnboundedSender<SessionMessage>,
    ) -> (Self, mpsc::UnboundedSender<ManagerMessage>) {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let (mut bus, tx) = SessionBus::new(request_sender);

        let handle = tokio::spawn(async move {
            let mut idle_interval = interval(Duration::from_secs(10));
            let mut transport = Transport::start(stream);
            let mut ctx = SessionContext::new(id);

            loop {
                let result = tokio::select! {
                    Some(msg) = Self::recv_client_message(&mut transport) => {
                        ctx.handle_client_msg(msg, &transport, &bus).await
                    }

                    Some(msg) = Self::recv_manager_message(&mut bus) => {
                        ctx.handle_manager_msg(msg, &transport, &bus)
                    }

                    _ = Self::recv_idle_timer(&mut idle_interval) => {
                        ctx.handle_idle_timer()
                    }

                    _ = cancel_rx.recv() => Err(()),
                };

                if result.is_err() { break }
            }
            transport.stop().await;
            let _ = bus.send(SessionMessage::Quit(Request::new(id, ())));
        });

        (
            Self {
                handle,
                cancel: cancel_tx,
            },
            tx,
        )
    }

    pub async fn stop(&self) {
        while self.cancel.send(()).is_ok() {
            time::sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }

    async fn recv_client_message(transport: &mut Transport) -> Option<Message> {
        transport.recv().await
    }

    async fn recv_manager_message(bus: &mut SessionBus) -> Option<ManagerMessage> {
        bus.recv().await
    }

    async fn recv_idle_timer(interval: &mut Interval) {
        interval.tick().await;
    }
}

impl SessionContext {
    fn new(id: SessionId) -> Self {
        Self {
            id, nickname: String::new(), username: String::new(), realname: String::new(), registration: RegistrationState::default(), last_pong: Instant::now(), interval: time::interval(Duration::from_secs(10)) 
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
        bus: &SessionBus,
    ) -> Result<(), ()> {
        debug!("session {:} received message: {:?}", self.id, msg);
        match msg.command() {
            Command::PING { token } => {
                self.last_pong = Instant::now();
                transport
                    .send(
                        Message::default()
                            .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                            .with_command(Command::PONG {
                                server: Some(CONFIG.server.name.clone()),
                                token: token.to_string(),
                            }),
                    )
                    .map_err(|_| ())
            },

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
                    self.send_welcome(transport)?;
                }
                Ok(())
            }

            Command::NICK { nickname } => {
                let (rpc_msg, rx) = RpcMessage::new(Request::new(self.id, nickname.clone()));
                bus.send(SessionMessage::RegisterNickname(rpc_msg))
                    .map_err(|_| ())?;

                if let Ok(Err(())) = rx.await {
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
                    return Ok(())
                }
                self.nickname = nickname.clone();
                self.registration.set(RegistrationState::NICK, true);
                if self.registration.check(RegistrationState::ALL) {
                    self.send_welcome(transport)?;
                }
                Ok(())
            }

            Command::PRIVMSG { targets, .. } => {
                if !self.registration.check(RegistrationState::ALL) {
                    return Ok(());
                }
                bus.send(SessionMessage::PrivateMessage(Request::new(
                    self.id,
                    (
                        targets.to_owned(),
                        msg.with_source(Source::default().with_name(self.nickname.clone())),
                    ),
                )))
                .map_err(|_| ())?;

                Ok(())
            }

            Command::JOIN { channels, keys } => {
                if !self.registration.check(RegistrationState::ALL) {
                    return Ok(());
                }

                let (rpc_msg, rx) =
                    RpcMessage::new(Request::new(self.id, (channels.clone(), keys.clone())));
                bus.send(SessionMessage::JoinChannels(rpc_msg))
                    .map_err(|_| ())?;
                if let Ok(joined_channels) = rx.await {
                    transport
                        .send(
                            msg.with_command(Command::JOIN {
                                channels: joined_channels,
                                keys: None,
                            })
                            .with_source(Source::default().with_name(self.nickname.clone())),
                        )
                        .map_err(|_| ())?
                }

                Ok(())
            }

            Command::QUIT { .. } => Err(()),

            _ => Ok(()),
        }
    }

    fn handle_manager_msg(
        &mut self,
        msg: ManagerMessage,
        transport: &Transport,
        bus: &SessionBus,
    ) -> Result<(), ()> {
        match msg {
            ManagerMessage::PrivateMessage(priv_message) => {
                debug!("session {:} sent message: {:?}", self.id, priv_message);
                transport.send(priv_message).map_err(|_| ())
            }
        }
    }

    fn handle_idle_timer(&mut self) -> Result<(), ()> {
        let now = Instant::now();
        if self.last_pong + self.interval.period() < now {
            return  Err(());
        }
        Ok(())
    }
}
