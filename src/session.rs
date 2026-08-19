use log::debug;
use std::{fmt::Display, time::Duration};

use crate::{
    config::CONFIG, manager::{Event, JoinChannelsInfo, PrivateMessageInfo, Request, SessionToManagerMsg, SessionToManagerSender}, transport::Transport
};
use irc_proto::message::{Command, Message, Source};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::{self, Instant, interval},
};



pub enum ManagerToSessionMsg {
    PrivateMessage(Message),
}

struct ManagerToSessionReceiver(mpsc::UnboundedReceiver<ManagerToSessionMsg>);
pub struct ManagerToSessionSender(mpsc::UnboundedSender<ManagerToSessionMsg>);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub usize);

impl Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}


struct SessionContext {
    id: SessionId,
    session_to_manager: SessionToManagerSender,

    nickname: String,
    username: String,
    realname: String,
    registration: RegistrationState,
    last_pong: Instant,
    interval: time::Interval
}

pub struct Session {
    handle: JoinHandle<()>,
    manager_to_session: ManagerToSessionSender,
    cancel: broadcast::Sender<()>,
}

impl Session {
    pub fn start(
        stream: TcpStream,
        id: SessionId,
        session_to_manager: SessionToManagerSender
    ) -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let (manager_to_session_tx, mut manager_to_session_rx) = Self::manager_to_session_channel();

        let handle = tokio::spawn(async move {
            let mut idle_interval = interval(Duration::from_secs(10));
            let mut transport = Transport::start(stream);
            let mut ctx = SessionContext::new(id, session_to_manager);

            loop {
                let result = tokio::select! {
                    Some(msg) = transport.recv() => {
                        ctx.handle_client_msg(msg, &transport).await
                    }

                    Some(msg) = manager_to_session_rx.0.recv() => {
                        ctx.handle_manager_msg(msg, &transport)
                    }

                    _ = idle_interval.tick() => {
                        ctx.handle_idle_timer()
                    }

                    _ = cancel_rx.recv() => Err(()),
                };

                if result.is_err() { break }
            }
            transport.stop().await;
            ctx.session_to_manager.0.send(SessionToManagerMsg::Quit(Event::new(id, ())));
        });

        Self {
            handle,
            manager_to_session: manager_to_session_tx,
            cancel: cancel_tx,
        }
    }

    pub fn send(&self, msg: ManagerToSessionMsg) -> Result<(), ()> {
        self.manager_to_session.0.send(msg).map_err(|_| ())
    }

    pub async fn stop(&self) {
        while self.cancel.send(()).is_ok() {
            time::sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }

    fn manager_to_session_channel() -> (ManagerToSessionSender, ManagerToSessionReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        (ManagerToSessionSender(tx), ManagerToSessionReceiver(rx))
    }
}

impl SessionContext {
    fn new(id: SessionId, session_to_manager: SessionToManagerSender) -> Self {
        Self {
            id, session_to_manager, nickname: String::new(), username: String::new(), realname: String::new(), registration: RegistrationState::default(), last_pong: Instant::now(), interval: time::interval(Duration::from_secs(10)) 
        }
    }

    fn send_message(&self, transport: &Transport, msg: Message) -> Result<(), ()> {
        debug!("message to {:}: {:?}", self.id, msg);
        transport .send(msg) .map_err(|_| ())
    }

    fn send_welcome(&self, transport: &Transport) -> Result<(), ()> {
        self.send_message(transport,
            Message::default()
            .with_source(Source::default().with_name(CONFIG.server.name.clone()))
            .with_command(Command::RPL_WELCOME {
                text: format!(
                          "Welcome to the {} Network, {}",
                          CONFIG.network_name.clone(),
                          self.nickname
                      ),
            })
        )?;
        self.send_message(transport,
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
        )?;
        self.send_message(transport,
            Message::default()
            .with_source(Source::default().with_name(CONFIG.server.name.clone()))
            .with_command(Command::RPL_CREATED {
                // "<client> :This server was created <datetime>"
                text: format!("This server was created {:?}", CONFIG.server.time),
            }),
        )?;
        self.send_message(transport,
            Message::default()
            .with_source(Source::default().with_name(CONFIG.server.name.clone()))
            .with_command(Command::RPL_MYINFO{
                // TODO:
                // "<client> <servername> <version> <available user modes> <available channel modes> [<channel modes with a parameter>]"
                text: format!("{} {} <available user modes> <available channel modes> [<channel modes with a parameter>]",
                          CONFIG.server.name, CONFIG.server.version),
            }),
        )?;

        Ok(())
    }

    async fn handle_client_msg(
        &mut self,
        msg: Message,
        transport: &Transport,
    ) -> Result<(), ()> {
        debug!("message from {:}: {:?}", self.id, msg);
        match msg.command() {
            Command::PING { token } => {
                self.last_pong = Instant::now();
                self.send_message(transport,
                    Message::default()
                    .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                    .with_command(Command::PONG {
                        server: Some(CONFIG.server.name.clone()),
                        token: token.to_string(),
                    }),
                )
            },

            Command::PASS { password } => {
                if self.registration.check(RegistrationState::ALL) {
                    return Ok(());
                }
                if CONFIG.server.password != *password {
                    self.send_message(transport,
                        Message::default()
                        .with_source(
                            Source::default().with_name(CONFIG.server.name.clone()),
                        )
                        .with_command(Command::ERR_PASSWDMISMATCH {
                            client: String::new(),
                        }),
                    )?
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
                let (request, rx) = Request::new(self.id, nickname.clone());
                self.session_to_manager.0.send(SessionToManagerMsg::RegisterNickname(request)).map_err(|_| ())?;

                if let Ok(Err(())) = rx.await {
                    self.send_message(transport,
                        Message::default()
                        .with_source(
                            Source::default().with_name(CONFIG.server.name.clone()),
                        )
                        .with_command(Command::ERR_NICKNAMEINUSE {
                            client: String::new(),
                            nick: String::new(),
                        }),
                    )?;
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
                self.session_to_manager.0.send(
                    SessionToManagerMsg::PrivateMessage(
                        Event::new(
                            self.id, PrivateMessageInfo{targets: targets.to_owned(), msg: msg.with_source(Source::default().with_name(self.nickname.clone()))}
                        )
                    )
                ).map_err(|_| ())?;

                Ok(())
            }

            Command::JOIN { channels, keys } => {
                if !self.registration.check(RegistrationState::ALL) {
                    return Ok(());
                }

                let (request, rx) =
                    Request::new(self.id, JoinChannelsInfo{names: channels.clone(), passwords: keys.clone()});
                self.session_to_manager.0.send(SessionToManagerMsg::JoinChannels(request)).map_err(|_| ())?;
                if let Ok(joined_channels) = rx.await {
                    self.send_message(transport,
                        msg.with_command(Command::JOIN {
                            channels: joined_channels,
                            keys: None,
                        })
                        .with_source(Source::default().with_name(self.nickname.clone())),
                    )?;
                }

                Ok(())
            }

            Command::QUIT { .. } => Err(()),

            _ => Ok(()),
        }
    }

    fn handle_manager_msg(
        &mut self,
        msg: ManagerToSessionMsg,
        transport: &Transport,
    ) -> Result<(), ()> {
        match msg {
            ManagerToSessionMsg::PrivateMessage(priv_message) => {
                self.send_message(transport, priv_message)
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
