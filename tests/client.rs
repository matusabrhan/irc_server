use irc_proto::message::{Command, Message, MessageBuilder};
use irc_server::transport::Transport;
use std::{net, time::Duration};
use tokio::{net::TcpStream, sync::broadcast};

pub struct Client {
    name: String,
    transport: Option<Transport>,
    shutdown_sig: broadcast::Sender<()>,
}

impl Client {
    pub fn new(user: &str) -> Self {
        Self {
            name: String::from(user),
            transport: None,
            shutdown_sig: broadcast::channel(1).0,
        }
    }

    pub async fn connect(&mut self, address: net::SocketAddr, password: Option<&str>) {
        if self.transport.is_some() {
            return;
        }
        let stream = TcpStream::connect(address)
            .await
            .expect("client could not connect");
        self.transport = Some(Transport::start(stream.into_std().unwrap()));

        if let Some(password) = password {
            self.register(password).expect("client could not register");
        }
    }

    pub async fn skip_msgs(&mut self, n: u32) {
        for _ in 0..n {
            self.read().await;
        }
    }

    pub fn send(&self, cmd: Command) -> Result<(), ()> {
        if let Some(transport) = &self.transport {
            transport.send(MessageBuilder::with_command(cmd).build().ok_or(())?)?
        }
        Ok(())
    }

    pub async fn read(&mut self) -> Option<Message> {
        if let Some(transport) = self.transport.as_mut() {
            return tokio::time::timeout(Duration::from_millis(100), transport.recv())
                .await
                .unwrap_or(None);
        }
        None
    }

    pub async fn disconnect(&mut self) {
        self.shutdown_sig
            .send(())
            .expect("client could not disconnect");
        if let Some(transport) = self.transport.as_mut() {
            transport.stop().await;
        }
        self.transport = None;
    }

    fn register(&self, password: &str) -> Result<(), ()> {
        self.send(Command::PASS { password })?;
        self.send(Command::USER {
            user: &format!("username_{:}", self.name),
            mode: "0",
            unused: "*",
            realname: &format!("realname_{:}", self.name),
        })?;
        self.send(Command::NICK {
            nickname: &self.name,
        })?;
        Ok(())
    }
}
