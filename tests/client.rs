use irc_proto::{
    connection::Connection,
    message::{Command, Message},
};
use std::{net, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
};

pub struct Client {
    name: String,
    connected: bool,
    read: mpsc::UnboundedReceiver<Message>,
    write: mpsc::UnboundedSender<Message>,
    shutdown_sig: broadcast::Sender<()>,
}

impl Client {
    pub fn new(user: &str) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<Message>();
        Self {
            name: String::from(user),
            connected: false,
            read: rx,
            write: tx,
            shutdown_sig: broadcast::channel(1).0,
        }
    }

    pub async fn connect(&mut self, address: net::SocketAddr, password: Option<&str>) {
        if self.connected {
            return;
        }
        let stream = TcpStream::connect(address)
            .await
            .expect("client could not connect");
        let (tx, rx) = spawn_rw_task(stream, self.shutdown_sig.subscribe());
        self.write = tx;
        self.read = rx;
        if let Some(password) = password {
            self.register(password).expect("client could not register");
        }
    }

    pub async fn skip_msgs(&mut self, n: u32) {
        for _ in 0..n {
            self.read().await;
        }
    }

    pub fn send(&self, cmd: Command) {
        self.write
            .send(Message::default().with_command(cmd))
            .expect("client could not send command")
    }

    pub async fn read(&mut self) -> Option<Message> {
        tokio::time::timeout(Duration::from_millis(100), self.read.recv())
            .await
            .unwrap_or(None)
    }

    pub fn disconnect(&mut self) {
        self.shutdown_sig
            .send(())
            .expect("client could not disconnect");
        self.connected = false;
    }

    fn register(&self, password: &str) -> Result<(), mpsc::error::SendError<Message>> {
        self.write
            .send(Message::default().with_command(Command::PASS {
                password: password.to_string(),
            }))?;
        self.write
            .send(Message::default().with_command(Command::USER {
                user: format!("username_{:}", self.name),
                mode: "0".to_string(),
                unused: "*".to_string(),
                realname: format!("realname_{:}", self.name),
            }))?;
        self.write
            .send(Message::default().with_command(Command::NICK {
                nickname: self.name.clone(),
            }))?;
        Ok(())
    }
}

fn spawn_rw_task(
    stream: TcpStream,
    mut shutdown: broadcast::Receiver<()>,
) -> (
    mpsc::UnboundedSender<Message>,
    mpsc::UnboundedReceiver<Message>,
) {
    let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Message>();
    let (read_tx, read_rx) = mpsc::unbounded_channel::<Message>();

    let mut conn = Connection::new(stream);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = conn.read() => {
                    match msg {
                        Ok(msg) => {
                            if let Err(_) = read_tx.send(msg) {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                msg = write_rx.recv() => {
                    match msg {
                        Some(msg) => {
                            if let Err(_) = conn.write(msg).await {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => break,
            }
        }
        write_rx.close();
    });
    (write_tx, read_rx)
}
