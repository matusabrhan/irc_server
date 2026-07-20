use irc_proto::{connection::Connection, message::Message};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};

pub struct Transport {
    handle: JoinHandle<()>,
    tx: mpsc::UnboundedSender<Message>,
    rx: mpsc::UnboundedReceiver<Message>,
    cancel: broadcast::Sender<()>,
}

impl Transport {
    pub fn start(stream: TcpStream) -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let (server_tx, mut client_rx) = mpsc::unbounded_channel::<Message>();
        let (client_tx, server_rx) = mpsc::unbounded_channel::<Message>();

        let mut conn = Connection::new(stream);
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = conn.read() => {
                        match msg {
                            Ok(msg) => {
                                if let Err(_) = client_tx.send(msg) {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    msg = client_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if let Err(_) = conn.write(msg).await {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    _ = cancel_rx.recv() => break,
                }
            }
            client_rx.close();
        });

        Self {
            handle,
            tx: server_tx,
            rx: server_rx,
            cancel: cancel_tx,
        }
    }

    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    pub fn send(&self, msg: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.tx.send(msg)
    }

    pub fn stop(&self) {
        self.cancel.send(());
    }
}
