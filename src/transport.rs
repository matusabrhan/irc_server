use std::time::Duration;

use irc_proto::{connection::Connection, message::Message};
use tokio::{
    net::TcpStream,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
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
                                if client_tx.send(msg).is_err() {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    msg = client_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                if conn.write(msg).await.is_err() {
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

    pub fn send(&self, msg: Message) -> Result<(), ()> {
        self.tx.send(msg).map_err(|_| ())
    }

    pub async fn stop(&self) {
        while self.cancel.send(()).is_ok() {
            time::sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }
}
