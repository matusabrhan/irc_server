use crate::manager::{ServerToManagerMsg, Manager};
use log::info;
use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpStream;
use tokio::{net::TcpListener, sync::broadcast, task::JoinHandle, time::sleep};

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

            let manager = Manager::start();

            loop {
                tokio::select! {
                    Ok((stream, _)) = listener.accept() => {
                        Self::handle_new_session(&manager, stream);
                    }

                    _ = cancel_rx.recv() => break,
                }
            }
            manager.stop().await;
        });

        info!("Server listening on {:}", address);
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

    fn handle_new_session(
        manager: &Manager,
        stream: TcpStream,
    ) {
        manager.get_server_to_manager_sender().0.send(ServerToManagerMsg::OpenSession(stream));
    }
}
