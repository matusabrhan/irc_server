use crate::{
    config::Config,
    manager::{Manager, ServerToManagerMsg},
};
use log;
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
    task::JoinHandle,
    time::sleep,
};

pub struct Server {
    handle: JoinHandle<()>,
    cancel: broadcast::Sender<()>,
}

impl Server {
    pub async fn start(config: Arc<Config>) -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);

        let config_copy = config.clone();
        let handle = tokio::spawn(async move {
            let listener = TcpListener::bind(config_copy.server.address)
                .await
                .expect("could not start server");

            let manager = Manager::start(config_copy.clone());

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

        log::info!("Server listening on {:}", config.server.address.clone());
        Self {
            handle,
            cancel: cancel_tx,
        }
    }

    pub async fn shutdown(&self) {
        log::info!("Server shutting down");
        while self.cancel.send(()).is_ok() {
            sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }

    fn handle_new_session(manager: &Manager, stream: TcpStream) {
        manager
            .get_server_to_manager_sender()
            .0
            .send(ServerToManagerMsg::OpenSession(stream));
    }
}
