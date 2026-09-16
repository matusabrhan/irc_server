use std::sync::Arc;

use irc_proto::enable_logging;
use irc_server::{config::Config, server::Server};

#[tokio::main]
async fn main() -> Result<(), ()> {
    enable_logging();

    let config = Arc::new(Config::new("config.toml"));
    let server = Server::start(config).await;
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen to event");
    server.shutdown().await;
    Ok(())
}
