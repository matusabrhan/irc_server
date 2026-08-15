use irc_proto::enable_logging;
use irc_server::{config::CONFIG, server::Server};

#[tokio::main]
async fn main() -> Result<(), ()> {
    enable_logging();

    let address = CONFIG.server.address;
    let server = Server::start(address).await;
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen to event");
    server.shutdown().await;
    Ok(())
}
