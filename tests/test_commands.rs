use irc_proto::{enable_logging, message::Command};
use irc_server::{
    config::{Config, Server as ServerConfig},
    server::Server,
};
use rand::RngExt;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast,
    time::sleep,
};

pub mod client;
use client::Client;

async fn start_server() -> (broadcast::Sender<()>, SocketAddr) {
    let mut rng = rand::rng();
    let (tx, mut rx) = broadcast::channel::<()>(1);
    let address = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(127, 0, 0, 1),
        rng.random_range(1025..u16::MAX) as u16,
    ));
    let mut config = Config::new("config.toml");
    config.server.address = address;

    let server = Server::start(Arc::new(config)).await;
    tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("could not stop server")
            .expect("could not stop server");
        server.shutdown().await;
    });
    sleep(Duration::from_micros(1)).await;
    return (tx, address);
}

#[tokio::test]
async fn test_ping() {
    let (server_stop, address) = start_server().await;

    let mut client = Client::new("user1");
    client.connect(address, None).await;
    client.send(Command::PING { token: "token" });
    let message = client.read().await.unwrap();
    assert_eq!(":server1 PONG server1 token\r\n", message.contents());
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_ping_multiple() {
    let (server_stop, address) = start_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();

    stream
        .write_all(b"PING token1\r\nPING token2\r\n")
        .await
        .unwrap();
    let mut response = [0; 30];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(":server1 PONG server1 token1\r\n".as_bytes(), response);

    let mut response = [0; 30];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(":server1 PONG server1 token2\r\n".as_bytes(), response);
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_invalid_message() {
    let (server_stop, address) = start_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();

    stream
        .write_all(b"PING token1\r\nINVALID\r\nPING token2\r\n")
        .await
        .unwrap();
    let mut response = [0; 30];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(":server1 PONG server1 token1\r\n".as_bytes(), response);

    let mut response = [0; 30];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(":server1 PONG server1 token2\r\n".as_bytes(), response);
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_partial() {
    let (server_stop, address) = start_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();

    stream.write_all(b"PING ").await.unwrap();
    sleep(Duration::from_millis(1)).await;
    stream.write_all(b"token1\r\n").await.unwrap();

    stream.write_all(b"PING token2\r\n").await.unwrap();
    let mut response = [0; 30];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(":server1 PONG server1 token2\r\n".as_bytes(), response);
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_register() {
    let (server_stop, address) = start_server().await;
    let mut client = Client::new("user1");
    client.connect(address, Some("password")).await;

    let message = client.read().await.unwrap();
    assert_eq!(
        ":server1 001 user1 :Welcome to the network Network, user1\r\n",
        message.contents()
    );
    let message = client.read().await.unwrap();
    assert_eq!(
        ":server1 002 user1 :Your host is server1, running version 0.1.0\r\n",
        message.contents()
    );
    let message = client.read().await.unwrap();
    assert!(message
        .contents()
        .starts_with(":server1 003 user1 :This server was created "));
    let message = client.read().await.unwrap();
    assert!(message
        .contents()
        .starts_with(":server1 004 user1 server1 "));
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_message() {
    let (server_stop, address) = start_server().await;

    let mut client1 = Client::new("user1");
    client1.connect(address, Some("password")).await;
    client1.skip_msgs(4).await;

    let mut client2 = Client::new("user2");
    client2.connect(address, Some("password")).await;
    client2.skip_msgs(4).await;

    client1.send(Command::PRIVMSG {
        targets: "user2",
        text: "hello",
    });

    let message = client2.read().await.unwrap();
    assert_eq!(
        ":user1 PRIVMSG user2 hello\r\n".to_string(),
        message.contents()
    );
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_channel() {
    let (server_stop, address) = start_server().await;

    let mut client1 = Client::new("user1");
    client1.connect(address, Some("password")).await;
    client1.skip_msgs(4).await;
    client1.send(Command::JOIN {
        channels: "#channel1",
        keys: None,
    });

    let message = client1.read().await.unwrap();
    assert_eq!(
        ":server1 JOIN #channel1\r\n".to_string(),
        message.contents()
    );

    let mut client2 = Client::new("user2");
    client2.connect(address, Some("password")).await;
    client2.skip_msgs(4).await;
    client2.send(Command::JOIN {
        channels: "#channel1",
        keys: None,
    });

    let message = client2.read().await.unwrap();
    assert_eq!(
        ":server1 JOIN #channel1\r\n".to_string(),
        message.contents(),
    );

    client1.send(Command::PRIVMSG {
        targets: "#channel1",
        text: "hello",
    });

    let message = client2.read().await.unwrap();
    assert_eq!(
        ":user1 PRIVMSG #channel1 hello\r\n".to_string(),
        message.contents(),
    );
    server_stop.send(()).expect("server stopped");
}

#[tokio::test]
async fn test_nickname_in_use() {
    let (server_stop, address) = start_server().await;

    let mut client1 = Client::new("user1");
    client1.connect(address, Some("password")).await;
    client1.skip_msgs(4).await;

    let mut client2 = Client::new("user1");
    client2.connect(address, Some("password")).await;
    let message = client2.read().await.unwrap();
    assert_eq!(
        ":server1 433 <client> user1\r\n".to_string(),
        message.contents(),
    );

    server_stop.send(()).expect("server stopped");
}
