use irc_proto::{
    enable_logging,
    message::{Command, IrcSerializable, Message},
};
use irc_server::server::Server;
use rand::Rng;
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::broadcast,
    time::sleep,
};

async fn register(stream: &mut TcpStream, nickname: String) {
    stream
        .write_all(
            Message::new(
                None,
                None,
                Command::PASS {
                    password: String::from("password"),
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();

    stream
        .write_all(
            Message::new(
                None,
                None,
                Command::NICK {
                    nickname: nickname.clone(),
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();

    stream
        .write_all(
            Message::new(
                None,
                None,
                Command::USER {
                    user: nickname.clone(),
                    mode: String::from("0"),
                    unused: String::from("*"),
                    realname: nickname.clone(),
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();

    sleep(Duration::from_micros(1)).await;
}

async fn start_server() -> (broadcast::Sender<()>, SocketAddr) {
    enable_logging();

    let mut rng = rand::rng();
    let (tx, mut rx) = broadcast::channel::<()>(1);
    let address = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::new(127, 0, 0, 1),
        rng.random_range(1025..u16::MAX) as u16,
    ));

    let server = Server::start(address)
        .await
        .expect("could not start server");
    tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(10), rx.recv()).await;
        server.shutdown().await;
    });
    sleep(Duration::from_micros(1)).await;
    return (tx, address);
}

#[tokio::test]
async fn test_ping() {
    let (server_stop, address) = start_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();

    let message = Message::new(
        None,
        None,
        Command::PING {
            token: String::from("token"),
        },
    );
    stream
        .write_all(message.to_vec_u8().as_slice())
        .await
        .unwrap();
    let mut response = [0; 29];
    stream.read_exact(&mut response).await.unwrap();

    assert_eq!(":server1 PONG server1 token\r\n".as_bytes(), response,);
    server_stop.send(());
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
    server_stop.send(());
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
    server_stop.send(());
}

#[tokio::test]
async fn test_partial() {
    let (server_stop, address) = start_server().await;
    let mut stream = TcpStream::connect(address).await.unwrap();

    stream.write_all(b"PING ").await.unwrap();
    sleep(Duration::from_millis(1)).await;
    stream.write_all(b"token1\r\n").await.unwrap();
    let mut response = [0; 30];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(":server1 PONG server1 token1\r\n".as_bytes(), response);
    server_stop.send(());
}

#[tokio::test]
async fn test_message() {
    let (server_stop, address) = start_server().await;

    let mut client1 = TcpStream::connect(address).await.unwrap();
    register(&mut client1, "nick1".to_string()).await;

    let mut client2 = TcpStream::connect(address).await.unwrap();
    register(&mut client2, "nick2".to_string()).await;

    tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;

    client1
        .write_all(
            Message::new(
                None,
                None,
                Command::PRIVMSG {
                    targets: String::from("nick2"),
                    text: String::from("hello"),
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();

    let mut response = [0; 28];
    client2.read(&mut response).await.unwrap();
    assert_eq!(":nick1 PRIVMSG nick2 hello\r\n".as_bytes(), &response);
    server_stop.send(());
}

#[tokio::test]
async fn test_channel() {
    let (server_stop, address) = start_server().await;

    let mut client1 = TcpStream::connect(address).await.unwrap();
    register(&mut client1, "nick1".to_string()).await;

    let mut client2 = TcpStream::connect(address).await.unwrap();
    register(&mut client2, "nick2".to_string()).await;

    client1
        .write_all(
            Message::new(
                None,
                None,
                Command::JOIN {
                    channels: String::from("#channel1"),
                    keys: None,
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();
    let mut response = [0; 23];
    client1.read(&mut response).await.unwrap();
    assert_eq!(":nick1 JOIN #channel1\r\n".as_bytes(), &response);

    client2
        .write_all(
            Message::new(
                None,
                None,
                Command::JOIN {
                    channels: String::from("#channel1"),
                    keys: None,
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();
    let mut response = [0; 23];
    client2.read(&mut response).await.unwrap();
    assert_eq!(":nick2 JOIN #channel1\r\n".as_bytes(), &response);

    client1
        .write_all(
            Message::new(
                None,
                None,
                Command::PRIVMSG {
                    targets: String::from("#channel1"),
                    text: String::from("hello"),
                },
            )
            .to_vec_u8()
            .as_slice(),
        )
        .await
        .unwrap();
    let mut response = [0; 32];
    client2.read(&mut response).await.unwrap();
    assert_eq!(":nick1 PRIVMSG #channel1 hello\r\n".as_bytes(), &response);
    server_stop.send(());
}
