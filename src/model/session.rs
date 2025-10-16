use irc_proto::message::Message;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(usize);

impl SessionId {
    pub const INVALID: SessionId = SessionId(0);
}

impl From<usize> for SessionId {
    fn from(value: usize) -> Self {
        SessionId(value)
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    id: SessionId,
    write: mpsc::UnboundedSender<Message>,
}

impl Session {
    pub fn new(id: SessionId, write: mpsc::UnboundedSender<Message>) -> Self {
        Self { id, write: write }
    }

    pub fn send(&self, msg: Message) -> Result<(), mpsc::error::SendError<Message>> {
        self.write.send(msg)
    }

    pub fn id(&self) -> SessionId {
        self.id
    }

    // pub async fn run(mut read: mpsc::UnboundedReceiver<Message>, mut ctx: Context) {
    //     let registered_ctx = loop {
    //         match read.recv().await {
    //             Some(msg) => {
    //                 info!("msg: {:?}", msg);
    //                 if Dispatcher::handle(&ctx, msg).await.is_err() {
    //                     ctx.shutdown().await;
    //                     return;
    //                 }
    //                 ctx = match ctx.register().await {
    //                     Ok(registered_ctx) => break registered_ctx,
    //                     Err(unregistered_ctx) => unregistered_ctx,
    //                 };
    //             }
    //             None => {
    //                 ctx.shutdown().await;
    //                 return;
    //             }
    //         }
    //     };
    //     info!(
    //         "registered user: {:?}",
    //         registered_ctx.get_user_view().await
    //     );
    //     while let Some(msg) = read.recv().await {
    //         info!("msg: {:?}", msg);
    //         if Dispatcher::handle(&registered_ctx, msg).await.is_err() {
    //             break;
    //         }
    //     }
    //     registered_ctx.shutdown().await;
    // }
}
