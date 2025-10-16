use crate::{
    config::CONFIG,
    dispatch::context::{Context, Registered, Unregistered},
    manager::user::UserManagerError,
};
use irc_proto::message::{Command as Cmd, Message, Source};

pub trait Dispatcher {
    fn validate(ctx: &Context<Self>, msg: &Message) -> Option<UserManagerError>
    where
        Self: Sized;

    fn process(
        ctx: &Context<Self>,
        msg: Message,
    ) -> impl std::future::Future<Output = Result<(), ()>> + Send
    where
        Self: Sized;

    async fn error(ctx: &Context<Self>, error: UserManagerError) -> Result<(), ()>
    where
        Self: Sized,
    {
        match error {
            UserManagerError::AlreadyRegistered => Ok(()),
            UserManagerError::PasswordMismatch => {
                ctx.reply(
                    Message::default()
                        .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                        .with_command(Cmd::ERR_PASSWDMISMATCH {
                            client: String::new(),
                        }),
                )
                .await
            }
            UserManagerError::NicknameInUse => {
                ctx.reply(
                    Message::default()
                        .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                        .with_command(Cmd::ERR_NICKNAMEINUSE {
                            client: String::new(),
                            nick: String::new(),
                        }),
                )
                .await
            }
        }
    }
}

impl Dispatcher for Unregistered {
    fn validate(ctx: &Context<Self>, msg: &Message) -> Option<UserManagerError>
    where
        Self: Sized,
    {
        None
    }

    async fn process(ctx: &Context<Self>, msg: Message) -> Result<(), ()>
    where
        Self: Sized,
    {
        match msg.command() {
            Cmd::PING { token } => {
                ctx.reply(
                    Message::default()
                        .with_source(Source::default().with_name(CONFIG.server.name.clone()))
                        .with_command(Cmd::PONG {
                            server: Some(CONFIG.server.name.clone()),
                            token: token.to_string(),
                        }),
                )
                .await
            }
            Cmd::PASS { password } => {
                if let Err(error) = ctx.authorize(&password).await {
                    Dispatcher::error(ctx, error).await?;
                }
                Ok(())
            }
            Cmd::USER { user, realname, .. } => {
                if let Err(error) = ctx.set_user(user, realname).await {
                    Dispatcher::error(ctx, error).await?;
                }
                Ok(())
            }
            Cmd::NICK { nickname } => {
                if let Err(error) = ctx.set_nickname(nickname).await {
                    Dispatcher::error(ctx, error).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl Dispatcher for Registered {
    fn validate(ctx: &Context<Self>, msg: &Message) -> Option<UserManagerError>
    where
        Self: Sized,
    {
        None
    }

    async fn process(ctx: &Context<Self>, msg: Message) -> Result<(), ()>
    where
        Self: Sized,
    {
        match msg.command() {
            Cmd::PRIVMSG { targets, .. } => {
                for target in targets.split(',') {
                    ctx.send_to_user(target, msg.clone()).await?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
