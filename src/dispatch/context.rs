use crate::{
    dispatch::dispatch::Dispatcher,
    manager::{
        session::SessionManager,
        user::{UserManager, UserManagerError},
    },
    model::{session::SessionId, user::UserId},
};
use irc_proto::message::{Message, Source};

pub struct Unregistered;
pub struct Registered;

pub struct Context<State = Unregistered> {
    session_id: SessionId,
    user_id: UserId,
    session_mgr: SessionManager,
    user_mgr: UserManager,
    // channel_mgr: ,
    state: std::marker::PhantomData<State>,
}

impl<State> Context<State> {
    pub fn new(
        session_id: SessionId,
        user_id: UserId,
        session_mgr: SessionManager,
        user_mgr: UserManager,
    ) -> Self {
        Self {
            session_id,
            user_id,
            session_mgr,
            user_mgr,
            state: std::marker::PhantomData::<State>,
        }
    }

    pub async fn set_nickname(&self, nickname: &str) -> Result<(), UserManagerError> {
        self.user_mgr.set_nickname(self.user_id, nickname).await
    }

    pub async fn reply(&self, msg: Message) -> Result<(), ()> {
        self.session_mgr.send(self.session_id, msg).await
    }
}

impl Context<Unregistered> {
    pub async fn register(self) -> Result<Context<Registered>, Context<Unregistered>> {
        if self.user_mgr.is_registered(self.user_id).await {
            self.user_mgr
                .set_session_id(self.user_id, self.session_id)
                .await;
            return Ok(Context {
                session_id: self.session_id,
                user_id: self.user_id,
                session_mgr: self.session_mgr,
                user_mgr: self.user_mgr,
                state: std::marker::PhantomData::<Registered>,
            });
        }
        Err(self)
    }

    pub async fn authorize(&self, password: &str) -> Result<(), UserManagerError> {
        self.user_mgr.authorize(self.user_id, password).await
    }

    pub async fn set_user(&self, username: &str, realname: &str) -> Result<(), UserManagerError> {
        self.user_mgr
            .set_user(self.user_id, username, realname)
            .await
    }

    pub async fn handle(&self, msg: Message) -> Result<(), ()> {
        match Dispatcher::validate(self, &msg) {
            None => Dispatcher::process(self, msg).await,
            Some(error) => Dispatcher::error(self, error).await,
        }
    }
}

impl Context<Registered> {
    pub async fn send_to_user(&self, user: &str, msg: Message) -> Result<(), ()> {
        if let Some(id) = self.user_mgr.lookup_nickname(user).await {
            return self.session_mgr.send(id, msg).await;
        }
        Ok(())
    }

    pub async fn get_nickname(&self) -> String {
        self.user_mgr
            .get_nickname(self.user_id)
            .await
            .unwrap_or(String::new())
    }

    pub async fn handle(&self, msg: Message) -> Result<(), ()> {
        match Dispatcher::validate(self, &msg) {
            None => {
                let msg = match msg.source().is_some() {
                    true => msg,
                    false => {
                        msg.with_source(Source::default().with_name(self.get_nickname().await))
                    }
                };
                Dispatcher::process(self, msg).await
            }
            Some(error) => Dispatcher::error(self, error).await,
        }
    }
}
