use crate::{
    config::CONFIG,
    model::{
        session::SessionId,
        user::{Registration, User, UserId},
    },
};
use log::debug;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

pub enum UserManagerError {
    AlreadyRegistered,
    PasswordMismatch,
    NicknameInUse,
}

#[derive(Debug, Clone)]
pub struct UserManager {
    inner: Arc<RwLock<HashMap<UserId, User>>>,
    next: Arc<AtomicUsize>,
}

impl UserManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            next: Arc::new(AtomicUsize::new(1)),
        }
    }

    fn allocate_id(&self) -> UserId {
        UserId::from(self.next.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn create_user(&self) -> UserId {
        let id = self.allocate_id();
        self.inner.write().await.insert(id, User::new(id));
        debug!("created user: {:?}", id);
        id
    }

    pub async fn delete_user(&self, id: UserId) {
        self.inner.write().await.remove(&id);
        debug!("deleted user: {:?}", id);
    }

    pub async fn authorize(&self, id: UserId, password: &str) -> Result<(), UserManagerError> {
        let mut inner = self.inner.write().await;
        if let Some(user) = inner.get_mut(&id) {
            if user.is_registered() {
                return Err(UserManagerError::AlreadyRegistered);
            }
            if CONFIG.server.password != password {
                return Err(UserManagerError::PasswordMismatch);
            }
            user.set_registrration(Registration::PASS);
        }
        Ok(())
    }

    pub async fn set_user(
        &self,
        id: UserId,
        username: &str,
        realname: &str,
    ) -> Result<(), UserManagerError> {
        let mut inner = self.inner.write().await;
        if let Some(user) = inner.get_mut(&id) {
            if user.is_registered() {
                return Err(UserManagerError::AlreadyRegistered);
            }
            user.set_username(username);
            user.set_realname(realname);
            user.set_registrration(Registration::USER);
        }
        Ok(())
    }

    pub async fn set_nickname(&self, id: UserId, nickname: &str) -> Result<(), UserManagerError> {
        let mut inner = self.inner.write().await;
        match inner
            .values()
            .find(|user| user.get_nickname() == nickname)
            .is_none()
        {
            true => {
                if let Some(user) = inner.get_mut(&id) {
                    user.set_nickname(nickname);
                    user.set_registrration(Registration::NICK);
                }
            }
            false => return Err(UserManagerError::NicknameInUse),
        }
        Ok(())
    }

    pub async fn set_session_id(&self, id: UserId, session_id: SessionId) {
        self.inner
            .write()
            .await
            .get_mut(&id)
            .map(|user| user.set_session_id(session_id));
    }

    pub async fn is_registered(&self, id: UserId) -> bool {
        self.inner
            .read()
            .await
            .get(&id)
            .map(|user| user.is_registered())
            .unwrap_or(false)
    }

    pub async fn lookup_nickname(&self, nickname: &str) -> Option<SessionId> {
        self.inner
            .read()
            .await
            .values()
            .find(|user| user.get_nickname() == nickname && user.is_registered())
            .map(|user| user.get_session_id())
    }

    pub async fn get_nickname(&self, id: UserId) -> Option<String> {
        self.inner
            .read()
            .await
            .get(&id)
            .map(|user| user.get_nickname().to_string())
    }
}
