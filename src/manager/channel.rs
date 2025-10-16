use crate::model::{
    channel::{Channel, ChannelId},
    user::UserId,
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

#[derive(Debug, Clone)]
pub struct UserManager {
    inner: Arc<RwLock<HashMap<ChannelId, Channel>>>,
    next: Arc<AtomicUsize>,
}

impl UserManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            next: Arc::new(AtomicUsize::new(ChannelId::START.into())),
        }
    }

    fn allocate_id(&self) -> ChannelId {
        ChannelId::from(self.next.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn create_channel(&self, name: String, owner: UserId) -> ChannelId {
        let id = self.allocate_id();
        self.inner
            .write()
            .await
            .insert(id, Channel::new(name, owner));
        debug!("created channel: {:?}", id);
        id
    }

    pub async fn delete_channel(&self, id: ChannelId) {
        self.inner.write().await.remove(&id);
        debug!("deleted channel: {:?}", id);
    }
}
