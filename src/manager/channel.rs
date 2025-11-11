use crate::{
    manager::channel,
    model::{
        channel::{Channel, ChannelId},
        user::UserId,
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

#[derive(Debug, Clone)]
pub struct ChannelManager {
    inner: Arc<RwLock<HashMap<ChannelId, Channel>>>,
    next: Arc<AtomicUsize>,
}

impl ChannelManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            next: Arc::new(AtomicUsize::new(ChannelId::START.into())),
        }
    }

    fn allocate_id(&self) -> ChannelId {
        ChannelId::from(self.next.fetch_add(1, Ordering::SeqCst))
    }

    async fn create_channel(&self, name: String, owner: UserId) -> ChannelId {
        let id = self.allocate_id();
        self.inner
            .write()
            .await
            .insert(id, Channel::new(id, name, owner));
        debug!("created channel: {:?}", id);
        id
    }

    pub async fn delete_channel(&self, id: ChannelId) {
        self.inner.write().await.remove(&id);
        debug!("deleted channel: {:?}", id);
    }

    pub async fn lookup_channel(&self, name: &str) -> Option<Vec<UserId>> {
        self.inner
            .read()
            .await
            .values()
            .find(|channel| channel.get_name() == name)
            .map(|channel| channel.get_members().to_vec())
    }

    pub async fn join_or_create(&self, name: &str, user_id: UserId) -> ChannelId {
        let mut inner = self.inner.write().await;
        match inner
            .values()
            .find(|channel| channel.get_name() == name)
            .map(|channel| channel.get_id())
        {
            Some(id) => {
                inner
                    .get_mut(&id)
                    .map(|channel| channel.add_member(user_id));
                id
            }
            None => {
                let id = self.allocate_id();
                inner.insert(id, Channel::new(id, name.to_string(), user_id));
                id
            }
        }
    }
}
