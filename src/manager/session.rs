use crate::model::session::{Session, SessionId};
use irc_proto::message::Message;
use log::debug;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone)]
pub struct SessionManager {
    inner: Arc<RwLock<HashMap<SessionId, Session>>>,
    next: Arc<AtomicUsize>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            next: Arc::new(AtomicUsize::new(1)),
        }
    }

    fn allocate_id(&self) -> SessionId {
        SessionId::from(self.next.fetch_add(1, Ordering::SeqCst))
    }

    pub async fn create_session(&self, write: mpsc::UnboundedSender<Message>) -> SessionId {
        let id = self.allocate_id();
        self.inner.write().await.insert(id, Session::new(id, write));
        debug!("created session: {:?}", id);
        id
    }

    pub async fn delete_session(&self, id: SessionId) {
        self.inner.write().await.remove(&id);
        debug!("deleted session: {:?}", id);
    }

    pub async fn send(&self, id: SessionId, msg: Message) -> Result<(), ()> {
        if let Some(session) = self.inner.read().await.get(&id) {
            if let Err(_) = session.send(msg) {
                self.delete_session(id).await;
                return Err(());
            }
        }
        Ok(())
    }
}
