use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use log::info;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};

use crate::server::{ManagerMessage, Request, ServerEndpoint, ServerMessage, SessionMessage};

pub struct Manager {
    handle: JoinHandle<()>,
    _request_sender: mpsc::UnboundedSender<SessionMessage>,
    cancel: broadcast::Sender<()>,
}

struct ManagerEndpoint<TReq, TRes> {
    request_receiver: mpsc::UnboundedReceiver<TReq>,
    _request_sender: mpsc::UnboundedSender<TReq>,
    response_sender_map: HashMap<usize, mpsc::UnboundedSender<TRes>>,
}

struct ManagerContext {
    usernames: HashMap<usize, String>,
}

impl ManagerEndpoint<SessionMessage, ManagerMessage> {
    fn new() -> Self {
        let (_request_sender, request_receiver) = mpsc::unbounded_channel::<SessionMessage>();

        Self {
            request_receiver,
            _request_sender,
            response_sender_map: HashMap::new(),
        }
    }

    pub fn new_session_sender(&self) -> mpsc::UnboundedSender<SessionMessage> {
        self._request_sender.clone()
    }

    pub fn add_sender(&mut self, id: usize, sender: mpsc::UnboundedSender<ManagerMessage>) {
        self.response_sender_map.insert(id, sender);
    }

    pub fn send_session(
        &self,
        id: usize,
        msg: ManagerMessage,
    ) -> Result<(), mpsc::error::SendError<ManagerMessage>> {
        self.response_sender_map
            .get(&id)
            .expect("id not found")
            .send(msg)
    }

    async fn recv_session(&mut self) -> Option<SessionMessage> {
        self.request_receiver.recv().await
    }
}

impl Manager {
    pub fn start(mut server_endpoint: ServerEndpoint) -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let mut endpoint = ManagerEndpoint::new();
        let _request_sender = endpoint.new_session_sender();

        let handle = tokio::spawn(async move {
            let mut ctx = ManagerContext::new();
            loop {
                tokio::select! {
                    Some(msg) = endpoint.recv_session() => {
                        ctx.handle_session_msg(&mut endpoint, msg);
                    }

                    Some(msg) = server_endpoint.recv() => {
                        match msg {
                            ServerMessage::RegisterSessionRequest(id, channel) => {
                                endpoint.response_sender_map.insert(id, channel);
                            }
                        };
                    }

                    _ = cancel_rx.recv() => break,

                }
            }
        });

        Self {
            handle,
            _request_sender,
            cancel: cancel_tx,
        }
    }

    pub fn new_request_sender(&self) -> mpsc::UnboundedSender<SessionMessage> {
        self._request_sender.clone()
    }

    pub async fn stop(&self) {
        while self.cancel.send(()).is_ok() {
            time::sleep(Duration::from_millis(100)).await;
        }
        if !self.handle.is_finished() {
            self.handle.abort();
        }
    }
}

impl ManagerContext {
    fn new() -> Self {
        Self {
            usernames: HashMap::new(),
        }
    }

    fn handle_session_msg(
        &mut self,
        endpoint: &mut ManagerEndpoint<SessionMessage, ManagerMessage>,
        msg: SessionMessage,
    ) {
        match msg {
            SessionMessage::RegisterNickname(rpc_msg) => {
                match self
                    .usernames
                    .values()
                    .find(|val| **val == rpc_msg.request.msg)
                {
                    Some(_) => {
                        let _ = rpc_msg.reply.send(Result::Err(()));
                    }
                    None => {
                        self.usernames
                            .insert(rpc_msg.request.id, rpc_msg.request.msg);
                        let _ = rpc_msg.reply.send(Result::Ok(()));
                    }
                }
            }

            SessionMessage::PrivateMessage(request) => {
                let source = self
                    .usernames
                    .get(&request.id)
                    .expect("id must be assigned");
                info!("targets: {:}", request.msg.0);
                for target in request.msg.0.split(",") {
                    if let Some(target_id) = self
                        .usernames
                        .iter()
                        .find(|(_, v)| **v == target)
                        .map(|(k, _)| *k)
                    {
                        info!("target id: {:}", target_id);
                        let _ = endpoint.send_session(
                            target_id,
                            ManagerMessage::PrivateMessage(request.msg.1.clone()),
                        );
                    }
                }
            }
        }
    }
}
