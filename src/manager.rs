use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};

use crate::ipc_bus::{ManagerBus, ManagerMessage, ServerBus, ServerMessage, SessionId, SessionMessage};

pub struct Manager {
    handle: JoinHandle<()>,
    _request_sender: mpsc::UnboundedSender<SessionMessage>,
    cancel: broadcast::Sender<()>,
}

struct ManagerContext {
    nicknames: HashMap<SessionId, String>,
    channels: HashMap<String, HashSet<SessionId>>,
}

impl Manager {
    pub fn start(server_bus: ServerBus) -> Self {
        let (cancel_tx, mut cancel_rx) = broadcast::channel(1);
        let (mut bus, mut server_receiver) = ManagerBus::new(server_bus);
        let _request_sender = bus.new_session_sender();

        let handle = tokio::spawn(async move {
            let mut ctx = ManagerContext::new();
            loop {
                tokio::select! {
                    Some(msg) = bus.session_recv() => {
                        ctx.handle_session_msg(&mut bus, msg);
                    }

                    Some(msg) = server_receiver.recv() => {
                        match msg {
                            ServerMessage::RegisterSession(id, channel) => {
                                bus.add_sender(id, channel);
                            }

                            ServerMessage::CloseSession(..) => {
                                unreachable!();
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
            nicknames: HashMap::new(),
            channels: HashMap::new(),
        }
    }

    fn find_nickname_id(&self, nickname: &str) -> Option<SessionId> {
        self.nicknames
            .iter()
            .find(|(_, v)| *v == nickname)
            .map(|(k, _)| *k)
    }

    fn handle_session_msg(&mut self, bus: &mut ManagerBus, msg: SessionMessage) {
        match msg {
            SessionMessage::RegisterNickname(rpc_msg) => {
                match self.find_nickname_id(&rpc_msg.request.msg) {
                    Some(_) => {
                        let _ = rpc_msg.reply.send(Result::Err(()));
                    }
                    None => {
                        self.nicknames
                            .insert(rpc_msg.request.id, rpc_msg.request.msg);
                        let _ = rpc_msg.reply.send(Result::Ok(()));
                    }
                }
            }

            SessionMessage::PrivateMessage(request) => {
                let source = self
                    .nicknames
                    .get(&request.id)
                    .expect("nick must be known when sending private message");
                for ref target in request.msg.0 {
                    if target == source {
                        continue;
                    }

                    match self.find_nickname_id(target) {
                        Some(target_id) => {
                            let _ = bus.session_send(
                                &target_id,
                                ManagerMessage::PrivateMessage(request.msg.1.clone()),
                            );
                        }
                        None => {
                            if let Some(channel_member_ids) = self.channels.get(target) {
                                for member_id in channel_member_ids {
                                    let _ = bus.session_send(
                                        member_id,
                                        ManagerMessage::PrivateMessage(request.msg.1.clone()),
                                    );
                                }
                            }
                        }
                    }
                }
            }

            SessionMessage::JoinChannels(rpc_msg) => {
                // TODO: handle channel passwords

                let mut joined_channels: Vec<String> = Vec::new();
                for channel_name in rpc_msg.request.msg.0 {
                    joined_channels.push(channel_name.clone());
                    self.channels
                        .entry(channel_name)
                        .or_default()
                        .insert(rpc_msg.request.id);
                }
                let _ = rpc_msg.reply.send(joined_channels);
            }

            SessionMessage::Quit(request) => {
                self.nicknames.remove(&request.id);
                for channel in self.channels.values_mut() {
                    channel.remove(&request.id);
                }
                bus.server_send(ServerMessage::CloseSession(request.id));
            }
        }
    }
}
