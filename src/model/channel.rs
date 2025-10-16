use crate::model::user::UserId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChannelId(usize);

impl ChannelId {
    pub const INVALID: ChannelId = ChannelId(0);
    pub const START: ChannelId = ChannelId(1);
}

impl From<usize> for ChannelId {
    fn from(value: usize) -> Self {
        ChannelId(value)
    }
}

impl Into<usize> for ChannelId {
    fn into(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct Channel {
    name: String,
    owner: UserId,
    members: Vec<UserId>,
}

impl Channel {
    pub fn new(name: String, owner: UserId) -> Channel {
        Self {
            name,
            owner,
            members: Vec::from([owner]),
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_members(&self) -> &[UserId] {
        self.members.as_slice()
    }

    pub fn add_member(&mut self, id: UserId) {
        self.members.push(id);
    }

    pub fn remove_member(&mut self, id: UserId) {
        self.members.retain(|&member| member != id);
    }
}
