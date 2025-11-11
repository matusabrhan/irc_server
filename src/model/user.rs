use crate::model::session::SessionId;
use bitflags::bitflags;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UserId(usize);

impl UserId {
    pub const INVALID: UserId = UserId(0);
}

impl From<usize> for UserId {
    fn from(value: usize) -> Self {
        UserId(value)
    }
}

bitflags! {
    #[derive(Debug, Clone, Default)]
    pub struct Registration: u8 {
        const PASS = 0b00000001;
        const NICK = 0b00000010;
        const USER = 0b00000100;
    }
}

#[derive(Debug, Clone)]
pub struct User {
    id: UserId,
    nickname: String,
    username: String,
    realname: String,
    registration: Registration,
    session_id: SessionId,
}

impl User {
    pub fn new(id: UserId, session_id: SessionId) -> Self {
        Self {
            id,
            nickname: String::new(),
            username: String::new(),
            realname: String::new(),
            registration: Registration::default(),
            session_id,
        }
    }

    pub fn get_nickname(&self) -> &str {
        &self.nickname
    }

    pub fn get_id(&self) -> UserId {
        self.id
    }

    pub fn get_session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn get_registration(&self) -> Registration {
        self.registration.clone()
    }

    pub fn is_registered(&self) -> bool {
        self.registration.is_all()
    }

    pub fn set_session_id(&mut self, id: SessionId) {
        self.session_id = id;
    }

    pub fn set_nickname(&mut self, nickname: &str) {
        self.nickname.clear();
        self.nickname.push_str(nickname);
    }

    pub fn set_username(&mut self, username: &str) {
        self.username.clear();
        self.username.push_str(username);
    }

    pub fn set_realname(&mut self, realname: &str) {
        self.realname.clear();
        self.realname.push_str(realname);
    }

    pub fn set_registrration(&mut self, registration: Registration) {
        self.registration.set(registration, true);
    }
}
