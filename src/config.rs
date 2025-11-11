use chrono::Local;
use once_cell::sync::Lazy;
use serde_derive::Deserialize;
use std::fs::read_to_string;
use std::net::SocketAddr;
use toml;

pub static CONFIG: Lazy<Config> = Lazy::new(|| Config::new("config.toml"));

#[derive(Deserialize, Clone)]
pub struct Config {
    pub server: Server,
    pub network_name: String,
}

#[derive(Deserialize, Clone)]
pub struct Server {
    pub name: String,
    pub password: String,
    pub address: SocketAddr,
    #[serde(skip_deserializing)]
    pub time: String,
    #[serde(skip_deserializing)]
    pub version: String,
}

impl Config {
    pub fn new(path: &str) -> Self {
        let contents = match read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                panic!("Could not read file \"{}\"", path);
            }
        };

        let mut config: Config = match toml::from_str(&contents) {
            Ok(c) => c,
            Err(_) => {
                panic!("Could not load data from file \"{}\"", path);
            }
        };

        config.server.time = Local::now().to_rfc3339();
        config.server.version = env!("CARGO_PKG_VERSION").to_string();

        return config;
    }
}
