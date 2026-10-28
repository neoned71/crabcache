use crate::db::{DbManager, SharedDb};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct ClientInfo {
    pub id: u64,
    pub name: String,
    pub db: usize,
}

#[derive(Clone)]
pub struct ServerState {
    pub dbm: SharedDb,
    pub bind: String,
    pub port: u16,
    pub dir: String,
    pub dbfilename: String,
    pub appendonly: bool,
    pub appendfilename: String,
    pub aof: Option<std::sync::Arc<parking_lot::Mutex<std::io::BufWriter<std::fs::File>>>>,
    pub requirepass: Option<String>,
    pub next_client_id: Arc<RwLock<u64>>,
    pub pubsub: Arc<PubSub>,
    pub start_time: std::time::Instant,
    pub config: Arc<RwLock<HashMap<String, String>>>,
}

impl ServerState {
    pub fn new(dbm: SharedDb, bind: String, port: u16) -> Self {
        let mut cfg = HashMap::new();
        cfg.insert("dir".into(), ".".into());
        cfg.insert("dbfilename".into(), "dump.rdb".into());
        cfg.insert("appendonly".into(), "no".into());
        cfg.insert("appendfilename".into(), "appendonly.aof".into());
        Self {
            dbm,
            bind,
            port,
            dir: ".".into(),
            dbfilename: "dump.rdb".into(),
            appendonly: false,
            appendfilename: "appendonly.aof".into(),
            aof: None,
            requirepass: None,
            next_client_id: Arc::new(RwLock::new(1)),
            pubsub: Arc::new(PubSub::new()),
            start_time: std::time::Instant::now(),
            config: Arc::new(RwLock::new(cfg)),
        }
    }
}

pub struct PubSub {
    pub global: broadcast::Sender<(Bytes, Bytes)>,
}

impl PubSub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self { global: tx }
    }

    pub fn get_or_create(&self, _ch: &Bytes) -> broadcast::Sender<(Bytes, Bytes)> {
        self.global.clone()
    }

    pub fn publish(&self, ch: &Bytes, msg: Bytes) -> usize {
        self.global.send((ch.clone(), msg)).unwrap_or(0)
    }
}
