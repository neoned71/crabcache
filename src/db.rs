use bytes::Bytes;
use dashmap::DashMap;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum Value {
    String(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<Bytes, Bytes>),
    ZSet {
        scores: HashMap<Bytes, f64>,
        ordered: BTreeSet<(OrderedFloat<f64>, Bytes)>,
    },
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Value,
    pub expires: Option<Instant>,
}

impl Entry {
    pub fn is_expired(&self) -> bool {
        self.expires.map(|t| Instant::now() >= t).unwrap_or(false)
    }
}

#[derive(Clone)]
pub struct Db {
    pub store: Arc<DashMap<Bytes, Entry>>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            store: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &Bytes) -> Option<Entry> {
        // lazy expiry
        if let Some(e) = self.store.get(key) {
            if e.is_expired() {
                drop(e);
                self.store.remove(key);
                return None;
            }
            return Some(e.clone());
        }
        None
    }

    pub fn remove(&self, key: &Bytes) -> bool {
        self.store.remove(key).is_some()
    }

    pub fn exists(&self, key: &Bytes) -> bool {
        self.get(key).is_some()
    }

    pub fn len(&self) -> usize {
        // remove expired lazily? just count non-expired approximate
        self.store.len()
    }

    pub fn flush(&self) {
        self.store.clear();
    }

    pub fn keys(&self) -> Vec<Bytes> {
        let now = Instant::now();
        let mut out = Vec::new();
        for r in self.store.iter() {
            let e = r.value();
            if let Some(t) = e.expires {
                if now >= t {
                    continue;
                }
            }
            out.push(r.key().clone());
        }
        out
    }

    pub fn random_key(&self) -> Option<Bytes> {
        // simple: first non-expired
        for r in self.store.iter() {
            let e = r.value();
            if e.is_expired() {
                continue;
            }
            return Some(r.key().clone());
        }
        None
    }

    pub fn ttl_ms(&self, key: &Bytes) -> i64 {
        // -2 not exists, -1 no ttl, else ms
        match self.store.get(key) {
            Some(e) => {
                if e.is_expired() {
                    drop(e);
                    self.store.remove(key);
                    return -2;
                }
                match e.expires {
                    Some(t) => {
                        let now = Instant::now();
                        if t <= now {
                            -2
                        } else {
                            (t - now).as_millis() as i64
                        }
                    }
                    None => -1,
                }
            }
            None => -2,
        }
    }

    pub fn set_expiry(&self, key: &Bytes, when: Instant) -> bool {
        if let Some(mut e) = self.store.get_mut(key) {
            if e.is_expired() {
                drop(e);
                self.store.remove(key);
                return false;
            }
            e.expires = Some(when);
            true
        } else {
            false
        }
    }

    pub fn persist(&self, key: &Bytes) -> bool {
        if let Some(mut e) = self.store.get_mut(key) {
            if e.is_expired() {
                drop(e);
                self.store.remove(key);
                return false;
            }
            if e.expires.is_some() {
                e.expires = None;
                return true;
            }
            false
        } else {
            false
        }
    }
}

pub struct DbManager {
    pub dbs: Vec<Arc<Db>>,
    pub default_db_count: usize,
}

impl DbManager {
    pub fn new(n: usize) -> Self {
        let mut dbs = Vec::with_capacity(n);
        for _ in 0..n {
            dbs.push(Arc::new(Db::new()));
        }
        Self {
            dbs,
            default_db_count: n,
        }
    }
}

pub type SharedDb = Arc<DbManager>;

// helper for expiry Instant from ms
pub fn instant_from_ms(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms)
}

pub fn unix_ms_now() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}