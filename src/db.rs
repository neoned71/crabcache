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
    Set(SetEncoding),
    Hash(HashEncoding),
    ZSet {
        scores: HashMap<Bytes, f64>,
        ordered: BTreeSet<(OrderedFloat<f64>, Bytes)>,
    },
}

#[derive(Debug, Clone)]
pub enum SetEncoding {
    Small(Vec<Bytes>),   // compact, linear scan; used for small sets
    Large(HashSet<Bytes>),
}

#[derive(Debug, Clone)]
pub enum HashEncoding {
    Small(Vec<(Bytes, Bytes)>),
    Large(HashMap<Bytes, Bytes>),
}

pub const SMALL_THRESHOLD: usize = 32;

impl SetEncoding {
    pub fn contains(&self, m: &Bytes) -> bool {
        match self {
            SetEncoding::Small(v) => v.iter().any(|x| x == m),
            SetEncoding::Large(s) => s.contains(m),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            SetEncoding::Small(v) => v.len(),
            SetEncoding::Large(s) => s.len(),
        }
    }
    pub fn insert(&mut self, m: Bytes) -> bool {
        match self {
            SetEncoding::Small(v) => {
                if v.iter().any(|x| x == &m) { return false; }
                v.push(m);
                if v.len() > SMALL_THRESHOLD {
                    let mut s = HashSet::new();
                    for x in v.drain(..) { s.insert(x); }
                    *self = SetEncoding::Large(s);
                }
                true
            }
            SetEncoding::Large(s) => s.insert(m),
        }
    }
    pub fn remove(&mut self, m: &Bytes) -> bool {
        match self {
            SetEncoding::Small(v) => {
                if let Some(i) = v.iter().position(|x| x == m) {
                    v.swap_remove(i);
                    true
                } else { false }
            }
            SetEncoding::Large(s) => s.remove(m),
        }
    }
    pub fn iter_vec(&self) -> Vec<Bytes> {
        match self {
            SetEncoding::Small(v) => v.clone(),
            SetEncoding::Large(s) => s.iter().cloned().collect(),
        }
    }
    pub fn pop_one(&mut self) -> Option<Bytes> {
        match self {
            SetEncoding::Small(v) => {
                if v.is_empty() { None } else { Some(v.swap_remove(0)) }
            }
            SetEncoding::Large(s) => {
                let m = s.iter().next().cloned()?;
                s.remove(&m);
                Some(m)
            }
        }
    }
}

impl HashEncoding {
    pub fn get(&self, f: &Bytes) -> Option<Bytes> {
        match self {
            HashEncoding::Small(v) => v.iter().find(|(k,_)| k == f).map(|(_,v)| v.clone()),
            HashEncoding::Large(m) => m.get(f).cloned(),
        }
    }
    pub fn contains_key(&self, f: &Bytes) -> bool {
        match self {
            HashEncoding::Small(v) => v.iter().any(|(k,_)| k == f),
            HashEncoding::Large(m) => m.contains_key(f),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            HashEncoding::Small(v) => v.len(),
            HashEncoding::Large(m) => m.len(),
        }
    }
    pub fn insert(&mut self, f: Bytes, v: Bytes) -> Option<Bytes> {
        // returns old value if present
        match self {
            HashEncoding::Small(vec) => {
                if let Some(i) = vec.iter().position(|(k,_)| k == &f) {
                    let old = vec[i].1.clone();
                    vec[i].1 = v;
                    if vec.len() > SMALL_THRESHOLD {
                        let mut m = HashMap::new();
                        for (k,val) in vec.drain(..) { m.insert(k,val); }
                        *self = HashEncoding::Large(m);
                    }
                    Some(old)
                } else {
                    vec.push((f, v));
                    if vec.len() > SMALL_THRESHOLD {
                        let mut m = HashMap::new();
                        for (k,val) in vec.drain(..) { m.insert(k,val); }
                        *self = HashEncoding::Large(m);
                    }
                    None
                }
            }
            HashEncoding::Large(m) => m.insert(f, v),
        }
    }
    pub fn remove(&mut self, f: &Bytes) -> Option<Bytes> {
        match self {
            HashEncoding::Small(v) => {
                if let Some(i) = v.iter().position(|(k,_)| k == f) {
                    Some(v.swap_remove(i).1)
                } else { None }
            }
            HashEncoding::Large(m) => m.remove(f),
        }
    }
    pub fn keys(&self) -> Vec<Bytes> {
        match self {
            HashEncoding::Small(v) => v.iter().map(|(k,_)| k.clone()).collect(),
            HashEncoding::Large(m) => m.keys().cloned().collect(),
        }
    }
    pub fn values(&self) -> Vec<Bytes> {
        match self {
            HashEncoding::Small(v) => v.iter().map(|(_,val)| val.clone()).collect(),
            HashEncoding::Large(m) => m.values().cloned().collect(),
        }
    }
    pub fn pairs(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            HashEncoding::Small(v) => v.clone(),
            HashEncoding::Large(m) => m.iter().map(|(k,v)| (k.clone(), v.clone())).collect(),
        }
    }
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
