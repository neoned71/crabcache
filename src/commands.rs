use crate::db::{self, Value};
use crate::state::{PubSub, ServerState};
use bytes::Bytes;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct ConnContext {
    pub id: u64,
    pub name: String,
    pub db: usize,
    pub protocol: i64, // 2 or 3
    pub authenticated: bool,
    pub multi: Option<Vec<crate::protocol::Frame>>,
    pub subscribed: bool,
    pub subscribed_channels: Vec<Bytes>,
    pub in_subscribed_mode: bool,
}

impl ConnContext {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            name: String::new(),
            db: 0,
            protocol: 2,
            authenticated: true,
            multi: None,
            subscribed: false,
            subscribed_channels: Vec::new(),
            in_subscribed_mode: false,
        }
    }
}

use crate::protocol::Frame;

fn db_of(state: &ServerState, ctx: &ConnContext) -> Arc<crate::db::Db> {
    state.dbm.dbs[ctx.db].clone()
}

fn arg_str(f: &Frame) -> Option<String> {
    f.as_str().map(|s| s.to_string())
}

fn args(cmd: &Frame) -> Option<Vec<Bytes>> {
    cmd.to_command_args()
}

fn wrong_arity(cmd: &str) -> Frame {
    Frame::Error(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    ))
}

fn ok() -> Frame {
    Frame::SimpleString("OK".to_string())
}

fn err(s: &str) -> Frame {
    Frame::Error(s.to_string())
}

fn nil() -> Frame {
    Frame::BulkString(None)
}

fn int(n: i64) -> Frame {
    Frame::Integer(n)
}

pub async fn dispatch(state: &ServerState, ctx: &mut ConnContext, frame: Frame) -> Vec<Frame> {
    // If in multi mode and command not control, queue it.
    let arr = match frame.to_command_args() {
        Some(a) if !a.is_empty() => a,
        _ => {
            // maybe a single frame
            if let Some(b) = frame.as_bytes() {
                // treat as command with no args? unlikely
                vec![b]
            } else {
                return vec![err("ERR invalid command")];
            }
        }
    };

    let name = String::from_utf8_lossy(&arr[0]).to_uppercase();
    let args = &arr[1..];

    // auth check
    if state.requirepass.is_some()
        && !ctx.authenticated
        && name != "AUTH"
        && name != "HELLO"
        && name != "QUIT"
        && name != "PING"
    {
        return vec![err("NOAUTH Authentication required.")];
    }

    // multi queuing
    let is_control = matches!(
        name.as_str(),
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" | "QUIT"
    );
    if ctx.multi.is_some() && !is_control {
        // queue raw frame? We need re-execute later. Store the original frame.
        // We stored args; reconstruct a frame array.
        let mut items: Vec<Frame> = Vec::with_capacity(arr.len());
        for b in &arr {
            items.push(Frame::BulkString(Some(b.clone())));
        }
        ctx.multi.as_mut().unwrap().push(Frame::Array(Some(items)));
        return vec![Frame::SimpleString("QUEUED".to_string())];
    }

    match name.as_str() {
        "PING" => {
            if args.is_empty() {
                if ctx.protocol >= 3 {
                    vec![Frame::SimpleString("PONG".to_string())]
                } else {
                    // redis returns PONG simple string in resp2
                    vec![Frame::SimpleString("PONG".to_string())]
                }
            } else {
                vec![Frame::BulkString(Some(args[0].clone()))]
            }
        }
        "ECHO" => {
            if args.len() != 1 {
                return vec![wrong_arity("echo")];
            }
            vec![Frame::BulkString(Some(args[0].clone()))]
        }
        "HELLO" => {
            // minimal
            if !args.is_empty() {
                if let Ok(v) = String::from_utf8_lossy(&args[0]).parse::<i64>() {
                    ctx.protocol = if v >= 3 { 3 } else { 2 };
                }
            }
            // return server info map (resp3) or simple
            if ctx.protocol >= 3 {
                let pairs = vec![
                    (
                        Frame::SimpleString("server".into()),
                        Frame::SimpleString("crabcache".into()),
                    ),
                    (
                        Frame::SimpleString("version".into()),
                        Frame::SimpleString("0.1.0".into()),
                    ),
                    (
                        Frame::SimpleString("proto".into()),
                        Frame::Integer(ctx.protocol),
                    ),
                    (
                        Frame::SimpleString("id".into()),
                        Frame::Integer(ctx.id as i64),
                    ),
                    (
                        Frame::SimpleString("mode".into()),
                        Frame::SimpleString("standalone".into()),
                    ),
                    (
                        Frame::SimpleString("role".into()),
                        Frame::SimpleString("master".into()),
                    ),
                ];
                vec![Frame::Map(pairs)]
            } else {
                vec![Frame::SimpleString("OK".into())]
            }
        }
        "QUIT" => {
            vec![Frame::SimpleString("OK".into())]
        }
        "AUTH" => {
            if args.is_empty() {
                return vec![wrong_arity("auth")];
            }
            let pass = String::from_utf8_lossy(&args[args.len() - 1]);
            if state.requirepass.as_ref() == Some(&pass.to_string()) || state.requirepass.is_none()
            {
                ctx.authenticated = true;
                vec![ok()]
            } else {
                vec![err("ERR invalid password")]
            }
        }
        "SELECT" => {
            if args.len() != 1 {
                return vec![wrong_arity("select")];
            }
            let n: usize = match String::from_utf8_lossy(&args[0]).parse() {
                Ok(v) => v,
                Err(_) => return vec![err("ERR invalid DB index")],
            };
            if n >= state.dbm.dbs.len() {
                return vec![err("ERR DB index is out of range")];
            }
            ctx.db = n;
            vec![ok()]
        }
        "FLUSHDB" | "FLUSHALL" => {
            let db = db_of(state, ctx);
            db.flush();
            vec![ok()]
        }
        "DBSIZE" => {
            let db = db_of(state, ctx);
            vec![int(db.len() as i64)]
        }
        "KEYS" => {
            let pattern = if args.is_empty() {
                "*".to_string()
            } else {
                String::from_utf8_lossy(&args[0]).to_string()
            };
            let db = db_of(state, ctx);
            let keys = db.keys();
            let filtered: Vec<Bytes> = keys
                .into_iter()
                .filter(|k| glob_match(&pattern, &String::from_utf8_lossy(k)))
                .collect();
            let mut out = Vec::new();
            for k in filtered {
                out.push(Frame::BulkString(Some(k)));
            }
            vec![Frame::Array(Some(out))]
        }
        "SCAN" => {
            // basic: SCAN cursor [MATCH pattern] [COUNT n]
            let cursor: usize = args
                .get(0)
                .and_then(|b| String::from_utf8_lossy(b).parse().ok())
                .unwrap_or(0);
            let db = db_of(state, ctx);
            let keys = db.keys();
            // return all in one go for simplicity, cursor 0
            let mut out_keys = Vec::new();
            for k in keys {
                out_keys.push(Frame::BulkString(Some(k)));
            }
            vec![Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from("0"))),
                Frame::Array(Some(out_keys)),
            ]))]
        }
        "TYPE" => {
            if args.len() != 1 {
                return vec![wrong_arity("type")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![Frame::SimpleString("none".into())],
                Some(e) => {
                    let t = match e.value {
                        Value::String(_) => "string",
                        Value::List(_) => "list",
                        Value::Set(_) => "set",
                        Value::Hash(_) => "hash",
                        Value::ZSet { .. } => "zset",
                    };
                    vec![Frame::SimpleString(t.into())]
                }
            }
        }
        "EXISTS" => {
            if args.is_empty() {
                return vec![wrong_arity("exists")];
            }
            let db = db_of(state, ctx);
            let mut c = 0i64;
            for k in args {
                if db.exists(k) {
                    c += 1;
                }
            }
            vec![int(c)]
        }
        "DEL" | "UNLINK" => {
            if args.is_empty() {
                return vec![wrong_arity("del")];
            }
            let db = db_of(state, ctx);
            let mut c = 0i64;
            for k in args {
                if db.remove(k) {
                    c += 1;
                }
            }
            vec![int(c)]
        }
        "RENAME" => {
            if args.len() != 2 {
                return vec![wrong_arity("rename")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                Some(e) => {
                    db.store.insert(args[1].clone(), e);
                    db.remove(&args[0]);
                    vec![ok()]
                }
                None => vec![err("ERR no such key")],
            }
        }
        "RENAMENX" => {
            if args.len() != 2 {
                return vec![wrong_arity("renamenx")];
            }
            let db = db_of(state, ctx);
            if db.exists(&args[1]) {
                return vec![int(0)];
            }
            match db.get(&args[0]) {
                Some(e) => {
                    db.store.insert(args[1].clone(), e);
                    db.remove(&args[0]);
                    vec![int(1)]
                }
                None => vec![err("ERR no such key")],
            }
        }
        // Strings
        "SET" => cmd_set(state, ctx, args).await,
        "GET" => {
            if args.len() != 1 {
                return vec![wrong_arity("get")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![nil()],
                Some(e) => match e.value {
                    Value::String(b) => vec![Frame::BulkString(Some(b))],
                    _ => vec![err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )],
                },
            }
        }
        "MGET" => {
            let db = db_of(state, ctx);
            let mut out = Vec::with_capacity(args.len());
            for k in args {
                match db.get(k) {
                    None => out.push(Frame::BulkString(None)),
                    Some(e) => match e.value {
                        Value::String(b) => out.push(Frame::BulkString(Some(b))),
                        _ => out.push(err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )),
                    },
                }
            }
            vec![Frame::Array(Some(out))]
        }
        "MSET" => {
            if args.len() < 2 || args.len() % 2 != 0 {
                return vec![wrong_arity("mset")];
            }
            let db = db_of(state, ctx);
            let mut i = 0;
            while i < args.len() {
                db.store.insert(
                    args[i].clone(),
                    crate::db::Entry {
                        value: Value::String(args[i + 1].clone()),
                        expires: None,
                    },
                );
                i += 2;
            }
            vec![ok()]
        }
        "INCR" | "DECR" | "INCRBY" | "DECRBY" => cmd_incr(state, ctx, &name, args).await,
        "INCRBYFLOAT" => {
            if args.len() != 2 {
                return vec![wrong_arity("incrbyfloat")];
            }
            let db = db_of(state, ctx);
            let inc: f64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(v) => v,
                Err(_) => return vec![err("ERR value is not a valid float")],
            };
            // get current
            let cur = match db.get(&args[0]) {
                None => 0.0f64,
                Some(e) => match e.value {
                    Value::String(b) => match String::from_utf8_lossy(&b).parse::<f64>() {
                        Ok(v) => v,
                        Err(_) => return vec![err("ERR value is not a valid float")],
                    },
                    _ => {
                        return vec![err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )];
                    }
                },
            };
            let newv = cur + inc;
            // format like redis: remove trailing zeros
            let s = format_float(newv);
            db.store.insert(
                args[0].clone(),
                crate::db::Entry {
                    value: Value::String(Bytes::from(s.clone())),
                    expires: None,
                },
            );
            vec![Frame::BulkString(Some(Bytes::from(s)))]
        }
        "APPEND" => {
            if args.len() != 2 {
                return vec![wrong_arity("append")];
            }
            let db = db_of(state, ctx);
            let mut entry = db.get(&args[0]);
            let newlen = match &mut entry {
                Some(e) => match &mut e.value {
                    Value::String(b) => {
                        let mut v = b.to_vec();
                        v.extend_from_slice(&args[1]);
                        *b = Bytes::from(v);
                        b.len() as i64
                    }
                    _ => {
                        return vec![err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        )];
                    }
                },
                None => {
                    let b = args[1].clone();
                    let l = b.len() as i64;
                    db.store.insert(
                        args[0].clone(),
                        crate::db::Entry {
                            value: Value::String(b),
                            expires: None,
                        },
                    );
                    l
                }
            };
            if let Some(e) = entry {
                // need to store back with expiry preserved; db.get returned clone; re-insert
                // Actually we mutated clone; need update store. Let's handle separately above? We already inserted for None.
                // For Some, update:
                db.store.insert(args[0].clone(), e);
            }
            vec![int(newlen)]
        }
        "STRLEN" => {
            if args.len() != 1 {
                return vec![wrong_arity("strlen")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::String(b) => vec![int(b.len() as i64)],
                    _ => vec![err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )],
                },
            }
        }
        "GETSET" => {
            if args.len() != 2 {
                return vec![wrong_arity("getset")];
            }
            let db = db_of(state, ctx);
            let old = db.get(&args[0]).map(|e| match e.value {
                Value::String(b) => Frame::BulkString(Some(b)),
                _ => nil(),
            });
            db.store.insert(
                args[0].clone(),
                crate::db::Entry {
                    value: Value::String(args[1].clone()),
                    expires: None,
                },
            );
            vec![old.unwrap_or_else(nil)]
        }
        "GETDEL" => {
            if args.len() != 1 {
                return vec![wrong_arity("getdel")];
            }
            let db = db_of(state, ctx);
            let old = db.get(&args[0]);
            db.remove(&args[0]);
            match old {
                None => vec![nil()],
                Some(e) => match e.value {
                    Value::String(b) => vec![Frame::BulkString(Some(b))],
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "GETEX" => {
            // GETEX key [EX s|PX ms|EXAT ts|PXAT tsms|PERSIST]
            if args.is_empty() {
                return vec![wrong_arity("getex")];
            }
            let db = db_of(state, ctx);
            let entry = db.get(&args[0]);
            // parse options
            let mut new_exp: Option<Option<Instant>> = None; // Some(None)=persist, Some(Some(t))=set
            let mut i = 1;
            while i < args.len() {
                let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
                match opt.as_str() {
                    "EX" => {
                        let s: i64 = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                        new_exp = Some(Some(
                            Instant::now() + std::time::Duration::from_secs(s as u64),
                        ));
                        i += 2;
                    }
                    "PX" => {
                        let ms: i64 = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                        new_exp = Some(Some(
                            Instant::now() + std::time::Duration::from_millis(ms as u64),
                        ));
                        i += 2;
                    }
                    "PERSIST" => {
                        new_exp = Some(None);
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }
            let val = entry.as_ref().and_then(|e| match &e.value {
                Value::String(b) => Some(b.clone()),
                _ => None,
            });
            if let Some(e) = entry {
                let mut ne = e.clone();
                if let Some(x) = new_exp {
                    ne.expires = x;
                }
                db.store.insert(args[0].clone(), ne);
            }
            vec![match val {
                Some(b) => Frame::BulkString(Some(b)),
                None => nil(),
            }]
        }
        "SETNX" => {
            if args.len() != 2 {
                return vec![wrong_arity("setnx")];
            }
            let db = db_of(state, ctx);
            if db.exists(&args[0]) {
                vec![int(0)]
            } else {
                db.store.insert(
                    args[0].clone(),
                    crate::db::Entry {
                        value: Value::String(args[1].clone()),
                        expires: None,
                    },
                );
                vec![int(1)]
            }
        }
        "SETEX" | "PSETEX" => {
            if args.len() != 3 {
                return vec![wrong_arity("setex")];
            }
            let db = db_of(state, ctx);
            let secs_or_ms: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            let dur = if name == "SETEX" {
                std::time::Duration::from_secs(secs_or_ms as u64)
            } else {
                std::time::Duration::from_millis(secs_or_ms as u64)
            };
            db.store.insert(
                args[0].clone(),
                crate::db::Entry {
                    value: Value::String(args[2].clone()),
                    expires: Some(Instant::now() + dur),
                },
            );
            vec![ok()]
        }
        "SET" => cmd_set(state, ctx, args).await,

        // TTL
        "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" => {
            cmd_expire(state, ctx, &name, args).await
        }
        "TTL" | "PTTL" => {
            if args.len() != 1 {
                return vec![wrong_arity("ttl")];
            }
            let db = db_of(state, ctx);
            let ms = db.ttl_ms(&args[0]);
            let v = if name == "TTL" {
                if ms < 0 { ms / 1000 } else { ms / 1000 }
            } else {
                ms
            };
            // for non-existent -2, no ttl -1 (already in ms units)
            let out = if name == "TTL" {
                if ms == -2 {
                    -2
                } else if ms == -1 {
                    -1
                } else {
                    ms / 1000
                }
            } else {
                ms
            };
            vec![int(out)]
        }
        "PERSIST" => {
            if args.len() != 1 {
                return vec![wrong_arity("persist")];
            }
            let db = db_of(state, ctx);
            vec![int(if db.persist(&args[0]) { 1 } else { 0 })]
        }

        // Lists
        "LPUSH" | "RPUSH" | "LPUSHX" | "RPUSHX" => cmd_list_push(state, ctx, &name, args).await,
        "LPOP" | "RPOP" => cmd_list_pop(state, ctx, &name, args).await,
        "LLEN" => {
            if args.len() != 1 {
                return vec![wrong_arity("llen")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::List(l) => vec![int(l.len() as i64)],
                    _ => vec![err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )],
                },
            }
        }
        "LRANGE" => {
            if args.len() != 3 {
                return vec![wrong_arity("lrange")];
            }
            let db = db_of(state, ctx);
            let start: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            let stop: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(-1);
            match db.get(&args[0]) {
                None => vec![Frame::Array(Some(vec![]))],
                Some(e) => match e.value {
                    Value::List(l) => {
                        let len = l.len() as i64;
                        let s = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start.min(len)
                        };
                        let mut st = if stop < 0 {
                            (len + stop).max(-1)
                        } else {
                            stop.min(len - 1)
                        };
                        if st < s {
                            vec![Frame::Array(Some(vec![]))]
                        } else {
                            let mut out = Vec::new();
                            for i in s..=st {
                                if let Some(v) = l.get(i as usize) {
                                    out.push(Frame::BulkString(Some(v.clone())));
                                }
                            }
                            vec![Frame::Array(Some(out))]
                        }
                    }
                    _ => vec![err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )],
                },
            }
        }
        "LINDEX" => {
            if args.len() != 2 {
                return vec![wrong_arity("lindex")];
            }
            let db = db_of(state, ctx);
            let idx: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            match db.get(&args[0]) {
                None => vec![nil()],
                Some(e) => match e.value {
                    Value::List(l) => {
                        let len = l.len() as i64;
                        let i = if idx < 0 { len + idx } else { idx };
                        if i < 0 || i >= len {
                            vec![nil()]
                        } else {
                            vec![Frame::BulkString(Some(l[i as usize].clone()))]
                        }
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "LSET" => {
            if args.len() != 3 {
                return vec![wrong_arity("lset")];
            }
            let db = db_of(state, ctx);
            let idx: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            match db.get(&args[0]) {
                None => vec![err("ERR no such key")],
                Some(mut e) => match &mut e.value {
                    Value::List(l) => {
                        let len = l.len() as i64;
                        let i = if idx < 0 { len + idx } else { idx };
                        if i < 0 || i >= len {
                            return vec![err("ERR index out of range")];
                        }
                        l[i as usize] = args[2].clone();
                        db.store.insert(args[0].clone(), e);
                        vec![ok()]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "LREM" => {
            if args.len() != 3 {
                return vec![wrong_arity("lrem")];
            }
            let db = db_of(state, ctx);
            let count: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            let val = &args[2];
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(mut e) => match &mut e.value {
                    Value::List(l) => {
                        let mut removed = 0i64;
                        if count == 0 {
                            // remove all
                            let mut i = 0;
                            while i < l.len() {
                                if l[i] == *val {
                                    l.remove(i);
                                    removed += 1;
                                } else {
                                    i += 1;
                                }
                            }
                        } else if count > 0 {
                            let mut i = 0;
                            let mut rem = count;
                            while i < l.len() && rem > 0 {
                                if l[i] == *val {
                                    l.remove(i);
                                    removed += 1;
                                    rem -= 1;
                                } else {
                                    i += 1;
                                }
                            }
                        } else {
                            // from tail
                            let mut rem = -count;
                            let mut i = l.len();
                            while i > 0 && rem > 0 {
                                i -= 1;
                                if l[i] == *val {
                                    l.remove(i);
                                    removed += 1;
                                    rem -= 1;
                                }
                            }
                        }
                        db.store.insert(args[0].clone(), e);
                        vec![int(removed)]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "LTRIM" => {
            if args.len() != 3 {
                return vec![wrong_arity("ltrim")];
            }
            let db = db_of(state, ctx);
            let start: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
            let stop: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(-1);
            match db.get(&args[0]) {
                None => vec![ok()],
                Some(mut e) => match &mut e.value {
                    Value::List(l) => {
                        let len = l.len() as i64;
                        let s = if start < 0 {
                            (len + start).max(0)
                        } else {
                            start.min(len)
                        } as usize;
                        let mut st = if stop < 0 {
                            (len + stop).max(-1)
                        } else {
                            stop.min(len - 1)
                        };
                        if st < s as i64 {
                            st = s as i64 - 1;
                        }
                        let new: VecDeque<Bytes> = l
                            .iter()
                            .skip(s)
                            .take((st as usize + 1).saturating_sub(s))
                            .cloned()
                            .collect();
                        *l = new;
                        db.store.insert(args[0].clone(), e);
                        vec![ok()]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }

        // Sets
        "SADD" => {
            if args.len() < 2 {
                return vec![wrong_arity("sadd")];
            }
            let db = db_of(state, ctx);
            let mut added = 0i64;
            if let Some(mut e) = db.store.get_mut(&args[0]) {
                if e.is_expired() {
                    drop(e);
                    db.store.remove(&args[0]);
                } else {
                    match &mut e.value {
                        Value::Set(s) => {
                            for m in &args[1..] {
                                if s.insert(m.clone()) {
                                    added += 1;
                                }
                            }
                            return vec![int(added)];
                        }
                        _ => return vec![err("WRONGTYPE...")],
                    }
                }
            }
            // create
            let mut set = HashSet::new();
            for m in &args[1..] {
                if set.insert(m.clone()) {
                    added += 1;
                }
            }
            db.store.insert(
                args[0].clone(),
                crate::db::Entry {
                    value: Value::Set(set),
                    expires: None,
                },
            );
            vec![int(added)]
        }
        "SREM" => {
            if args.len() < 2 {
                return vec![wrong_arity("srem")];
            }
            let db = db_of(state, ctx);
            let mut removed = 0i64;
            if let Some(mut e) = db.get(&args[0]) {
                if let Value::Set(s) = &mut e.value {
                    for m in &args[1..] {
                        if s.remove(m) {
                            removed += 1;
                        }
                    }
                    db.store.insert(args[0].clone(), e);
                } else {
                    return vec![err("WRONGTYPE...")];
                }
            }
            vec![int(removed)]
        }
        "SMEMBERS" => {
            if args.len() != 1 {
                return vec![wrong_arity("smembers")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![Frame::Array(Some(vec![]))],
                Some(e) => match e.value {
                    Value::Set(s) => {
                        let out: Vec<Frame> = s
                            .iter()
                            .map(|m| Frame::BulkString(Some(m.clone())))
                            .collect();
                        vec![Frame::Array(Some(out))]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "SCARD" => {
            if args.len() != 1 {
                return vec![wrong_arity("scard")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::Set(s) => vec![int(s.len() as i64)],
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "SISMEMBER" => {
            if args.len() != 2 {
                return vec![wrong_arity("sismember")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::Set(s) => vec![int(if s.contains(&args[1]) { 1 } else { 0 })],
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "SPOP" => {
            if args.is_empty() || args.len() > 2 {
                return vec![wrong_arity("spop")];
            }
            let db = db_of(state, ctx);
            let count: i64 = args
                .get(1)
                .and_then(|b| String::from_utf8_lossy(b).parse().ok())
                .unwrap_or(1);
            // single vs multi
            let single = args.len() == 1;
            match db.get(&args[0]) {
                None => {
                    if single {
                        vec![nil()]
                    } else {
                        vec![Frame::Array(Some(vec![]))]
                    }
                }
                Some(mut e) => match &mut e.value {
                    Value::Set(s) => {
                        let mut popped = Vec::new();
                        for _ in 0..count {
                            if let Some(m) = s.iter().next().cloned() {
                                s.remove(&m);
                                popped.push(m);
                            } else {
                                break;
                            }
                        }
                        db.store.insert(args[0].clone(), e);
                        if single {
                            if popped.is_empty() {
                                vec![nil()]
                            } else {
                                vec![Frame::BulkString(Some(popped[0].clone()))]
                            }
                        } else {
                            let out: Vec<Frame> = popped
                                .into_iter()
                                .map(|m| Frame::BulkString(Some(m)))
                                .collect();
                            vec![Frame::Array(Some(out))]
                        }
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }

        // Hashes
        "HSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return vec![wrong_arity("hset")];
            }
            let db = db_of(state, ctx);
            let mut added = 0i64;
            if let Some(mut e) = db.store.get_mut(&args[0]) {
                if e.is_expired() {
                    drop(e);
                    db.store.remove(&args[0]);
                } else {
                    match &mut e.value {
                        Value::Hash(h) => {
                            let mut i = 1;
                            while i < args.len() {
                                if h.insert(args[i].clone(), args[i + 1].clone()).is_none() {
                                    added += 1;
                                }
                                i += 2;
                            }
                            return vec![int(added)];
                        }
                        _ => return vec![err("WRONGTYPE...")],
                    }
                }
            }
            let mut h = HashMap::new();
            let mut i = 1;
            while i < args.len() {
                if h.insert(args[i].clone(), args[i + 1].clone()).is_none() {
                    added += 1;
                }
                i += 2;
            }
            db.store.insert(
                args[0].clone(),
                crate::db::Entry {
                    value: Value::Hash(h),
                    expires: None,
                },
            );
            vec![int(added)]
        }
        "HGET" => {
            if args.len() != 2 {
                return vec![wrong_arity("hget")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![nil()],
                Some(e) => match e.value {
                    Value::Hash(h) => match h.get(&args[1]) {
                        Some(v) => vec![Frame::BulkString(Some(v.clone()))],
                        None => vec![nil()],
                    },
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "HMGET" => {
            if args.len() < 2 {
                return vec![wrong_arity("hmget")];
            }
            let db = db_of(state, ctx);
            let mut out = Vec::new();
            if let Some(e) = db.get(&args[0]) {
                if let Value::Hash(h) = e.value {
                    for f in &args[1..] {
                        match h.get(f) {
                            Some(v) => out.push(Frame::BulkString(Some(v.clone()))),
                            None => out.push(nil()),
                        }
                    }
                } else {
                    return vec![err("WRONGTYPE...")];
                }
            } else {
                for _ in &args[1..] {
                    out.push(nil());
                }
            }
            vec![Frame::Array(Some(out))]
        }
        "HGETALL" => {
            if args.len() != 1 {
                return vec![wrong_arity("hgetall")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![Frame::Array(Some(vec![]))],
                Some(e) => match e.value {
                    Value::Hash(h) => {
                        let mut out = Vec::new();
                        for (k, v) in h.iter() {
                            out.push(Frame::BulkString(Some(k.clone())));
                            out.push(Frame::BulkString(Some(v.clone())));
                        }
                        vec![Frame::Array(Some(out))]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "HDEL" => {
            if args.len() < 2 {
                return vec![wrong_arity("hdel")];
            }
            let db = db_of(state, ctx);
            let mut removed = 0i64;
            if let Some(mut e) = db.get(&args[0]) {
                if let Value::Hash(h) = &mut e.value {
                    for f in &args[1..] {
                        if h.remove(f).is_some() {
                            removed += 1;
                        }
                    }
                    db.store.insert(args[0].clone(), e);
                } else {
                    return vec![err("WRONGTYPE...")];
                }
            }
            vec![int(removed)]
        }
        "HEXISTS" => {
            if args.len() != 2 {
                return vec![wrong_arity("hexists")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::Hash(h) => vec![int(if h.contains_key(&args[1]) { 1 } else { 0 })],
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "HLEN" => {
            if args.len() != 1 {
                return vec![wrong_arity("hlen")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::Hash(h) => vec![int(h.len() as i64)],
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "HKEYS" | "HVALS" => {
            if args.len() != 1 {
                return vec![wrong_arity("hkeys")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![Frame::Array(Some(vec![]))],
                Some(e) => match e.value {
                    Value::Hash(h) => {
                        let out: Vec<Frame> = if name == "HKEYS" {
                            h.keys()
                                .map(|k| Frame::BulkString(Some(k.clone())))
                                .collect()
                        } else {
                            h.values()
                                .map(|v| Frame::BulkString(Some(v.clone())))
                                .collect()
                        };
                        vec![Frame::Array(Some(out))]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "HINCRBY" => {
            if args.len() != 3 {
                return vec![wrong_arity("hincrby")];
            }
            let db = db_of(state, ctx);
            let inc: i64 = String::from_utf8_lossy(&args[2]).parse().unwrap_or(0);
            match db.get(&args[0]) {
                None => {
                    let mut h = HashMap::new();
                    h.insert(args[1].clone(), Bytes::from(inc.to_string()));
                    db.store.insert(
                        args[0].clone(),
                        crate::db::Entry {
                            value: Value::Hash(h),
                            expires: None,
                        },
                    );
                    vec![int(inc)]
                }
                Some(mut e) => match &mut e.value {
                    Value::Hash(h) => {
                        let cur: i64 = h
                            .get(&args[1])
                            .and_then(|b| String::from_utf8_lossy(b).parse().ok())
                            .unwrap_or(0);
                        let newv = cur + inc;
                        h.insert(args[1].clone(), Bytes::from(newv.to_string()));
                        db.store.insert(args[0].clone(), e);
                        vec![int(newv)]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }

        // Sorted sets
        "ZADD" => cmd_zadd(state, ctx, args).await,
        "ZREM" => {
            if args.len() < 2 {
                return vec![wrong_arity("zrem")];
            }
            let db = db_of(state, ctx);
            let mut removed = 0i64;
            if let Some(mut e) = db.get(&args[0]) {
                if let Value::ZSet { scores, ordered } = &mut e.value {
                    for m in &args[1..] {
                        if let Some(sc) = scores.remove(m) {
                            ordered.remove(&(ordered_float::OrderedFloat(sc), m.clone()));
                            removed += 1;
                        }
                    }
                    db.store.insert(args[0].clone(), e);
                } else {
                    return vec![err("WRONGTYPE...")];
                }
            }
            vec![int(removed)]
        }
        "ZSCORE" => {
            if args.len() != 2 {
                return vec![wrong_arity("zscore")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![nil()],
                Some(e) => match e.value {
                    Value::ZSet { scores, .. } => match scores.get(&args[1]) {
                        Some(sc) => vec![Frame::BulkString(Some(Bytes::from(format_float(*sc))))],
                        None => vec![nil()],
                    },
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "ZCARD" => {
            if args.len() != 1 {
                return vec![wrong_arity("zcard")];
            }
            let db = db_of(state, ctx);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::ZSet { scores, .. } => vec![int(scores.len() as i64)],
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "ZRANK" | "ZREVRANK" => {
            if args.len() != 2 {
                return vec![wrong_arity("zrank")];
            }
            let db = db_of(state, ctx);
            let rev = name == "ZREVRANK";
            match db.get(&args[0]) {
                None => vec![nil()],
                Some(e) => match e.value {
                    Value::ZSet { ordered, .. } => {
                        let mut rank = 0i64;
                        let mut found = false;
                        let mut r = 0i64;
                        for (_, m) in ordered.iter() {
                            if *m == args[1] {
                                found = true;
                                rank = r;
                                break;
                            }
                            r += 1;
                        }
                        if !found {
                            vec![nil()]
                        } else {
                            let total = ordered.len() as i64;
                            if rev {
                                vec![int(total - 1 - rank)]
                            } else {
                                vec![int(rank)]
                            }
                        }
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" => {
            cmd_zrange(state, ctx, &name, args).await
        }
        "ZCOUNT" => {
            if args.len() != 3 {
                return vec![wrong_arity("zcount")];
            }
            let db = db_of(state, ctx);
            let (min, max, min_excl, max_excl) = parse_score_range(&args[1], &args[2]);
            match db.get(&args[0]) {
                None => vec![int(0)],
                Some(e) => match e.value {
                    Value::ZSet { ordered, .. } => {
                        let mut c = 0i64;
                        for (sc, _) in ordered.iter() {
                            let v = sc.0;
                            if score_in_range(v, min, max, min_excl, max_excl) {
                                c += 1;
                            }
                        }
                        vec![int(c)]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }
        "ZINCRBY" => {
            if args.len() != 3 {
                return vec![wrong_arity("zincrby")];
            }
            let db = db_of(state, ctx);
            let inc: f64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0.0);
            let member = &args[2];
            match db.get(&args[0]) {
                None => {
                    let mut scores = HashMap::new();
                    let mut ordered = BTreeSet::new();
                    scores.insert(member.clone(), inc);
                    ordered.insert((ordered_float::OrderedFloat(inc), member.clone()));
                    db.store.insert(
                        args[0].clone(),
                        crate::db::Entry {
                            value: Value::ZSet { scores, ordered },
                            expires: None,
                        },
                    );
                    vec![Frame::BulkString(Some(Bytes::from(format_float(inc))))]
                }
                Some(mut e) => match &mut e.value {
                    Value::ZSet { scores, ordered } => {
                        if let Some(sc) = scores.get(member) {
                            ordered.remove(&(ordered_float::OrderedFloat(*sc), member.clone()));
                        }
                        let cur = scores.get(member).copied().unwrap_or(0.0);
                        let newv = cur + inc;
                        scores.insert(member.clone(), newv);
                        ordered.insert((ordered_float::OrderedFloat(newv), member.clone()));
                        db.store.insert(args[0].clone(), e);
                        vec![Frame::BulkString(Some(Bytes::from(format_float(newv))))]
                    }
                    _ => vec![err("WRONGTYPE...")],
                },
            }
        }

        // Pub/Sub
        "PUBLISH" => {
            if args.len() != 2 {
                return vec![wrong_arity("publish")];
            }
            let n = state.pubsub.publish(&args[0], args[1].clone());
            vec![int(n as i64)]
        }
        "SUBSCRIBE" => {
            // returns confirmations; connection enters subscribed mode (handled by server)
            ctx.in_subscribed_mode = true;
            let mut out = Vec::new();
            for ch in args {
                ctx.subscribed_channels.push(ch.clone());
                let tx = state.pubsub.get_or_create(ch);
                // store receiver? We'll handle in server loop. Here just confirmation.
                let _ = tx;
                out.push(Frame::Array(Some(vec![
                    Frame::BulkString(Some(Bytes::from("subscribe"))),
                    Frame::BulkString(Some(ch.clone())),
                    Frame::Integer(ctx.subscribed_channels.len() as i64),
                ])));
            }
            out
        }
        "UNSUBSCRIBE" => {
            let channels: Vec<Bytes> = if args.is_empty() {
                ctx.subscribed_channels.clone()
            } else {
                args.to_vec()
            };
            let mut out = Vec::new();
            for ch in channels {
                ctx.subscribed_channels.retain(|c| c != &ch);
                out.push(Frame::Array(Some(vec![
                    Frame::BulkString(Some(Bytes::from("unsubscribe"))),
                    Frame::BulkString(Some(ch)),
                    Frame::Integer(ctx.subscribed_channels.len() as i64),
                ])));
            }
            if ctx.subscribed_channels.is_empty() {
                ctx.in_subscribed_mode = false;
            }
            out
        }
        "PSUBSCRIBE" | "PUNSUBSCRIBE" => {
            vec![err("ERR not implemented")]
        }

        // Transactions
        "MULTI" => {
            ctx.multi = Some(Vec::new());
            vec![ok()]
        }
        "EXEC" => {
            if ctx.multi.is_none() {
                return vec![err("ERR EXEC without MULTI")];
            }
            let queued = ctx.multi.take().unwrap();
            let mut out = Vec::new();
            for q in queued {
                // q is Array of bulk strings
                let r = Box::pin(dispatch(state, ctx, q)).await;
                // dispatch may return multiple; flatten? Redis EXEC returns array of replies.
                if r.len() == 1 {
                    out.push(r.into_iter().next().unwrap());
                } else {
                    out.push(Frame::Array(Some(r)));
                }
            }
            vec![Frame::Array(Some(out))]
        }
        "DISCARD" => {
            if ctx.multi.is_none() {
                return vec![err("ERR DISCARD without MULTI")];
            }
            ctx.multi = None;
            vec![ok()]
        }
        "WATCH" | "UNWATCH" => {
            vec![ok()]
        }

        // Server
        "INFO" => {
            let uptime = state.start_time.elapsed().as_secs();
            let info = format!(
                "# Server\r\nredis_version:7.2.0\r\nredis_mode:standalone\r\nos:macos\r\narch_bits:64\r\nuptime_in_seconds:{}\r\n",
                uptime
            );
            vec![Frame::BulkString(Some(Bytes::from(info)))]
        }
        "CONFIG" => {
            if args.is_empty() {
                return vec![wrong_arity("config")];
            }
            let sub = String::from_utf8_lossy(&args[0]).to_uppercase();
            match sub.as_str() {
                "GET" => {
                    let pat = args
                        .get(1)
                        .map(|b| String::from_utf8_lossy(b).to_string())
                        .unwrap_or_else(|| "*".into());
                    let cfg = state.config.read();
                    let mut out = Vec::new();
                    for (k, v) in cfg.iter() {
                        if glob_match(&pat, k) {
                            out.push(Frame::BulkString(Some(Bytes::from(k.clone()))));
                            out.push(Frame::BulkString(Some(Bytes::from(v.clone()))));
                        }
                    }
                    vec![Frame::Array(Some(out))]
                }
                "SET" => {
                    if args.len() < 3 {
                        return vec![wrong_arity("config")];
                    }
                    let k = String::from_utf8_lossy(&args[1]).to_string();
                    let v = String::from_utf8_lossy(&args[2]).to_string();
                    state.config.write().insert(k, v);
                    vec![ok()]
                }
                "RESETSTAT" => vec![ok()],
                _ => vec![err("ERR unknown subcommand")],
            }
        }
        "CLIENT" => {
            let sub = args
                .get(0)
                .map(|b| String::from_utf8_lossy(b).to_uppercase())
                .unwrap_or_default();
            match sub.as_str() {
                "SETNAME" => {
                    if args.len() != 2 {
                        return vec![wrong_arity("client")];
                    }
                    ctx.name = String::from_utf8_lossy(&args[1]).to_string();
                    vec![ok()]
                }
                "GETNAME" => vec![Frame::BulkString(if ctx.name.is_empty() {
                    None
                } else {
                    Some(Bytes::from(ctx.name.clone()))
                })],
                "ID" => vec![int(ctx.id as i64)],
                "LIST" => vec![Frame::BulkString(Some(Bytes::from(format!(
                    "id={} name={}\n",
                    ctx.id, ctx.name
                ))))],
                "INFO" => vec![Frame::BulkString(Some(Bytes::from(format!(
                    "id={} name={}\n",
                    ctx.id, ctx.name
                ))))],
                _ => vec![ok()],
            }
        }
        "COMMAND" => {
            // return empty array or count
            if args
                .get(0)
                .map(|b| String::from_utf8_lossy(b).to_uppercase())
                == Some("COUNT".to_string())
            {
                vec![int(200)]
            } else {
                // return array of command names minimal
                vec![Frame::Array(Some(vec![]))]
            }
        }
        "TIME" => {
            let now = db::unix_ms_now();
            let sec = now / 1000;
            let us = (now % 1000) * 1000;
            vec![Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from(sec.to_string()))),
                Frame::BulkString(Some(Bytes::from(us.to_string()))),
            ]))]
        }
        "RANDOMKEY" => {
            let db = db_of(state, ctx);
            match db.random_key() {
                Some(k) => vec![Frame::BulkString(Some(k))],
                None => vec![nil()],
            }
        }
        // ACL
        "ACL" => {
            vec![Frame::SimpleString("OK".into())]
        }
        "BGSAVE" | "SAVE" => {
            vec![ok()]
        }
        "LASTSAVE" => vec![int(0)],
        "WAIT" => vec![int(0)],

        _ => vec![Frame::Error(format!("ERR unknown command '{}'", name))],
    }
}

// ---------- helpers ----------

async fn cmd_set(state: &ServerState, ctx: &ConnContext, args: &[Bytes]) -> Vec<Frame> {
    // SET key value [EX s] [PX ms] [EXAT ts] [PXAT tsms] [NX|XX] [GET] [KEEPTTL]
    if args.len() < 2 {
        return vec![wrong_arity("set")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    let val = &args[1];
    // parse options
    let mut ex: Option<Instant> = None;
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut keepttl = false;
    let mut i = 2;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "EX" => {
                let s: i64 = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                ex = Some(Instant::now() + std::time::Duration::from_secs(s as u64));
                i += 2;
            }
            "PX" => {
                let ms: i64 = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                ex = Some(Instant::now() + std::time::Duration::from_millis(ms as u64));
                i += 2;
            }
            "EXAT" => {
                let ts: i64 = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                let now = db::unix_ms_now() as i64;
                let delta_ms = (ts * 1000) - now;
                ex =
                    Some(Instant::now() + std::time::Duration::from_millis(delta_ms.max(0) as u64));
                i += 2;
            }
            "PXAT" => {
                let ts: i64 = String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                let now = db::unix_ms_now() as i64;
                let delta_ms = ts - now;
                ex =
                    Some(Instant::now() + std::time::Duration::from_millis(delta_ms.max(0) as u64));
                i += 2;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GET" => {
                get = true;
                i += 1;
            }
            "KEEPTTL" => {
                keepttl = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    let exists = db.exists(key);
    if nx && exists {
        if get {
            // return old value
            let old = db.get(key).and_then(|e| match e.value {
                Value::String(b) => Some(b),
                _ => None,
            });
            return vec![match old {
                Some(b) => Frame::BulkString(Some(b)),
                None => nil(),
            }];
        }
        return vec![nil()];
    }
    if xx && !exists {
        if get {
            return vec![nil()];
        }
        return vec![nil()];
    }

    let old_val = if get {
        db.get(key).and_then(|e| match e.value {
            Value::String(b) => Some(b),
            _ => None,
        })
    } else {
        None
    };

    // keepttl: preserve expiry if not explicitly set
    let expires = if keepttl && ex.is_none() {
        db.get(key).and_then(|e| e.expires)
    } else {
        ex
    };

    if let Some(mut e) = db.store.get_mut(key) {
        e.value = Value::String(val.clone());
        e.expires = expires;
    } else {
        db.store.insert(
            key.clone(),
            crate::db::Entry {
                value: Value::String(val.clone()),
                expires,
            },
        );
    }

    if get {
        vec![match old_val {
            Some(b) => Frame::BulkString(Some(b)),
            None => nil(),
        }]
    } else {
        vec![ok()]
    }
}

async fn cmd_expire(
    state: &ServerState,
    ctx: &ConnContext,
    name: &str,
    args: &[Bytes],
) -> Vec<Frame> {
    if args.len() < 2 {
        return vec![wrong_arity("expire")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    if !db.exists(key) {
        return vec![int(0)];
    }
    let n: i64 = String::from_utf8_lossy(&args[1]).parse().unwrap_or(0);
    let when = match name {
        "EXPIRE" => Instant::now() + std::time::Duration::from_secs(n as u64),
        "PEXPIRE" => Instant::now() + std::time::Duration::from_millis(n as u64),
        "EXPIREAT" => {
            // n is unix seconds
            let now = db::unix_ms_now() as i64;
            let delta = (n * 1000) - now;
            Instant::now() + std::time::Duration::from_millis(delta.max(0) as u64)
        }
        "PEXPIREAT" => {
            let now = db::unix_ms_now() as i64;
            let delta = n - now;
            Instant::now() + std::time::Duration::from_millis(delta.max(0) as u64)
        }
        _ => Instant::now(),
    };
    // NX/XX/GT/LT options (redis 7) - skip for simplicity but parse
    let ok = db.set_expiry(key, when);
    vec![int(if ok { 1 } else { 0 })]
}

async fn cmd_incr(
    state: &ServerState,
    ctx: &ConnContext,
    name: &str,
    args: &[Bytes],
) -> Vec<Frame> {
    if args.is_empty() {
        return vec![wrong_arity("incr")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    let delta: i64 = match name {
        "INCR" => 1,
        "DECR" => -1,
        "INCRBY" => args
            .get(1)
            .and_then(|b| String::from_utf8_lossy(b).parse().ok())
            .unwrap_or(0),
        "DECRBY" => {
            -(args
                .get(1)
                .and_then(|b| String::from_utf8_lossy(b).parse().ok())
                .unwrap_or(0))
        }
        _ => 1,
    };
    // mutate in place
    if let Some(mut e) = db.store.get_mut(key) {
        if e.is_expired() {
            drop(e);
            db.store.remove(key);
        } else {
            match &mut e.value {
                Value::String(b) => {
                    if String::from_utf8_lossy(b).parse::<i64>().is_err() {
                        return vec![err("ERR value is not an integer or out of range")];
                    }
                    let cur: i64 = String::from_utf8_lossy(b).parse().unwrap_or(0);
                    let newv = cur + delta;
                    *b = Bytes::from(newv.to_string());
                    return vec![int(newv)];
                }
                _ => {
                    return vec![err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )]
                }
            }
        }
    }
    db.store.insert(
        key.clone(),
        crate::db::Entry {
            value: Value::String(Bytes::from(delta.to_string())),
            expires: None,
        },
    );
    vec![int(delta)]
}

async fn cmd_list_push(
    state: &ServerState,
    ctx: &ConnContext,
    name: &str,
    args: &[Bytes],
) -> Vec<Frame> {
    if args.len() < 2 {
        return vec![wrong_arity("push")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    let vals = &args[1..];
    let push_left = name.starts_with('L') || name.starts_with('l');
    let x = name.ends_with('X') || name.ends_with('x');

    if x && !db.exists(key) {
        return vec![int(0)];
    }

    // mutate in place to avoid O(n) clone
    if let Some(mut e) = db.store.get_mut(key) {
        // expired?
        if e.is_expired() {
            drop(e);
            db.store.remove(key); /* fall through */
        } else {
            match &mut e.value {
                Value::List(dq) => {
                    for v in vals {
                        if push_left {
                            dq.push_front(v.clone());
                        } else {
                            dq.push_back(v.clone());
                        }
                    }
                    let l = dq.len() as i64;
                    return vec![int(l)];
                }
                _ => {
                    return vec![err(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    )];
                }
            }
        }
    }

    // create new
    let mut dq = VecDeque::new();
    for v in vals {
        if push_left {
            dq.push_front(v.clone());
        } else {
            dq.push_back(v.clone());
        }
    }
    let l = dq.len() as i64;
    db.store.insert(
        key.clone(),
        crate::db::Entry {
            value: Value::List(dq),
            expires: None,
        },
    );
    vec![int(l)]
}

async fn cmd_list_pop(
    state: &ServerState,
    ctx: &ConnContext,
    name: &str,
    args: &[Bytes],
) -> Vec<Frame> {
    if args.is_empty() {
        return vec![wrong_arity("pop")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    let count: usize = args
        .get(1)
        .and_then(|b| String::from_utf8_lossy(b).parse().ok())
        .unwrap_or(1);
    let multi = args.len() >= 2;
    let left = name.starts_with('L') || name.starts_with('l');

    // mutate in place
    let mut popped = Vec::new();
    let mut wrongtype = false;
    let mut missing = false;
    if let Some(mut e) = db.store.get_mut(key) {
        if e.is_expired() {
            drop(e);
            db.store.remove(key);
            missing = true;
        } else {
            match &mut e.value {
                Value::List(dq) => {
                    for _ in 0..count {
                        let v = if left { dq.pop_front() } else { dq.pop_back() };
                        match v {
                            Some(x) => popped.push(x),
                            None => break,
                        }
                    }
                    let empty = dq.is_empty();
                    drop(e);
                    if empty {
                        db.store.remove(key);
                    }
                }
                _ => {
                    wrongtype = true;
                }
            }
        }
    } else {
        missing = true;
    }
    if wrongtype {
        return vec![err(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        )];
    }
    if missing && popped.is_empty() {
        if multi {
            return vec![Frame::Array(Some(vec![]))];
        } else {
            return vec![nil()];
        }
    }
    if multi {
        let out: Vec<Frame> = popped
            .into_iter()
            .map(|x| Frame::BulkString(Some(x)))
            .collect();
        vec![Frame::Array(Some(out))]
    } else {
        if popped.is_empty() {
            vec![nil()]
        } else {
            vec![Frame::BulkString(Some(popped[0].clone()))]
        }
    }
}

async fn cmd_zadd(state: &ServerState, ctx: &ConnContext, args: &[Bytes]) -> Vec<Frame> {
    if args.len() < 3 {
        return vec![wrong_arity("zadd")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    // parse options: NX/XX/GT/LT/CH/INCR
    let mut i = 1;
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut incr = false;
    // skip options
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            "INCR" => {
                incr = true;
                i += 1;
            }
            "GT" | "LT" => {
                i += 1;
            }
            _ => break,
        }
    }
    // remaining must be score member pairs
    let pairs = &args[i..];
    if pairs.len() % 2 != 0 {
        return vec![err("ERR syntax error")];
    }

    let mut added = 0i64;
    let mut changed = 0i64;

    // expired check
    if db.store.contains_key(key) {
        if let Some(e) = db.store.get(key) {
            if e.is_expired() {
                drop(e);
                db.store.remove(key);
            }
        }
    }
    // Determine if exists
    let exists = db.store.contains_key(key);
    // type check
    if exists {
        if let Some(e) = db.store.get(key) {
            if !matches!(e.value, Value::ZSet { .. }) {
                return vec![err(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )];
            }
        }
    }

    // We'll mutate in place if exists, else build new then insert.
    let mut new_score_for_incr: Option<f64> = None;
    let mut j = 0;

    if exists {
        if let Some(mut e) = db.store.get_mut(key) {
            if let Value::ZSet { scores, ordered } = &mut e.value {
                while j < pairs.len() {
                    let score: f64 = match String::from_utf8_lossy(&pairs[j]).parse() {
                        Ok(v) => v,
                        Err(_) => return vec![err("ERR value is not a valid float")],
                    };
                    let member = pairs[j + 1].clone();
                    let old = scores.get(&member).copied();
                    if incr && pairs.len() == 2 {
                        let cur = old.unwrap_or(0.0);
                        let nv = cur + score;
                        if let Some(s) = old {
                            ordered.remove(&(ordered_float::OrderedFloat(s), member.clone()));
                        }
                        scores.insert(member.clone(), nv);
                        ordered.insert((ordered_float::OrderedFloat(nv), member.clone()));
                        new_score_for_incr = Some(nv);
                        break;
                    }
                    if nx && old.is_some() {
                        j += 2;
                        continue;
                    }
                    if xx && old.is_none() {
                        j += 2;
                        continue;
                    }
                    if old.is_some() {
                        let os = old.unwrap();
                        if os != score {
                            ordered.remove(&(ordered_float::OrderedFloat(os), member.clone()));
                            ordered.insert((ordered_float::OrderedFloat(score), member.clone()));
                            scores.insert(member.clone(), score);
                            changed += 1;
                        }
                    } else {
                        scores.insert(member.clone(), score);
                        ordered.insert((ordered_float::OrderedFloat(score), member.clone()));
                        added += 1;
                        changed += 1;
                    }
                    j += 2;
                }
            }
        }
    } else {
        // build new
        let mut scores = HashMap::new();
        let mut ordered = BTreeSet::new();
        while j < pairs.len() {
            let score: f64 = match String::from_utf8_lossy(&pairs[j]).parse() {
                Ok(v) => v,
                Err(_) => return vec![err("ERR value is not a valid float")],
            };
            let member = pairs[j + 1].clone();
            if incr && pairs.len() == 2 {
                let nv = score;
                scores.insert(member.clone(), nv);
                ordered.insert((ordered_float::OrderedFloat(nv), member.clone()));
                new_score_for_incr = Some(nv);
                added = 1;
                changed = 1;
                break;
            }
            if nx && false {}
            scores.insert(member.clone(), score);
            ordered.insert((ordered_float::OrderedFloat(score), member.clone()));
            added += 1;
            changed += 1;
            j += 2;
        }
        db.store.insert(
            key.clone(),
            crate::db::Entry {
                value: Value::ZSet { scores, ordered },
                expires: None,
            },
        );
    }

    if incr {
        return vec![match new_score_for_incr {
            Some(v) => Frame::BulkString(Some(Bytes::from(format_float(v)))),
            None => nil(),
        }];
    }
    let ret = if ch { changed } else { added };
    vec![int(ret)]
}

async fn cmd_zrange(
    state: &ServerState,
    ctx: &ConnContext,
    name: &str,
    args: &[Bytes],
) -> Vec<Frame> {
    if args.len() < 3 {
        return vec![wrong_arity("zrange")];
    }
    let db = db_of(state, ctx);
    let key = &args[0];
    let rev = name.contains("REV");
    let byscore = name.contains("BYSCORE") || name.to_uppercase().contains("BYSCORE");
    // detect WITHSCORES
    let mut withscores = false;
    let mut limit: Option<(i64, i64)> = None;
    // parse trailing options
    let mut core_args = args.to_vec();
    // find WITHSCORES
    let mut i = 3;
    // args: key min max [BYSCORE] [REV] [LIMIT off cnt] [WITHSCORES]
    // We'll handle positional min/max in args[1],args[2]
    // trailing options from index 3
    let mut idx = 3usize;
    while idx < args.len() {
        let opt = String::from_utf8_lossy(&args[idx]).to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                withscores = true;
                idx += 1;
            }
            "REV" => {
                /* already */
                idx += 1;
            }
            "BYSCORE" => {
                idx += 1;
            }
            "LIMIT" => {
                let off: i64 = String::from_utf8_lossy(&args[idx + 1]).parse().unwrap_or(0);
                let cnt: i64 = String::from_utf8_lossy(&args[idx + 2]).parse().unwrap_or(0);
                limit = Some((off, cnt));
                idx += 3;
            }
            _ => {
                idx += 1;
            }
        }
    }

    let mut min_arg = String::from_utf8_lossy(&args[1]).to_string();
    let mut max_arg = String::from_utf8_lossy(&args[2]).to_string();
    if rev && byscore {
        std::mem::swap(&mut min_arg, &mut max_arg);
    }

    match db.get(key) {
        None => vec![Frame::Array(Some(vec![]))],
        Some(e) => match e.value {
            Value::ZSet { ordered, .. } => {
                // collect members in order
                let mut members: Vec<(f64, Bytes)> =
                    ordered.iter().map(|(s, m)| (s.0, m.clone())).collect();
                if rev {
                    members.reverse();
                }
                // filter by score or rank
                let filtered: Vec<(f64, Bytes)> = if byscore {
                    let (min, max, min_excl, max_excl) = parse_score_range_str(&min_arg, &max_arg);
                    members
                        .into_iter()
                        .filter(|(sc, _)| score_in_range(*sc, min, max, min_excl, max_excl))
                        .collect()
                } else {
                    // rank range
                    let len = members.len() as i64;
                    let start: i64 = min_arg.parse().unwrap_or(0);
                    let stop: i64 = max_arg.parse().unwrap_or(-1);
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    } as usize;
                    let mut st = if stop < 0 {
                        (len + stop).max(-1)
                    } else {
                        stop.min(len - 1)
                    };
                    if st < s as i64 {
                        vec![]
                    } else {
                        members
                            .into_iter()
                            .skip(s)
                            .take((st as usize + 1).saturating_sub(s))
                            .collect()
                    }
                };
                // apply limit
                let filtered = if let Some((off, cnt)) = limit {
                    if cnt < 0 {
                        filtered
                    } else {
                        filtered
                            .into_iter()
                            .skip(off.max(0) as usize)
                            .take(cnt as usize)
                            .collect()
                    }
                } else {
                    filtered
                };
                // build output
                let mut out = Vec::new();
                for (sc, m) in filtered {
                    out.push(Frame::BulkString(Some(m)));
                    if withscores {
                        out.push(Frame::BulkString(Some(Bytes::from(format_float(sc)))));
                    }
                }
                // If withscores, output should be flat list of member,score pairs? Redis returns array of members, or with scores flat. Actually ZRANGE ... WITHSCORES returns array where each element is [member, score]?
                // Redis 6.2+: ZRANGE returns array of members; WITHSCORES returns array of [member, score] arrays? Let's check: redis-cli ZRANGE z 0 -1 WITHSCORES returns flat? Actually returns list: member score member score (flat) in older; newer returns array of strings alternating? redis-cli prints them alternating. Protocol: array of bulk strings alternating member,score. We'll do flat.
                vec![Frame::Array(Some(out))]
            }
            _ => vec![err("WRONGTYPE...")],
        },
    }
}

fn parse_score_range(min: &Bytes, max: &Bytes) -> (f64, f64, bool, bool) {
    parse_score_range_str(&String::from_utf8_lossy(min), &String::from_utf8_lossy(max))
}

fn parse_score_range_str(min: &str, max: &str) -> (f64, f64, bool, bool) {
    let (min_excl, minv) = if let Some(rest) = min.strip_prefix('(') {
        (true, rest.parse::<f64>().unwrap_or(f64::MIN))
    } else if min == "-inf" {
        (false, f64::MIN)
    } else {
        (false, min.parse::<f64>().unwrap_or(f64::MIN))
    };
    let (max_excl, maxv) = if let Some(rest) = max.strip_prefix('(') {
        (true, rest.parse::<f64>().unwrap_or(f64::MAX))
    } else if max == "+inf" {
        (false, f64::MAX)
    } else {
        (false, max.parse::<f64>().unwrap_or(f64::MAX))
    };
    (minv, maxv, min_excl, max_excl)
}

fn score_in_range(v: f64, min: f64, max: f64, min_excl: bool, max_excl: bool) -> bool {
    let lo = if min_excl { v > min } else { v >= min };
    let hi = if max_excl { v < max } else { v <= max };
    lo && hi
}

fn format_float(v: f64) -> String {
    // redis-style: trim trailing zeros but keep precision
    if v == v.trunc() && v.abs() < 1e16 {
        // integer-valued float: redis prints "5" not "5.0"? Actually redis prints "5" for integer scores? redis-cli prints "5". Let's print without decimal if integer.
        format!("{}", v as i64)
    } else {
        // use shortest representation
        let s = format!("{}", v);
        s
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    // simple glob with * and ?
    // use recursive
    fn rec(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => {
                // match zero or more
                if p.len() == 1 {
                    return true;
                }
                for i in 0..=t.len() {
                    if rec(&p[1..], &t[i..]) {
                        return true;
                    }
                }
                false
            }
            b'?' => {
                if t.is_empty() {
                    false
                } else {
                    rec(&p[1..], &t[1..])
                }
            }
            c => {
                if t.is_empty() {
                    false
                } else if t[0] == c {
                    rec(&p[1..], &t[1..])
                } else {
                    false
                }
            }
        }
    }
    rec(pattern.as_bytes(), text.as_bytes())
}
