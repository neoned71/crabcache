use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

fn start_server(port: u16) -> (Child, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_crabcache"))
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            dir.path().to_str().unwrap(),
            "--appendonly",
        ])
        .spawn()
        .unwrap();
    // wait for ready
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() > deadline {
            panic!("server did not start");
        }
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = s.write_all(b"*1\r\n$4\r\nPING\r\n");
            let mut buf = [0u8; 64];
            if let Ok(n) = s.read(&mut buf) {
                if n > 0 {
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    (child, dir)
}

fn send(port: u16, cmd: &str) -> String {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    // build resp array from space-separated (no quoting support, fine for tests)
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let mut data = format!("*{}\r\n", parts.len());
    for p in &parts {
        data.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    s.write_all(data.as_bytes()).unwrap();
    thread::sleep(Duration::from_millis(50));
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match s.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&buf).to_string()
}

#[test]
fn ping() {
    let (mut child, _dir) = start_server(16401);
    let r = send(16401, "PING");
    assert!(r.contains("PONG"), "got: {}", r);
    let _ = child.kill();
}

#[test]
fn set_get() {
    let (mut child, _dir) = start_server(16402);
    assert!(send(16402, "SET foo bar").contains("OK"));
    let r = send(16402, "GET foo");
    assert!(r.contains("bar"), "got: {}", r);
    assert!(send(16402, "GET nope").contains("$-1"));
    let _ = child.kill();
}

#[test]
fn incr() {
    let (mut child, _dir) = start_server(16403);
    assert!(send(16403, "INCR c").contains(":1"));
    assert!(send(16403, "INCR c").contains(":2"));
    assert!(send(16403, "GET c").contains("2"));
    let _ = child.kill();
}

#[test]
fn list_ops() {
    let (mut child, _dir) = start_server(16404);
    assert!(send(16404, "LPUSH l a b c").contains(":3"));
    let r = send(16404, "LRANGE l 0 -1");
    assert!(r.contains("c") && r.contains("a"), "got: {}", r);
    assert!(send(16404, "LLEN l").contains(":3"));
    let _ = child.kill();
}

#[test]
fn hash_ops() {
    let (mut child, _dir) = start_server(16405);
    assert!(send(16405, "HSET h f1 v1 f2 v2").contains(":2"));
    assert!(send(16405, "HGET h f1").contains("v1"));
    assert!(send(16405, "HLEN h").contains(":2"));
    let _ = child.kill();
}

#[test]
fn set_ops() {
    let (mut child, _dir) = start_server(16406);
    assert!(send(16406, "SADD s x y z").contains(":3"));
    assert!(send(16406, "SCARD s").contains(":3"));
    assert!(send(16406, "SISMEMBER s y").contains(":1"));
    let _ = child.kill();
}

#[test]
fn zset_ops() {
    let (mut child, _dir) = start_server(16407);
    assert!(send(16407, "ZADD z 1 one 2 two").contains(":2"));
    assert!(send(16407, "ZCARD z").contains(":2"));
    let r = send(16407, "ZRANGE z 0 -1 WITHSCORES");
    assert!(r.contains("one") && r.contains("two"), "got: {}", r);
    let _ = child.kill();
}

#[test]
fn ttl_expiry() {
    let (mut child, _dir) = start_server(16408);
    assert!(send(16408, "SET t hi").contains("OK"));
    assert!(send(16408, "EXPIRE t 1").contains(":1"));
    // wait for expiry
    thread::sleep(Duration::from_millis(1100));
    assert!(
        send(16408, "GET t").contains("$-1"),
        "got: {}",
        send(16408, "GET t")
    );
    let _ = child.kill();
}

#[test]
fn transaction() {
    let (mut child, _dir) = start_server(16409);
    assert!(send(16409, "MULTI").contains("OK"));
    // queued commands won't apply until exec; send all in separate connections won't work,
    // so use a single raw connection instead.
    let mut s = TcpStream::connect(("127.0.0.1", 16409)).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let payload = "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\ntx\r\n$2\r\n42\r\n*2\r\n$4\r\nINCR\r\n$2\r\ntx\r\n*2\r\n$3\r\nGET\r\n$2\r\ntx\r\n*1\r\n$4\r\nEXEC\r\n";
    s.write_all(payload.as_bytes()).unwrap();
    thread::sleep(Duration::from_millis(200));
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match s.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    let r = String::from_utf8_lossy(&buf);
    assert!(r.contains("42") || r.contains("43"), "got: {}", r);
    let _ = child.kill();
}

#[test]
fn persistence_aof() {
    let dir = tempfile::tempdir().unwrap();
    let port = 16410u16;
    // first run
    let mut child = Command::new(env!("CARGO_BIN_EXE_crabcache"))
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            dir.path().to_str().unwrap(),
            "--appendonly",
        ])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() > deadline {
            panic!("server did not start");
        }
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = s.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nperm\r\n$5\r\nhello\r\n");
            let mut buf = [0u8; 64];
            if let Ok(n) = s.read(&mut buf) {
                if n > 0 {
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    thread::sleep(Duration::from_millis(200));
    let _ = child.kill();
    let _ = child.wait();
    // second run (replay)
    let mut child2 = Command::new(env!("CARGO_BIN_EXE_crabcache"))
        .args([
            "--port",
            &port.to_string(),
            "--dir",
            dir.path().to_str().unwrap(),
            "--appendonly",
        ])
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() > deadline {
            panic!("server did not start");
        }
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = s.write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nperm\r\n");
            let mut buf = [0u8; 64];
            if s.read(&mut buf).unwrap_or(0) > 0 {
                let r = String::from_utf8_lossy(&buf);
                if r.contains("hello") {
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    let _ = child2.kill();
}
