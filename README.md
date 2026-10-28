<p align="center">
  <img src="assets/crabcache.png" alt="crabcache" width="420" />
</p>

<p align="center">
  <a href="https://github.com/neoned71/crabcache/actions/workflows/ci.yml"><img src="https://github.com/neoned71/crabcache/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://github.com/neoned71/crabcache/releases"><img src="https://img.shields.io/github/v/release/neoned71/crabcache" alt="Release" /></a>
  <a href="#license"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="MIT" /></a>
  <img src="https://img.shields.io/badge/rust-stable-orange.svg" alt="Rust" />
  <img src="https://img.shields.io/badge/protocol-RESP2%2FRESP3-red.svg" alt="RESP" />
</p>

# crabcache

> A Redis-protocol-compatible in-memory data store, **rebuilt from scratch in Rust** — and it beats `redis-server` on throughput.

`crabcache` speaks the Redis Serialization Protocol (RESP2 and RESP3), so **existing Redis clients
work unchanged**: `redis-cli`, `redis-py`, `ioredis`, `go-redis`, `Jedis`, the Rust `redis` crate,
and anything else that talks RESP.

```sh
redis-cli -p 6379 SET hello world EX 60
# OK
redis-cli -p 6379 GET hello
# "world"
```

## ⚡ Performance — it's faster than Redis

Single-threaded mode (`--threads 1`) matches Redis's own single-event-loop model and **beats
`redis-server` on every benchmarked command**:

| Command | redis-server 8.10.3 (rps) | crabcache (rps) | crabcache / redis | p50 latency (redis → crabcache) |
|---|---:|---:|---:|---:|
| SET | 189,753 | **239,808** | 1.26x | 0.22 ms → 0.10 ms |
| GET | 202,429 | **232,558** | 1.15x | 0.21 ms → 0.10 ms |
| INCR | 199,601 | **220,751** | 1.11x | 0.22 ms → 0.11 ms |
| LPUSH | 197,628 | **228,311** | 1.16x | 0.22 ms → 0.10 ms |
| RPUSH | 197,628 | **229,885** | 1.16x | 0.22 ms → 0.10 ms |
| LPOP | 191,205 | **216,920** | 1.13x | 0.22 ms → 0.10 ms |
| RPOP | 192,308 | **212,766** | 1.11x | 0.22 ms → 0.11 ms |
| SADD | 195,312 | **221,239** | 1.13x | 0.22 ms → 0.10 ms |
| HSET | 196,078 | **219,298** | 1.12x | 0.22 ms → 0.11 ms |
| ZADD | 193,798 | **235,849** | 1.22x | 0.22 ms → 0.10 ms |

> 100k requests, 50 clients, loopback, macOS Apple Silicon. `redis-benchmark -n 100000 -c 50 -q`.
> Full methodology and raw output: [`BENCHMARKS.md`](BENCHMARKS.md).

### How we got here — optimization progression (SET, rps)

| Step | Commit | Change | rps | Δ |
|---|---|---|---:|---:|
| 0 | — | Baseline (multi-threaded, fsync per cmd) | 131,579 | — |
| 1 | `705926a` | AOF batching (BufWriter + 100 ms flush) | 143,472 | +9% |
| 2 | `bf70e78` | mimalloc global allocator | 144,509 | +1% |
| 3 | `e4bc267` | Mutate strings in place (no clone+reinsert) | 144,300 | ~0% |
| 4 | `bd2838b` | Compact Set/Hash encodings (Vec for small) | 143,885 | ~0% (SADD/HSET +37%) |
| 5 | `2087ef2` | Reduce RESP parser allocations | 143,678 | ~0% |
| 6 | `9b7beac` | Single-threaded mode (`--threads 1`) | **239,808** | **+67%** |

**The big lesson:** Redis is fast *because* it's single-threaded. Matching that model (eliminating DashMap lock contention and tokio scheduling overhead) was worth more than all the micro-optimizations combined.

## Why Rust for a Redis server?

- **Memory safety** without a GC — no pause-the-world stalls.
- **Embeddable** — link `crabcache` as a library into your Rust app instead of running a separate process.
- **No runtime dependency** — single static-ish binary, no Node/Python/VM.
- **MIT licensed** — unlike the newer AGPL Rust alternative (KiviDB).

## Features

### Data types & commands

| Type | Commands |
|---|---|
| **Strings** | `SET` `GET` `MSET` `MGET` `INCR` `INCRBY` `INCRBYFLOAT` `DECR` `DECRBY` `APPEND` `STRLEN` `GETSET` `GETDEL` `GETEX` `SETNX` `SETEX` `PSETEX` (`SET` supports `EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`GET`/`KEEPTTL`) |
| **Keys** | `DEL` `UNLINK` `EXISTS` `TYPE` `RENAME` `RENAMENX` `KEYS` `SCAN` `RANDOMKEY` `DBSIZE` `FLUSHDB` `FLUSHALL` |
| **TTL** | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| **Lists** | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LREM` `LTRIM` |
| **Sets** | `SADD` `SREM` `SMEMBERS` `SCARD` `SISMEMBER` `SPOP` (compact Vec encoding for small sets) |
| **Hashes** | `HSET` `HGET` `HMGET` `HGETALL` `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HINCRBY` (compact Vec encoding for small hashes) |
| **Sorted sets** | `ZADD` `ZREM` `ZSCORE` `ZCARD` `ZRANK` `ZREVRANK` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZCOUNT` `ZINCRBY` |
| **Pub/Sub** | `PUBLISH` `SUBSCRIBE` `UNSUBSCRIBE` (global broadcast, multi-channel per connection) |
| **Transactions** | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |
| **Server / conn** | `PING` `ECHO` `HELLO` `QUIT` `AUTH` `SELECT` `INFO` `CONFIG GET/SET` `CLIENT` `COMMAND` `TIME` `ACL` `SAVE` `BGSAVE` `LASTSAVE` `WAIT` |

### Persistence
- **AOF (append-only file)**: every command is buffered and flushed every 100 ms; on startup the file is replayed to rebuild state. Enable with `--appendonly`.
- RDB snapshots are not implemented yet (`SAVE`/`BGSAVE` are no-ops).

## Install

### From source (cargo)

```sh
cargo install --git https://github.com/neoned71/crabcache
# or, from a clone:
cargo install --path .
```

### Docker

```sh
docker build -t crabcache .
docker run -p 6379:6379 -v crabcache-data:/data crabcache
```

### Pre-built binaries

macOS (aarch64 + x86_64) are published on the
[Releases](https://github.com/neoned71/crabcache/releases) page. Linux/Windows builds are
produced by the release workflow (see Actions).

## Quick start

### Build

```sh
git clone https://github.com/neoned71/crabcache
cd crabcache
cargo build --release
```

### Run (single-threaded = fastest)

```sh
./target/release/crabcache --port 6379 --dir ./data --appendonly --threads 1
```

```sh
redis-cli -p 6379
127.0.0.1:6379> SET hello world EX 60
OK
127.0.0.1:6379> GET hello
"world"
127.0.0.1:6379> LPUSH q a b c
(integer) 3
127.0.0.1:6379> RPOP q
"a"
```

### Configuration (CLI flags)

| Flag | Default | Description |
|---|---|---|
| `--bind` | `127.0.0.1` | Interface to listen on |
| `--port` | `6379` | TCP port |
| `--dir` | `.` | Working directory for persistence files |
| `--dbfilename` | `dump.rdb` | RDB filename (reserved) |
| `--appendonly` | `false` | Enable AOF persistence |
| `--threads` | `0` (= CPU count) | Worker threads; `1` = single-threaded (fastest) |

## Architecture

```
src/
├── main.rs       # CLI, AOF open + replay, mimalloc, runtime setup
├── server.rs     # TCP accept loop, per-connection read/write, broadcast forwarding
├── protocol.rs   # RESP2/RESP3 parser + serializer (Frame enum)
├── commands.rs   # dispatch() — every command as (state, ctx, args) -> Vec<Frame>
├── db.rs         # Db / Entry / DbManager (DashMap-backed, lazy expiry, compact encodings)
└── state.rs      # ServerState, PubSub (global broadcast channel)
```

Key design points:

- **One `DashMap` per database** — lock-free concurrent access.
- **Lazy expiry** — expired keys removed on access.
- **Per-connection context** (`ConnContext`) holds selected DB, MULTI queue, subscribed channels, auth state, protocol version.
- **Commands are pure-ish functions** returning `Vec<Frame>`, decoupling protocol from logic.
- **Compact encodings** — small sets/hashes use `Vec` (linear scan, cache-friendly) and promote to `HashSet`/`HashMap` past a threshold.
- **Single-threaded mode** matches Redis's event-loop model for maximum throughput.

## Compatibility with real Redis

Verified with a harness that runs the same command sequence against `redis-server` and `crabcache`
and compares raw RESP bytes.

- ✅ All basic-to-intermediate single-node workloads (cache, counters, lists/sets/hashes/zsets,
  queues, pub/sub, transactions) behave identically.
- ⚠️ `KEYS`/`SMEMBERS`/`HGETALL` iteration order is unspecified and differs (as between Redis versions).
- ⚠️ Error message wording and a few float-formatting edge cases may differ.
- ❌ Not compatible with: replication, cluster, Lua (`EVAL`), streams, modules, blocking commands
  with timeouts, `SORT`, geo, HyperLogLog, full-text/vector search.

## Limitations

- **No replication** — single node only.
- **No cluster mode**.
- **No Lua scripting** (`EVAL`/`EVALSHA`).
- **No streams**.
- **No RDB snapshots** (AOF only).
- **No modules**.
- **No maxmemory eviction** — memory grows unbounded.
- **No active expiry** — only lazy expiry on access.
- **Blocking commands** (`BLPOP`, etc.) are not truly blocking.
- **No TLS**, no Unix sockets.

## Roadmap / next steps

- [ ] True blocking list commands (`BLPOP`/`BRPOP`/`BLMOVE`)
- [ ] RDB dump/load
- [ ] Active expiry sampling + maxmemory policies (LRU/LFU)
- [ ] Lua scripting (`EVAL`)
- [ ] Streams (`XADD`/`XREAD`/…)
- [ ] Master/replica replication (PSYNC)
- [ ] Cluster mode
- [ ] TLS + Unix domain sockets
- [ ] Benchmark on Linux with `perf`/`flamegraph`
- [ ] Publish to crates.io
- [ ] Docker Hub image + GitHub Releases for all architectures

## Testing

```sh
cargo test            # 6 unit + 10 integration tests
cargo clippy          # advisory
cargo fmt --check
```

Integration tests spin up a real `crabcache` binary and exercise it over TCP, including an AOF
persistence round-trip.

## Contributing

Issues and PRs welcome. Good first contributions (see open issues):

- Implement blocking list commands.
- Add RDB snapshot dump/load.
- Implement Lua scripting (`EVAL`).
- Add streams.
- Active expiry + maxmemory eviction.

Please keep new code `rustfmt`-clean.

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgements

- [Redis](https://redis.io/) and Salvatore Sanfilippo — for the protocol and the design.
- [Tokio](https://tokio.rs/), [DashMap](https://github.com/xacrimon/dashmap), [bytes](https://github.com/tokio-rs/bytes), [mimalloc](https://github.com/purpleprotocol/mimalloc_rust) — the foundation.
- Inspired by `mini-redis` (Tokio's teaching example) and `reredis`.

---

## 📣 LinkedIn post (ready to copy)

> 🦀 I rebuilt Redis in Rust — and it's faster.
>
> Over the last few days I wrote **crabcache**, a Redis-protocol-compatible in-memory store from scratch in Rust. It speaks RESP2/RESP3, so every existing Redis client (`redis-cli`, `redis-py`, `ioredis`, `go-redis`…) works unchanged.
>
> The surprising part: in single-threaded mode it **beats redis-server** on throughput:
>
> • SET 240k vs 190k rps (+26%)
> • GET 233k vs 202k rps (+15%)
> • ZADD 236k vs 194k rps (+22%)
> • ~0.10 ms p50 latency vs ~0.21 ms
>
> How? The big lesson was architectural, not micro-optimization: Redis is fast *because* it's single-threaded. Once I matched that model (one event loop, no lock contention), throughput jumped +67%. Micro-opts (AOF batching, mimalloc, compact encodings, in-place mutation) added only a few percent each.
>
> What's implemented: strings, keys, TTL, lists, sets, hashes, sorted sets, pub/sub, MULTI/EXEC transactions, and AOF persistence with replay. MIT licensed.
>
> What's NOT (yet): replication, cluster, Lua, streams, RDB. Single node only.
>
> Repo: https://github.com/neoned71/crabcache
>
> If you've ever wanted to read a Redis-like server end to end in a memory-safe language — or contribute a missing command — come take a look. Issues labeled "good first issue" are up.
>
> #rust #redis #opensource #databases #performance