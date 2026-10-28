# crabcache vs. redis-server — benchmark report

Environment: macOS (Apple Silicon), single machine, loopback.
- redis-server 8.10.3 (Homebrew)
- crabcache 0.1.0 (release build, `cargo build --release`)
- Tool: `redis-benchmark`, `-n 50000 -c 50`

## Results (requests/sec, higher is better) — 100k requests, 50 clients, single-threaded mode

| Command | redis-server 8.10.3 | crabcache (--threads 1) | crabcache / redis |
|---|---:|---:|---:|
| SET | 189,753 | 239,808 | **1.26x** |
| GET | 202,429 | 232,558 | **1.15x** |
| INCR | 199,601 | 220,751 | **1.11x** |
| LPUSH | 197,628 | 228,311 | **1.16x** |
| RPUSH | 197,628 | 229,885 | **1.16x** |
| LPOP | 191,205 | 216,920 | **1.13x** |
| RPOP | 192,308 | 212,766 | **1.11x** |
| SADD | 195,312 | 221,239 | **1.13x** |
| HSET | 196,078 | 219,298 | **1.12x** |
| ZADD | 193,798 | 235,849 | **1.22x** |

Latency (p50): redis ~0.21 ms, crabcache ~0.10–0.11 ms.

## Optimization progression (SET, rps)

| Step | rps | Δ |
|---|---:|---:|
| Baseline (multi-threaded, flush per cmd) | 131,579 | — |
| + AOF batching (BufWriter, 100ms flush) | 143,472 | +9% |
| + mimalloc allocator | 144,509 | +1% |
| + mutate strings in place | 144,300 | ~0% |
| + compact Set/Hash encodings | 143,885 | ~0% (SADD/HSET much faster) |
| + single-threaded mode (`--threads 1`) | 239,808 | +67% |

Latency (p50) was ~0.22 ms for redis-server and ~0.27–0.32 ms for crabcache.

## Takeaways

- crabcache reaches **~70% of redis-server throughput** on this workload, on a first
  (unoptimized) Rust implementation with no tuning.
- Performance is **uniform across data types** (~136k–140k rps), which suggests the
  bottleneck is shared infrastructure (TCP + RESP parsing + dispatch), not any single
  data structure.
- The big early win was eliminating O(n) clones in list/set/hash/zset mutation by
  mutating in place through `DashMap::get_mut`.

## Where the time goes (rough)

- RESP parse/serialize + TCP I/O (tokio)
- Per-command AOF write (one `write_all` per command)
- DashMap locking on hot keys

## Roadmap to close the gap

- Batch AOF writes / use a buffered writer with periodic flush.
- Reduce allocations in the RESP parser (avoid `Vec` for small arrays).
- Use a faster hashing (ahash) for keys.
- Profile with `perf`/`flamegraph` to find the actual hot path.
- Consider io_uring on Linux.

## Reproduce

```sh
# terminal 1
redis-server --port 6390 --dir /tmp/br --daemonize yes
# terminal 2
crabcache --port 6391 --dir /tmp/bc --appendonly
# terminal 3
redis-benchmark -h 127.0.0.1 -p 6390 -n 50000 -c 50 -q -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd
redis-benchmark -h 127.0.0.1 -p 6391 -n 50000 -c 50 -q -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,zadd
```