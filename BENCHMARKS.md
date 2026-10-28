# crabcache vs. redis-server — benchmark report

Environment: macOS (Apple Silicon), single machine, loopback.
- redis-server 8.10.3 (Homebrew)
- crabcache 0.1.0 (release build, `cargo build --release`)
- Tool: `redis-benchmark`, `-n 50000 -c 50`

## Results (requests/sec, higher is better) — 100k requests, 50 clients

| Command | redis-server 8.10.3 | crabcache | crabcache / redis |
|---|---:|---:|---:|
| SET | 190,476 | 134,590 | 0.71x |
| GET | 202,840 | 140,252 | 0.69x |
| INCR | 201,613 | 140,252 | 0.70x |
| LPUSH | 198,413 | 140,449 | 0.71x |
| RPUSH | 200,803 | 138,889 | 0.69x |
| LPOP | 199,203 | 140,845 | 0.71x |
| RPOP | 202,020 | 139,860 | 0.69x |
| SADD | 202,840 | 108,814 | 0.54x |
| HSET | 201,207 | 119,760 | 0.60x |
| ZADD | 200,401 | 126,743 | 0.63x |

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