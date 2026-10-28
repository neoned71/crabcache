# crabcache vs. redis-server — benchmark report

Environment: macOS (Apple Silicon), single machine, loopback.
- redis-server 8.10.3 (Homebrew)
- crabcache 0.1.0 (release build, `cargo build --release`)
- Tool: `redis-benchmark`, `-n 50000 -c 50`

## Results (requests/sec, higher is better)

| Command | redis-server 8.10.3 | crabcache | crabcache / redis |
|---|---:|---:|---:|
| SET | 185,185 | 131,579 | 0.71x |
| GET | 199,203 | 140,449 | 0.70x |
| INCR | 199,203 | 139,276 | 0.70x |
| LPUSH | 192,308 | 138,504 | 0.72x |
| RPUSH | 197,628 | 138,889 | 0.70x |
| LPOP | 190,840 | 139,276 | 0.73x |
| RPOP | 192,308 | 139,665 | 0.73x |
| SADD | 186,567 | 136,986 | 0.73x |
| HSET | 187,970 | 136,240 | 0.72x |
| ZADD | 189,394 | 135,870 | 0.72x |

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