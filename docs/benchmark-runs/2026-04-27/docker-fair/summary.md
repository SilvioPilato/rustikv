# rustikv vs Redis — Fair Comparison (Docker, 2026-04-27)

Both servers run as containers on the same Docker bridge network, with the
benchmark client running as a third container on the same network. This
neutralizes the host-vs-container networking gap that distorted the previous
[2026-04-25 run](../../2026-04-25/redis-comparison-analysis.md), where rustikv
was on host loopback and Redis was reached through Docker Desktop's
WSL2/Hyper-V port-forward.

## Setup

- Compose file: [bench/docker-compose.yml](../../../../bench/docker-compose.yml)
- Dockerfile: [bench/Dockerfile](../../../../bench/Dockerfile)
- Both servers use the same fairness profile (log writes, no per-op fsync):
  - **Redis**: `--save "" --appendonly yes --appendfsync no` (AOF, OS-flushed)
  - **rustikv**: `--engine lsm --fsync-interval never`
- Workload: 10,000 sequential ops per phase, single client connection,
  request/response per op (no pipelining), keys `bench:key:{:08}`.
- State is reset (`compose down -v`) between value-size runs.

## Results

### WRITE throughput (ops/sec, higher is better)

| Payload | rustikv | Redis  | rustikv vs Redis |
|---------|---------|--------|------------------|
| 100 B   | 28,037  | 24,941 | **1.12×**        |
| 1 KB    | 23,865  | 25,106 | 0.95×            |
| 10 KB   | 15,300  | 18,998 | 0.81×            |
| 100 KB  | 331     | 5,866  | 0.06×            |

### READ throughput (ops/sec, higher is better)

| Payload | rustikv | Redis  | rustikv vs Redis |
|---------|---------|--------|------------------|
| 100 B   | 30,615  | 26,397 | **1.16×**        |
| 1 KB    | 29,555  | 27,397 | 1.08×            |
| 10 KB   | 27,826  | 25,606 | 1.09×            |
| 100 KB  | 2,171   | 16,014 | 0.14×            |

### p99 latency (lower is better)

| Payload | rustikv WRITE | Redis WRITE | rustikv READ | Redis READ |
|---------|---------------|-------------|--------------|-----------|
| 100 B   | 98.5 µs       | 112.5 µs    | 82.1 µs      | 103.7 µs  |
| 1 KB    | 113.3 µs      | 114.2 µs    | 93.8 µs      | 109.1 µs  |
| 10 KB   | 147.0 µs      | 126.4 µs    | 91.6 µs      | 103.7 µs  |
| 100 KB  | 468.2 µs *    | 408.7 µs    | 1.79 ms      | 147.3 µs  |

\* The 100 KB rustikv WRITE max is 1.59 s — the p99 number understates a long
tail driven by compaction / large segment flushes.

## Takeaways

1. **The earlier ~17× advantage was an artifact of the host-vs-container
   network path**, not of rustikv being faster than Redis. With both in the
   same Docker network, results are within 10–20 % of each other for small
   payloads.
2. **At small payloads (≤ 1 KB), rustikv is competitive with Redis** —
   slightly ahead on reads, neck-and-neck on writes. Likely explanation: BFFP
   is a simpler binary protocol than RESP; per-op CPU cost is lower.
3. **At 10 KB, Redis pulls ahead on writes**, rustikv stays slightly ahead on
   reads.
4. **At 100 KB, the LSM engine collapses on writes** (~330 ops/sec, 1.6 s tail
   latency) and reads suffer too. This is where Redis's pure-memory model
   wins: rustikv is paying disk I/O, block compression, and likely compaction
   stalls on each large write. The block cache on reads keeps it from being
   even worse.

## Reproduce

```bash
cd bench
docker compose up -d --build redis rustikv
docker compose run --rm bench                      # default 100B
VALUE_SIZE=1024  docker compose run --rm bench
VALUE_SIZE=10240 docker compose run --rm bench
VALUE_SIZE=102400 docker compose run --rm bench
docker compose down -v
```
