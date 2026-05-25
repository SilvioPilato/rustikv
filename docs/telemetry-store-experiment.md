# rustikv as a Telemetry Store — Experimental Use Case

## What it is

rustikv is a didactic TCP key-value store with two pluggable storage engines:

- **KV (Bitcask)** — hash index in memory, O(1) reads, append-only writes. Best for random access by exact key.
- **LSM** — write-optimized, memtable flushed to sorted SSTables. Supports ordered scans (`RANGE`, `PREFIX`) and server-side aggregation. Best for sequential/time-series workloads.

For telemetry, **LSM is the right engine** — its write path is append-only and absorbs bursts well, and ordered scans enable time-window queries when you design keys with timestamps (e.g. `cpu:host1:1700000000`).

## How it fits telemetry today

The full telemetry feature path has shipped. Everything below works now on the LSM engine:

| Need | Capability |
|------|-----------|
| High write throughput | LSM engine — append-only writes |
| Batch ingestion | `MSET` (and `MWRITETTL`) — many points per round trip |
| Bounded retention | `TTL` per key (and write-with-TTL); expired points drop at compaction |
| Counter/rate metrics | `INCR` — atomic, preserves any existing TTL |
| Time-range queries | `RANGE <start> <end>` — sorted points in a window |
| Metric-namespace queries | `PREFIX <prefix>` — every point under a namespace |
| Cardinality | `COUNT <prefix>` / `COUNT <start> <end>` — series count without pulling values |
| Server-side rollups | `SUM` / `AVG` / `MIN` / `MAX` over a prefix or range |
| Storage efficiency | LZ77 block compression on SSTables |
| Fast existence checks | Bloom filter per SSTable |

The earlier "what's missing" list is gone: TTL, INCR, PREFIX, COUNT, and SUM/AVG/MIN/MAX are all merged.

## Key schema for time series

Encode the timestamp into the key so lexicographic order matches time order:

```
<metric>:<zero-padded-epoch-seconds>      e.g.   pi.cpu.user:0001748169600
```

Fixed-width zero-padding is what makes the LSM ordered scans line up with numeric time. With that, a metric's history is a contiguous key range, and a time window is a `RANGE` over two boundary keys.

## Worked end-to-end flow

The example below uses the `rustikli` REPL verbs for clarity. (`WRITETTL <key> <seconds> <value>` and `MWRITETTL <seconds> <k> <v> ...` set values with a TTL in one step.)

```text
# Ingest three samples of pi.cpu.user with a 24h TTL (retention)
rustikli> MWRITETTL 86400 pi.cpu.user:0001748169600 10 pi.cpu.user:0001748169660 20 pi.cpu.user:0001748169720 30

# All points for one metric (namespace scan)
rustikli> PREFIX pi.cpu.user:
pi.cpu.user:0001748169600
10
pi.cpu.user:0001748169660
20
pi.cpu.user:0001748169720
30

# Points in a time window [t0, t1]
rustikli> RANGE pi.cpu.user:0001748169600 pi.cpu.user:0001748169720
...

# Cardinality: how many samples in the window
rustikli> COUNT pi.cpu.user:0001748169600 pi.cpu.user:0001748169720
3

# Server-side rollups over the window (no raw values pulled to the client)
rustikli> AVG pi.cpu.user:0001748169600 pi.cpu.user:0001748169720
20
rustikli> MIN pi.cpu.user:0001748169600 pi.cpu.user:0001748169720
10
rustikli> MAX pi.cpu.user:0001748169600 pi.cpu.user:0001748169720
30
```

Counter-style metrics use `INCR` instead of `WRITE`:

```text
rustikli> INCR pi.http.requests:0001748169600
1
rustikli> INCR pi.http.requests:0001748169600
2
```

## Ingestion and visualization: the telemetry gateway

rustikv speaks a custom binary protocol (BFFP), so no off-the-shelf collector or
dashboard talks to it directly. The companion project
**[rustikv-telemetry-gateway](https://github.com/SilvioPilato/rustikv-telemetry-gateway)**
bridges both ends:

- **Ingest:** accepts **Graphite plaintext** (`metric.path value timestamp`) from
  any collector (collectd, Telegraf) and writes batched `MSET`s with a TTL,
  building exactly the `<metric>:<padded-ts>` keys above.
- **Serve:** an HTTP `/query` endpoint returns JSON time series for Grafana's
  Infinity datasource. For `agg=avg|min|max|sum` it splits the window into
  buckets and issues one `AvgRange`/`MinRange`/`MaxRange`/`SumRange` per bucket —
  so downsampling runs **server-side** in rustikv, not in the client.

A Raspberry Pi running collectd → gateway → rustikv → Grafana is the reference
setup. The gateway reuses rustikv's `bffp` module (git dependency) for the wire
protocol; rustikv itself needs no changes.

## What to skip for now

Replication, consistent hashing, and partitioning are all single-node concerns for this experiment. Cross-metric downsampling beyond fixed buckets can be handled client-side. These are future concerns if the experiment outgrows a single node.

## Feature path — complete

```
TTL  →  INCR  →  PREFIX  →  COUNT  →  SUM/AVG/MIN/MAX        ✅ all shipped
```

The first two unlocked the core telemetry patterns (bounded retention, counters);
PREFIX/COUNT made namespaces ergonomic; server-side aggregation removed the
client-side scan as the scalability bottleneck.
