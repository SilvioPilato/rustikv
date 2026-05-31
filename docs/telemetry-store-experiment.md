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
| Batch ingestion | `MSET` — many points per round trip; default TTL applied automatically |
| Bounded retention | Per-collection default TTL (`CREATE COLLECTION metrics 86400`); also per-key TTL as fallback |
| Separate retention per metric family | Collections — each with its own keyspace, WAL, SSTables, and default TTL |
| Counter/rate metrics | `INCR` — atomic, preserves any existing TTL |
| Time-range queries | `RANGE <start> <end>` — sorted points in a window |
| Metric-namespace queries | `PREFIX <prefix>` — every point under a namespace |
| Cardinality | `COUNT <prefix>` / `COUNT <start> <end>` — series count without pulling values |
| Server-side rollups | `SUM` / `AVG` / `MIN` / `MAX` over a prefix or range |
| Storage efficiency | LZ77 block compression on SSTables |
| Fast existence checks | Bloom filter per SSTable |

The earlier "what's missing" list is gone: TTL, INCR, PREFIX, COUNT, SUM/AVG/MIN/MAX, and per-collection default TTL are all merged.

## Key schema for time series

Encode the timestamp into the key so lexicographic order matches time order:

```
<metric>:<zero-padded-epoch-seconds>      e.g.   pi.cpu.user:0001748169600
```

Fixed-width zero-padding is what makes the LSM ordered scans line up with numeric time. With that, a metric's history is a contiguous key range, and a time window is a `RANGE` over two boundary keys.

## Worked end-to-end flow

With collections, retention is declared once at collection creation — individual writes use plain `MSET` and the default TTL is applied automatically.

```text
# Create a collection with 24h default TTL (do this once at startup)
rustikli> CREATE COLLECTION metrics 86400
OK

# Switch to it
rustikli> USE metrics
OK

# Ingest three samples — no per-key TTL needed, the collection default applies
rustikli> MSET pi.cpu.user:0001748169600 10 pi.cpu.user:0001748169660 20 pi.cpu.user:0001748169720 30
OK

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

Counter-style metrics use `INCR` instead of `WRITE` (TTL applied on first create):

```text
rustikli> INCR pi.http.requests:0001748169600
1
rustikli> INCR pi.http.requests:0001748169600
2
```

You can also create **separate collections per metric family** with different retention windows:

```text
rustikli> CREATE COLLECTION cpu 86400        # 24h retention
rustikli> CREATE COLLECTION memory 604800    # 7-day retention
rustikli> SHOW COLLECTIONS
cpu     86400
memory  604800
segment 0
```

Each collection has its own keyspace, WAL, and SSTable files — compaction and TTL expiry run independently.

## Ingestion and visualization: the telemetry gateway

rustikv speaks a custom binary protocol (BFFP), so no off-the-shelf collector or
dashboard talks to it directly. The companion project
**[rustikv-telemetry-gateway](https://github.com/SilvioPilato/rustikv-telemetry-gateway)**
bridges both ends:

- **Ingest:** accepts **Graphite plaintext** (`metric.path value timestamp`) from
  any collector (collectd, Telegraf) and writes batched `MSET`s. With a
  per-collection default TTL set at startup, the gateway no longer needs to pass
  a TTL on each write — it issues `USE <collection>` once per connection and
  plain `MSET`s carry the collection's retention automatically, building exactly
  the `<metric>:<padded-ts>` keys above.
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
TTL  →  INCR  →  PREFIX  →  COUNT  →  SUM/AVG/MIN/MAX  →  Collections + per-collection TTL        ✅ all shipped
```

The first two unlocked the core telemetry patterns (bounded retention, counters);
PREFIX/COUNT made namespaces ergonomic; server-side aggregation removed the
client-side scan as the scalability bottleneck; collections give each metric
family its own isolated keyspace with retention declared once at the collection
level rather than repeated on every write.
