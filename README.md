# rustikv

**⚠️ Experimental Project: This software is for experimental purposes only and is not intended for production use. Use at your own risk.**

This project implements a simple key-value store that communicates over TCP, built while reading *Designing Data-Intensive Applications*. It supports two storage engines selectable at startup:

- **KV (Bitcask)** — hash index in memory, append-only segment files, hint files for fast startup.
- **LSM** — in-memory memtable (BTreeMap) flushed to sorted string table (SSTable) segments, with sparse index and Bloom filter per segment. SSTables are written as fixed-size blocks (default 4 KB) with optional per-block LZ77 compression (`--block-size-kb`, `--block-compression`). Supports two pluggable compaction strategies:
  - **Size-tiered** (default) — groups similarly-sized SSTables into buckets and compacts when a bucket reaches a threshold. Good write throughput.
  - **Leveled** — LevelDB-style level-based compaction. L0 triggers on file count; L1+ trigger on byte budget (10× per level). Better read performance and space amplification.

On startup, if an existing database directory is provided, the server rebuilds its state from segment files so that previously stored data is available immediately.
The purpose of the project is merely didactical, but if you want to tinker with it feel free to do it.

## Server architecture

The server uses a **single-acceptor + N-worker** model. One blocking thread owns the `TcpListener` and round-robins each accepted connection to a worker via an `mpsc` channel + `Waker`. Each worker owns its own `EventLoop`, `Registry`, and `HashMap<Token, Connection>` and runs single-threaded — once a connection lands on worker K it lives there for life. Each `Connection` holds a state machine that incrementally parses BFFP frames out of a non-blocking byte stream. Built on the sibling [`mio-runtime`](https://github.com/SilvioPilato/mio-runtime) crate.

Worker count defaults to `std::thread::available_parallelism()` and can be overridden via `--workers N`. Idle connections are reaped via a per-worker 100 ms timer sweep against `--read-timeout-secs`.

## Building

To build the project, use Cargo:

```sh
cargo build
```

## Running

To run the server, provide a directory path for the database as a command-line argument. The server creates a segment file inside that directory named `<segment_name>_<timestamp>.db` (e.g., `/tmp/mydb/segment_1700000000.db`):

```sh
cargo run -- <db_directory> [options]
```

### Options

| Flag                        | Description                           | Default         |
| --------------------------- | ------------------------------------- | --------------- |
| `-t`, `--tcp`               | TCP address to listen on              | `0.0.0.0:6666`  |
| `-n`, `--name`              | Segment file name prefix              | `segment`       |
| `-msb`, `--max-segments-bytes` | Max bytes per segment before rolling | `52428800` (50MB) |
| `-fsync`, `--fsync-interval` | Fsync strategy: `always`, `never`, `every:N` (every N writes), `every:Ns` (every N seconds) | `always` |
| `-e`, `--engine`             | Storage engine: `kv` (Bitcask) or `lsm` (LSM-tree) | `kv` |
| `-cr`, `--compaction-ratio`  | Auto-compact when dead/total bytes exceeds this ratio (LSM+KV). `0.0` disables | `0.0` |
| `-cms`, `--compaction-max-segments` | Auto-compact when segment count exceeds this limit. `0` disables | `0` |
| `-ss`, `--storage-strategy`  | LSM compaction strategy: `size-tiered` or `leveled` | `size-tiered` |
| `-lnl`, `--leveled-num-levels` | Number of levels for leveled compaction | `4` |
| `-ll0`, `--leveled-l0-threshold` | L0 file count before compaction (leveled only) | `4` |
| `-ll1`, `--leveled-l1-max-bytes` | L1 max size in bytes; each subsequent level is 10× larger (leveled only) | `10485760` (10 MB) |
| `-rts`, `--read-timeout-secs` | Idle timeout per connection in seconds. Connections inactive for this long are closed by a per-worker sweep. `0` disables | `30` |
| `-mc`, `--max-connections` | Maximum concurrent connections. `0` = unlimited | `1000` |
| `-w`, `--workers` | Number of worker event loops. `0` = auto-detect via `available_parallelism()` | `0` (auto) |

### Examples

```sh
# Start with defaults (listens on 0.0.0.0:6666, segment prefix "segment")
cargo run -- /tmp/mydb

# Custom port and segment name
cargo run -- /tmp/mydb --tcp 127.0.0.1:7777 --name mydata
```

## Testing

To run all tests:

```sh
cargo test
```

## Client (rustikli)

`rustikli` is an interactive REPL client for the server:

```sh
cargo run --bin rustikli -- [--host <addr>]
```

| Flag | Description | Default |
|------|-------------|---------|
| `-h`, `--host` | Server TCP address | `127.0.0.1:6666` |

Once connected, type commands directly:

```text
rustikli> WRITE mykey hello world
rustikli> READ mykey
hello world
rustikli> EXISTS mykey
rustikli> LIST
rustikli> DELETE mykey
rustikli> STATS
rustikli> COMPACT
rustikli> QUIT
```

## Benchmark tool (kvbench)

`kvbench` connects to the server, runs write and read phases, and reports throughput and latency stats.

```sh
cargo run --bin kvbench -- [OPTIONS]
```

| Flag | Description | Default |
| ---- | ----------- | ------- |
| `-h`, `--host` | Server TCP address | `127.0.0.1:6666` |
| `-n`, `--count` | Number of keys | `10000` |
| `-s`, `--value-size` | Value size in bytes | `100` |
| `-m`, `--miss-ratio` | Fraction of reads targeting non-existent keys (0.0–1.0) | `0.0` |
| `--mode` | `sequential` or `concurrent` | `sequential` |
| `--writers` | Writer threads (concurrent mode only) | `4` |
| `--readers` | Reader threads (concurrent mode only) | `4` |

### Sequential mode

Writes N keys then reads them back on a single connection. Use `--miss-ratio` to mix in non-existent keys and observe NOT_FOUND rate and its impact on read latency (especially visible on LSM, where Bloom filters short-circuit the lookup).

```sh
cargo run --bin kvbench -- -n 10000 -s 100
cargo run --bin kvbench -- -n 10000 --miss-ratio 0.3
```

### Concurrent mode

Spawns writer and reader threads simultaneously, each on its own TCP connection. All threads start at a barrier so load is applied at the same instant. Reports write and read stats separately, plus an aggregate throughput over the shared wall time.

```sh
cargo run --bin kvbench -- -n 10000 --mode concurrent --writers 4 --readers 4
```

Reads in concurrent mode may hit keys not yet written — those count as misses and reflect real contention behaviour under mixed load.

### Comparing engines

The most instructive comparison is KV vs LSM with `--fsync-interval never` to isolate engine behaviour from I/O cost:

```sh
# Terminal 1 — KV engine
cargo run -- C:/Temp/db-kv --engine kv --fsync-interval never
# Terminal 1 — LSM engine
cargo run -- C:/Temp/db-lsm --engine lsm --fsync-interval never

# Terminal 2 — benchmark
cargo run --bin kvbench -- -n 10000 -s 100
```

Expected pattern: writes are roughly equal (both append-only); KV reads are ~2× faster (O(1) hash index vs SSTable merge scan — read amplification from DDIA Ch. 3).

## Commands

The server uses a **binary length-prefixed protocol** (not plain text). Each request is a frame: a 4-byte big-endian payload length, followed by a 1-byte op code, followed by op-specific fields. Responses use the same framing with a 1-byte status byte.

### Supported commands

| Command | Op code | Description |
|---------|---------|-------------|
| `READ <key>` | 1 | Returns the value for `key`, or NOT_FOUND if absent |
| `WRITE <key> <value>` | 2 | Stores `value` under `key`. Values may contain spaces |
| `DELETE <key>` | 3 | Removes `key`. Returns NOT_FOUND if key does not exist |
| `COMPACT` | 4 | Triggers background compaction. Returns NOOP if already running |
| `STATS` | 5 | Returns runtime counters as `key=value` pairs |
| `LIST` | 6 | Returns all live keys as a list of strings |
| `EXISTS <key>` | 7 | Returns OK if `key` exists, NOT_FOUND if absent. On LSM, uses the Bloom filter for fast negative lookups |
| `PING` | 8 | Returns `PONG`. Useful as a health check |
| `MGET <key1> <key2> ...` | 9 | Fetches multiple keys in one round trip. Response is a flat list of `key, value` pairs; missing keys return a null byte (`\0`) as the value |
| `MSET <k1> <v1> <k2> <v2> ...` | 10 | Writes multiple key-value pairs in one round trip |
| `RANGE <start> <end>` | 11 | **LSM only.** Returns all live key-value pairs whose keys fall in the inclusive range `[start, end]`, in sorted order. Response is a flat list of `key, value` pairs. Returns an error on the KV engine (hash index has no ordering). Returns empty if `start > end` |

### STATS fields

* `compacting` — whether compaction is currently running
* `compaction_count` — number of completed compactions
* `last_compact_start_ms` / `last_compact_end_ms` — timestamps of last compaction
* `write_blocked_attempts` — writes that arrived during compaction
* `write_blocked_total_ms` — total time writes spent waiting for the lock
* `reads` / `writes` / `deletes` — operation counters
* `active_connections` — current number of connected clients
