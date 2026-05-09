# Multi-EventLoop TCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `rustikv`'s thread-per-connection TCP server with a single-acceptor + N-worker-EventLoop architecture built on the `mio-runtime` crate.

**Architecture:** One blocking acceptor thread owns the `TcpListener`, round-robins each accepted stream to a worker via `mpsc::Sender<TcpStream>` + `mio_runtime::Waker::wake()`. Each worker owns its own `EventLoop`, `Registry`, and `HashMap<Token, Connection>` and runs single-threaded; once a connection lands on worker K it lives there for life. `Connection` holds incremental BFFP parse state. Per-worker 100 ms periodic timer sweeps idle connections (mio-runtime's wheel is 512 ms-bounded).

**Tech Stack:** Rust 2024 edition, `mio` (transitive via `mio-runtime`), `mio-runtime` (sibling repo, git rev-pinned), `std::sync::mpsc` for cross-thread handoff, `std::thread::available_parallelism()` for default worker count.

**Spec:** [docs/superpowers/specs/2026-05-03-multi-eventloop-tcp-server-design.md](../specs/2026-05-03-multi-eventloop-tcp-server-design.md)

---

## File Structure

**New files:**

- [src/server/mod.rs](../../../src/server/mod.rs) — `Server`, `ServerConfig`, public re-exports
- [src/server/acceptor.rs](../../../src/server/acceptor.rs) — Acceptor thread loop + handoff logic
- [src/server/worker.rs](../../../src/server/worker.rs) — `Worker`, `WorkerHandler`, `EventHandler` impl
- [src/server/connection.rs](../../../src/server/connection.rs) — `Connection`, `ParseState`, `ConnectionAction`, read/write state machine
- [src/server/dispatch.rs](../../../src/server/dispatch.rs) — Pure `dispatch(cmd, engine, stats, cfg) -> Vec<u8>` lifted from today's [src/main.rs:212-388](../../../src/main.rs#L212-L388), plus `CompactionCfg` + `maybe_trigger_compaction`
- [tests/server_dispatch.rs](../../../tests/server_dispatch.rs) — Pure dispatch unit tests (one per Command variant)
- [tests/server_connection.rs](../../../tests/server_connection.rs) — Connection state machine tests (synthetic buffers, no sockets)
- [tests/server_multi_worker.rs](../../../tests/server_multi_worker.rs) — End-to-end multi-worker integration test
- [tests/server_idle_timeout.rs](../../../tests/server_idle_timeout.rs) — Idle timeout integration test
- [tests/server_max_connections.rs](../../../tests/server_max_connections.rs) — Connection cap integration test
- [tests/server_graceful_close.rs](../../../tests/server_graceful_close.rs) — Partial-frame close integration test

**Modified files:**

- [Cargo.toml](../../../Cargo.toml) — add `mio-runtime` git dep
- [src/lib.rs](../../../src/lib.rs) — add `pub mod server;`
- [src/main.rs](../../../src/main.rs) — slim from ~430 lines to ~30 lines: build engine + stats + `Server::start`, wait
- [src/settings.rs](../../../src/settings.rs) — add `--workers N` flag, update help text
- [README.md](../../../README.md) — note the new `--workers` flag and the multi-loop architecture

---

## Task Breakdown

### Task 1: Add `mio-runtime` dependency and module scaffolding

**Files:**
- Modify: [Cargo.toml](../../../Cargo.toml)
- Modify: [src/lib.rs](../../../src/lib.rs)
- Create: `src/server/mod.rs` (skeleton)
- Create: `src/server/acceptor.rs` (skeleton)
- Create: `src/server/worker.rs` (skeleton)
- Create: `src/server/connection.rs` (skeleton)
- Create: `src/server/dispatch.rs` (skeleton)

- [ ] **Step 1: Add `mio-runtime` git dep to `Cargo.toml`**

Insert after the existing `[dependencies]` block:

```toml
mio-runtime = { git = "https://github.com/SilvioPilato/mio-runtime.git", rev = "aaad7cbaf7ade7f8dda9963dd47edd48abfeed28" }
mio = { version = "1", features = ["net", "os-poll"] }
```

`mio-runtime` brings `mio` in transitively, but we need a direct `mio` line so we can write `mio::net::TcpStream` and `mio::Interest` in our own code without going through a re-export. The version (`1.x`) must match what `mio-runtime` declares — check `../mio-runtime/Cargo.toml` if there's any doubt.

- [ ] **Step 2: Run `cargo build` to fetch the dep and confirm it resolves**

```bash
cargo build 2>&1 | tail -20
```

Expected: `mio-runtime` and `mio` show up in the dep tree, build succeeds (no source changes yet).

- [ ] **Step 3: Create `src/server/mod.rs` skeleton**

```rust
pub mod acceptor;
pub mod connection;
pub mod dispatch;
pub mod worker;

pub use connection::{Connection, ConnectionAction, ParseState};
pub use dispatch::{CompactionCfg, dispatch, maybe_trigger_compaction};
pub use worker::{Worker, WorkerHandler};
```

(`Server` and `ServerConfig` come in Task 6.)

- [ ] **Step 4: Create empty skeleton files**

`src/server/acceptor.rs`, `src/server/worker.rs`, `src/server/connection.rs`, `src/server/dispatch.rs` — each with a single line:

```rust
// implemented in subsequent tasks
```

- [ ] **Step 5: Add `pub mod server;` to `src/lib.rs`**

Insert in alphabetical order (after `pub mod segment;`, before `pub mod settings;`).

- [ ] **Step 6: Run `cargo check`**

```bash
cargo check 2>&1 | tail -10
```

Expected: compiles clean. Empty modules are fine.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/server/
git commit -m "#70 — Add mio-runtime dep and src/server/ module skeleton"
```

---

### Task 2: Pure `dispatch` function — lift from `main.rs`, add unit tests

The dispatch function is a pure transformation from `Command` to response `Vec<u8>` with side effects against `Arc<dyn StorageEngine>` and `Arc<Stats>`. Lifting it now (before any of the loop machinery) lets us unit-test the request-handling logic in isolation against an in-memory engine — no sockets needed.

**Files:**
- Create: `src/server/dispatch.rs`
- Create: `tests/server_dispatch.rs`

- [ ] **Step 1: Write the dispatch module — `CompactionCfg` + `dispatch` + `maybe_trigger_compaction`**

`src/server/dispatch.rs`:

```rust
use crate::bffp::{Command, ResponseStatus, encode_frame};
use crate::engine::{RangeScan, StorageEngine};
use crate::lsmengine::LsmEngine;
use crate::stats::Stats;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Instant;

#[derive(Clone, Copy)]
pub struct CompactionCfg {
    pub ratio: f32,
    pub max_segment: usize,
}

pub fn dispatch(
    cmd: Command,
    engine: &Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) -> Vec<u8> {
    match cmd {
        Command::Write(key, value) => {
            if stats.compacting.load(Ordering::Relaxed) {
                stats.write_blocked_attempts.fetch_add(1, Ordering::Relaxed);
            }
            let lock_start = Instant::now();
            let result = engine.set(&key, &value);
            let lock_elapsed = lock_start.elapsed().as_millis() as u64;
            stats
                .write_blocked_total_ms
                .fetch_add(lock_elapsed, Ordering::Relaxed);
            match result {
                Ok(_) => {
                    stats.writes.fetch_add(1, Ordering::Relaxed);
                    maybe_trigger_compaction(engine.clone(), stats, cfg);
                    encode_frame(ResponseStatus::Ok, &[])
                }
                Err(err) => encode_frame(ResponseStatus::Error, &[err.to_string()]),
            }
        }
        Command::Read(key) => {
            stats.reads.fetch_add(1, Ordering::Relaxed);
            match engine.get(&key) {
                Ok(Some((_, v))) => encode_frame(ResponseStatus::Ok, &[v]),
                Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
                Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
            }
        }
        Command::Delete(key) => match engine.delete(&key) {
            Ok(Some(())) => {
                stats.deletes.fetch_add(1, Ordering::Relaxed);
                maybe_trigger_compaction(engine.clone(), stats, cfg);
                encode_frame(ResponseStatus::Ok, &[])
            }
            Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
            Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
        },
        Command::Compact => {
            if stats
                .compacting
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return encode_frame(ResponseStatus::Noop, &[]);
            }
            stats
                .last_compact_start_ms
                .store(Stats::now_ms(), Ordering::Relaxed);
            let engine_clone = Arc::clone(engine);
            let stats_clone = Arc::clone(stats);
            thread::spawn(move || {
                engine_clone.compact().unwrap();
                stats_clone
                    .last_compact_end_ms
                    .store(Stats::now_ms(), Ordering::Relaxed);
                stats_clone.compacting.store(false, Ordering::Release);
                stats_clone.compaction_count.fetch_add(1, Ordering::Relaxed);
            });
            encode_frame(ResponseStatus::Ok, &[])
        }
        Command::Stats => encode_frame(ResponseStatus::Ok, &[stats.snapshot()]),
        Command::Invalid(op_code) => encode_frame(
            ResponseStatus::Error,
            &[format!("Invalid op code: {}", op_code)],
        ),
        Command::List => match engine.list_keys() {
            Ok(keys) => encode_frame(ResponseStatus::Ok, &keys),
            Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
        },
        Command::Exists(key) => {
            if engine.exists(&key) {
                encode_frame(ResponseStatus::Ok, &[])
            } else {
                encode_frame(ResponseStatus::NotFound, &[])
            }
        }
        Command::Ping => encode_frame(ResponseStatus::Ok, &["PONG".to_string()]),
        Command::Mget(keys) => match engine.mget(keys) {
            Ok(items) => {
                let key_count = items.len() as u64;
                let flat: Vec<String> = items
                    .into_iter()
                    .flat_map(|(k, v)| match v {
                        Some(value) => [k, value],
                        None => [k, String::from("\0")],
                    })
                    .collect();
                stats.reads.fetch_add(key_count, Ordering::Relaxed);
                encode_frame(ResponseStatus::Ok, &flat)
            }
            Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
        },
        Command::Mset(items) => {
            if stats.compacting.load(Ordering::Relaxed) {
                stats.write_blocked_attempts.fetch_add(1, Ordering::Relaxed);
            }
            let item_count = items.len() as u64;
            let lock_start = Instant::now();
            let result = engine.mset(items);
            let lock_elapsed = lock_start.elapsed().as_millis() as u64;
            stats
                .write_blocked_total_ms
                .fetch_add(lock_elapsed, Ordering::Relaxed);
            match result {
                Ok(_) => {
                    stats.writes.fetch_add(item_count, Ordering::Relaxed);
                    maybe_trigger_compaction(engine.clone(), stats, cfg);
                    encode_frame(ResponseStatus::Ok, &[])
                }
                Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
            }
        }
        Command::Range(start, end) => match engine.as_any().downcast_ref::<LsmEngine>() {
            Some(lsm) => match lsm.range(&start, &end) {
                Ok(results) => {
                    let count = results.len();
                    let res: Vec<String> =
                        results.into_iter().flat_map(|(k, v)| [k, v]).collect();
                    stats.reads.fetch_add(count as u64, Ordering::Relaxed);
                    encode_frame(ResponseStatus::Ok, &res)
                }
                Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
            },
            None => encode_frame(
                ResponseStatus::Error,
                &["RANGE not supported by KV engine".to_string()],
            ),
        },
    }
}

pub fn maybe_trigger_compaction(
    engine: Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) {
    let should_compact = (cfg.ratio > 0.0
        && engine.total_bytes() > 0
        && engine.dead_bytes() as f32 / engine.total_bytes() as f32 > cfg.ratio)
        || (cfg.max_segment > 0 && engine.segment_count() > cfg.max_segment);

    if !should_compact {
        return;
    }
    if stats
        .compacting
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }
    stats
        .last_compact_start_ms
        .store(Stats::now_ms(), Ordering::Relaxed);
    let stats_clone = Arc::clone(stats);
    thread::spawn(move || {
        engine.compact().unwrap();
        stats_clone
            .last_compact_end_ms
            .store(Stats::now_ms(), Ordering::Relaxed);
        stats_clone.compacting.store(false, Ordering::Release);
        stats_clone.compaction_count.fetch_add(1, Ordering::Relaxed);
    });
}
```

This is a verbatim lift from [src/main.rs:212-430](../../../src/main.rs#L212-L430) — same logic, same Stats accounting, same compaction CAS. Two cosmetic differences only: (1) takes a borrowed `Arc<dyn StorageEngine>` and clones internally where needed, (2) takes a `CompactionCfg` struct instead of two loose `f32` + `usize` args.

- [ ] **Step 2: Add a `tests/server_dispatch.rs` test for `Command::Ping` (the simplest case)**

```rust
use rustikv::bffp::{Command, ResponseStatus, decode_response_frame};
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::server::{CompactionCfg, dispatch};
use rustikv::settings::FSyncStrategy;
use rustikv::stats::Stats;
use std::io::Cursor;
use std::sync::Arc;

fn make_engine(test_name: &str) -> (Arc<dyn StorageEngine>, tempdir::TempDir) {
    let dir = tempdir::TempDir::new(test_name).expect("tempdir");
    let engine = KVEngine::new(
        dir.path().to_str().unwrap(),
        "seg",
        1024 * 1024,
        FSyncStrategy::Never,
    )
    .expect("engine");
    (Arc::new(engine), dir)
}

fn cfg() -> CompactionCfg {
    CompactionCfg { ratio: 0.0, max_segment: 0 }
}

#[test]
fn dispatch_ping_returns_pong() {
    let (engine, _dir) = make_engine("dispatch_ping");
    let stats = Arc::new(Stats::new());

    let response = dispatch(Command::Ping, &engine, &stats, &cfg());

    let mut cursor = Cursor::new(response);
    let (status, args) = decode_response_frame(&mut cursor).expect("decode");
    assert_eq!(status, ResponseStatus::Ok);
    assert_eq!(args, vec!["PONG"]);
}
```

Note: the existing tests in `tests/` use `tempdir`, but I notice it isn't in `Cargo.toml`'s `[dependencies]`. Check the `[dev-dependencies]` block (or absence thereof) and add it if needed:

```bash
grep -n tempdir Cargo.toml tests/*.rs | head -5
```

If existing tests use `tempdir` but Cargo.toml doesn't declare it, it's likely a `[dev-dependencies]` entry that needs to be added (or the existing tests use a different temp-directory mechanism — adapt accordingly). If you have to add it:

```toml
[dev-dependencies]
tempdir = "0.3"
```

- [ ] **Step 3: Run the test — expect compile error or pass**

```bash
cargo test --test server_dispatch dispatch_ping_returns_pong 2>&1 | tail -20
```

Expected: PASS (the dispatch function is fully implemented; the test just verifies the wiring).

- [ ] **Step 4: Add a test for `Command::Write` + `Command::Read` round-trip**

```rust
#[test]
fn dispatch_write_then_read_round_trips() {
    let (engine, _dir) = make_engine("dispatch_write_read");
    let stats = Arc::new(Stats::new());

    let write_resp = dispatch(
        Command::Write("k1".to_string(), "v1".to_string()),
        &engine,
        &stats,
        &cfg(),
    );
    let mut cur = Cursor::new(write_resp);
    let (status, _) = decode_response_frame(&mut cur).expect("decode write");
    assert_eq!(status, ResponseStatus::Ok);
    assert_eq!(stats.writes.load(std::sync::atomic::Ordering::Relaxed), 1);

    let read_resp = dispatch(Command::Read("k1".to_string()), &engine, &stats, &cfg());
    let mut cur = Cursor::new(read_resp);
    let (status, args) = decode_response_frame(&mut cur).expect("decode read");
    assert_eq!(status, ResponseStatus::Ok);
    assert_eq!(args, vec!["v1"]);
}
```

- [ ] **Step 5: Add tests for `Read` of missing key, `Delete`, `List`, `Exists`, `Invalid`, `Stats`**

Six more focused tests — one per Command variant, each ~10-15 lines. Keep them independent (each builds its own engine + stats). Skip `Mget`/`Mset` (covered by their own existing tests in [tests/mget_mset.rs](../../../tests/mget_mset.rs)) and skip `Range` (LSM-only, plus existing coverage).

Skip `Compact` for now — its `thread::spawn` makes deterministic testing flaky. Covered by the existing [tests/compact_async.rs](../../../tests/compact_async.rs).

- [ ] **Step 6: Run all dispatch tests**

```bash
cargo test --test server_dispatch 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 7: Run clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add src/server/dispatch.rs tests/server_dispatch.rs Cargo.toml
git commit -m "#70 — Lift dispatch logic from main.rs into pure server::dispatch"
```

---

### Task 3: `Connection` and `ParseState` — incremental BFFP framing

**Files:**
- Create: `src/server/connection.rs`
- Create: `tests/server_connection.rs`

The trick to making this testable without sockets: factor the read/write socket I/O at the top of `on_readable` / `on_writable` and put the *parse machine* in a separate `drive_parse(&mut self, engine, stats, cfg)` method that operates on `self.read_buf` and produces bytes into `self.write_buf`. Tests preset `read_buf` directly and call `drive_parse`.

- [ ] **Step 1: Define types in `src/server/connection.rs`**

```rust
use crate::bffp::{ResponseStatus, decode_input_frame, encode_frame};
use crate::engine::StorageEngine;
use crate::record::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use crate::server::dispatch::{CompactionCfg, dispatch};
use crate::stats::Stats;
use mio::Interest;
use mio::net::TcpStream;
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::Instant;

pub const MAX_REQUEST_BYTES: usize = MAX_KEY_SIZE + MAX_VALUE_SIZE + 1024;
pub const READ_BUF_CHUNK: usize = 4096;

#[derive(Debug, PartialEq, Eq)]
pub enum ParseState {
    ReadingHeader,
    ReadingPayload { remaining: usize },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ConnectionAction {
    /// Connection still alive; if `reregister` is `Some`, caller should
    /// call `registry.reregister(&mut conn.stream, token, interest)`.
    Continue { reregister: Option<Interest> },
    /// Connection should be torn down.
    Close,
}

pub struct Connection {
    pub stream: TcpStream,
    pub read_buf: Vec<u8>,
    pub write_buf: Vec<u8>,
    pub write_offset: usize,
    pub parse_state: ParseState,
    pub last_read_at: Instant,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            read_buf: Vec::with_capacity(READ_BUF_CHUNK),
            write_buf: Vec::new(),
            write_offset: 0,
            parse_state: ParseState::ReadingHeader,
            last_read_at: Instant::now(),
        }
    }

    /// Drive the parse state machine over `read_buf`, dispatching any
    /// fully-framed Commands and appending responses to `write_buf`.
    /// Pure with respect to socket I/O — testable without a stream.
    /// Returns `false` if the connection must be torn down (e.g., oversize frame).
    pub fn drive_parse(
        &mut self,
        engine: &Arc<dyn StorageEngine>,
        stats: &Arc<Stats>,
        cfg: &CompactionCfg,
    ) -> bool {
        loop {
            match self.parse_state {
                ParseState::ReadingHeader => {
                    if self.read_buf.len() < 4 {
                        return true;
                    }
                    let frame_len = u32::from_be_bytes([
                        self.read_buf[0],
                        self.read_buf[1],
                        self.read_buf[2],
                        self.read_buf[3],
                    ]) as usize;
                    if frame_len > MAX_REQUEST_BYTES {
                        let err = encode_frame(
                            ResponseStatus::Error,
                            &[format!("Frame too large: {} bytes", frame_len)],
                        );
                        self.write_buf.extend_from_slice(&err);
                        return false; // close
                    }
                    self.parse_state = ParseState::ReadingPayload { remaining: frame_len };
                }
                ParseState::ReadingPayload { remaining } => {
                    if self.read_buf.len() < 4 + remaining {
                        return true;
                    }
                    let total = 4 + remaining;
                    let frame_bytes: Vec<u8> = self.read_buf.drain(0..total).collect();
                    let cmd = match decode_input_frame(&frame_bytes) {
                        Ok(c) => c,
                        Err(_) => return false, // close on malformed frame
                    };
                    let resp = dispatch(cmd, engine, stats, cfg);
                    self.write_buf.extend_from_slice(&resp);
                    self.parse_state = ParseState::ReadingHeader;
                }
            }
        }
    }

    /// Drain the socket into `read_buf` until WouldBlock or EOF, then drive
    /// the parse machine, then return an action indicating what (if any)
    /// reregistration is needed.
    pub fn on_readable(
        &mut self,
        engine: &Arc<dyn StorageEngine>,
        stats: &Arc<Stats>,
        cfg: &CompactionCfg,
    ) -> ConnectionAction {
        let mut tmp = [0u8; READ_BUF_CHUNK];
        loop {
            match self.stream.read(&mut tmp) {
                Ok(0) => return ConnectionAction::Close, // EOF
                Ok(n) => {
                    self.last_read_at = Instant::now();
                    self.read_buf.extend_from_slice(&tmp[..n]);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => return ConnectionAction::Close,
            }
        }

        if !self.drive_parse(engine, stats, cfg) {
            // Even on close, flush whatever error frame got buffered.
            let _ = self.flush_write();
            return ConnectionAction::Close;
        }

        if self.write_buf.is_empty() {
            ConnectionAction::Continue { reregister: None }
        } else {
            ConnectionAction::Continue {
                reregister: Some(Interest::READABLE | Interest::WRITABLE),
            }
        }
    }

    /// Drain `write_buf[write_offset..]` to the socket until WouldBlock.
    /// Returns an action indicating reregistration if fully flushed.
    pub fn on_writable(&mut self) -> ConnectionAction {
        match self.flush_write() {
            Ok(true) => ConnectionAction::Continue {
                reregister: Some(Interest::READABLE),
            },
            Ok(false) => ConnectionAction::Continue { reregister: None },
            Err(_) => ConnectionAction::Close,
        }
    }

    /// Drain pending bytes; returns Ok(true) if fully drained, Ok(false)
    /// if more bytes remain (WouldBlock encountered).
    fn flush_write(&mut self) -> io::Result<bool> {
        while self.write_offset < self.write_buf.len() {
            match self.stream.write(&self.write_buf[self.write_offset..]) {
                Ok(0) => return Ok(false),
                Ok(n) => self.write_offset += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        self.write_buf.clear();
        self.write_offset = 0;
        Ok(true)
    }
}
```

- [ ] **Step 2: Add a test that drives `drive_parse` with a single complete frame**

`tests/server_connection.rs`:

```rust
use rustikv::bffp::{Command, ResponseStatus, decode_response_frame, encode_frame_input};
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::server::{CompactionCfg, Connection, ParseState};
use rustikv::settings::FSyncStrategy;
use rustikv::stats::Stats;
use std::io::Cursor;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

/// Build a Connection wrapping one end of a connected socketpair.
/// The stream isn't actually read/written in these unit tests — drive_parse
/// only touches the buffers — but Connection requires a real `mio::net::TcpStream`.
fn make_connection() -> Connection {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let _client = TcpStream::connect(addr).expect("connect");
    let (server_std, _) = listener.accept().expect("accept");
    server_std.set_nonblocking(true).expect("nonblock");
    let mio_stream = mio::net::TcpStream::from_std(server_std);
    Connection::new(mio_stream)
}

fn make_engine(name: &str) -> (Arc<dyn StorageEngine>, tempdir::TempDir) {
    let dir = tempdir::TempDir::new(name).expect("tempdir");
    let engine = KVEngine::new(
        dir.path().to_str().unwrap(),
        "seg",
        1024 * 1024,
        FSyncStrategy::Never,
    )
    .expect("engine");
    (Arc::new(engine), dir)
}

fn cfg() -> CompactionCfg {
    CompactionCfg { ratio: 0.0, max_segment: 0 }
}

#[test]
fn drive_parse_single_complete_frame_dispatches() {
    let mut conn = make_connection();
    let (engine, _dir) = make_engine("conn_single_frame");
    let stats = Arc::new(Stats::new());

    // Inject a complete PING frame into read_buf.
    // (Use bffp encoder for inputs — verify its name in src/bffp.rs;
    // the rest of the codebase calls one variant for inputs and another for responses.)
    let frame = encode_frame_input(Command::Ping);
    conn.read_buf.extend_from_slice(&frame);

    assert!(conn.drive_parse(&engine, &stats, &cfg()));
    assert_eq!(conn.parse_state, ParseState::ReadingHeader);
    assert!(conn.read_buf.is_empty());
    assert!(!conn.write_buf.is_empty());

    let mut cur = Cursor::new(conn.write_buf.clone());
    let (status, args) = decode_response_frame(&mut cur).expect("decode");
    assert_eq!(status, ResponseStatus::Ok);
    assert_eq!(args, vec!["PONG"]);
}
```

**Note:** `encode_frame_input` may not exist with that exact name. Check `src/bffp.rs` for the function that encodes a `Command` into a request frame (it's the function the existing tests use; look at [tests/ping.rs](../../../tests/ping.rs) for a working call). If the public API only exposes raw frame helpers, build the frame manually:

```rust
let payload = vec![ /* op byte for Ping */ ];
let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
frame.extend_from_slice(&payload);
```

Use whichever path matches the existing `bffp` API.

- [ ] **Step 3: Run the test**

```bash
cargo test --test server_connection drive_parse_single_complete_frame_dispatches 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 4: Add a split-header test (header bytes arrive across two reads)**

```rust
#[test]
fn drive_parse_split_header_waits_for_more_bytes() {
    let mut conn = make_connection();
    let (engine, _dir) = make_engine("conn_split_header");
    let stats = Arc::new(Stats::new());

    // First push only 2 of the 4 header bytes.
    conn.read_buf.extend_from_slice(&[0u8, 0u8]);
    assert!(conn.drive_parse(&engine, &stats, &cfg()));
    assert_eq!(conn.parse_state, ParseState::ReadingHeader);
    assert!(conn.write_buf.is_empty());

    // Now push the rest of a complete PING frame.
    let full_frame = /* same as Step 2 */;
    conn.read_buf.extend_from_slice(&full_frame[2..]);
    assert!(conn.drive_parse(&engine, &stats, &cfg()));
    assert!(conn.read_buf.is_empty());
    assert!(!conn.write_buf.is_empty());
}
```

- [ ] **Step 5: Add a back-to-back-frames test (pipelining)**

Push two complete PING frames into `read_buf` in one go; assert `drive_parse` consumes both and emits two responses.

- [ ] **Step 6: Add an oversize-frame test**

```rust
#[test]
fn drive_parse_oversize_frame_emits_error_and_closes() {
    let mut conn = make_connection();
    let (engine, _dir) = make_engine("conn_oversize");
    let stats = Arc::new(Stats::new());

    // Header announces a frame larger than MAX_REQUEST_BYTES.
    let huge_len = (rustikv::server::MAX_REQUEST_BYTES + 1) as u32;
    conn.read_buf.extend_from_slice(&huge_len.to_be_bytes());

    let alive = conn.drive_parse(&engine, &stats, &cfg());
    assert!(!alive, "oversize frame must signal close");
    assert!(!conn.write_buf.is_empty(), "error frame must be buffered");
}
```

(`MAX_REQUEST_BYTES` and `READ_BUF_CHUNK` should be re-exported from `src/server/mod.rs` if the test references them — add `pub use connection::{MAX_REQUEST_BYTES, READ_BUF_CHUNK};` if not already.)

- [ ] **Step 7: Add a malformed-frame test**

Push 4 header bytes announcing a small payload, then push garbage bytes. `decode_input_frame` returns `Err`; `drive_parse` returns `false`.

- [ ] **Step 8: Run all connection tests, fmt, clippy**

```bash
cargo test --test server_connection 2>&1 | tail -10
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: all pass, clippy clean.

- [ ] **Step 9: Commit**

```bash
git add src/server/connection.rs src/server/mod.rs tests/server_connection.rs
git commit -m "#70 — Add Connection + ParseState + drive_parse with unit coverage"
```

---

### Task 4: `WorkerHandler` + `EventHandler` impl

**Files:**
- Modify: `src/server/worker.rs`
- (No new tests — `WorkerHandler` is exercised by the integration tests in Task 10. Unit-testing it requires constructing a `mio_runtime::Registry`, which the runtime crate doesn't expose a public constructor for.)

- [ ] **Step 1: Implement `WorkerHandler` in `src/server/worker.rs`**

```rust
use crate::engine::StorageEngine;
use crate::server::connection::{Connection, ConnectionAction};
use crate::server::dispatch::CompactionCfg;
use crate::stats::Stats;
use mio::Interest;
use mio_runtime::{
    EventHandler, EventLoop, ReadyState, Registry, StopHandle, TimerId, Token, Waker,
};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

const SWEEP_INTERVAL: Duration = Duration::from_millis(100);
const TIMER_WHEEL_CAPACITY: Duration = Duration::from_millis(512);

pub struct WorkerHandler {
    connections: HashMap<Token, Connection>,
    next_token: usize,
    incoming: Receiver<mio::net::TcpStream>,
    engine: Arc<dyn StorageEngine>,
    stats: Arc<Stats>,
    compaction_cfg: CompactionCfg,
    idle_timeout: Option<Duration>,
    sweep_timer: Option<TimerId>,
}

impl WorkerHandler {
    pub fn new(
        incoming: Receiver<mio::net::TcpStream>,
        engine: Arc<dyn StorageEngine>,
        stats: Arc<Stats>,
        compaction_cfg: CompactionCfg,
        idle_timeout: Option<Duration>,
    ) -> Self {
        Self {
            connections: HashMap::new(),
            next_token: 1,
            incoming,
            engine,
            stats,
            compaction_cfg,
            idle_timeout,
            sweep_timer: None,
        }
    }

    fn close(&mut self, registry: &Registry, token: Token) {
        if let Some(mut conn) = self.connections.remove(&token) {
            let _ = registry.deregister(&mut conn.stream);
            self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl EventHandler for WorkerHandler {
    fn on_event(&mut self, registry: &Registry, token: Token, ready: ReadyState) {
        let conn = match self.connections.get_mut(&token) {
            Some(c) => c,
            None => return, // stale event after close
        };

        let mut close_after = false;
        let mut reregister: Option<Interest> = None;

        if ready.readable() {
            match conn.on_readable(&self.engine, &self.stats, &self.compaction_cfg) {
                ConnectionAction::Close => close_after = true,
                ConnectionAction::Continue { reregister: r } => reregister = r,
            }
        }

        // Opportunistic write: if on_readable buffered output, attempt to drain
        // before yielding back to the event loop.
        if !close_after && !conn.write_buf.is_empty() {
            match conn.on_writable() {
                ConnectionAction::Close => close_after = true,
                ConnectionAction::Continue { reregister: r } => reregister = r.or(reregister),
            }
        }

        if !close_after && ready.writable() && !conn.write_buf.is_empty() {
            match conn.on_writable() {
                ConnectionAction::Close => close_after = true,
                ConnectionAction::Continue { reregister: r } => reregister = r.or(reregister),
            }
        }

        if let Some(interest) = reregister {
            if let Err(_) = registry.reregister(&mut conn.stream, token, interest) {
                close_after = true;
            }
        }

        if close_after {
            self.close(registry, token);
        }
    }

    fn on_timer(&mut self, registry: &Registry, timer_id: TimerId) {
        if Some(timer_id) != self.sweep_timer {
            return;
        }
        let now = Instant::now();
        let timeout = match self.idle_timeout {
            Some(t) => t,
            None => return,
        };
        let stale: Vec<Token> = self
            .connections
            .iter()
            .filter(|(_, c)| now.duration_since(c.last_read_at) > timeout)
            .map(|(t, _)| *t)
            .collect();
        for token in stale {
            self.close(registry, token);
        }
        self.sweep_timer = Some(registry.insert_timer(SWEEP_INTERVAL));
    }

    fn on_wake(&mut self, registry: &Registry) {
        while let Ok(mut stream) = self.incoming.try_recv() {
            let token = Token(self.next_token);
            self.next_token += 1;
            if registry
                .register(&mut stream, token, Interest::READABLE)
                .is_err()
            {
                self.stats.active_connections.fetch_sub(1, Ordering::Relaxed);
                continue;
            }
            self.connections.insert(token, Connection::new(stream));
        }
    }
}

pub struct Worker {
    pub event_loop: EventLoop,
    pub handler: WorkerHandler,
    pub waker: Waker,
    pub stop: StopHandle,
}

impl Worker {
    pub fn build(
        incoming: Receiver<mio::net::TcpStream>,
        engine: Arc<dyn StorageEngine>,
        stats: Arc<Stats>,
        compaction_cfg: CompactionCfg,
        idle_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        let event_loop = EventLoop::new(TIMER_WHEEL_CAPACITY)?;
        let waker = event_loop.waker();
        let stop = event_loop.stop_handle();
        let handler = WorkerHandler::new(incoming, engine, stats, compaction_cfg, idle_timeout);
        Ok(Self { event_loop, handler, waker, stop })
    }

    /// Consumes self, runs the event loop on the current thread.
    pub fn run(mut self) -> io::Result<()> {
        // Schedule the first sweep tick if idle timeouts are enabled.
        // We need to do this inside `run` because `Registry` only exists
        // during a callback. Workaround: schedule from the first on_wake.
        // Cleaner: pass `&Registry` to a one-shot setup hook, but the
        // mio-runtime EventHandler trait doesn't have one. So we lazy-init
        // in on_wake (always called at least once when the acceptor wakes
        // us with the first connection) — see Task 9 for the lazy init.
        self.event_loop.run(&mut self.handler)
    }
}
```

**Note:** Sweep timer scheduling is deferred to Task 9 — `on_timer` is wired up here but `sweep_timer` stays `None` until Task 9 implements lazy initialization in `on_wake`. The behavior is correct in both states (no sweep means no idle disconnects).

- [ ] **Step 2: Update `src/server/mod.rs` to re-export `Worker`, `WorkerHandler`**

Already done in Task 1's skeleton — verify the line is uncommented:

```rust
pub use worker::{Worker, WorkerHandler};
```

- [ ] **Step 3: Add MAX_REQUEST_BYTES re-export if not already**

```rust
pub use connection::{Connection, ConnectionAction, MAX_REQUEST_BYTES, ParseState, READ_BUF_CHUNK};
```

- [ ] **Step 4: `cargo check` to verify the worker compiles**

```bash
cargo check 2>&1 | tail -20
```

Expected: compiles. If it doesn't (e.g., missing trait import, signature mismatch with `mio_runtime::EventHandler`), fix until clean. Refer to `../mio-runtime/src/handler.rs` for the exact trait signature.

- [ ] **Step 5: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/server/worker.rs src/server/mod.rs
git commit -m "#70 — Implement WorkerHandler EventHandler impl and Worker::build"
```

---

### Task 5: Acceptor thread

**Files:**
- Modify: `src/server/acceptor.rs`

- [ ] **Step 1: Implement the acceptor in `src/server/acceptor.rs`**

```rust
use crate::bffp::{ResponseStatus, encode_frame};
use crate::stats::Stats;
use mio_runtime::Waker;
use std::io::{self, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::time::Duration;

pub struct Acceptor {
    pub listener: TcpListener,
    pub senders: Vec<Sender<mio::net::TcpStream>>,
    pub wakers: Vec<Waker>,
    pub stats: Arc<Stats>,
    pub max_connections: usize,
    pub stop: Arc<AtomicBool>,
}

impl Acceptor {
    /// Run the acceptor loop until `stop` flips to true.
    /// Blocks the calling thread; intended to be invoked inside
    /// `thread::spawn`.
    pub fn run(self) -> io::Result<()> {
        // Set a short read timeout on the listener so the accept loop can
        // periodically check `self.stop`. Without this, a long-idle server
        // would block in accept() and never observe the stop flag.
        // (Connection accept reads the kernel's pending-connection queue;
        // set_nonblocking + busy-loop is wasteful, so we use a timeout
        // via set_read_timeout on a clone — except std::net::TcpListener
        // doesn't expose set_read_timeout. The clean alternative is:
        // set_nonblocking(true) and sleep briefly between WouldBlock
        // returns, OR add a self-pipe trick. For simplicity, use
        // set_nonblocking(true) + 100ms sleep on WouldBlock.)
        self.listener.set_nonblocking(true)?;

        let mut next: usize = 0;
        let n = self.senders.len();

        while !self.stop.load(Ordering::Relaxed) {
            match self.listener.accept() {
                Ok((std_stream, _peer)) => {
                    let active = self.stats.active_connections.load(Ordering::Relaxed);
                    if self.max_connections > 0 && active >= self.max_connections as i64 {
                        let mut s = std_stream;
                        let _ = s.write_all(&encode_frame(
                            ResponseStatus::Error,
                            &[format!(
                                "Rejecting client, max connections reached: {}",
                                active
                            )],
                        ));
                        continue;
                    }

                    if std_stream.set_nonblocking(true).is_err() {
                        continue;
                    }
                    let mio_stream = mio::net::TcpStream::from_std(std_stream);
                    self.stats
                        .active_connections
                        .fetch_add(1, Ordering::Relaxed);

                    if self.senders[next].send(mio_stream).is_err() {
                        // worker thread died; roll back the count
                        self.stats
                            .active_connections
                            .fetch_sub(1, Ordering::Relaxed);
                        eprintln!("worker {} channel closed", next);
                    } else if self.wakers[next].wake().is_err() {
                        eprintln!("worker {} waker failed", next);
                    }

                    next = (next + 1) % n;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => {
                    eprintln!("accept error: {}", e);
                }
            }
        }
        Ok(())
    }
}
```

**Note on the 50 ms WouldBlock sleep:** the alternative to a sleep loop is a self-pipe / event-driven acceptor (register the listener with its own mio::Poll). Either is fine; the sleep keeps the implementation trivial and the wakeup latency for shutdown is at most 50 ms — acceptable for this scope.

- [ ] **Step 2: Add `pub use acceptor::Acceptor;` to `src/server/mod.rs`**

- [ ] **Step 3: `cargo check`**

```bash
cargo check 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 4: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/server/acceptor.rs src/server/mod.rs
git commit -m "#70 — Implement Acceptor thread with round-robin handoff"
```

---

### Task 6: `Server::start` orchestration

**Files:**
- Modify: `src/server/mod.rs`

- [ ] **Step 1: Add `Server` and `ServerConfig` to `src/server/mod.rs`**

Append at the bottom (after the `pub use` re-exports):

```rust
use crate::engine::StorageEngine;
use crate::stats::Stats;
use mio_runtime::StopHandle;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub struct ServerConfig {
    pub addr: String,
    pub workers: usize,
    pub max_connections: usize,
    pub read_timeout: Option<Duration>,
    pub compaction: CompactionCfg,
    pub addr_file_dir: String, // existing behavior: write resolved addr to <dir>/server.addr
}

pub struct Server {
    workers: Vec<JoinHandle<io::Result<()>>>,
    acceptor: JoinHandle<io::Result<()>>,
    worker_stops: Vec<StopHandle>,
    acceptor_stop: Arc<AtomicBool>,
}

impl Server {
    pub fn start(
        cfg: ServerConfig,
        engine: Arc<dyn StorageEngine>,
        stats: Arc<Stats>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(&cfg.addr)?;
        let actual_addr = listener.local_addr()?;
        let connect_addr = match actual_addr {
            SocketAddr::V4(a) if a.ip().is_unspecified() => {
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, a.port()))
            }
            SocketAddr::V6(a) if a.ip().is_unspecified() => {
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, a.port(), 0, 0))
            }
            other => other,
        };
        let addr_file = format!("{}/server.addr", cfg.addr_file_dir);
        std::fs::write(&addr_file, connect_addr.to_string())?;

        let mut senders = Vec::with_capacity(cfg.workers);
        let mut wakers = Vec::with_capacity(cfg.workers);
        let mut worker_stops = Vec::with_capacity(cfg.workers);
        let mut workers = Vec::with_capacity(cfg.workers);

        for i in 0..cfg.workers {
            let (tx, rx) = mpsc::channel::<mio::net::TcpStream>();
            let worker = Worker::build(
                rx,
                Arc::clone(&engine),
                Arc::clone(&stats),
                cfg.compaction,
                cfg.read_timeout,
            )?;
            senders.push(tx);
            wakers.push(worker.waker.clone());
            worker_stops.push(worker.stop.clone());

            let handle = thread::Builder::new()
                .name(format!("rustikv-worker-{i}"))
                .spawn(move || worker.run())?;
            workers.push(handle);
        }

        let acceptor_stop = Arc::new(AtomicBool::new(false));
        let acceptor = Acceptor {
            listener,
            senders,
            wakers,
            stats: Arc::clone(&stats),
            max_connections: cfg.max_connections,
            stop: Arc::clone(&acceptor_stop),
        };
        let acceptor_handle = thread::Builder::new()
            .name("rustikv-acceptor".to_string())
            .spawn(move || acceptor.run())?;

        Ok(Server {
            workers,
            acceptor: acceptor_handle,
            worker_stops,
            acceptor_stop,
        })
    }

    /// Block the calling thread until the acceptor thread finishes
    /// (i.e., the process is asked to exit). For now this is a join on the
    /// acceptor; workers shut down when the process exits.
    pub fn wait(self) -> io::Result<()> {
        self.acceptor
            .join()
            .map_err(|_| io::Error::other("acceptor join failed"))?
    }

    /// Signal acceptor + all workers to stop. Useful for tests.
    pub fn stop(&self) {
        self.acceptor_stop.store(true, Ordering::Relaxed);
        for s in &self.worker_stops {
            s.stop();
        }
    }
}
```

- [ ] **Step 2: `cargo check`**

```bash
cargo check 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 3: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/server/mod.rs
git commit -m "#70 — Add Server::start orchestration over acceptor + workers"
```

---

### Task 7: `--workers` CLI flag

**Files:**
- Modify: `src/settings.rs`

- [ ] **Step 1: Add `workers: Option<usize>` field**

In `Settings`, after `pub max_connections: usize,`:

```rust
    pub workers: Option<usize>,
```

In the default-construction block in `get_from_args`, after `max_connections: 1000,`:

```rust
            workers: None,
```

- [ ] **Step 2: Parse `-w` / `--workers` in the args loop**

In the `match arg.as_str()` block (after `-mc` / `--max-connections`):

```rust
                "-w" | "--workers" => {
                    if let Some(value) = args_iter.next() {
                        let n: usize = value.parse().expect("Invalid workers count provided");
                        if n == 0 {
                            panic!("--workers must be > 0");
                        }
                        settings.workers = Some(n);
                    }
                }
```

- [ ] **Step 3: Update `print_help`**

Add after the `-mc` block:

```rust
        println!("  -w, --workers <N>");
        println!(
            "                         Worker thread count (default: available_parallelism())"
        );
```

- [ ] **Step 4: `cargo check`**

```bash
cargo check 2>&1 | tail -10
```

Expected: clean. (`main.rs` will still reference the old field set, but since `workers` is additive it doesn't break the build.)

- [ ] **Step 5: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/settings.rs
git commit -m "#70 — Add --workers CLI flag"
```

---

### Task 8: Wire `main.rs` to `Server::start` and delete the old TCP loop

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Replace the body of `main` with the slimmed version**

Replace the entire contents of `src/main.rs` with:

```rust
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::leveled::Leveled;
use rustikv::lsmengine::LsmEngine;
use rustikv::server::{CompactionCfg, Server, ServerConfig};
use rustikv::settings::{BlockCompression, EngineType, Settings, StorageStrategy};
use rustikv::size_tiered::SizeTiered;
use rustikv::stats::Stats;
use std::io;
use std::sync::Arc;
use std::time::Duration;

fn main() -> io::Result<()> {
    let settings = Settings::get_from_args();

    let database: Arc<dyn StorageEngine> = match settings.engine {
        EngineType::KV => match KVEngine::from_dir(
            &settings.db_file_path,
            &settings.db_name,
            settings.max_segment_bytes,
            settings.sync_strategy,
        )? {
            Some(db) => Arc::new(db),
            None => Arc::new(KVEngine::new(
                &settings.db_file_path,
                &settings.db_name,
                settings.max_segment_bytes,
                settings.sync_strategy,
            )?),
        },
        EngineType::Lsm => {
            let block_size_bytes = (settings.block_size_kb * 1024) as usize;
            let block_compression_enabled =
                matches!(settings.block_compression, BlockCompression::Lz77);
            let strategy: Box<dyn rustikv::storage_strategy::StorageStrategy> =
                match settings.storage_strategy {
                    StorageStrategy::SizeTiered => Box::new(SizeTiered::load_from_dir(
                        &settings.db_file_path,
                        &settings.db_name,
                        4,
                        32,
                        block_size_bytes,
                        block_compression_enabled,
                    )?),
                    StorageStrategy::Leveled => Box::new(Leveled::load_from_dir(
                        &settings.db_file_path,
                        &settings.db_name,
                        settings.leveled_num_levels,
                        settings.leveled_l0_threshold,
                        settings.leveled_l1_max_bytes,
                        block_size_bytes,
                        block_compression_enabled,
                    )?),
                };

            Arc::new(LsmEngine::from_dir(
                &settings.db_file_path,
                &settings.db_name,
                settings.max_segment_bytes as usize,
                strategy,
                block_size_bytes,
                block_compression_enabled,
            )?)
        }
    };

    let stats = Arc::new(Stats::new());

    let workers = settings
        .workers
        .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(4);

    let read_timeout = if settings.read_timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(settings.read_timeout_secs))
    };

    let cfg = ServerConfig {
        addr: settings.tcp_addr.clone(),
        workers,
        max_connections: settings.max_connections,
        read_timeout,
        compaction: CompactionCfg {
            ratio: settings.compaction_ratio,
            max_segment: settings.compaction_max_segment,
        },
        addr_file_dir: settings.db_file_path.clone(),
    };

    let server = Server::start(cfg, database, stats)?;
    server.wait()
}
```

The 250-line `handle_stream` / `handle_stream_inner` / `maybe_trigger_compaction` block from today's `main.rs` is fully gone — its logic now lives in `src/server/dispatch.rs` (committed in Task 2).

- [ ] **Step 2: `cargo build`**

```bash
cargo build 2>&1 | tail -20
```

Expected: clean build.

- [ ] **Step 3: Run the existing test suite — every existing test must still pass**

```bash
cargo test 2>&1 | tail -30
```

Expected: all existing tests pass. The TCP integration tests (`compact_tcp.rs`, `ping.rs`, `concurrency.rs`, `mget_mset.rs`, `compact_async.rs`) spawn the binary as a subprocess and talk to it over TCP — these are the real validation that the new server is behaviorally compatible.

If any test fails, **stop and diagnose before continuing.** Common causes:
- Wrong `Interest` flags (e.g., not reregistering after a write completes)
- Connection state corruption between back-to-back frames
- `active_connections` not decrementing on close (shows up as max-connections rejection in tests that open many sockets)

- [ ] **Step 4: Smoke-test by hand**

```bash
mkdir -p /tmp/rustikv70 && cargo run --bin rustikv -- /tmp/rustikv70 --tcp 127.0.0.1:6666 &
sleep 1
echo -ne '\x00\x00\x00\x01\x09' | nc -q 1 127.0.0.1 6666 | xxd
# Expected: a Pong response frame (op 0x09 is Ping; verify in src/bffp.rs)
kill %1
```

(If `nc` isn't installed on Windows, use `cargo run --bin rustikli -- 127.0.0.1:6666` and type `PING`.)

- [ ] **Step 5: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "#70 — Wire main.rs to Server::start, drop old thread-per-conn loop"
```

---

### Task 9: Idle timeout sweep — lazy init in `on_wake`

**Files:**
- Modify: `src/server/worker.rs`

- [ ] **Step 1: Add lazy sweep-timer initialization to `WorkerHandler::on_wake`**

At the top of the `on_wake` method, before the `while let Ok(...)` loop:

```rust
fn on_wake(&mut self, registry: &Registry) {
    // Lazy-init the sweep timer on the first wake. We can't schedule it
    // before run() starts because Registry is only available inside callbacks.
    if self.sweep_timer.is_none() && self.idle_timeout.is_some() {
        self.sweep_timer = Some(registry.insert_timer(SWEEP_INTERVAL));
    }

    while let Ok(mut stream) = self.incoming.try_recv() {
        // ... existing body unchanged ...
    }
}
```

- [ ] **Step 2: `cargo build`**

```bash
cargo build 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 3: Run all existing tests — they must still pass**

```bash
cargo test 2>&1 | tail -20
```

Expected: all pass. (No idle-timeout integration test yet — that's Task 10.)

- [ ] **Step 4: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add src/server/worker.rs
git commit -m "#70 — Lazy-init idle sweep timer on first on_wake"
```

---

### Task 10: New integration tests

**Files:**
- Create: `tests/server_multi_worker.rs`
- Create: `tests/server_idle_timeout.rs`
- Create: `tests/server_max_connections.rs`
- Create: `tests/server_graceful_close.rs`

These follow the existing pattern from [tests/compact_tcp.rs:13-60](../../../tests/compact_tcp.rs#L13-L60) — spawn the binary as a subprocess, read the `server.addr` file to find the port, talk over a real socket. Reuse the `ServerProcess` helper pattern; you may want to extract it into a small `tests/common.rs` module if duplication becomes painful, but copy-paste is also fine for this iteration.

- [ ] **Step 1: `tests/server_multi_worker.rs` — N=4 workers, 100 concurrent clients**

Each client thread opens its own TCP connection, performs 10 `WRITE`/`READ` pairs against unique keys, asserts every response is correct, then closes. After all clients join, send a `STATS` command and assert `active_connections=0`.

```rust
// Sketch — fill in per the existing ServerProcess pattern.
#[test]
fn multi_worker_handles_concurrent_clients() {
    let server = ServerProcess::start_with_workers("/tmp/rustikv70_mw", 4);
    let n_clients = 100;
    let ops_per_client = 10;
    let mut handles = vec![];
    for client_id in 0..n_clients {
        let addr = server.connect_addr.clone();
        handles.push(std::thread::spawn(move || {
            let mut stream = TcpStream::connect(&addr).expect("connect");
            for op in 0..ops_per_client {
                let key = format!("c{}_k{}", client_id, op);
                let value = format!("v{}_{}", client_id, op);
                // WRITE
                let frame = encode_input_frame(Command::Write(key.clone(), value.clone()));
                stream.write_all(&frame).expect("write");
                let resp = read_response(&mut stream).expect("read resp");
                assert_eq!(resp.0, ResponseStatus::Ok);
                // READ
                let frame = encode_input_frame(Command::Read(key));
                stream.write_all(&frame).expect("write");
                let resp = read_response(&mut stream).expect("read resp");
                assert_eq!(resp.0, ResponseStatus::Ok);
                assert_eq!(resp.1, vec![value]);
            }
        }));
    }
    for h in handles { h.join().expect("join"); }
    // small grace period for stats accounting
    std::thread::sleep(Duration::from_millis(200));
    // STATS check via a fresh connection
    let mut stream = TcpStream::connect(&server.connect_addr).expect("connect stats");
    let frame = encode_input_frame(Command::Stats);
    stream.write_all(&frame).expect("write stats");
    let resp = read_response(&mut stream).expect("read stats");
    let snapshot = &resp.1[0];
    // active_connections should be 1 (just this connection); writes/reads should
    // total at least n_clients * ops_per_client respectively.
    assert!(snapshot.contains("active_connections=1"));
}
```

`ServerProcess::start_with_workers(dir, n)` adds `--workers N` to the spawn args.

- [ ] **Step 2: Run multi_worker test**

```bash
cargo test --test server_multi_worker 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 3: `tests/server_idle_timeout.rs` — connection closed after idle**

Spawn the server with `--read-timeout-secs 1`. Open a connection, write nothing for 1.5 s, then attempt a read. Server should have closed the socket; client read returns 0 bytes.

```rust
#[test]
fn idle_connection_is_closed_after_timeout() {
    let server = ServerProcess::start_with_args(
        "/tmp/rustikv70_idle",
        &["--workers", "1", "--read-timeout-secs", "1"],
    );
    let mut stream = TcpStream::connect(&server.connect_addr).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(3))).expect("rto");

    std::thread::sleep(Duration::from_millis(1500));

    let mut buf = [0u8; 16];
    let n = stream.read(&mut buf).expect("read after idle");
    assert_eq!(n, 0, "server should have closed the idle connection");
}
```

- [ ] **Step 4: Run idle_timeout test**

```bash
cargo test --test server_idle_timeout 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 5: `tests/server_max_connections.rs` — third connection rejected**

```rust
#[test]
fn third_connection_rejected_with_error_frame() {
    let server = ServerProcess::start_with_args(
        "/tmp/rustikv70_maxconn",
        &["--workers", "1", "--max-connections", "2"],
    );
    let _c1 = TcpStream::connect(&server.connect_addr).expect("c1");
    let _c2 = TcpStream::connect(&server.connect_addr).expect("c2");
    // small grace for the acceptor to bump active_connections
    std::thread::sleep(Duration::from_millis(100));

    let mut c3 = TcpStream::connect(&server.connect_addr).expect("c3 connect");
    let resp = read_response(&mut c3).expect("read rejection");
    assert_eq!(resp.0, ResponseStatus::Error);
    assert!(resp.1[0].contains("max connections reached"));
}
```

- [ ] **Step 6: Run max_connections test**

```bash
cargo test --test server_max_connections 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 7: `tests/server_graceful_close.rs` — partial frame doesn't leak**

```rust
#[test]
fn partial_frame_close_decrements_active_connections() {
    let server = ServerProcess::start_with_args("/tmp/rustikv70_partial", &["--workers", "1"]);

    {
        let mut stream = TcpStream::connect(&server.connect_addr).expect("connect");
        // Send 3 of 4 header bytes, then drop.
        stream.write_all(&[0u8, 0u8, 0u8]).expect("partial write");
        // Stream drops here -> server sees EOF, must decrement counter.
    }
    std::thread::sleep(Duration::from_millis(200));

    let mut stats_stream = TcpStream::connect(&server.connect_addr).expect("connect stats");
    let frame = encode_input_frame(Command::Stats);
    stats_stream.write_all(&frame).expect("write stats");
    let resp = read_response(&mut stats_stream).expect("read stats");
    let snapshot = &resp.1[0];
    // Only the stats connection should be active.
    assert!(snapshot.contains("active_connections=1"),
            "expected active_connections=1, got snapshot:\n{}", snapshot);
}
```

- [ ] **Step 8: Run graceful_close test**

```bash
cargo test --test server_graceful_close 2>&1 | tail -10
```

Expected: PASS.

- [ ] **Step 9: Run the full test suite end-to-end**

```bash
cargo test 2>&1 | tail -30
```

Expected: every test passes.

- [ ] **Step 10: Clippy + fmt**

```bash
cargo fmt && cargo clippy -- -D warnings 2>&1 | tail -10
```

Expected: clean.

- [ ] **Step 11: Commit**

```bash
git add tests/server_multi_worker.rs tests/server_idle_timeout.rs \
        tests/server_max_connections.rs tests/server_graceful_close.rs
git commit -m "#70 — Add integration coverage for multi-worker, idle, max-conn, partial-close"
```

---

### Task 11: README update + final pre-commit pass + PR prep

**Files:**
- Modify: [README.md](../../../README.md)
- Modify: [TASKS.md](../../../TASKS.md) (move #32 to Closed Tasks with supersession note; #70 stays in In Progress until PR opens)
- Modify: `../mio-runtime/TASKS.md` (move #6 to Closed Tasks with realisation note) — **do this from a separate branch in the mio-runtime repo, not in this PR.**

- [ ] **Step 1: Update README.md**

Look at the existing README for the "Usage" or "CLI" section and add the `--workers N` flag with a one-line description. Also add a sentence about the multi-loop architecture in whatever section describes the server.

```bash
grep -n -A2 'max-connections\|read-timeout' README.md | head -20
```

Add `--workers` near the existing connection-related flags.

- [ ] **Step 2: Update TASKS.md**

Move `#32 — Async I/O with tokio` from **Open Tasks** to **Closed Tasks** with the note `Superseded by #70`.

- [ ] **Step 3: Run the full pre-commit checklist**

```bash
cargo fmt --check && cargo clippy -- -D warnings && cargo test
```

All three must pass clean.

- [ ] **Step 4: Commit README + TASKS update**

```bash
git add README.md TASKS.md
git commit -m "#70 — Document --workers flag, supersede #32 in TASKS"
```

- [ ] **Step 5: Move #70 from In Progress to Closed Tasks (per AGENTS.md)**

```bash
# Edit TASKS.md: move the ## #70 block from "# In Progress" to "# Closed Tasks".
# Add a placeholder PR line — actual URL gets filled in after `gh pr create`.
```

- [ ] **Step 6: Push branch and open PR**

```bash
git push -u origin 70-multi-eventloop-tcp-server
gh pr create --title "#70 — Multi-EventLoop TCP server: acceptor + worker pool on mio-runtime" \
  --body "$(cat <<'EOF'
## Summary

- Replaces thread-per-connection TCP server with a single-acceptor + N-worker-EventLoop architecture built on the `mio-runtime` sibling crate (consumed via git rev pin).
- Round-robin handoff via `mpsc::Sender<TcpStream>` + `mio_runtime::Waker::wake()`. One `EventLoop` per worker thread; connection affinity for life.
- Per-connection state machine for incremental BFFP framing (`Connection` + `ParseState`).
- Idle timeouts (#31's `--read-timeout-secs`) preserved via a per-worker 100 ms periodic sweep — `mio-runtime`'s timer wheel is 512 ms-bounded so per-connection second-scale timers aren't usable directly.
- New CLI flag `--workers N` (default `available_parallelism()`).
- Supersedes #32 (tokio-based async I/O).
- Realises mio-runtime task #6 in a multi-loop variant; mio-runtime needs no code changes.

Spec: `docs/superpowers/specs/2026-05-03-multi-eventloop-tcp-server-design.md`
Plan: `docs/superpowers/plans/2026-05-03-multi-eventloop-tcp-server.md`

## Test plan

- [x] `cargo fmt --check` clean
- [x] `cargo clippy -- -D warnings` clean
- [x] `cargo test` — full suite passes (existing TCP integration tests are the cross-check that wire-protocol behavior is preserved)
- [x] New: `tests/server_dispatch.rs` — pure dispatch unit tests
- [x] New: `tests/server_connection.rs` — Connection state machine tests (no sockets)
- [x] New: `tests/server_multi_worker.rs` — 4 workers × 100 concurrent clients × 10 ops
- [x] New: `tests/server_idle_timeout.rs` — connection closed after idle
- [x] New: `tests/server_max_connections.rs` — connection cap enforcement
- [x] New: `tests/server_graceful_close.rs` — partial-frame close cleanup

Opened via Claude
EOF
)"
```

- [ ] **Step 7: After PR opens, backfill the URL into TASKS.md**

```bash
# Edit the #70 block in TASKS.md, replacing the placeholder with:
# PR: <https://github.com/SilvioPilato/rustikv/pull/N>
git add TASKS.md
git commit -m "#70 — Backfill PR link"
git push
```

- [ ] **Step 8: Open the follow-up `mio-runtime` PR**

In the `../mio-runtime` worktree:

```bash
git checkout -b 6-realised-by-rustikv-70
# Edit ../mio-runtime/TASKS.md: move ## #6 from Open Tasks to Closed Tasks
# with the note: "Realised by rustikv #N (multi-loop variant). No mio-runtime
# code changes required — EventLoop/Registry support N independent loops by
# construction (!Send/!Clone is a per-instance constraint)."
git commit -m "#6 — Move to Closed Tasks (realised by rustikv #N, multi-loop variant)"
git push -u origin 6-realised-by-rustikv-70
gh pr create --title "#6 — Mark realised by rustikv multi-loop server" --body "..."
```

- [ ] **Step 9: After PRs merge, ask the user if they want to checkout back to `main`** (per AGENTS.md).

---

## Open questions / risks

These were tagged in the spec; flagging again here so an executor reading only the plan doesn't miss them:

1. **Worker imbalance.** Round-robin is blind to load. If imbalance shows up in benches, switch to least-loaded selection in the acceptor (track per-worker counts; pick `senders.iter().enumerate().min_by_key(|(i, _)| counts[*i])`). Out of scope for this plan.
2. **Slow-call blast radius.** A long `RANGE` on worker K stalls all of K's connections. Profile after this lands; consider offloading slow ops to a thread pool in a follow-up task.
3. **mio-runtime rev pin.** Bumping the rev is manual. A breaking change in `EventHandler` signature or `Registry` API breaks rustikv's build at bump time — fix at the bump commit.
4. **Sweep granularity.** 100 ms with K connections per worker is K `Instant` comparisons every 100 ms. At 10k connections per worker that's 100k cmps/sec — trivial CPU, but noted as the only periodic cost.
