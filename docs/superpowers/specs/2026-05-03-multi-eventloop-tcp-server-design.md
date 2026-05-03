# Multi-EventLoop TCP Server: Acceptor + Worker Pool on `mio-runtime`

**Date:** 2026-05-03
**Scope:** Replace `rustikv`'s thread-per-connection TCP server with a single-acceptor + N-worker-EventLoop architecture built on the local `mio-runtime` crate. Read/idle timeouts (today's `--read-timeout-secs` from #31) are preserved via per-worker periodic sweep. Out of scope: tagged/multiplexed BFFP frames (#69), client pipelining bench (#68), connection migration between workers, `tokio` (#32 is superseded by this work).

## Problem

Today's TCP server in [src/main.rs](../../../src/main.rs) is a textbook accept-loop:

- `std::net::TcpListener::incoming()` blocks on `accept()`.
- Each accepted connection gets its own OS thread via `thread::spawn`.
- Each thread runs `handle_stream_inner`, which uses `BufReader::read_exact` on the blocking socket — one connection per thread, one thread per connection.
- Read timeouts use `TcpStream::set_read_timeout` (#31).
- `--max-connections` gates new accepts against an atomic counter (#31).

Consequences:

- **Memory per idle peer is one OS thread's stack** (default 2 MiB on Linux/Windows). 1k idle connections = ~2 GiB of thread stacks.
- **No upper bound on thread count beyond `--max-connections`**, which is a coarse global gate.
- **Threading model is at odds with where the project is going** — replication (#33), partitioning (#34), and out-of-order responses (#69) all benefit from a small fixed pool of long-lived workers rather than spawning a fresh thread per connection.

The `mio-runtime` crate (built in parallel at `../mio-runtime`) provides exactly the primitives needed: a single-threaded `EventLoop`, a `Registry` for I/O source registration, a hashed timer wheel (512 slot × 1 ms), and a `Waker` for cross-thread signalling. `mio-runtime` task #6 specifies the consumer-side migration but assumes a *single* `EventLoop` on the main thread.

## Design

### 1. Architecture: One Acceptor Thread + N Worker EventLoops

```
       ┌────────────────────────────┐
       │  acceptor thread           │  std::net::TcpListener (blocking)
       │  - enforces max_conns      │
       │  - round-robin next worker │
       └──┬─────────────┬───────────┘
          │ Sender[i]   │ Waker.wake()
          ▼             ▼
   ┌──────────┐   ┌──────────┐  ...  ┌──────────┐
   │ Worker 0 │   │ Worker 1 │       │ Worker N │
   │ EventLoop│   │ EventLoop│       │ EventLoop│
   │ HashMap< │   │ HashMap< │       │ HashMap< │
   │  Token,  │   │  Token,  │       │  Token,  │
   │  Conn>   │   │  Conn>   │       │  Conn>   │
   └──────────┘   └──────────┘       └──────────┘
        │              │                   │
        └──────────────┴───────────────────┘
                       │
              Arc<dyn StorageEngine> + Arc<Stats>
```

**Affinity.** Once the acceptor sends a stream to worker K, it lives on worker K for life. No migration. This preserves single-threaded simplicity *within* a worker — no per-connection locks, no cross-worker state.

**Why multi-loop instead of single-loop (mio-runtime #6 default).** The recently-completed engine-internal concurrency (#61) made `StorageEngine` `&self`-callable from any thread. A single EventLoop calling `engine.set/get/...` synchronously throws that parallelism away — every engine call serializes through the dispatch thread, and a slow `RANGE` or cold SSTable read stalls every other connection. With N workers each owning a loop, slow calls only stall the 1/N of connections sharing that worker.

**Why single acceptor + handoff instead of `SO_REUSEPORT` per worker.** `SO_REUSEPORT` does kernel-side load balancing on Linux/BSD. **Windows has no equivalent** — multiple sockets bound to the same port don't load-balance, last bound wins. The acceptor + handoff model is portable across all platforms `mio` supports, composes cleanly with `mio-runtime`'s existing `Waker` surface (no runtime changes needed), and yields connection affinity automatically.

### 2. Module Layout

New module tree under [src/server/](../../../src/server/):

| Path | Purpose |
|------|---------|
| `src/server/mod.rs` | Public re-exports (`Server`, `ServerConfig`) |
| `src/server/acceptor.rs` | Acceptor thread: blocking `TcpListener::accept` loop, round-robin handoff, `--max-connections` gate |
| `src/server/worker.rs` | `Worker { event_loop, handler }`; `WorkerHandler: EventHandler` |
| `src/server/connection.rs` | `Connection` struct + `ParseState` enum + read/write state machine |
| `src/server/dispatch.rs` | Pure `dispatch(cmd, engine, stats, cfg) -> Vec<u8>` lifted from today's match-on-Command |

[src/main.rs](../../../src/main.rs) shrinks to: build engine + stats, parse settings, build & start `Server`, wait. The 250+ lines of TCP handling in today's `main.rs` move into `src/server/`.

### 3. Acceptor Thread

```rust
pub struct Acceptor {
    listener: std::net::TcpListener,
    senders: Vec<mpsc::Sender<mio::net::TcpStream>>,
    wakers: Vec<mio_runtime::Waker>,
    next: usize,
    stats: Arc<Stats>,
    max_connections: usize,
}
```

Loop body (blocking thread):

1. `let (std_stream, _peer) = listener.accept()?;`
2. If `max_connections > 0 && stats.active_connections.load(Relaxed) >= max_connections`:
   - Convert to `mio::net::TcpStream`, `set_nonblocking(true)`, write the existing "max connections reached" error frame, drop. Continue.
3. Otherwise:
   - `std_stream.set_nonblocking(true)?;`
   - `let mio_stream = mio::net::TcpStream::from_std(std_stream);`
   - `stats.active_connections.fetch_add(1, Relaxed);`
   - `senders[next].send(mio_stream).expect("worker channel closed");`
   - `wakers[next].wake()?;`
   - `next = (next + 1) % senders.len();`

`accept()` errors are logged and the loop continues (today's behavior in [src/main.rs:142](../../../src/main.rs#L142)).

Listener binding stays in `Server::start` so the existing `0.0.0.0 → 127.0.0.1` rewrite and `server.addr` file emission ([src/main.rs:90-101](../../../src/main.rs#L90-L101)) work unchanged.

### 4. Worker EventLoop

```rust
pub struct WorkerHandler {
    connections: HashMap<Token, Connection>,
    next_token: usize,
    incoming: mpsc::Receiver<mio::net::TcpStream>,
    engine: Arc<dyn StorageEngine>,
    stats: Arc<Stats>,
    compaction_cfg: CompactionCfg,
    idle_timeout: Option<Duration>,
    sweep_interval: Duration,  // 100 ms
    sweep_timer: Option<TimerId>,
}
```

Token allocation: `next_token` starts at 1 (mio-runtime reserves `usize::MAX` for the internal waker; 0 has no special meaning here since workers don't own a listener).

`EventHandler` impl:

- **`on_event(registry, token, ready)`**: look up `Connection` in `connections`. If `ready.readable()`, call `Connection::on_readable(engine, stats, cfg)` to drain socket + parse + dispatch + buffer responses. **If `on_readable` reports buffered output, opportunistically call `Connection::on_writable` immediately** — for the common case (request fits in one read, response fits in the socket's send buffer) this avoids a wasted loop turn waiting for `WRITABLE` readiness. Then, if `ready.writable()` (i.e., the source was already armed for write), call `on_writable` to drain. Apply the resulting reregister request via `registry.reregister(&mut conn.stream, token, ...)`. If any step returns `ConnectionAction::Close`, deregister + remove + `stats.active_connections.fetch_sub(1, Relaxed)`.

- **`on_wake(registry)`**: drain `incoming` to empty:
  ```rust
  while let Ok(mut stream) = self.incoming.try_recv() {
      let token = Token(self.next_token);
      self.next_token += 1;
      registry.register(&mut stream, token, ReadyState { readable: true, writable: false })?;
      let conn = Connection::new(stream);
      self.connections.insert(token, conn);
  }
  ```
  Coalesced wakes are fine — `mio::Waker` is level-triggered and `mio-runtime` emits at most one `on_wake` per loop iteration regardless of how many `wake()` calls fired.

- **`on_timer(registry, timer_id)`**: if `Some(timer_id) == self.sweep_timer`, run idle sweep (Section 6) and rearm.

### 5. `Connection` and `ParseState`

```rust
pub struct Connection {
    stream: mio::net::TcpStream,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
    write_offset: usize,
    parse_state: ParseState,
    last_read_at: Instant,
}

pub enum ParseState {
    ReadingHeader,                       // < 4 bytes buffered
    ReadingPayload { remaining: usize }, // header parsed; `remaining` bytes left in body
}
```

**Read path (`Connection::on_readable`)** — drain socket until `WouldBlock`:

1. `let mut tmp = [0u8; 4096]; loop { match stream.read(&mut tmp) { ... } }`.
2. `Ok(0)` → EOF, return `Close`.
3. `Ok(n)` → `last_read_at = Instant::now(); read_buf.extend_from_slice(&tmp[..n]);` then advance `parse_state`:
   - **`ReadingHeader`**: if `read_buf.len() >= 4`, parse `frame_len = u32::from_be_bytes(read_buf[0..4])`. Validate `frame_len <= MAX_REQUEST_BYTES` ([src/main.rs:175](../../../src/main.rs#L175)); on overflow, append a "Frame too large" error frame to `write_buf` and return `Close`. (Today's code writes the error and continues ([src/main.rs:196-203](../../../src/main.rs#L196-L203)), but in a streaming parser the malformed frame's payload may be partially buffered and recovery is hairy — closing the connection matches "be strict about untrusted framing.") Otherwise transition to `ReadingPayload { remaining: frame_len as usize }`.
   - **`ReadingPayload { remaining }`**: if `read_buf.len() >= 4 + remaining`, slice the full frame, call `bffp::decode_input_frame(full_frame)?`, then `dispatch::dispatch(cmd, &engine, &stats, &cfg)` to produce a response `Vec<u8>`, append to `write_buf`, drain the consumed bytes from `read_buf` (`read_buf.drain(0..4 + remaining);`), reset `parse_state = ReadingHeader`. Loop again — there may be another full frame in the buffer (pipelining, #68).
4. `Err(e) if e.kind() == WouldBlock` → break read loop.
5. `Err(_)` → return `Close`.
6. After read loop: if `write_buf.is_empty()`, return `Continue { reregister: None }`. If `write_buf` has bytes, return `Continue { reregister: Some(READABLE | WRITABLE) }` — caller reregisters the source.

**Write path (`Connection::on_writable`)** — drain `write_buf` until `WouldBlock`:

1. `loop { match stream.write(&write_buf[write_offset..]) { ... } }`.
2. `Ok(0)` → treat as transient, break.
3. `Ok(n)` → `write_offset += n;` continue.
4. `Err(WouldBlock)` → break.
5. `Err(_)` → return `Close`.
6. After loop: if `write_offset == write_buf.len()`, clear both (`write_buf.clear(); write_offset = 0;`) and return `Continue { reregister: Some(READABLE) }`. Otherwise return `Continue { reregister: None }` (still mid-write, keep current interest).

The reregistration mechanic is bubbled to the caller because `Connection` doesn't own the `Registry` — `WorkerHandler::on_event` calls `registry.reregister(&mut conn.stream, token, ready_state)` based on the action returned.

### 6. Read Timeouts: Periodic Sweep

`mio-runtime`'s timer wheel is bounded at **512 ms** (ADR-001 §2, asserted by mio-runtime task #2). Per-connection second-scale timers would panic. Instead: one repeating periodic timer per worker, sweeps connections.

**Setup (in `WorkerHandler::start`, called once before `event_loop.run`):**

```rust
if let Some(_) = self.idle_timeout {
    self.sweep_timer = Some(registry.insert_timer(self.sweep_interval));
}
```

`sweep_interval = Duration::from_millis(100)` — well inside the 512 ms wheel. Coarse is fine because `read_timeout_secs` is in seconds.

**On tick (`on_timer`):**

```rust
if Some(id) == self.sweep_timer {
    let now = Instant::now();
    let timeout = self.idle_timeout.unwrap();
    let stale: Vec<Token> = self.connections.iter()
        .filter(|(_, c)| now.duration_since(c.last_read_at) > timeout)
        .map(|(t, _)| *t)
        .collect();
    for token in stale {
        if let Some(mut conn) = self.connections.remove(&token) {
            let _ = registry.deregister(&mut conn.stream);
            self.stats.active_connections.fetch_sub(1, Relaxed);
        }
    }
    self.sweep_timer = Some(registry.insert_timer(self.sweep_interval));  // rearm
}
```

`--read-timeout-secs 0` (today's "disable" sentinel from [src/main.rs:106-109](../../../src/main.rs#L106-L109)) sets `idle_timeout = None` and skips the schedule entirely.

Same observable semantics as today: idle = no incoming bytes for N seconds. Resets on every successful read (including partial frames).

### 7. Dispatch: Pure Function

[src/main.rs:212-388](../../../src/main.rs#L212-L388)'s match-on-`Command` becomes:

```rust
pub fn dispatch(
    cmd: Command,
    engine: &Arc<dyn StorageEngine>,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) -> Vec<u8> {
    match cmd {
        Command::Write(key, value) => { /* unchanged logic */ }
        Command::Read(key) => { /* unchanged */ }
        // ... all 12 variants, lifted verbatim
    }
}
```

The body is identical to today; only the wrapping changes (no `BufReader`, no `write_all` — return `Vec<u8>` and let the caller append to `write_buf`).

`maybe_trigger_compaction` ([src/main.rs:395-430](../../../src/main.rs#L395-L430)) moves into `dispatch.rs` unchanged. It still spawns a background `thread::spawn` for the actual compaction work; the `stats.compacting` CAS guard already prevents concurrent triggers across workers.

**Note on `thread::spawn` in compaction:** the "no per-connection threads" goal does not extend to compaction. Compaction is bounded-rate (gated by `stats.compacting`) and synchronously running it on a worker would freeze that worker's connections for the duration. Offloading via `thread::spawn` keeps the worker loop responsive — this is intentional and complements the multi-loop design rather than contradicting it.

### 8. `Server` Construction

```rust
pub struct ServerConfig {
    pub workers: usize,                     // CLI: --workers, default available_parallelism()
    pub max_connections: usize,
    pub read_timeout: Option<Duration>,
    pub compaction: CompactionCfg,
    pub addr: String,
}

pub struct Server {
    listener: std::net::TcpListener,
    workers: Vec<JoinHandle<io::Result<()>>>,
    acceptor: JoinHandle<io::Result<()>>,
}

impl Server {
    pub fn start(
        cfg: ServerConfig,
        engine: Arc<dyn StorageEngine>,
        stats: Arc<Stats>,
    ) -> io::Result<Self> { ... }
}
```

Build sequence in `Server::start`:

1. Bind `std::net::TcpListener`.
2. For `i in 0..cfg.workers`:
   - Create `mpsc::channel::<mio::net::TcpStream>()`.
   - Build `Worker { event_loop: EventLoop::new()?, handler: WorkerHandler::new(...) }`.
   - Capture `event_loop.waker()` for the acceptor.
   - `thread::Builder::new().name(format!("rustikv-worker-{i}")).spawn(move || worker.run())`.
3. Build `Acceptor { listener: cloned, senders, wakers, ... }`.
4. Spawn acceptor thread.

Worker count default uses `std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)` — std-only, no new dependency.

**CLI wiring.** `--workers <N>` is parsed in [src/settings.rs](../../../src/settings.rs) alongside the existing flags (`--max-connections`, `--read-timeout-secs`, etc.), surfaced as `Settings::workers: Option<usize>`. `Server::start` resolves `None` to `available_parallelism()`. Help text and `Settings::print_help()` updated accordingly.

### 9. Error Handling

| Failure | Behavior |
|---------|----------|
| Per-connection read/write/parse error | Deregister, remove from HashMap, decrement counter. Worker loop continues. |
| Frame too large | Close connection (no in-band recovery — buffer state would be ambiguous). |
| `accept()` returns `Err` | Log to stderr, loop continues (today's behavior). |
| Worker panic | Process aborts. Educational scope; not handled. |
| Compaction error | `unwrap()` (today's behavior in [src/main.rs:286](../../../src/main.rs#L286), [src/main.rs:421](../../../src/main.rs#L421)). |
| Channel send fails (worker thread died) | Acceptor `.expect()`s; process aborts. |

### 10. Stats Accounting

`Stats` already has `AtomicI64 active_connections`. Migration:

- **Before:** worker thread `+1` after spawn, `-1` on stream end.
- **After:** acceptor `+1` after handoff to channel; worker `-1` on connection close (EOF, error, or sweep).

The "max connections reached" pre-flight check stays in the acceptor; the existing rejection error frame is sent and the connection is dropped without ever reaching a worker, so no `+1` happens for rejected peers (matches today's [src/main.rs:111-122](../../../src/main.rs#L111-L122)).

All other counters (`reads`, `writes`, `deletes`, `compaction_count`, `write_blocked_*`, etc.) are touched only inside `dispatch`, which sees the same `Arc<Stats>` it does today.

### 11. Dependency on `mio-runtime`

Cargo.toml addition:

```toml
mio-runtime = { git = "https://github.com/SilvioPilato/mio-runtime.git", rev = "<sha>" }
```

`mio` itself is brought in transitively. Pinning by `rev` rather than `branch` gives reproducible builds and insulates rustikv from upstream churn — the rev gets bumped explicitly when this consumer needs a new mio-runtime feature. Fresh clones build without requiring a sibling `mio-runtime` checkout. The cost is that mio-runtime changes must be `git push`-ed before rustikv can consume them; if iteration becomes painful, a `[patch]` override can be added later to point at a local path during development without changing the manifest's declared source.

The `mio-runtime` task #6 description in `../mio-runtime/TASKS.md` says "single `EventLoop` on the main thread". That sentence is now stale — the runtime supports N independent loops by construction (`EventLoop` and `Registry` are `!Send`/`!Clone` per-instance, not anti-multi-loop). The rustikv PR description will note this; no separate `mio-runtime` PR is being opened. (The runtime requires no code changes for multi-loop usage — it Just Works.)

**mio-runtime #6 task hygiene.** When this rustikv PR lands, mio-runtime #6 is considered satisfied — it described the consumer-side migration, and the consumer-side migration is done (in a different shape than originally written). #6 will be moved to **Closed Tasks** in `../mio-runtime/TASKS.md` with a "Realised by rustikv #N (multi-loop variant)" note, in the same hand as #32 is being superseded on the rustikv side (Section 12).

### 12. Migration of #32 ("Async I/O with tokio")

Task #32 in [TASKS.md](../../../TASKS.md) proposes the same goal (replace thread-per-connection) using `tokio`. It is **superseded** by this work. The new task entry will reference #32 as superseded; #32 stays in **Open Tasks** but will be moved to **Closed Tasks** with a "Superseded by #N" note when the new task lands.

## Testing

### Unit tests (in [tests/](../../../tests/))

- **`tests/dispatch.rs`** — one test per `Command` variant against an in-memory engine. No sockets. Asserts the response bytes match what today's `handle_stream_inner` produces.
- **`tests/connection_parse.rs`** — feed bytes to `Connection::on_readable` via a mock that simulates a non-blocking read returning chunks of varying sizes. Assert `ParseState` transitions correctly across split-header, split-payload, and back-to-back frames.

### Integration tests (existing, must continue to pass)

All current integration tests in `tests/` connect over TCP and send framed commands. They must work unchanged — the wire protocol is unchanged.

### New integration tests

- **`tests/multi_worker.rs`** — start server with `--workers 4`, spawn 100 client threads each performing 10 `WRITE`/`READ` pairs against unique keys. Assert all 1000 responses are correct, `active_connections` returns to 0 after clients disconnect.
- **`tests/idle_timeout.rs`** — start server with `--read-timeout-secs 1`, connect, write nothing for 1.5 s, assert connection is closed by server (next read returns 0 bytes).
- **`tests/max_connections.rs`** — start server with `--max-connections 2`, open 2 connections, attempt 3rd, assert error frame received and 3rd connection drops.
- **`tests/graceful_close.rs`** — connect, send partial frame (3 bytes of a 4-byte header), close client. Assert server cleans up: `active_connections` decrements, no leaked tokens (verifiable via `STATS`).

## Open Questions / Risks

1. **Worker imbalance.** Round-robin is simple but blind to load. A worker holding 100 long-lived idle connections vs. one with 100 short-lived burst connections will see very different request rates. For the educational scope this is fine; if it becomes a problem the dispatch can switch to least-loaded (`senders.iter().min_by_key(|s| current_count_for_worker(s))`) — left as a future tweak, not part of this work.

2. **Slow-call blast radius.** A `RANGE` over a large LSM keyspace on worker K stalls every connection on K until it returns. With `available_parallelism()` workers, blast radius is roughly 1/N. Mitigations (offload slow ops to a thread pool, or split read/write workers) are deferred to a follow-up task once profiling identifies which calls actually hurt.

3. **`mio-runtime` is unreleased and consumed via git rev.** Bumping the pinned rev is a manual step, and a breaking change in `mio-runtime`'s public surface (e.g., `EventHandler` signature) breaks rustikv's build at bump time. Acceptable for now — both repos are developed in tandem by the same author, and the rev pin makes the breakage explicit (the bump commit shows exactly which upstream change caused the churn).

4. **Read-timeout sweep granularity.** A 100 ms sweep with K connections does K `Instant` comparisons every 100 ms per worker. At 10k connections per worker this is 100k comparisons/sec — still trivial, but worth noting as the only periodic CPU cost.
