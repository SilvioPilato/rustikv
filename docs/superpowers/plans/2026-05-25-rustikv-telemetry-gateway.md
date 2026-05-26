# rustikv Telemetry Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Rust gateway that ingests Graphite-plaintext metrics into rustikv and serves an HTTP/JSON query endpoint for Grafana, plus refresh the rustikv telemetry doc (task #85).

**Architecture:** One Rust binary in a new repo (`rustikv-telemetry-gateway`) with two listeners. The **ingest** listener (TCP) parses Graphite plaintext lines, batches them, and writes them to rustikv as `MSET`-with-TTL frames. The **query** listener (HTTP) parses `/query` requests, fans out to rustikv `RANGE`/`AvgRange`/`MinRange`/`MaxRange`/`SumRange` over time buckets, and returns a JSON time series. Both reuse the `rustikv` crate's `bffp` module for the wire protocol; no rustikv code changes.

**Tech Stack:** Rust (edition 2024), std-only except the `rustikv` git dependency for `bffp`. HTTP/1.1 GET handling and JSON output are hand-rolled (small, dependency-light, matches the rustikv project's didactic ethos). Tests in a `tests/` directory (mirroring rustikv convention — no inline `#[cfg(test)]`).

**Spec:** `docs/superpowers/specs/2026-05-25-rustikv-telemetry-gateway-design.md`

**Reviewer notes folded in:**
- `RANGE` responses are a *flat positional* list (key, value, key, value, …) with no delimiter — parsers consume two strings per point.
- `bffp::encode_command` takes `Command` **by value** — each send consumes the command.
- `PREFIX`/`COUNT` are NOT in the gateway query path (illustrative in the spec only); they appear only in the #85 doc examples.

---

## Repo location & conventions

- New repo: `C:\Users\Silvio\Dev\rustikv-telemetry-gateway` (sibling to `kv-store`).
- The `rustikv` dependency pins to a commit on rustikv **`main`** that already has `bffp` public (any recent main HEAD, e.g. `5548efb`). It does NOT depend on the unpushed `85-telemetry-gateway` branch.
- Follow rustikv conventions: `cargo fmt`, `cargo clippy -- -D warnings`, `cargo test` before each commit; all tests in `tests/`.

## File structure

| Path | Responsibility |
|------|----------------|
| `Cargo.toml` | Package + `rustikv` git dep |
| `src/main.rs` | Parse config, spawn ingest + query threads, join |
| `src/config.rs` | `Config` struct + parse from args/env (ports, rustikv addr, TTL, batch size, flush ms, max buckets) |
| `src/schema.rs` | `to_key(metric, ts) -> String`, `parse_point_key(key) -> Option<(String, i64)>`, `TS_WIDTH = 10` (pure) |
| `src/graphite.rs` | `parse_line(&str) -> Result<Sample, ParseError>`; `Sample { metric, value: f64, ts: i64 }` (pure) |
| `src/http.rs` | `parse_request_line(&str) -> Option<Request>`, query-param parsing (pure) + minimal HTTP/1.1 read/write helpers |
| `src/json.rs` | `points_to_json(&[Point]) -> String`; `Point { time_ms: i64, value: f64 }` (pure) |
| `src/rustikv_client.rs` | `Client::connect(addr)`, `send(Command) -> io::Result<DecodedResponse>`, reconnect |
| `src/ingest.rs` | TCP listener; read lines; batch (flush on N lines or T ms); `Command::Mset` send |
| `src/query.rs` | HTTP listener; route `/query`; bucket fan-out to aggregation; serialize JSON |
| `tests/schema.rs`, `tests/graphite.rs`, `tests/http_parse.rs`, `tests/json.rs` | Unit tests for pure modules |
| `tests/wire_contract.rs` | Byte-layout assertions guarding against `bffp` drift |
| `tests/e2e.rs` | `#[ignore]`d end-to-end test against a live rustikv |
| `README.md`, `examples/collectd-graphite.conf`, `examples/grafana-panel.json` | Docs + sample configs |
| `AGENTS.md`, `.gitignore` | Conventions + ignores |

---

## Task 0: Scaffold the repo

**Files:** Create `Cargo.toml`, `.gitignore`, `src/main.rs` (stub), `AGENTS.md`, `README.md` (skeleton).

- [ ] **Step 1: Create the crate**

```bash
cd C:/Users/Silvio/Dev
cargo new rustikv-telemetry-gateway --bin
cd rustikv-telemetry-gateway
```

- [ ] **Step 2: Write `Cargo.toml`**

```toml
[package]
name = "rustikv-telemetry-gateway"
version = "0.1.0"
edition = "2024"

[dependencies]
rustikv = { git = "https://github.com/SilvioPilato/rustikv.git", branch = "main" }
```

- [ ] **Step 3: Write `.gitignore`**

```
/target
Cargo.lock
```

(Binary crate — but pin via `Cargo.lock` is optional for an app; keep it simple and ignore it for this prototype.)

- [ ] **Step 4: Stub `src/main.rs`**

```rust
fn main() {
    println!("rustikv-telemetry-gateway");
}
```

- [ ] **Step 5: Verify it builds (downloads the rustikv dep)**

Run: `cargo build`
Expected: compiles; rustikv crate fetched from GitHub.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml .gitignore src/main.rs
git commit -m "chore: scaffold rustikv-telemetry-gateway crate"
```

---

## Task 1: `schema` module (pure)

**Files:** Create `src/schema.rs`, `tests/schema.rs`. Modify `src/main.rs` (add `mod schema;` — or add a `src/lib.rs`; see Step 0a).

- [ ] **Step 0a: Expose modules to tests via a lib target**

Add `src/lib.rs` so integration tests in `tests/` can `use rustikv_telemetry_gateway::schema;`:

```rust
pub mod schema;
```

And keep `src/main.rs` using the lib crate (`use rustikv_telemetry_gateway::schema;`). Add each new `pub mod` to `lib.rs` as tasks introduce modules.

- [ ] **Step 1: Write the failing test** — `tests/schema.rs`

```rust
use rustikv_telemetry_gateway::schema::{parse_point_key, to_key, TS_WIDTH};

#[test]
fn to_key_zero_pads_timestamp_to_fixed_width() {
    assert_eq!(to_key("pi.cpu.user", 1_748_169_600), "pi.cpu.user:0001748169600");
    assert_eq!(to_key("pi.cpu.user", 0).len(), "pi.cpu.user:".len() + TS_WIDTH);
}

#[test]
fn keys_sort_lexicographically_in_timestamp_order() {
    let earlier = to_key("m", 100);
    let later = to_key("m", 2000);
    assert!(earlier < later, "lexicographic order must match numeric time order");
}

#[test]
fn parse_point_key_round_trips() {
    let k = to_key("pi.mem.used", 1_748_169_600);
    assert_eq!(parse_point_key(&k), Some(("pi.mem.used".to_string(), 1_748_169_600)));
}

#[test]
fn parse_point_key_rejects_malformed() {
    assert_eq!(parse_point_key("no-colon"), None);
    assert_eq!(parse_point_key("metric:notanumber"), None);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test schema`
Expected: FAIL (module/functions not defined).

- [ ] **Step 3: Write minimal implementation** — `src/schema.rs`

```rust
/// Fixed width for the zero-padded epoch-seconds suffix. 10 digits covers
/// epoch seconds through the year 2286, keeping lexicographic order == time order.
pub const TS_WIDTH: usize = 10;

/// Build a rustikv key: `<metric>:<zero-padded-epoch-seconds>`.
pub fn to_key(metric: &str, ts_secs: i64) -> String {
    format!("{metric}:{ts_secs:0width$}", width = TS_WIDTH)
}

/// Split a point key back into (metric, ts_secs). Returns None if malformed.
/// The timestamp is the final colon-delimited segment.
pub fn parse_point_key(key: &str) -> Option<(String, i64)> {
    let idx = key.rfind(':')?;
    let (metric, ts) = (&key[..idx], &key[idx + 1..]);
    if metric.is_empty() {
        return None;
    }
    let ts_secs: i64 = ts.parse().ok()?;
    Some((metric.to_string(), ts_secs))
}
```

Add `pub mod schema;` to `src/lib.rs`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test schema`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/schema.rs tests/schema.rs
git commit -m "feat: schema module for timestamped rustikv keys"
```

---

## Task 2: `graphite` line parser (pure)

**Files:** Create `src/graphite.rs`, `tests/graphite.rs`. Modify `src/lib.rs`.

- [ ] **Step 1: Write the failing test** — `tests/graphite.rs`

```rust
use rustikv_telemetry_gateway::graphite::{parse_line, Sample};

#[test]
fn parses_valid_line() {
    let s = parse_line("pi.cpu.user 12.5 1748169600").unwrap();
    assert_eq!(s, Sample { metric: "pi.cpu.user".to_string(), value: 12.5, ts: 1_748_169_600 });
}

#[test]
fn accepts_integer_and_negative_values() {
    assert_eq!(parse_line("m 42 100").unwrap().value, 42.0);
    assert_eq!(parse_line("m -3.5 100").unwrap().value, -3.5);
}

#[test]
fn rejects_malformed_lines() {
    assert!(parse_line("too few").is_err());
    assert!(parse_line("m notanumber 100").is_err());
    assert!(parse_line("m 1.0 notatimestamp").is_err());
    assert!(parse_line("").is_err());
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test graphite` → FAIL.

- [ ] **Step 3: Implement** — `src/graphite.rs`

```rust
#[derive(Debug, PartialEq)]
pub struct Sample {
    pub metric: String,
    pub value: f64,
    pub ts: i64,
}

#[derive(Debug, PartialEq)]
pub struct ParseError(pub String);

/// Parse one Graphite plaintext line: `metric.path value timestamp`.
pub fn parse_line(line: &str) -> Result<Sample, ParseError> {
    let mut parts = line.split_whitespace();
    let metric = parts.next().ok_or_else(|| ParseError("missing metric".into()))?;
    let value_s = parts.next().ok_or_else(|| ParseError("missing value".into()))?;
    let ts_s = parts.next().ok_or_else(|| ParseError("missing timestamp".into()))?;
    if parts.next().is_some() {
        return Err(ParseError("too many fields".into()));
    }
    let value: f64 = value_s.parse().map_err(|_| ParseError(format!("bad value: {value_s}")))?;
    let ts: i64 = ts_s.parse().map_err(|_| ParseError(format!("bad timestamp: {ts_s}")))?;
    Ok(Sample { metric: metric.to_string(), value, ts })
}
```

Add `pub mod graphite;` to `src/lib.rs`.

- [ ] **Step 4: Run to verify pass** — `cargo test --test graphite` → PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/graphite.rs tests/graphite.rs
git commit -m "feat: graphite plaintext line parser"
```

---

## Task 3: minimal HTTP request parsing (pure)

**Files:** Create `src/http.rs`, `tests/http_parse.rs`. Modify `src/lib.rs`.

Only the *pure parsing* is TDD'd here; socket read/write helpers are added in Task 8 where they're integration-exercised.

- [ ] **Step 1: Write the failing test** — `tests/http_parse.rs`

```rust
use rustikv_telemetry_gateway::http::{parse_request_target, QueryParams};

#[test]
fn parses_path_and_query_params() {
    let t = parse_request_target("GET /query?metric=pi.cpu.user&from=100&to=200&agg=avg&step=60 HTTP/1.1").unwrap();
    assert_eq!(t.path, "/query");
    assert_eq!(t.params.get("metric"), Some("pi.cpu.user"));
    assert_eq!(t.params.get("from"), Some("100"));
    assert_eq!(t.params.get("agg"), Some("avg"));
    assert_eq!(t.params.get("step"), Some("60"));
}

#[test]
fn parses_path_without_query() {
    let t = parse_request_target("GET /health HTTP/1.1").unwrap();
    assert_eq!(t.path, "/health");
    assert_eq!(t.params.get("metric"), None);
}

#[test]
fn rejects_non_get_and_malformed() {
    assert!(parse_request_target("POST /query HTTP/1.1").is_none());
    assert!(parse_request_target("garbage").is_none());
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test http_parse` → FAIL.

- [ ] **Step 3: Implement** — `src/http.rs` (parsing portion)

```rust
use std::collections::HashMap;

pub struct QueryParams(HashMap<String, String>);
impl QueryParams {
    pub fn get(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(|s| s.as_str())
    }
}

pub struct RequestTarget {
    pub path: String,
    pub params: QueryParams,
}

/// Parse an HTTP request line of the form `GET /path?a=b&c=d HTTP/1.1`.
/// Returns None for non-GET or malformed lines. Values are NOT percent-decoded
/// (metric names and integer params used here don't require it).
pub fn parse_request_target(line: &str) -> Option<RequestTarget> {
    let mut parts = line.split_whitespace();
    if parts.next()? != "GET" {
        return None;
    }
    let target = parts.next()?;
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q),
        None => (target, ""),
    };
    let mut params = HashMap::new();
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        if let Some((k, v)) = pair.split_once('=') {
            params.insert(k.to_string(), v.to_string());
        }
    }
    Some(RequestTarget { path: path.to_string(), params: QueryParams(params) })
}
```

Add `pub mod http;` to `src/lib.rs`.

- [ ] **Step 4: Run to verify pass** — `cargo test --test http_parse` → PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/http.rs tests/http_parse.rs
git commit -m "feat: minimal HTTP request-target parser"
```

---

## Task 4: JSON points serializer (pure)

**Files:** Create `src/json.rs`, `tests/json.rs`. Modify `src/lib.rs`.

- [ ] **Step 1: Write the failing test** — `tests/json.rs`

```rust
use rustikv_telemetry_gateway::json::{points_to_json, Point};

#[test]
fn serializes_points_array() {
    let pts = vec![
        Point { time_ms: 100_000, value: 12.5 },
        Point { time_ms: 160_000, value: 13.0 },
    ];
    assert_eq!(
        points_to_json(&pts),
        r#"[{"time":100000,"value":12.5},{"time":160000,"value":13}]"#
    );
}

#[test]
fn serializes_empty_array() {
    assert_eq!(points_to_json(&[]), "[]");
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test json` → FAIL.

- [ ] **Step 3: Implement** — `src/json.rs`

```rust
pub struct Point {
    pub time_ms: i64,
    pub value: f64,
}

/// Serialize points as a JSON array of `{"time":<ms>,"value":<f64>}`.
/// Grafana's Infinity datasource maps this directly to a time series.
pub fn points_to_json(points: &[Point]) -> String {
    let mut out = String::from("[");
    for (i, p) in points.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        // `{}` on f64 prints 13.0 as "13"; that is valid JSON and fine for Grafana.
        out.push_str(&format!(r#"{{"time":{},"value":{}}}"#, p.time_ms, p.value));
    }
    out.push(']');
    out
}
```

Add `pub mod json;` to `src/lib.rs`.

- [ ] **Step 4: Run to verify pass** — `cargo test --test json` → PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/json.rs tests/json.rs
git commit -m "feat: JSON points serializer for Grafana"
```

---

## Task 5: `rustikv_client` + wire contract test

**Files:** Create `src/rustikv_client.rs`, `tests/wire_contract.rs`. Modify `src/lib.rs`.

The client's live behavior is covered by the e2e test (Task 10). Here we add the client and a contract test that pins the `bffp` byte layout the gateway depends on, guarding against silent protocol drift on a crate bump.

- [ ] **Step 1: Write the failing test** — `tests/wire_contract.rs`

```rust
use rustikv::bffp::{encode_command, Command};

// Frame layout (from rustikv bffp.rs): total_len(4) | op(1) | flags(1) | key_len(2)
// | key | value_len(4) | value | [ttl(4) if flag&1]. Write op code = 2, HAS_TTL flag = 1.
#[test]
fn write_with_ttl_byte_layout_is_stable() {
    let bytes = encode_command(Command::Write("k".into(), "v".into(), Some(60)));
    // total_len = op(1)+flags(1)+key_len(2)+value_len(4)+key(1)+value(1)+ttl(4) = 14
    assert_eq!(&bytes[0..4], &14u32.to_be_bytes());
    assert_eq!(bytes[4], 2, "Write op code");
    assert_eq!(bytes[5], 1, "HAS_TTL flag");
    assert_eq!(&bytes[6..8], &1u16.to_be_bytes(), "key_len");
    assert_eq!(bytes[8], b'k');
    assert_eq!(&bytes[9..13], &1u32.to_be_bytes(), "value_len");
    assert_eq!(bytes[13], b'v');
    assert_eq!(&bytes[14..18], &60u32.to_be_bytes(), "ttl seconds");
}
```

- [ ] **Step 2: Run to verify** — `cargo test --test wire_contract`.
  Expected: PASS immediately (this asserts existing rustikv behavior). If it FAILS, the pinned rustikv revision's protocol differs from this plan — STOP and reconcile before continuing.

- [ ] **Step 3: Implement the client** — `src/rustikv_client.rs`

```rust
use std::io::{self, Read, Write};
use std::net::TcpStream;

use rustikv::bffp::{decode_response_frame, encode_command, Command, DecodedResponse};

pub struct Client {
    addr: String,
    stream: TcpStream,
}

impl Client {
    pub fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { addr: addr.to_string(), stream })
    }

    /// Send one command (consumed by `encode_command`) and read the decoded response.
    pub fn send(&mut self, command: Command) -> io::Result<DecodedResponse> {
        let frame = encode_command(command);
        self.stream.write_all(&frame)?;

        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf)?;
        let body_len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; body_len];
        self.stream.read_exact(&mut body)?;

        let mut full = Vec::with_capacity(4 + body_len);
        full.extend_from_slice(&len_buf);
        full.extend_from_slice(&body);
        decode_response_frame(&full)
    }

    /// Reconnect after a dropped connection.
    pub fn reconnect(&mut self) -> io::Result<()> {
        self.stream = TcpStream::connect(&self.addr)?;
        Ok(())
    }
}
```

Add `pub mod rustikv_client;` to `src/lib.rs`.

- [ ] **Step 4: Verify build + contract** — `cargo build && cargo test --test wire_contract` → PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/rustikv_client.rs tests/wire_contract.rs
git commit -m "feat: rustikv BFFP client + wire contract test"
```

---

## Task 6: `config` module

**Files:** Create `src/config.rs`. Modify `src/lib.rs`.

- [ ] **Step 1: Write the failing test** — append to `tests/schema.rs`? No — create `tests/config.rs`.

```rust
use rustikv_telemetry_gateway::config::Config;

#[test]
fn defaults_are_sane() {
    let c = Config::default();
    assert_eq!(c.ingest_addr, "0.0.0.0:2003");
    assert_eq!(c.query_addr, "0.0.0.0:8080");
    assert_eq!(c.rustikv_addr, "127.0.0.1:6666");
    assert_eq!(c.ttl_secs, 86_400);
    assert!(c.batch_max_lines > 0 && c.flush_ms > 0 && c.max_buckets > 0);
}

#[test]
fn parses_overrides_from_args() {
    let c = Config::from_args(["--rustikv", "10.0.0.5:6666", "--ttl", "3600"].iter().map(|s| s.to_string()));
    assert_eq!(c.rustikv_addr, "10.0.0.5:6666");
    assert_eq!(c.ttl_secs, 3600);
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test config` → FAIL.

- [ ] **Step 3: Implement** — `src/config.rs`

```rust
#[derive(Clone)]
pub struct Config {
    pub ingest_addr: String,
    pub query_addr: String,
    pub rustikv_addr: String,
    pub ttl_secs: u32,
    pub batch_max_lines: usize,
    pub flush_ms: u64,
    pub max_buckets: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ingest_addr: "0.0.0.0:2003".into(),
            query_addr: "0.0.0.0:8080".into(),
            rustikv_addr: "127.0.0.1:6666".into(),
            ttl_secs: 86_400,
            batch_max_lines: 200,
            flush_ms: 1000,
            max_buckets: 2000,
        }
    }
}

impl Config {
    pub fn from_args(args: impl Iterator<Item = String>) -> Self {
        let mut c = Config::default();
        let mut it = args.peekable();
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--ingest" => if let Some(v) = it.next() { c.ingest_addr = v; },
                "--query" => if let Some(v) = it.next() { c.query_addr = v; },
                "--rustikv" => if let Some(v) = it.next() { c.rustikv_addr = v; },
                "--ttl" => if let Some(v) = it.next() { c.ttl_secs = v.parse().expect("--ttl seconds"); },
                "--batch" => if let Some(v) = it.next() { c.batch_max_lines = v.parse().expect("--batch lines"); },
                "--flush-ms" => if let Some(v) = it.next() { c.flush_ms = v.parse().expect("--flush-ms"); },
                "--max-buckets" => if let Some(v) = it.next() { c.max_buckets = v.parse().expect("--max-buckets"); },
                other => eprintln!("ignoring unknown arg: {other}"),
            }
        }
        c
    }
}
```

Add `pub mod config;` to `src/lib.rs`.

- [ ] **Step 4: Run to verify pass** — `cargo test --test config` → PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/config.rs tests/config.rs
git commit -m "feat: gateway config (args + defaults)"
```

---

## Task 7: `ingest` listener

**Files:** Create `src/ingest.rs`. Modify `src/lib.rs`. Exercised end-to-end in Task 10 (no isolated unit test — it's pure IO glue over already-tested parsers).

- [ ] **Step 1: Implement** — `src/ingest.rs`

```rust
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::time::{Duration, Instant};

use rustikv::bffp::Command;

use crate::config::Config;
use crate::graphite::parse_line;
use crate::rustikv_client::Client;
use crate::schema::to_key;

/// Accept Graphite-plaintext connections and forward batched samples to rustikv.
/// Single inbound connection at a time (sufficient for one collector); serve()
/// loops accepting connections sequentially.
pub fn serve(config: Config) -> std::io::Result<()> {
    let listener = TcpListener::bind(&config.ingest_addr)?;
    eprintln!("ingest listening on {}", config.ingest_addr);
    let mut client = Client::connect(&config.rustikv_addr)?;

    for conn in listener.incoming() {
        let conn = match conn {
            Ok(c) => c,
            Err(e) => { eprintln!("accept error: {e}"); continue; }
        };
        if let Err(e) = handle_conn(conn, &config, &mut client) {
            eprintln!("connection ended: {e}");
        }
    }
    Ok(())
}

fn handle_conn(conn: std::net::TcpStream, config: &Config, client: &mut Client) -> std::io::Result<()> {
    let reader = BufReader::new(conn);
    let mut batch: Vec<(String, String, Option<u32>)> = Vec::new();
    let mut last_flush = Instant::now();
    let flush_after = Duration::from_millis(config.flush_ms);

    for line in reader.lines() {
        let line = line?;
        match parse_line(&line) {
            Ok(s) => batch.push((to_key(&s.metric, s.ts), s.value.to_string(), Some(config.ttl_secs))),
            Err(e) => eprintln!("skip line {line:?}: {}", e.0),
        }
        if batch.len() >= config.batch_max_lines || last_flush.elapsed() >= flush_after {
            flush(client, &mut batch, config);
            last_flush = Instant::now();
        }
    }
    flush(client, &mut batch, config); // flush remaining on disconnect
    Ok(())
}

fn flush(client: &mut Client, batch: &mut Vec<(String, String, Option<u32>)>, config: &Config) {
    if batch.is_empty() {
        return;
    }
    let items = std::mem::take(batch);
    let n = items.len();
    if let Err(e) = client.send(Command::Mset(items)) {
        eprintln!("mset of {n} failed: {e}; reconnecting");
        let _ = client.reconnect_to(&config.rustikv_addr);
    }
}
```

Note: add a `reconnect_to(&mut self, addr: &str)` convenience or reuse `reconnect()` (the client already stores `addr`); adjust to `client.reconnect()` to match Task 5's signature. **Use `client.reconnect()`** and drop the `config` arg from `flush` if unused.

- [ ] **Step 2: Build** — `cargo build`. Fix signature mismatches (use `client.reconnect()`).

- [ ] **Step 3: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/ingest.rs
git commit -m "feat: Graphite ingest listener with batched MSET"
```

---

## Task 8: `query` listener (HTTP → aggregation → JSON)

**Files:** Modify `src/http.rs` (add socket read/write helpers), create `src/query.rs`. Modify `src/lib.rs`.

The bucket math is the one piece worth a focused unit test.

- [ ] **Step 1: Write the failing test for bucket boundaries** — `tests/buckets.rs`

```rust
use rustikv_telemetry_gateway::query::bucket_ranges;

#[test]
fn splits_window_into_step_sized_buckets() {
    // [100, 280) with step 60 -> buckets [100,160),[160,220),[220,280)
    let b = bucket_ranges(100, 280, 60);
    assert_eq!(b, vec![(100, 160), (160, 220), (220, 280)]);
}

#[test]
fn last_bucket_clamps_to_end() {
    let b = bucket_ranges(100, 250, 60);
    assert_eq!(b, vec![(100, 160), (160, 220), (220, 250)]);
}

#[test]
fn empty_when_from_ge_to() {
    assert!(bucket_ranges(200, 100, 60).is_empty());
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test buckets` → FAIL.

- [ ] **Step 3: Implement `bucket_ranges` + the query handler** — `src/query.rs`

```rust
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

use rustikv::bffp::{Command, ResponseStatus};

use crate::config::Config;
use crate::http::parse_request_target;
use crate::json::{points_to_json, Point};
use crate::rustikv_client::Client;
use crate::schema::to_key;

/// Half-open `[from, to)` buckets of `step` seconds; last bucket clamps to `to`.
pub fn bucket_ranges(from: i64, to: i64, step: i64) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    let mut start = from;
    while start < to {
        let end = (start + step).min(to);
        out.push((start, end));
        start = end;
    }
    out
}

pub fn serve(config: Config) -> std::io::Result<()> {
    let listener = TcpListener::bind(&config.query_addr)?;
    eprintln!("query listening on {}", config.query_addr);
    let mut client = Client::connect(&config.rustikv_addr)?;
    for conn in listener.incoming().flatten() {
        if let Err(e) = handle(conn, &config, &mut client) {
            eprintln!("query conn error: {e}");
            let _ = client.reconnect();
        }
    }
    Ok(())
}

fn handle(mut conn: TcpStream, config: &Config, client: &mut Client) -> std::io::Result<()> {
    // Read just the request line (first line); we ignore headers/body for GET.
    let mut reader = BufReader::new(conn.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;

    let resp = match build_response(line.trim_end(), config, client) {
        Ok(json) => http_ok(&json),
        Err((code, msg)) => http_err(code, &msg),
    };
    conn.write_all(resp.as_bytes())
}

fn build_response(req_line: &str, config: &Config, client: &mut Client) -> Result<String, (u16, String)> {
    let target = parse_request_target(req_line).ok_or((400, "bad request line".into()))?;
    if target.path == "/health" {
        return Ok("ok".into());
    }
    if target.path != "/query" {
        return Err((404, "not found".into()));
    }
    let p = &target.params;
    let metric = p.get("metric").ok_or((400, "metric required".into()))?;
    let from: i64 = p.get("from").ok_or((400, "from required".into()))?.parse().map_err(|_| (400u16, "bad from".to_string()))?;
    let to: i64 = p.get("to").ok_or((400, "to required".into()))?.parse().map_err(|_| (400u16, "bad to".to_string()))?;
    let agg = p.get("agg").unwrap_or("raw");

    if agg == "raw" {
        let points = query_raw(client, metric, from, to).map_err(|e| (502u16, e))?;
        return Ok(points_to_json(&points));
    }

    let step: i64 = p.get("step").ok_or((400, "step required for aggregation".into()))?.parse().map_err(|_| (400u16, "bad step".to_string()))?;
    if step <= 0 {
        return Err((400, "step must be positive".into()));
    }
    let buckets = bucket_ranges(from, to, step);
    if buckets.len() > config.max_buckets {
        return Err((400, format!("too many buckets ({} > {})", buckets.len(), config.max_buckets)));
    }
    let mut points = Vec::new();
    for (b_from, b_to) in buckets {
        if let Some(v) = query_agg(client, agg, metric, b_from, b_to).map_err(|e| (502u16, e))? {
            points.push(Point { time_ms: b_from * 1000, value: v });
        }
    }
    Ok(points_to_json(&points))
}

/// agg=raw: one RANGE over [from, to]; response is a FLAT list key,value,key,value...
fn query_raw(client: &mut Client, metric: &str, from: i64, to: i64) -> Result<Vec<Point>, String> {
    let resp = client
        .send(Command::Range(to_key(metric, from), to_key(metric, to)))
        .map_err(|e| e.to_string())?;
    let mut points = Vec::new();
    let mut it = resp.payload.chunks_exact(2); // [key, value] pairs
    for pair in &mut it {
        if let (Some((_, ts)), Ok(v)) = (crate::schema::parse_point_key(&pair[0]), pair[1].parse::<f64>()) {
            points.push(Point { time_ms: ts * 1000, value: v });
        }
    }
    Ok(points)
}

/// agg=avg|min|max|sum over one bucket via the matching *Range op.
/// Returns None when the bucket is empty (rustikv replies NotFound / "Not found").
fn query_agg(client: &mut Client, agg: &str, metric: &str, from: i64, to: i64) -> Result<Option<f64>, String> {
    let (lo, hi) = (to_key(metric, from), to_key(metric, to));
    let cmd = match agg {
        "avg" => Command::AvgRange(lo, hi),
        "min" => Command::MinRange(lo, hi),
        "max" => Command::MaxRange(lo, hi),
        "sum" => Command::SumRange(lo, hi),
        other => return Err(format!("unknown agg: {other}")),
    };
    let resp = client.send(cmd).map_err(|e| e.to_string())?;
    match resp.status {
        ResponseStatus::Ok => resp.payload.first().and_then(|s| s.parse::<f64>().ok())
            .map(|v| Ok(Some(v)))
            .unwrap_or(Ok(None)),
        ResponseStatus::NotFound => Ok(None),
        ResponseStatus::Error => Err(resp.payload.join("; ")),
        ResponseStatus::Noop => Ok(None),
    }
}

fn http_ok(body: &str) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    )
}

fn http_err(code: u16, msg: &str) -> String {
    let body = format!(r#"{{"error":"{msg}"}}"#);
    format!(
        "HTTP/1.1 {code} ERR\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(), body
    )
}
```

Note on the RANGE bound: rustikv `RANGE` is inclusive `[start, end]`. Buckets are half-open in the gateway to avoid double-counting a point on a boundary in `raw` mode; for aggregation each `*Range` is inclusive, so a sample exactly on a bucket boundary can fall in two adjacent buckets. For the prototype this is acceptable; document it. (A later refinement: subtract 1 second from `hi` for aggregation buckets.)

Add `pub mod query;` to `src/lib.rs`.

- [ ] **Step 4: Run to verify pass** — `cargo test --test buckets` → PASS; `cargo build` → OK.

- [ ] **Step 5: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lib.rs src/query.rs tests/buckets.rs
git commit -m "feat: HTTP query endpoint with server-side bucketed aggregation"
```

---

## Task 9: wire up `main.rs`

**Files:** Modify `src/main.rs`.

- [ ] **Step 1: Implement**

```rust
use std::thread;

use rustikv_telemetry_gateway::config::Config;
use rustikv_telemetry_gateway::{ingest, query};

fn main() -> std::io::Result<()> {
    let config = Config::from_args(std::env::args().skip(1));
    let ingest_cfg = config.clone();
    let ingest_handle = thread::spawn(move || {
        if let Err(e) = ingest::serve(ingest_cfg) {
            eprintln!("ingest fatal: {e}");
        }
    });
    let query_handle = thread::spawn(move || {
        if let Err(e) = query::serve(config) {
            eprintln!("query fatal: {e}");
        }
    });
    ingest_handle.join().unwrap();
    query_handle.join().unwrap();
    Ok(())
}
```

- [ ] **Step 2: Build + run smoke** — `cargo build`. (Full run needs a rustikv server; covered next.)

- [ ] **Step 3: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/main.rs
git commit -m "feat: wire ingest + query listeners in main"
```

---

## Task 10: end-to-end test + sample configs + README

**Files:** Create `tests/e2e.rs`, `examples/collectd-graphite.conf`, `examples/grafana-panel.json`, fill in `README.md`.

- [ ] **Step 1: Write the `#[ignore]`d e2e test** — `tests/e2e.rs`

This test assumes a rustikv **LSM** server is reachable at `$RUSTIKV_TEST_ADDR` (default `127.0.0.1:6666`). It is ignored by default so `cargo test` stays green without external setup.

```rust
use std::io::{Read, Write};
use std::net::TcpStream;

fn rustikv_addr() -> String {
    std::env::var("RUSTIKV_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:6666".into())
}

#[test]
#[ignore = "requires a live rustikv LSM server and the gateway running; see README"]
fn ingest_then_query_roundtrip() {
    // Assumes gateway ingest on 127.0.0.1:2003 and query on 127.0.0.1:8080.
    let mut ingest = TcpStream::connect("127.0.0.1:2003").unwrap();
    for (i, ts) in [1000, 1060, 1120].iter().enumerate() {
        writeln!(ingest, "test.metric {}.0 {}", (i + 1) * 10, ts).unwrap();
    }
    drop(ingest); // triggers final flush
    std::thread::sleep(std::time::Duration::from_millis(500));

    let mut q = TcpStream::connect("127.0.0.1:8080").unwrap();
    write!(q, "GET /query?metric=test.metric&from=900&to=1200&agg=avg&step=300 HTTP/1.1\r\n\r\n").unwrap();
    let mut resp = String::new();
    q.read_to_string(&mut resp).unwrap();
    let body = resp.split("\r\n\r\n").nth(1).unwrap();
    // avg of 10,20,30 = 20
    assert!(body.contains(r#""value":20"#), "got: {body}");
    let _ = rustikv_addr();
}
```

- [ ] **Step 2: Manual e2e verification** (document exact commands in README; run them once):

```bash
# Terminal 1 — rustikv LSM
cd ../kv-store && cargo run -- C:/Temp/gw-db --engine lsm

# Terminal 2 — gateway
cd ../rustikv-telemetry-gateway && cargo run

# Terminal 3 — feed a Graphite line and query
# (PowerShell) push a sample:
$c = New-Object Net.Sockets.TcpClient("127.0.0.1",2003); $s=$c.GetStream()
$b=[Text.Encoding]::ASCII.GetBytes("demo.metric 42.0 1748169600`n"); $s.Write($b,0,$b.Length); $c.Close()
# query it:
curl "http://127.0.0.1:8080/query?metric=demo.metric&from=1748169000&to=1748170000&agg=avg&step=300"
```

Expected: JSON array containing `"value":42`.

- [ ] **Step 3: Run the gated e2e test** (with all three terminals up):

Run: `cargo test --test e2e -- --ignored`
Expected: PASS.

- [ ] **Step 4: Write `examples/collectd-graphite.conf`** (Pi side)

```
LoadPlugin write_graphite
<Plugin write_graphite>
  <Node "gateway">
    Host "GATEWAY_IP"
    Port "2003"
    Protocol "tcp"
    Prefix "pi."
    StoreRates true
    EscapeCharacter "."
  </Node>
</Plugin>
```

- [ ] **Step 5: Write `examples/grafana-panel.json`** — a minimal Infinity-datasource time-series panel querying `http://GATEWAY_IP:8080/query?metric=pi.cpu.user&from=${__from:date:seconds}&to=${__to:date:seconds}&agg=avg&step=60`, parsing JSON with `time` (ms) and `value` columns. (Fill with a working panel skeleton.)

- [ ] **Step 6: Write `README.md`** — what it is, the architecture diagram from the spec, how to build/run, the manual verification commands, the Graphite key schema, the `/query` API, and Grafana Infinity-datasource setup notes.

- [ ] **Step 7: Commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add tests/e2e.rs examples README.md
git commit -m "test+docs: e2e roundtrip, sample collectd/Grafana configs, README"
```

---

## Task 11: rustikv doc refresh (task #85) — separate repo

**Files (in `C:\Users\Silvio\Dev\kv-store`, branch `85-telemetry-gateway`):** Modify `docs/telemetry-store-experiment.md`, `TASKS.md`.

This is documentation-only in the rustikv repo. Follow rustikv's task workflow.

- [ ] **Step 1:** In `docs/telemetry-store-experiment.md`, move SUM/AVG/MIN/MAX and COUNT from "What's missing"/"nice to have" into the "How it fits telemetry today" table as shipped capabilities.

- [ ] **Step 2:** Update the "Suggested path" diagram to mark the full path complete.

- [ ] **Step 3:** Add a worked end-to-end example section: timestamped-key `MSET` writes (`metric:<padded-ts>`), `RANGE`/`PREFIX` windowing, `COUNT` cardinality, `AVG`/`MIN`/`MAX` over a window — framed around the gateway → Grafana flow, linking to the gateway repo.

- [ ] **Step 4:** Move task `#85` from **Open Tasks** to **Closed Tasks** in `TASKS.md`.

- [ ] **Step 5: Pre-commit + commit** (rustikv checklist)

```bash
cd C:/Users/Silvio/Dev/kv-store
cargo fmt && cargo clippy -- -D warnings && cargo test
git add docs/telemetry-store-experiment.md TASKS.md
git commit -m "#85 — refresh telemetry experiment doc; full feature path complete"
```

- [ ] **Step 6: PR** — push branch `85-telemetry-gateway`, open PR titled `#85 — Refresh telemetry experiment doc once aggregation lands`, body including `Opened via Claude`, add the PR link to `#85` in `TASKS.md`. Then ask the user whether to check out back to `main`.

---

## Done criteria

- Gateway repo: `cargo fmt`/`clippy -D warnings`/`test` all green; manual e2e (Graphite in → Grafana-shaped JSON out) verified; `cargo test --test e2e -- --ignored` passes against a live rustikv.
- A collectd-equipped Pi pointed at the gateway produces queryable series; a Grafana Infinity panel renders them with server-side `avg`/`min`/`max` downsampling.
- rustikv `#85` doc refreshed and PR'd.
