# COUNT Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `COUNT <prefix>` and `COUNT <start> <end>` TCP commands that return the number of live keys without materialising values — the fourth step of the telemetry use-case path.

**Architecture:** Two new op codes (CountPrefix=15, CountRange=16) wire through the same five-layer stack as RANGE/PREFIX: bffp → cli → engine trait → lsmengine → dispatch. The engine implementation accumulates a `BTreeSet<String>` of live keys (insert on live write, remove on tombstone/expiry) — structurally identical to the `BTreeMap` in `range`/`prefix` but without ever reading values. Both variants return a single integer in an Ok frame, the same shape as INCR.

**Tech Stack:** Rust, Cargo. No new dependencies.

---

## File Map

| File | Change |
|------|--------|
| `src/bffp.rs` | Add `CountPrefix`/`CountRange` to `Command`, `CountPrefix=15`/`CountRange=16` to `OpCode`, decode + encode arms |
| `src/cli.rs` | Add `"COUNT"` arm: arity 2 → CountPrefix, arity 3 → CountRange |
| `src/engine.rs` | Add `count_prefix` + `count_range` to `RangeScan` trait |
| `src/lsmengine.rs` | Implement both methods with BTreeSet merge; add `BTreeSet` to imports |
| `src/server/dispatch.rs` | Add `Command::CountPrefix` + `Command::CountRange` arms |
| `tests/lsm_count.rs` | Create — engine-level tests for both variants |
| `tests/count_command.rs` | Create — BFFP round-trips, CLI parse tests, dispatch end-to-end, stats assertion |
| `README.md` | Add COUNT rows (ops 15 and 16) to the command table |

---

## Task 1 — Protocol: bffp.rs + stub dispatch arms

Adding CountPrefix/CountRange to the Command enum breaks the exhaustive match in dispatch.rs. This task adds the new symbols to bffp.rs **and** temporary stub dispatch arms so the codebase compiles throughout.

**Files:**
- Modify: `src/bffp.rs`
- Modify: `src/server/dispatch.rs` (stubs only — replaced in Task 4)
- Create: `tests/count_command.rs` (BFFP round-trip tests only for now)

- [ ] **Step 1: Write failing BFFP round-trip tests**

Create `tests/count_command.rs` with only the codec tests:

```rust
use rustikv::bffp::{Command, decode_input_frame, decode_response_frame, encode_command};

// --- CountPrefix round-trips ---

#[test]
fn count_prefix_command_round_trips() {
    let frame = encode_command(Command::CountPrefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected Command::CountPrefix"),
    }
}

#[test]
fn count_prefix_command_round_trips_empty() {
    let frame = encode_command(Command::CountPrefix(String::new()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountPrefix(p) => assert_eq!(p, ""),
        _ => panic!("expected Command::CountPrefix"),
    }
}

// --- CountRange round-trips ---

#[test]
fn count_range_command_round_trips() {
    let frame = encode_command(Command::CountRange("a".to_string(), "z".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected Command::CountRange"),
    }
}

#[test]
fn count_range_command_round_trips_equal_bounds() {
    let frame = encode_command(Command::CountRange("k".to_string(), "k".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::CountRange(s, e) => {
            assert_eq!(s, "k");
            assert_eq!(e, "k");
        }
        _ => panic!("expected Command::CountRange"),
    }
}
```

- [ ] **Step 2: Run tests — expect compile error**

```
cargo test --test count_command 2>&1 | head -20
```

Expected: compile error — `Command::CountPrefix` does not exist.

- [ ] **Step 3: Add CountPrefix + CountRange to `src/bffp.rs`**

In the `Command` enum, after `Prefix(String)` (line 30):

```rust
    CountPrefix(String),
    CountRange(String, String),
```

In the `OpCode` enum, after `Prefix = 14` (line 48):

```rust
    CountPrefix = 15,
    CountRange = 16,
```

In the `TryFrom<u8>` impl for `OpCode`, after `14 => Ok(OpCode::Prefix)` (line 69):

```rust
            15 => Ok(OpCode::CountPrefix),
            16 => Ok(OpCode::CountRange),
```

In `decode_input_frame`, after `Ok(OpCode::Prefix) => Ok(Command::Prefix(read_key(&mut cur)?))` (line 174):

```rust
        Ok(OpCode::CountPrefix) => Ok(Command::CountPrefix(read_key(&mut cur)?)),
        Ok(OpCode::CountRange) => {
            let start = read_key(&mut cur)?;
            let end = read_key(&mut cur)?;
            Ok(Command::CountRange(start, end))
        }
```

In `encode_command`, after the `Command::Prefix` arm (after line 444):

```rust
        Command::CountPrefix(prefix) => {
            // | total_len(4) | OpCode::CountPrefix(1) | key_len(2) | prefix |
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
            let key_len = prefix.len() as u16;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::CountPrefix as u8]).unwrap();
            payload.write_all(&key_len.to_be_bytes()).unwrap();
            payload.write_all(prefix.as_bytes()).unwrap();
            payload.into_inner()
        }
        Command::CountRange(start, end) => {
            // | total_len(4) | OpCode::CountRange(1) | start_len(2) | start | end_len(2) | end |
            let start_len = start.len() as u16;
            let end_len = end.len() as u16;
            let total_len = OP_CODE_SIZE as u32
                + KEY_LEN_SIZE as u32 * 2
                + start_len as u32
                + end_len as u32;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::CountRange as u8]).unwrap();
            payload.write_all(&start_len.to_be_bytes()).unwrap();
            payload.write_all(start.as_bytes()).unwrap();
            payload.write_all(&end_len.to_be_bytes()).unwrap();
            payload.write_all(end.as_bytes()).unwrap();
            payload.into_inner()
        }
```

- [ ] **Step 4: Add stub dispatch arms to `src/server/dispatch.rs`**

The match in `dispatch` is now non-exhaustive. Add stubs after the `Command::Prefix` arm (after line 267):

```rust
        Command::CountPrefix(_) | Command::CountRange(_, _) => {
            encode_frame(ResponseStatus::Error, &["not yet implemented".to_string()])
        }
```

- [ ] **Step 5: Run round-trip tests — expect PASS**

```
cargo test --test count_command 2>&1
```

Expected: 4 round-trip tests pass.

- [ ] **Step 6: Run full test suite and clippy**

```
cargo fmt && cargo clippy -- -D warnings && cargo test
```

Expected: all tests pass, zero clippy warnings.

- [ ] **Step 7: Commit**

```
git add src/bffp.rs src/server/dispatch.rs tests/count_command.rs
git commit -m "#51 — add CountPrefix/CountRange to bffp + stub dispatch arms"
```

---

## Task 2 — CLI: parse COUNT arm in cli.rs

**Files:**
- Modify: `src/cli.rs`
- Modify: `tests/count_command.rs` (add CLI parse tests)

- [ ] **Step 1: Add CLI parse tests to `tests/count_command.rs`**

Append to the file:

```rust
use rustikv::cli::{ParseResult, parse_command};

fn count_cmd(line: &str) -> Command {
    match parse_command(line) {
        ParseResult::Cmd(c) => c,
        ParseResult::Quit => panic!("expected Cmd, got Quit"),
        ParseResult::InvalidInput(m) => panic!("expected Cmd, got InvalidInput: {m}"),
    }
}

fn count_is_invalid(line: &str) -> bool {
    matches!(parse_command(line), ParseResult::InvalidInput(_))
}

#[test]
fn cli_count_arity_2_parses_as_prefix() {
    match count_cmd("COUNT cpu:") {
        Command::CountPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected CountPrefix"),
    }
}

// Note: an actual empty-string prefix cannot be sent via split_whitespace CLI
// (COUNT with no second token is arity 1 → InvalidInput). Empty-prefix semantics
// are exercised at the engine level in lsm_count.rs and at the dispatch level
// in count_command.rs. No CLI test needed here.

#[test]
fn cli_count_arity_3_parses_as_range() {
    match count_cmd("COUNT a z") {
        Command::CountRange(s, e) => {
            assert_eq!(s, "a");
            assert_eq!(e, "z");
        }
        _ => panic!("expected CountRange"),
    }
}

#[test]
fn cli_count_arity_1_is_invalid() {
    assert!(count_is_invalid("COUNT"));
}

#[test]
fn cli_count_arity_4_is_invalid() {
    assert!(count_is_invalid("COUNT a b c"));
}

#[test]
fn cli_count_case_insensitive() {
    match count_cmd("count cpu:") {
        Command::CountPrefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected CountPrefix"),
    }
}
```

- [ ] **Step 2: Run CLI tests — expect FAIL**

```
cargo test --test count_command cli_count 2>&1
```

Expected: tests panic with "expected Cmd, got InvalidInput" — COUNT arm not yet parsed.

- [ ] **Step 3: Add COUNT arm to `src/cli.rs`**

After the `"PREFIX"` arm (after line 98), add:

```rust
        "COUNT" => match words.len() {
            2 => ParseResult::Cmd(Command::CountPrefix(words[1].to_string())),
            3 => ParseResult::Cmd(Command::CountRange(
                words[1].to_string(),
                words[2].to_string(),
            )),
            _ => ParseResult::InvalidInput(
                "Usage: COUNT <prefix> | COUNT <start> <end>".to_string(),
            ),
        },
```

- [ ] **Step 4: Run CLI tests — expect PASS**

```
cargo test --test count_command cli_count 2>&1
```

Expected: all 6 CLI tests pass.

- [ ] **Step 5: Run full suite and clippy**

```
cargo fmt && cargo clippy -- -D warnings && cargo test
```

Expected: all tests pass, zero warnings.

- [ ] **Step 6: Commit**

```
git add src/cli.rs tests/count_command.rs
git commit -m "#51 — add COUNT arm to cli.rs parser"
```

---

## Task 3 — Engine: RangeScan trait + LsmEngine implementation

**Files:**
- Modify: `src/engine.rs`
- Modify: `src/lsmengine.rs`
- Create: `tests/lsm_count.rs`

- [ ] **Step 1: Write failing engine-level tests**

Create `tests/lsm_count.rs`:

```rust
use rustikv::engine::{RangeScan, StorageEngine};
use rustikv::lsmengine::LsmEngine;
use rustikv::size_tiered::SizeTiered;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

fn temp_dir(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_lsm_count_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn new_engine(dir: &str, db_name: &str, max_memtable_bytes: usize) -> LsmEngine {
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    LsmEngine::new(dir, db_name, max_memtable_bytes, strategy, 4096, true).unwrap()
}

fn engine_from_dir(dir: &str, db_name: &str, max_memtable_bytes: usize) -> LsmEngine {
    let strategy =
        Box::new(SizeTiered::load_from_dir(dir, db_name, 4, 32, 4096, true).unwrap());
    LsmEngine::from_dir(dir, db_name, max_memtable_bytes, strategy, 4096, true).unwrap()
}

fn past_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 10_000
}

const BIG_MEMTABLE: usize = 1_048_576;

// ── count_prefix ──────────────────────────────────────────────────────────────

#[test]
fn count_prefix_basic_memtable() {
    let dir = temp_dir("cp_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.set("mem:a", "3").unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 2);
}

#[test]
fn count_prefix_no_match_returns_zero() {
    let dir = temp_dir("cp_nomatch");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();

    assert_eq!(engine.count_prefix("disk:").unwrap(), 0);
}

#[test]
fn count_prefix_tombstone_excluded() {
    let dir = temp_dir("cp_tomb");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.delete("cpu:b").unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_expired_excluded() {
    let dir = temp_dir("cp_exp");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set_with_ttl("cpu:b", "2", Some(past_ms())).unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_empty_counts_all_live() {
    let dir = temp_dir("cp_empty");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();

    assert_eq!(engine.count_prefix("").unwrap(), 3);
}

#[test]
fn count_prefix_full_key_match() {
    // prefix == key should count that one key
    let dir = temp_dir("cp_fullkey");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu", "1").unwrap();

    assert_eq!(engine.count_prefix("cpu").unwrap(), 1);
}

#[test]
fn count_prefix_boundary_not_substring_range() {
    // "cpu:" must NOT count "cpuz" — starts_with, not <= comparison
    let dir = temp_dir("cp_boundary");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpuz", "2").unwrap();

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_tombstone_in_memtable_hides_flushed() {
    let dir = temp_dir("cp_cross_tomb");
    {
        let engine = new_engine(&dir, "test", 1); // tiny → flush per write
        engine.set("cpu:a", "1").unwrap();
        engine.set("cpu:b", "2").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("cpu:b").unwrap(); // tombstone in memtable, value in SSTable

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_memtable_overwrite_shadows_sstable() {
    let dir = temp_dir("cp_overwrite");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("cpu:a", "old").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.set("cpu:a", "new").unwrap();

    // Still 1 key (not 2) — overwrite, not a new key
    assert_eq!(engine.count_prefix("cpu:").unwrap(), 1);
}

#[test]
fn count_prefix_multi_segment_spread() {
    let dir = temp_dir("cp_multiseg");
    {
        let engine = new_engine(&dir, "test", 1); // flush per write
        engine.set("aaa", "x").unwrap();
        engine.set("cpu:1", "1").unwrap();
        engine.set("mmm", "x").unwrap();
        engine.set("cpu:2", "2").unwrap();
        engine.set("zzz", "x").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);

    assert_eq!(engine.count_prefix("cpu:").unwrap(), 2);
}

// ── count_range ───────────────────────────────────────────────────────────────

#[test]
fn count_range_basic_memtable() {
    let dir = temp_dir("cr_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.set("d", "4").unwrap();

    assert_eq!(engine.count_range("b", "c").unwrap(), 2);
}

#[test]
fn count_range_inverted_bounds_returns_zero() {
    let dir = temp_dir("cr_inverted");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();

    assert_eq!(engine.count_range("z", "a").unwrap(), 0);
}

#[test]
fn count_range_tombstone_excluded() {
    let dir = temp_dir("cr_tomb");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    engine.set("c", "3").unwrap();
    engine.delete("b").unwrap();

    assert_eq!(engine.count_range("a", "c").unwrap(), 2);
}

#[test]
fn count_range_expired_excluded() {
    let dir = temp_dir("cr_exp");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE);
    engine.set("a", "1").unwrap();
    engine.set_with_ttl("b", "2", Some(past_ms())).unwrap();
    engine.set("c", "3").unwrap();

    assert_eq!(engine.count_range("a", "c").unwrap(), 2);
}

#[test]
fn count_range_spans_memtable_and_segment() {
    let dir = temp_dir("cr_span");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("a", "1").unwrap();
        engine.set("c", "3").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.set("b", "2").unwrap(); // stays in memtable

    assert_eq!(engine.count_range("a", "c").unwrap(), 3);
}

#[test]
fn count_range_tombstone_in_memtable_hides_flushed() {
    let dir = temp_dir("cr_cross_tomb");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("a", "1").unwrap();
        engine.set("b", "2").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("a").unwrap();

    assert_eq!(engine.count_range("a", "b").unwrap(), 1);
}

// ── Differential equivalence (invariant 5) ───────────────────────────────────

#[test]
fn count_prefix_equals_prefix_len() {
    // count_prefix(p) must equal prefix(p).len() for any engine state
    let dir = temp_dir("cp_diff");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("aaa", "x").unwrap();
        engine.set("cpu:1", "v1").unwrap();
        engine.set("cpu:2", "v2").unwrap();
        engine.set("mmm", "x").unwrap();
        engine.set("cpu:3", "v3").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("cpu:2").unwrap(); // tombstone in memtable

    for prefix in &["cpu:", "aaa", "mmm", "disk:", ""] {
        let via_count = engine.count_prefix(prefix).unwrap();
        let via_prefix = engine.prefix(prefix).unwrap().len();
        assert_eq!(
            via_count, via_prefix,
            "count_prefix({prefix:?}) = {via_count} but prefix({prefix:?}).len() = {via_prefix}"
        );
    }
}

#[test]
fn count_range_equals_range_len() {
    let dir = temp_dir("cr_diff");
    {
        let engine = new_engine(&dir, "test", 1);
        engine.set("a", "1").unwrap();
        engine.set("b", "2").unwrap();
        engine.set("c", "3").unwrap();
        engine.set("d", "4").unwrap();
        engine.set("e", "5").unwrap();
    }
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE);
    engine.delete("c").unwrap();

    for (start, end) in &[("a", "e"), ("b", "d"), ("z", "a"), ("c", "c"), ("a", "a")] {
        let via_count = engine.count_range(start, end).unwrap();
        let via_range = engine.range(start, end).unwrap().len();
        assert_eq!(
            via_count, via_range,
            "count_range({start:?},{end:?}) = {via_count} but range({start:?},{end:?}).len() = {via_range}"
        );
    }
}
```

- [ ] **Step 2: Run engine tests — expect compile error**

```
cargo test --test lsm_count 2>&1 | head -20
```

Expected: compile error — `count_prefix`/`count_range` not found on `LsmEngine`.

- [ ] **Step 3: Add methods to the RangeScan trait in `src/engine.rs`**

In `pub trait RangeScan`, add after `fn prefix`:

```rust
    fn count_prefix(&self, prefix: &str) -> io::Result<usize>;
    fn count_range(&self, start: &str, end: &str) -> io::Result<usize>;
```

- [ ] **Step 4: Add BTreeSet to imports in `src/lsmengine.rs`**

Change line 7 from:
```rust
    collections::{BTreeMap, HashSet},
```
to:
```rust
    collections::{BTreeMap, BTreeSet, HashSet},
```

- [ ] **Step 5: Implement count_prefix and count_range in `src/lsmengine.rs`**

In `impl RangeScan for LsmEngine`, add these two methods after the closing `}` of `fn prefix` (after line 654):

```rust
    fn count_prefix(&self, prefix: &str) -> io::Result<usize> {
        let now_ms = now_ms();
        let mut set: BTreeSet<String> = BTreeSet::new();
        let successor = prefix_successor(prefix);

        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            let segments: Vec<&SSTable> = match successor.as_deref() {
                Some(end) => storage_strategy.iter_files_for_range(prefix, end).collect(),
                None => storage_strategy.iter_all().collect(),
            };
            for segment in segments {
                for result in segment.iter()? {
                    let record = result?;
                    if !record.key.starts_with(prefix) {
                        continue;
                    }
                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        set.remove(&record.key);
                        continue;
                    }
                    if record.header.is_tombstone() {
                        set.remove(&record.key);
                        continue;
                    }
                    set.insert(record.key);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (k, entry) in memtable
                    .entries()
                    .range::<str, _>((Included(prefix), Unbounded))
                {
                    if !k.starts_with(prefix) {
                        break;
                    }
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => {
                            set.remove(k);
                        }
                        (Some(_), _) => {
                            set.insert(k.clone());
                        }
                        (None, _) => {
                            set.remove(k);
                        }
                    };
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (k, entry) in memtable
                .entries()
                .range::<str, _>((Included(prefix), Unbounded))
            {
                if !k.starts_with(prefix) {
                    break;
                }
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => {
                        set.remove(k);
                    }
                    (Some(_), _) => {
                        set.insert(k.clone());
                    }
                    (None, _) => {
                        set.remove(k);
                    }
                };
            }
        }

        Ok(set.len())
    }

    fn count_range(&self, start: &str, end: &str) -> io::Result<usize> {
        if start > end {
            return Ok(0);
        }

        let now_ms = now_ms();
        let mut set: BTreeSet<String> = BTreeSet::new();

        {
            let storage_strategy = self.shared.storage_strategy.read().unwrap();
            for segment in storage_strategy.iter_files_for_range(start, end) {
                for result in segment.iter()? {
                    let record = result?;
                    if record.key.as_str() < start || record.key.as_str() > end {
                        continue;
                    }
                    if let Some(expiry_ms) = record.header.expiry_ms
                        && is_expired(expiry_ms, now_ms)
                    {
                        set.remove(&record.key);
                        continue;
                    }
                    if record.header.is_tombstone() {
                        set.remove(&record.key);
                        continue;
                    }
                    set.insert(record.key);
                }
            }
        }

        {
            let immutable = self.shared.immutable.read().unwrap();
            if let Some(memtable) = immutable.as_ref() {
                for (k, entry) in memtable
                    .entries()
                    .range::<str, _>((Included(start), Included(end)))
                {
                    match (entry.value.as_deref(), entry.expiry_ms) {
                        (Some(_), Some(ms)) if is_expired(ms, now_ms) => {
                            set.remove(k);
                        }
                        (Some(_), _) => {
                            set.insert(k.clone());
                        }
                        (None, _) => {
                            set.remove(k);
                        }
                    };
                }
            }
        }

        {
            let memtable = self.shared.active.read().unwrap();
            for (k, entry) in memtable
                .entries()
                .range::<str, _>((Included(start), Included(end)))
            {
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => {
                        set.remove(k);
                    }
                    (Some(_), _) => {
                        set.insert(k.clone());
                    }
                    (None, _) => {
                        set.remove(k);
                    }
                };
            }
        }

        Ok(set.len())
    }
```

- [ ] **Step 6: Run engine tests — expect PASS**

```
cargo test --test lsm_count 2>&1
```

Expected: all tests pass.

- [ ] **Step 7: Run full suite and clippy**

```
cargo fmt && cargo clippy -- -D warnings && cargo test
```

Expected: all tests pass, zero warnings.

- [ ] **Step 8: Commit**

```
git add src/engine.rs src/lsmengine.rs tests/lsm_count.rs
git commit -m "#51 — add count_prefix + count_range to RangeScan trait and LsmEngine"
```

---

## Task 4 — Dispatch: real arms + full end-to-end tests

Replace the stubs from Task 1 with real dispatch arms, and add the remaining dispatch/stats tests to `tests/count_command.rs`.

**Files:**
- Modify: `src/server/dispatch.rs`
- Modify: `tests/count_command.rs`

- [ ] **Step 1: Add dispatch and stats tests to `tests/count_command.rs`**

Append to the file:

```rust
use rustikv::engine::StorageEngine;
use rustikv::kvengine::KVEngine;
use rustikv::lsmengine::LsmEngine;
use rustikv::server::{CompactionCfg, dispatch};
use rustikv::bffp::ResponseStatus;
use rustikv::settings::FSyncStrategy;
use rustikv::size_tiered::SizeTiered;
use rustikv::stats::Stats;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

fn temp_dir_d(suffix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut path = env::temp_dir();
    path.push(format!("kv_count_cmd_{}_{}", suffix, nanos));
    fs::create_dir_all(&path).unwrap();
    path.to_string_lossy().to_string()
}

fn kv_engine(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir_d(suffix);
    Arc::new(KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).unwrap())
}

fn lsm_engine(suffix: &str) -> Arc<dyn StorageEngine> {
    let dir = temp_dir_d(suffix);
    let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
    Arc::new(LsmEngine::new(&dir, "seg", 1_048_576, strategy, 4096, true).unwrap())
}

fn no_compact() -> CompactionCfg {
    CompactionCfg { ratio: 0.0, max_segment: 0 }
}

// --- End-to-end dispatch ---

#[test]
fn dispatch_count_prefix_on_lsm_returns_correct_count() {
    let db = lsm_engine("disp_cp");
    db.set("cpu:a", "1").unwrap();
    db.set("cpu:b", "2").unwrap();
    db.set("mem:a", "3").unwrap();
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::CountPrefix("cpu:".to_string()), &db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();

    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["2".to_string()]);
}

#[test]
fn dispatch_count_range_on_lsm_returns_correct_count() {
    let db = lsm_engine("disp_cr");
    db.set("a", "1").unwrap();
    db.set("b", "2").unwrap();
    db.set("c", "3").unwrap();
    db.set("d", "4").unwrap();
    let stats = Arc::new(Stats::new());

    let resp = dispatch(Command::CountRange("b".to_string(), "c".to_string()), &db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();

    assert!(matches!(decoded.status, ResponseStatus::Ok));
    assert_eq!(decoded.payload, vec!["2".to_string()]);
}

#[test]
fn dispatch_count_prefix_on_kv_is_not_supported() {
    let db = kv_engine("disp_cp_kv");
    let stats = Arc::new(Stats::new());
    let resp = dispatch(Command::CountPrefix("cpu:".to_string()), &db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
}

#[test]
fn dispatch_count_range_on_kv_is_not_supported() {
    let db = kv_engine("disp_cr_kv");
    let stats = Arc::new(Stats::new());
    let resp = dispatch(Command::CountRange("a".to_string(), "z".to_string()), &db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
}

#[test]
fn count_command_bumps_reads_by_one_not_by_match_count() {
    // The dispatch arms must bump stats.reads by 1 per call (not by the number of
    // matched keys). This pins the deliberate divergence from RANGE/PREFIX.
    let db = lsm_engine("disp_stats");
    db.set("cpu:a", "1").unwrap();
    db.set("cpu:b", "2").unwrap();
    db.set("cpu:c", "3").unwrap();
    let stats = Arc::new(Stats::new());

    let before = stats.reads.load(Ordering::Relaxed);
    dispatch(Command::CountPrefix("cpu:".to_string()), &db, &stats, &no_compact());
    let after = stats.reads.load(Ordering::Relaxed);

    assert_eq!(
        after - before,
        1,
        "COUNT must bump stats.reads by 1, not by the {} matched keys",
        3
    );

    let before = stats.reads.load(Ordering::Relaxed);
    dispatch(Command::CountRange("cpu:a".to_string(), "cpu:c".to_string()), &db, &stats, &no_compact());
    let after = stats.reads.load(Ordering::Relaxed);

    assert_eq!(after - before, 1, "COUNT <start> <end> must also bump reads by 1");
}
```

- [ ] **Step 2: Run dispatch tests — expect FAIL**

```
cargo test --test count_command dispatch_count 2>&1
cargo test --test count_command count_command_bumps 2>&1
```

Expected: tests fail — stubs return Error instead of the actual count.

- [ ] **Step 3: Replace stub dispatch arms in `src/server/dispatch.rs`**

Replace the stub added in Task 1:
```rust
        Command::CountPrefix(_) | Command::CountRange(_, _) => {
            encode_frame(ResponseStatus::Error, &["not yet implemented".to_string()])
        }
```

with:

```rust
        Command::CountPrefix(prefix) => {
            log_verbose(format!(
                "Parsed COUNT <prefix> command: prefix='{}'",
                prefix
            ));
            match database.as_any().downcast_ref::<LsmEngine>() {
                Some(lsm) => match lsm.count_prefix(&prefix) {
                    Ok(count) => {
                        stats.reads.fetch_add(1, Ordering::Relaxed);
                        encode_frame(ResponseStatus::Ok, &[count.to_string()])
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                },
                None => encode_frame(
                    ResponseStatus::Error,
                    &["COUNT not supported by KV engine".to_string()],
                ),
            }
        }
        Command::CountRange(start, end) => {
            log_verbose(format!(
                "Parsed COUNT <start> <end> command: start='{}' end='{}'",
                start, end
            ));
            match database.as_any().downcast_ref::<LsmEngine>() {
                Some(lsm) => match lsm.count_range(&start, &end) {
                    Ok(count) => {
                        stats.reads.fetch_add(1, Ordering::Relaxed);
                        encode_frame(ResponseStatus::Ok, &[count.to_string()])
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                },
                None => encode_frame(
                    ResponseStatus::Error,
                    &["COUNT not supported by KV engine".to_string()],
                ),
            }
        }
```

- [ ] **Step 4: Run all count_command tests — expect PASS**

```
cargo test --test count_command 2>&1
```

Expected: all tests pass.

- [ ] **Step 5: Run full suite and clippy**

```
cargo fmt && cargo clippy -- -D warnings && cargo test
```

Expected: all tests pass, zero warnings.

- [ ] **Step 6: Commit**

```
git add src/server/dispatch.rs tests/count_command.rs
git commit -m "#51 — real COUNT dispatch arms + full end-to-end and stats tests"
```

---

## Task 5 — Docs: README command table

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add COUNT rows to the command table**

After the `PREFIX` row (line 174), add:

```markdown
| `COUNT <prefix>` | 15 | **LSM only.** Returns the number of live keys starting with `<prefix>`. Empty prefix counts all live keys. Returns an error on the KV engine |
| `COUNT <start> <end>` | 16 | **LSM only.** Returns the number of live keys in the inclusive range `[start, end]`. Returns 0 if `start > end`. Returns an error on the KV engine |
```

- [ ] **Step 2: Run full suite**

```
cargo fmt && cargo clippy -- -D warnings && cargo test
```

Expected: all tests pass, zero warnings.

- [ ] **Step 3: Commit**

```
git add README.md
git commit -m "#51 — add COUNT command rows to README"
```

---

## Pre-PR checklist

Before opening the PR:

- [ ] `cargo fmt` — no formatting changes
- [ ] `cargo clippy -- -D warnings` — zero warnings
- [ ] `cargo test` — all tests pass
- [ ] README updated (done in Task 5)
- [ ] `TASKS.md` — move #51 from Open Tasks to Closed Tasks, add PR link after opening
