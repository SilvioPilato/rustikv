# PREFIX command (#50) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `PREFIX <prefix>` TCP command that returns all live `(key, value)` pairs whose keys start with the given string, LSM-only, mirroring `RANGE` (#48).

**Architecture:** A new BFFP op code + `Command` variant carries a single string; `cli.rs` parses it; a `prefix()` method on the existing `RangeScan` trait performs a three-tier merge (SSTables → immutable memtable → active memtable) reusing `range`'s logic but with `key.starts_with(prefix)` as the membership test and a lexicographic-successor bound (`utils::prefix_successor`) as a segment-pruning hint only; `dispatch.rs` downcasts to `LsmEngine` and returns a KV-not-supported error otherwise.

**Tech Stack:** Rust, Cargo. Tests in `tests/` (no inline `#[cfg(test)]` per AGENTS.md). Spec: `docs/superpowers/specs/2026-05-24-prefix-command-design.md`.

**Conventions:** Per AGENTS.md the per-commit gate is `cargo fmt` → `cargo clippy -- -D warnings` → `cargo test`, in that order. Branch is `50-prefix`. Run the full gate before each commit (the per-task commands below show the focused test; the full gate is implied before committing).

**Invariant references** below (Inv N) point at the numbered invariants in the spec.

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `src/utils.rs` | Modify | Add `prefix_successor` + private `next_scalar` (pure string logic) |
| `src/bffp.rs` | Modify | `OpCode::Prefix = 14`, `Command::Prefix(String)`, decode + encode arms |
| `src/cli.rs` | Modify | `"PREFIX"` parse arm (arity 2) |
| `src/engine.rs` | Modify | Add `prefix()` to the `RangeScan` trait |
| `src/lsmengine.rs` | Modify | `impl RangeScan::prefix` for `LsmEngine` (the scan) |
| `src/server/dispatch.rs` | Modify | `Command::Prefix` dispatch arm |
| `README.md` | Modify | Add `PREFIX` (op 14) row to command table |
| `tests/prefix_successor.rs` | Create | Unit tests for the successor helper (Inv 3) |
| `tests/lsmengine.rs` | Modify | Engine-level `prefix_*` tests (Inv 1,2,4,5,6,9) |
| `tests/rustikli.rs` | Modify | CLI parse tests for `PREFIX` |
| `tests/prefix_command.rs` | Create | End-to-end BFFP dispatch tests incl. KV-not-supported |

---

## Task 1: `prefix_successor` helper (the riskiest unit — do it first, fully TDD)

**Files:**
- Create: `tests/prefix_successor.rs`
- Modify: `src/utils.rs`

- [ ] **Step 1: Write the failing tests**

Create `tests/prefix_successor.rs`:

```rust
use rustikv::utils::prefix_successor;

#[test]
fn ascii_increments_last_char() {
    assert_eq!(prefix_successor("cpu").as_deref(), Some("cpv"));
}

#[test]
fn successor_is_strictly_above_all_matching_keys() {
    // every string starting with "cpu" must be < successor
    let succ = prefix_successor("cpu").unwrap();
    assert!("cpu".to_string() < succ);
    assert!("cpu:host1".to_string() < succ);
    assert!("cpu\u{10FFFF}".to_string() < succ);
    // and the next sibling prefix is NOT below it
    assert!("cpv".to_string() >= succ);
}

#[test]
fn empty_prefix_has_no_successor() {
    assert_eq!(prefix_successor(""), None);
}

#[test]
fn trailing_char_max_carries_to_previous_scalar() {
    // "a\u{10FFFF}" -> drop the MAX, bump 'a' -> "b"
    assert_eq!(prefix_successor("a\u{10FFFF}").as_deref(), Some("b"));
}

#[test]
fn all_char_max_has_no_successor() {
    assert_eq!(prefix_successor("\u{10FFFF}\u{10FFFF}"), None);
}

#[test]
fn skips_surrogate_gap() {
    // U+D7FF + 1 would be a surrogate (U+D800); must jump to U+E000
    assert_eq!(prefix_successor("\u{D7FF}").as_deref(), Some("\u{E000}"));
}

#[test]
fn multibyte_prefix_increments_last_scalar() {
    // 'é' (U+00E9) -> U+00EA
    assert_eq!(prefix_successor("é").as_deref(), Some("\u{00EA}"));
}
```

- [ ] **Step 2: Run to verify it fails (does not compile — `prefix_successor` undefined)**

Run: `cargo test --test prefix_successor`
Expected: FAIL — `cannot find function prefix_successor`

- [ ] **Step 3: Implement in `src/utils.rs`**

```rust
/// Next Unicode scalar after `c`, skipping the surrogate gap. `None` for char::MAX.
fn next_scalar(c: char) -> Option<char> {
    match c {
        '\u{D7FF}' => Some('\u{E000}'), // jump the surrogate gap U+D800..=U+DFFF
        '\u{10FFFF}' => None,           // char::MAX — no successor
        _ => char::from_u32(c as u32 + 1), // guaranteed Some for all other scalars
    }
}

/// Least string strictly greater than every string starting with `prefix`.
/// `None` => no finite successor exists (empty prefix, or trailing char::MAX),
/// meaning "scan everything".
pub fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        if let Some(next) = next_scalar(last) {
            let mut s: String = chars.iter().collect();
            s.push(next);
            return Some(s);
        }
        // last was char::MAX: drop it and try to bump the preceding scalar.
    }
    None
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --test prefix_successor`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add src/utils.rs tests/prefix_successor.rs
git commit -m "#50 — prefix_successor helper for prefix-scan upper bound"
```

---

## Task 2: BFFP protocol (op code, command, encode/decode round-trip)

**Files:**
- Modify: `src/bffp.rs` (Command enum ~29, OpCode ~46, TryFrom ~66, decode ~170, encode ~431)
- Create test in: `tests/prefix_command.rs` (round-trip portion; full dispatch added in Task 5)

- [ ] **Step 1: Write the failing round-trip test**

Create `tests/prefix_command.rs` (start with just the codec test; engine/dispatch helpers added in Task 5):

```rust
use rustikv::bffp::{Command, decode_input_frame, encode_command};

#[test]
fn prefix_command_round_trips() {
    let frame = encode_command(Command::Prefix("cpu:".to_string()));
    match decode_input_frame(&frame).unwrap() {
        Command::Prefix(p) => assert_eq!(p, "cpu:"),
        _ => panic!("expected Command::Prefix"),
    }
}

#[test]
fn prefix_command_round_trips_empty() {
    let frame = encode_command(Command::Prefix(String::new()));
    match decode_input_frame(&frame).unwrap() {
        Command::Prefix(p) => assert_eq!(p, ""),
        _ => panic!("expected Command::Prefix"),
    }
}
```

> Confirm the decode fn name (`decode_input_frame`) against `src/bffp.rs`; match whatever the existing `incr_command.rs` / `bffp_ttl.rs` tests import.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --test prefix_command`
Expected: FAIL — no `Command::Prefix` variant.

- [ ] **Step 3: Implement the four edits in `src/bffp.rs`**

```rust
// Command enum (after `Incr(String),`):
    Prefix(String),

// OpCode enum (after `Incr = 13,`):
    Prefix = 14,

// TryFrom<u8> (after `13 => Ok(OpCode::Incr),`):
            14 => Ok(OpCode::Prefix),

// decode_input_frame, after the Incr arm:
        Ok(OpCode::Prefix) => Ok(Command::Prefix(read_key(&mut cur)?)),

// encode_command, after the Incr arm — mirrors Incr exactly:
        Command::Prefix(prefix) => {
            // | total_len(4) | OpCode::Prefix(1) | key_len(2) | prefix |
            let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
            let key_len = prefix.len() as u16;
            payload.write_all(&total_len.to_be_bytes()).unwrap();
            payload.write_all(&[OpCode::Prefix as u8]).unwrap();
            payload.write_all(&key_len.to_be_bytes()).unwrap();
            payload.write_all(prefix.as_bytes()).unwrap();
            payload.into_inner()
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --test prefix_command`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/bffp.rs tests/prefix_command.rs
git commit -m "#50 — BFFP: OpCode::Prefix (14) + Command::Prefix encode/decode"
```

---

## Task 3: CLI parse arm

**Files:**
- Modify: `src/cli.rs` (after the `"RANGE"` arm ~91)
- Modify: `tests/rustikli.rs`

- [ ] **Step 1: Write the failing parse tests**

Add to `tests/rustikli.rs` (match the existing parse-test style there):

```rust
#[test]
fn parses_prefix_command() {
    match parse_command("PREFIX cpu:") {
        ParseResult::Cmd(Command::Prefix(p)) => assert_eq!(p, "cpu:"),
        other => panic!("expected Prefix, got {:?}", other),
    }
}

#[test]
fn prefix_wrong_arity_is_invalid() {
    assert!(matches!(parse_command("PREFIX"), ParseResult::InvalidInput(_)));
    assert!(matches!(parse_command("PREFIX a b"), ParseResult::InvalidInput(_)));
}
```

> Use the same imports/helpers the existing `parse_command` tests in `tests/rustikli.rs` use (they already cover `RANGE`/`TTL`).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --test rustikli`
Expected: FAIL — `PREFIX` parses to whatever the fallthrough produces, not `Command::Prefix`.

- [ ] **Step 3: Implement in `src/cli.rs` (after the `"RANGE"` arm)**

```rust
        "PREFIX" => {
            if words.len() == 2 {
                ParseResult::Cmd(Command::Prefix(words[1].to_string()))
            } else {
                ParseResult::InvalidInput("Usage: PREFIX <prefix>".to_string())
            }
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --test rustikli`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cli.rs tests/rustikli.rs
git commit -m "#50 — cli: PREFIX parse arm"
```

---

## Task 4: Engine — `prefix()` on `RangeScan` + LSM scan (the core)

**Files:**
- Modify: `src/engine.rs` (`RangeScan` trait ~31)
- Modify: `src/lsmengine.rs` (imports ~1/14; `impl RangeScan for LsmEngine` ~516)
- Modify: `tests/lsmengine.rs` (after the `// --- RangeScan tests ---` block)

> **Compilation note:** adding `prefix` to the trait and implementing it on `LsmEngine` must land together or the crate won't build. Write the tests first (they'll fail to compile), then make both edits in one step.

- [ ] **Step 1: Write the failing engine tests**

Add to `tests/lsmengine.rs` (reuse the existing `temp_dir`, `new_engine`, `engine_from_dir`, `BIG_MEMTABLE` helpers):

```rust
// --- PREFIX (RangeScan::prefix) tests ---

#[test]
fn prefix_basic_memtable() {
    let dir = temp_dir("prefix_basic");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.set("mem:a", "3").unwrap();

    let results = engine.prefix("cpu:").unwrap();
    assert_eq!(
        results,
        vec![
            ("cpu:a".to_string(), "1".to_string()),
            ("cpu:b".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn prefix_excludes_non_matching_boundary() {
    // Inv 1: starts_with, NOT range <=. "cpu" must not match "cpv"/"cpz".
    let dir = temp_dir("prefix_boundary");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu1", "1").unwrap();
    engine.set("cpv", "2").unwrap();

    let results = engine.prefix("cpu").unwrap();
    assert_eq!(results, vec![("cpu1".to_string(), "1".to_string())]);
}

#[test]
fn prefix_full_key_match() {
    let dir = temp_dir("prefix_fullkey");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu", "1").unwrap();
    let results = engine.prefix("cpu").unwrap();
    assert_eq!(results, vec![("cpu".to_string(), "1".to_string())]);
}

#[test]
fn prefix_no_match_returns_empty() {
    let dir = temp_dir("prefix_nomatch");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "1").unwrap();
    assert!(engine.prefix("disk:").unwrap().is_empty());
}

#[test]
fn prefix_empty_returns_all_live_keys() {
    // Inv 9
    let dir = temp_dir("prefix_empty");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("a", "1").unwrap();
    engine.set("b", "2").unwrap();
    let keys: Vec<String> = engine.prefix("").unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn prefix_tombstone_suppression() {
    // Inv 4
    let dir = temp_dir("prefix_tomb");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    engine.delete("cpu:b").unwrap();
    let results = engine.prefix("cpu:").unwrap();
    assert_eq!(results, vec![("cpu:a".to_string(), "1".to_string())]);
}

#[test]
fn prefix_returns_sorted_order() {
    // Inv 6
    let dir = temp_dir("prefix_sorted");
    let engine = new_engine(&dir, "test", BIG_MEMTABLE).unwrap();
    engine.set("cpu:c", "3").unwrap();
    engine.set("cpu:a", "1").unwrap();
    engine.set("cpu:b", "2").unwrap();
    let keys: Vec<String> = engine.prefix("cpu:").unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec!["cpu:a", "cpu:b", "cpu:c"]);
}

#[test]
fn prefix_pruning_is_result_neutral_across_segments() {
    // Inv 2: matching keys scattered across multiple SSTables. Use a tiny
    // memtable threshold so each write flushes to its own segment; reload, then
    // assert prefix() == brute-force starts_with over list_keys()+get().
    let dir = temp_dir("prefix_pruning");
    {
        let engine = new_engine(&dir, "test", 1).unwrap(); // tiny -> flush per write
        engine.set("aaa", "1").unwrap();
        engine.set("cpu:1", "x").unwrap();
        engine.set("mmm", "2").unwrap();
        engine.set("cpu:2", "y").unwrap();
        engine.set("zzz", "3").unwrap();
    } // drop joins background flush
    let engine = engine_from_dir(&dir, "test", BIG_MEMTABLE).unwrap();

    let mut expected: Vec<(String, String)> = engine
        .list_keys().unwrap().into_iter()
        .filter(|k| k.starts_with("cpu:"))
        .map(|k| (k.clone(), engine.get(&k).unwrap().unwrap().1))
        .collect();
    expected.sort();

    assert_eq!(engine.prefix("cpu:").unwrap(), expected);
}
```

> Verify `get` returns `(key, value)` (it returns `Option<(String, String)>` per `engine.rs`); adjust the `.1` accessor if the tuple shape differs. If `engine_from_dir` has a different name in this file, use the one the existing `from_dir_reloads_segments` test uses.

- [ ] **Step 2: Run to verify it fails (does not compile — no `prefix` method)**

Run: `cargo test --test lsmengine`
Expected: FAIL — `no method named prefix found for ... LsmEngine`.

- [ ] **Step 3a: Add to the `RangeScan` trait in `src/engine.rs`**

```rust
pub trait RangeScan {
    fn range(&self, start: &str, end: &str) -> io::Result<Vec<(String, String)>>;
    fn prefix(&self, prefix: &str) -> io::Result<Vec<(String, String)>>;
}
```

- [ ] **Step 3b: Update imports in `src/lsmengine.rs`**

```rust
use std::ops::Bound::{Included, Unbounded};
use crate::utils::{is_expired, now_ms, prefix_successor};
```

- [ ] **Step 3c: Implement `prefix` inside `impl RangeScan for LsmEngine`**

```rust
fn prefix(&self, prefix: &str) -> io::Result<Vec<(String, String)>> {
    let now_ms = now_ms();
    let mut b_map: BTreeMap<String, String> = BTreeMap::new();

    // Pruning hint only (Inv 2). None => no finite successor, scan all (Inv 3/9).
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
                    continue; // Inv 1
                }
                if let Some(expiry_ms) = record.header.expiry_ms
                    && is_expired(expiry_ms, now_ms)
                {
                    b_map.remove(&record.key);
                    continue;
                }
                if record.header.is_tombstone() {
                    b_map.remove(&record.key);
                    continue;
                }
                b_map.insert(record.key, record.value);
            }
        }
    }

    {
        let immutable = self.shared.immutable.read().unwrap();
        if let Some(memtable) = immutable.as_ref() {
            for (k, entry) in memtable.entries().range::<str, _>((Included(prefix), Unbounded)) {
                if !k.starts_with(prefix) {
                    break; // sorted keys: matches are contiguous from `prefix`
                }
                match (entry.value.as_deref(), entry.expiry_ms) {
                    (Some(_), Some(ms)) if is_expired(ms, now_ms) => b_map.remove(k),
                    (Some(val), _) => b_map.insert(k.clone(), val.to_string()),
                    (None, _) => b_map.remove(k),
                };
            }
        }
    }

    {
        let memtable = self.shared.active.read().unwrap();
        for (k, entry) in memtable.entries().range::<str, _>((Included(prefix), Unbounded)) {
            if !k.starts_with(prefix) {
                break;
            }
            match (entry.value.as_deref(), entry.expiry_ms) {
                (Some(_), Some(ms)) if is_expired(ms, now_ms) => b_map.remove(k),
                (Some(val), _) => b_map.insert(k.clone(), val.to_string()),
                (None, _) => b_map.remove(k),
            };
        }
    }

    Ok(b_map.into_iter().collect())
}
```

> If any *other* type implements `RangeScan` it now needs a `prefix` impl too. Per current code only `LsmEngine` does — confirm with `grep -rn "impl RangeScan" src/`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --test lsmengine`
Expected: PASS (existing range tests + 8 new prefix tests)

- [ ] **Step 5: Commit**

```bash
git add src/engine.rs src/lsmengine.rs tests/lsmengine.rs
git commit -m "#50 — LsmEngine::prefix scan via RangeScan trait"
```

---

## Task 5: Dispatch wiring + end-to-end tests (incl. KV-not-supported)

**Files:**
- Modify: `src/server/dispatch.rs` (after the `Command::Incr` arm ~246)
- Modify: `tests/prefix_command.rs` (add dispatch tests; reuse helpers from `tests/incr_command.rs`)

- [ ] **Step 1: Write the failing end-to-end tests**

Append to `tests/prefix_command.rs` (copy the `temp_dir` / `lsm_engine` / `kv_engine` / `no_compact` helpers from `tests/incr_command.rs`, plus `Stats`/`dispatch` imports):

```rust
#[test]
fn dispatch_prefix_on_lsm_returns_matching_pairs() {
    let db = lsm_engine("disp_ok");
    db.set("cpu:a", "1").unwrap();
    db.set("cpu:b", "2").unwrap();
    db.set("mem:a", "3").unwrap();
    let stats = Arc::new(Stats::new());

    let frame = encode_command(Command::Prefix("cpu:".to_string()));
    let cmd = decode_input_frame(&frame).unwrap();
    let resp = dispatch(cmd, &db, &stats, &no_compact());

    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Ok));
    // payload flattens to [k, v, k, v]
    assert_eq!(decoded.payload, vec!["cpu:a", "1", "cpu:b", "2"]);
}

#[test]
fn dispatch_prefix_on_kv_is_not_supported() {
    let db = kv_engine("disp_kv");
    let stats = Arc::new(Stats::new());
    let frame = encode_command(Command::Prefix("cpu:".to_string()));
    let cmd = decode_input_frame(&frame).unwrap();
    let resp = dispatch(cmd, &db, &stats, &no_compact());
    let decoded = decode_response_frame(&resp).unwrap();
    assert!(matches!(decoded.status, ResponseStatus::Error));
}
```

> Add the imports `decode_response_frame`, `ResponseStatus`, `dispatch`, `CompactionCfg`, `Stats`, engines, etc. exactly as `tests/incr_command.rs` does.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test --test prefix_command`
Expected: FAIL — `Command::Prefix` is unhandled in `dispatch` (non-exhaustive match won't compile, or returns wrong frame).

- [ ] **Step 3: Implement the dispatch arm in `src/server/dispatch.rs` (after `Command::Incr`)**

```rust
        Command::Prefix(prefix) => {
            log_verbose(format!("Parsed PREFIX command: prefix='{}'", prefix));
            match database.as_any().downcast_ref::<LsmEngine>() {
                Some(lsm) => match lsm.prefix(&prefix) {
                    Ok(results) => {
                        let results_count = results.len();
                        let res: Vec<String> =
                            results.into_iter().flat_map(|(k, v)| [k, v]).collect();
                        stats.reads.fetch_add(results_count as u64, Ordering::Relaxed);
                        encode_frame(ResponseStatus::Ok, &res)
                    }
                    Err(error) => encode_frame(ResponseStatus::Error, &[error.to_string()]),
                },
                None => encode_frame(
                    ResponseStatus::Error,
                    &["PREFIX not supported by KV engine".to_string()],
                ),
            }
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test --test prefix_command`
Expected: PASS (4 tests total)

- [ ] **Step 5: Commit**

```bash
git add src/server/dispatch.rs tests/prefix_command.rs
git commit -m "#50 — dispatch PREFIX (LSM only; KV returns not-supported)"
```

---

## Task 6: README + finalize task bookkeeping

**Files:**
- Modify: `README.md` (command table)
- Modify: `TASKS.md` (move #50 In Progress → Closed Tasks with PR link)

- [ ] **Step 1: Add the `PREFIX` row to the README command table**

Find the command table (it lists `RANGE` op 11, `TTL` op 12, `INCR` op 13) and add:

```
| `PREFIX <prefix>` | 14 | LSM only. Returns all live key/value pairs whose key starts with `<prefix>`. Empty prefix returns all keys. KV engine returns an error. |
```

> Match the exact column layout of the existing rows.

- [ ] **Step 2: Run the full gate**

Run, in order:
```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```
Expected: fmt clean, zero clippy warnings, all tests pass.

- [ ] **Step 3: Move #50 to Closed Tasks in `TASKS.md`**

Cut the `## #50` section from **In Progress** to **Closed Tasks**, append a one-line summary and the PR link (added after the PR is opened).

- [ ] **Step 4: Commit**

```bash
git add README.md TASKS.md
git commit -m "#50 — README PREFIX row; move task to Closed Tasks"
```

- [ ] **Step 5: Open the PR**

```bash
git push -u origin 50-prefix
gh pr create --title "#50 — PREFIX command (LSM only)" --body "$(cat <<'EOF'
Adds the `PREFIX <prefix>` TCP command (LSM only), returning all live key/value
pairs whose keys start with `<prefix>`. Mirrors RANGE (#48). Empty prefix
returns all keys; KV engine returns an error.

Spec: docs/superpowers/specs/2026-05-24-prefix-command-design.md

Opened via Claude
EOF
)"
```

Then add the PR link back to the `## #50` block in `TASKS.md` and commit.

---

## Verification Checklist (maps to spec invariants)

- [ ] Inv 1 — `prefix_excludes_non_matching_boundary` ("cpu" ∌ "cpv")
- [ ] Inv 2 — `prefix_pruning_is_result_neutral_across_segments`
- [ ] Inv 3 — `tests/prefix_successor.rs` (surrogate gap, char::MAX carry, empty)
- [ ] Inv 4 — `prefix_tombstone_suppression`
- [ ] Inv 5 — dedup is structural (BTreeMap); covered implicitly by multi-segment test
- [ ] Inv 6 — `prefix_returns_sorted_order`
- [ ] Inv 9 — `prefix_empty_returns_all_live_keys`
- [ ] KV-not-supported — `dispatch_prefix_on_kv_is_not_supported`
- [ ] Full gate: `cargo fmt` && `cargo clippy -- -D warnings` && `cargo test`
```
