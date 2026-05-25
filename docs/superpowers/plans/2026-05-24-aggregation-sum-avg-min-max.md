# Aggregation (SUM/AVG/MIN/MAX) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add eight LSM-only TCP commands (`SUM`/`AVG`/`MIN`/`MAX`, each in prefix and range variants) that reduce live numeric values matching a query to a single scalar.

**Architecture:** Each command delegates to the existing `prefix()`/`range()` three-tier merge to resolve live key-value pairs, parses values as `f64` via a shared `resolve_numeric` helper, then folds. Eight new BFFP op codes (17–24), eight `Command` variants, four CLI arms, eight `RangeScan` trait methods, and eight dispatch arms — all modelled on the existing `COUNT` implementation.

**Tech Stack:** Rust / Cargo. No new dependencies. Builds on `src/bffp.rs`, `src/cli.rs`, `src/engine.rs`, `src/lsmengine.rs`, `src/server/dispatch.rs`.

**Spec:** `docs/superpowers/specs/2026-05-24-aggregation-design.md`

---

## File map

| File | Change |
|---|---|
| `src/bffp.rs` | Add 8 `Command` variants, 8 `OpCode` entries, 8 decode arms, 8 encode arms |
| `src/cli.rs` | Add 4 parse arms (`SUM`/`AVG`/`MIN`/`MAX`) with arity dispatch |
| `src/engine.rs` | Extend `RangeScan` with 8 new methods returning `io::Result<Option<f64>>` |
| `src/lsmengine.rs` | Add `resolve_numeric` free fn + `impl RangeScan` for 8 methods |
| `src/server/dispatch.rs` | Add 8 dispatch arms |
| `tests/lsm_aggregate.rs` | New — engine-level tests |
| `tests/aggregate_command.rs` | New — end-to-end BFFP + CLI + dispatch tests |
| `README.md` | Add 8 rows to command table |

---

## Task 1: Protocol types (`src/bffp.rs`)

**Files:**
- Modify: `src/bffp.rs`

The `Command` enum and `OpCode` enum must exist before any test can compile. Add all 8 variants and op codes now, plus the decode and encode arms that are exact clones of the `CountPrefix`/`CountRange` patterns.

- [ ] **Step 1: Add 8 `Command` variants to the `Command` enum**

  Open `src/bffp.rs`. After the `CountRange(String, String)` variant (line 32), add:

  ```rust
  SumPrefix(String),
  SumRange(String, String),
  AvgPrefix(String),
  AvgRange(String, String),
  MinPrefix(String),
  MinRange(String, String),
  MaxPrefix(String),
  MaxRange(String, String),
  ```

- [ ] **Step 2: Add 8 `OpCode` entries**

  After `CountRange = 16`, add:

  ```rust
  SumPrefix = 17,
  SumRange = 18,
  AvgPrefix = 19,
  AvgRange = 20,
  MinPrefix = 21,
  MinRange = 22,
  MaxPrefix = 23,
  MaxRange = 24,
  ```

- [ ] **Step 3: Add 8 arms to `TryFrom<u8> for OpCode`**

  After `16 => Ok(OpCode::CountRange),`, add:

  ```rust
  17 => Ok(OpCode::SumPrefix),
  18 => Ok(OpCode::SumRange),
  19 => Ok(OpCode::AvgPrefix),
  20 => Ok(OpCode::AvgRange),
  21 => Ok(OpCode::MinPrefix),
  22 => Ok(OpCode::MinRange),
  23 => Ok(OpCode::MaxPrefix),
  24 => Ok(OpCode::MaxRange),
  ```

- [ ] **Step 4: Add 8 decode arms to `decode_input_frame`**

  After the `CountRange` decode arm, add (each Prefix arm mirrors CountPrefix; each Range arm mirrors CountRange):

  ```rust
  Ok(OpCode::SumPrefix) => Ok(Command::SumPrefix(read_key(&mut cur)?)),
  Ok(OpCode::SumRange) => {
      let start = read_key(&mut cur)?;
      let end = read_key(&mut cur)?;
      Ok(Command::SumRange(start, end))
  }
  Ok(OpCode::AvgPrefix) => Ok(Command::AvgPrefix(read_key(&mut cur)?)),
  Ok(OpCode::AvgRange) => {
      let start = read_key(&mut cur)?;
      let end = read_key(&mut cur)?;
      Ok(Command::AvgRange(start, end))
  }
  Ok(OpCode::MinPrefix) => Ok(Command::MinPrefix(read_key(&mut cur)?)),
  Ok(OpCode::MinRange) => {
      let start = read_key(&mut cur)?;
      let end = read_key(&mut cur)?;
      Ok(Command::MinRange(start, end))
  }
  Ok(OpCode::MaxPrefix) => Ok(Command::MaxPrefix(read_key(&mut cur)?)),
  Ok(OpCode::MaxRange) => {
      let start = read_key(&mut cur)?;
      let end = read_key(&mut cur)?;
      Ok(Command::MaxRange(start, end))
  }
  ```

  Place these **before** the `Err(_) => Ok(Command::Invalid(...))` arm.

- [ ] **Step 5: Add 8 encode arms to `encode_command`**

  In the `encode_command` match, after the `CountRange` arm, add the following. The four `*Prefix` arms are identical to `CountPrefix` except for the op code; the four `*Range` arms are identical to `CountRange`.

  ```rust
  Command::SumPrefix(prefix) => {
      let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
      let key_len = prefix.len() as u16;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::SumPrefix as u8]).unwrap();
      payload.write_all(&key_len.to_be_bytes()).unwrap();
      payload.write_all(prefix.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::SumRange(start, end) => {
      let start_len = start.len() as u16;
      let end_len = end.len() as u16;
      let total_len =
          OP_CODE_SIZE as u32 + KEY_LEN_SIZE as u32 * 2 + start_len as u32 + end_len as u32;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::SumRange as u8]).unwrap();
      payload.write_all(&start_len.to_be_bytes()).unwrap();
      payload.write_all(start.as_bytes()).unwrap();
      payload.write_all(&end_len.to_be_bytes()).unwrap();
      payload.write_all(end.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::AvgPrefix(prefix) => {
      let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
      let key_len = prefix.len() as u16;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::AvgPrefix as u8]).unwrap();
      payload.write_all(&key_len.to_be_bytes()).unwrap();
      payload.write_all(prefix.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::AvgRange(start, end) => {
      let start_len = start.len() as u16;
      let end_len = end.len() as u16;
      let total_len =
          OP_CODE_SIZE as u32 + KEY_LEN_SIZE as u32 * 2 + start_len as u32 + end_len as u32;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::AvgRange as u8]).unwrap();
      payload.write_all(&start_len.to_be_bytes()).unwrap();
      payload.write_all(start.as_bytes()).unwrap();
      payload.write_all(&end_len.to_be_bytes()).unwrap();
      payload.write_all(end.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::MinPrefix(prefix) => {
      let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
      let key_len = prefix.len() as u16;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::MinPrefix as u8]).unwrap();
      payload.write_all(&key_len.to_be_bytes()).unwrap();
      payload.write_all(prefix.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::MinRange(start, end) => {
      let start_len = start.len() as u16;
      let end_len = end.len() as u16;
      let total_len =
          OP_CODE_SIZE as u32 + KEY_LEN_SIZE as u32 * 2 + start_len as u32 + end_len as u32;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::MinRange as u8]).unwrap();
      payload.write_all(&start_len.to_be_bytes()).unwrap();
      payload.write_all(start.as_bytes()).unwrap();
      payload.write_all(&end_len.to_be_bytes()).unwrap();
      payload.write_all(end.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::MaxPrefix(prefix) => {
      let total_len = (OP_CODE_SIZE + KEY_LEN_SIZE + prefix.len()) as u32;
      let key_len = prefix.len() as u16;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::MaxPrefix as u8]).unwrap();
      payload.write_all(&key_len.to_be_bytes()).unwrap();
      payload.write_all(prefix.as_bytes()).unwrap();
      payload.into_inner()
  }
  Command::MaxRange(start, end) => {
      let start_len = start.len() as u16;
      let end_len = end.len() as u16;
      let total_len =
          OP_CODE_SIZE as u32 + KEY_LEN_SIZE as u32 * 2 + start_len as u32 + end_len as u32;
      payload.write_all(&total_len.to_be_bytes()).unwrap();
      payload.write_all(&[OpCode::MaxRange as u8]).unwrap();
      payload.write_all(&start_len.to_be_bytes()).unwrap();
      payload.write_all(start.as_bytes()).unwrap();
      payload.write_all(&end_len.to_be_bytes()).unwrap();
      payload.write_all(end.as_bytes()).unwrap();
      payload.into_inner()
  }
  ```

- [ ] **Step 6: Verify it compiles**

  ```
  cargo build 2>&1
  ```

  Expected: compiles. If the `encode_command` match is now non-exhaustive (because `Command::Invalid` must remain last), ensure `Invalid` is still the final arm.

- [ ] **Step 7: Commit**

  ```
  git add src/bffp.rs
  git commit -m "#84 — add 8 aggregation op codes and Command variants to bffp"
  ```

---

## Task 2: CLI parse arms (`src/cli.rs`)

**Files:**
- Modify: `src/cli.rs`

- [ ] **Step 1: Write the failing CLI parse tests**

  Create `tests/aggregate_command.rs` with just the CLI section for now (dispatch tests come in Task 5). Write the full import block now so Task 5's appended code compiles without duplicates:

  ```rust
  use rustikv::bffp::{Command, ResponseStatus, decode_input_frame, decode_response_frame, encode_command};
  use rustikv::cli::{ParseResult, parse_command};

  fn agg_cmd(line: &str) -> Command {
      match parse_command(line) {
          ParseResult::Cmd(c) => c,
          ParseResult::Quit => panic!("expected Cmd, got Quit"),
          ParseResult::InvalidInput(m) => panic!("expected Cmd, got InvalidInput: {m}"),
      }
  }

  fn agg_is_invalid(line: &str) -> bool {
      matches!(parse_command(line), ParseResult::InvalidInput(_))
  }

  // --- SUM ---

  #[test]
  fn cli_sum_arity_2_parses_as_prefix() {
      match agg_cmd("SUM cpu:") {
          Command::SumPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected SumPrefix"),
      }
  }

  #[test]
  fn cli_sum_arity_3_parses_as_range() {
      match agg_cmd("SUM a z") {
          Command::SumRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected SumRange"),
      }
  }

  #[test]
  fn cli_sum_wrong_arity_is_invalid() {
      assert!(agg_is_invalid("SUM"));
      assert!(agg_is_invalid("SUM a b c"));
  }

  #[test]
  fn cli_sum_case_insensitive() {
      match agg_cmd("sum cpu:") {
          Command::SumPrefix(_) => {}
          _ => panic!("expected SumPrefix"),
      }
  }

  // --- AVG ---

  #[test]
  fn cli_avg_arity_2_parses_as_prefix() {
      match agg_cmd("AVG cpu:") {
          Command::AvgPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected AvgPrefix"),
      }
  }

  #[test]
  fn cli_avg_arity_3_parses_as_range() {
      match agg_cmd("AVG a z") {
          Command::AvgRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected AvgRange"),
      }
  }

  #[test]
  fn cli_avg_wrong_arity_is_invalid() {
      assert!(agg_is_invalid("AVG"));
      assert!(agg_is_invalid("AVG a b c"));
  }

  // --- MIN ---

  #[test]
  fn cli_min_arity_2_parses_as_prefix() {
      match agg_cmd("MIN cpu:") {
          Command::MinPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected MinPrefix"),
      }
  }

  #[test]
  fn cli_min_arity_3_parses_as_range() {
      match agg_cmd("MIN a z") {
          Command::MinRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected MinRange"),
      }
  }

  #[test]
  fn cli_min_wrong_arity_is_invalid() {
      assert!(agg_is_invalid("MIN"));
      assert!(agg_is_invalid("MIN a b c"));
  }

  // --- MAX ---

  #[test]
  fn cli_max_arity_2_parses_as_prefix() {
      match agg_cmd("MAX cpu:") {
          Command::MaxPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected MaxPrefix"),
      }
  }

  #[test]
  fn cli_max_arity_3_parses_as_range() {
      match agg_cmd("MAX a z") {
          Command::MaxRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected MaxRange"),
      }
  }

  #[test]
  fn cli_max_wrong_arity_is_invalid() {
      assert!(agg_is_invalid("MAX"));
      assert!(agg_is_invalid("MAX a b c"));
  }
  ```

- [ ] **Step 2: Run to confirm tests fail**

  ```
  cargo test --test aggregate_command cli_ 2>&1
  ```

  Expected: compile error or test failures (CLI arms not yet added).

- [ ] **Step 3: Add 4 parse arms to `src/cli.rs`**

  In `parse_command`, after the `"COUNT"` arm, add:

  ```rust
  "SUM" => match words.len() {
      2 => ParseResult::Cmd(Command::SumPrefix(words[1].to_string())),
      3 => ParseResult::Cmd(Command::SumRange(words[1].to_string(), words[2].to_string())),
      _ => ParseResult::InvalidInput("Usage: SUM <prefix> | SUM <start> <end>".to_string()),
  },
  "AVG" => match words.len() {
      2 => ParseResult::Cmd(Command::AvgPrefix(words[1].to_string())),
      3 => ParseResult::Cmd(Command::AvgRange(words[1].to_string(), words[2].to_string())),
      _ => ParseResult::InvalidInput("Usage: AVG <prefix> | AVG <start> <end>".to_string()),
  },
  "MIN" => match words.len() {
      2 => ParseResult::Cmd(Command::MinPrefix(words[1].to_string())),
      3 => ParseResult::Cmd(Command::MinRange(words[1].to_string(), words[2].to_string())),
      _ => ParseResult::InvalidInput("Usage: MIN <prefix> | MIN <start> <end>".to_string()),
  },
  "MAX" => match words.len() {
      2 => ParseResult::Cmd(Command::MaxPrefix(words[1].to_string())),
      3 => ParseResult::Cmd(Command::MaxRange(words[1].to_string(), words[2].to_string())),
      _ => ParseResult::InvalidInput("Usage: MAX <prefix> | MAX <start> <end>".to_string()),
  },
  ```

- [ ] **Step 4: Run CLI tests — expect pass**

  ```
  cargo test --test aggregate_command cli_ 2>&1
  ```

  Expected: all `cli_*` tests pass.

- [ ] **Step 5: Commit**

  ```
  git add src/cli.rs tests/aggregate_command.rs
  git commit -m "#84 — add SUM/AVG/MIN/MAX CLI parse arms and CLI tests"
  ```

---

## Task 3: Engine trait (`src/engine.rs`)

**Files:**
- Modify: `src/engine.rs`

Extend `RangeScan` with 8 new method signatures. `LsmEngine` will not implement them yet — the compiler will complain, which confirms we know what to implement next.

- [ ] **Step 1: Add 8 method signatures to `RangeScan`**

  In `src/engine.rs`, extend the `RangeScan` trait after `count_range`:

  ```rust
  fn sum_prefix(&self, prefix: &str) -> io::Result<Option<f64>>;
  fn sum_range(&self, start: &str, end: &str) -> io::Result<Option<f64>>;
  fn avg_prefix(&self, prefix: &str) -> io::Result<Option<f64>>;
  fn avg_range(&self, start: &str, end: &str) -> io::Result<Option<f64>>;
  fn min_prefix(&self, prefix: &str) -> io::Result<Option<f64>>;
  fn min_range(&self, start: &str, end: &str) -> io::Result<Option<f64>>;
  fn max_prefix(&self, prefix: &str) -> io::Result<Option<f64>>;
  fn max_range(&self, start: &str, end: &str) -> io::Result<Option<f64>>;
  ```

- [ ] **Step 2: Verify the compiler points to the missing impl**

  ```
  cargo build 2>&1
  ```

  Expected: compile error on `LsmEngine` — "not all trait items implemented". This confirms the trait extension is wired correctly.

---

## Task 4: Engine implementation (`src/lsmengine.rs`)

**Files:**
- Modify: `src/lsmengine.rs`
- Create: `tests/lsm_aggregate.rs`

- [ ] **Step 1: Write the failing engine-level tests**

  Create `tests/lsm_aggregate.rs`:

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
      path.push(format!("kv_lsm_agg_{}_{}", suffix, nanos));
      fs::create_dir_all(&path).unwrap();
      path.to_string_lossy().to_string()
  }

  fn new_engine(dir: &str) -> LsmEngine {
      let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
      LsmEngine::new(dir, "test", 1_048_576, strategy, 4096, true).unwrap()
  }

  fn past_ms() -> u64 {
      SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .unwrap()
          .as_millis() as u64
          - 10_000
  }

  // ── sum_prefix ────────────────────────────────────────────────────────────────

  #[test]
  fn sum_prefix_basic() {
      let engine = new_engine(&temp_dir("sp_basic"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set("cpu:b", "2.0").unwrap();
      engine.set("mem:a", "99.0").unwrap();
      assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(3.0));
  }

  #[test]
  fn sum_prefix_empty_match_returns_none() {
      let engine = new_engine(&temp_dir("sp_empty"));
      engine.set("cpu:a", "1.0").unwrap();
      assert_eq!(engine.sum_prefix("disk:").unwrap(), None);
  }

  #[test]
  fn sum_prefix_tombstone_excluded() {
      let engine = new_engine(&temp_dir("sp_tomb"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set("cpu:b", "2.0").unwrap();
      engine.delete("cpu:b").unwrap();
      assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
  }

  #[test]
  fn sum_prefix_expired_excluded() {
      let engine = new_engine(&temp_dir("sp_exp"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set_with_ttl("cpu:b", "2.0", Some(past_ms())).unwrap();
      assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
  }

  #[test]
  fn sum_prefix_non_numeric_live_value_errors() {
      let engine = new_engine(&temp_dir("sp_err"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set("cpu:b", "not-a-number").unwrap();
      let err = engine.sum_prefix("cpu:").unwrap_err();
      assert!(
          err.to_string().contains("SUM") && err.to_string().contains("cpu:b"),
          "error should mention SUM and the key, got: {}",
          err
      );
  }

  #[test]
  fn sum_prefix_non_numeric_tombstoned_does_not_error() {
      let engine = new_engine(&temp_dir("sp_tomb_nn"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set("cpu:b", "not-a-number").unwrap();
      engine.delete("cpu:b").unwrap();
      // tombstoned non-numeric key must not surface as an error
      assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
  }

  #[test]
  fn sum_prefix_non_numeric_expired_does_not_error() {
      let engine = new_engine(&temp_dir("sp_exp_nn"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set_with_ttl("cpu:b", "not-a-number", Some(past_ms())).unwrap();
      assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(1.0));
  }

  // ── sum_range ─────────────────────────────────────────────────────────────────

  #[test]
  fn sum_range_basic() {
      let engine = new_engine(&temp_dir("sr_basic"));
      engine.set("a", "1.0").unwrap();
      engine.set("b", "2.0").unwrap();
      engine.set("c", "3.0").unwrap();
      engine.set("z", "99.0").unwrap();
      assert_eq!(engine.sum_range("a", "c").unwrap(), Some(6.0));
  }

  #[test]
  fn sum_range_inverted_returns_none() {
      let engine = new_engine(&temp_dir("sr_inv"));
      engine.set("a", "1.0").unwrap();
      assert_eq!(engine.sum_range("z", "a").unwrap(), None);
  }

  #[test]
  fn sum_range_empty_match_returns_none() {
      let engine = new_engine(&temp_dir("sr_empty"));
      engine.set("a", "1.0").unwrap();
      assert_eq!(engine.sum_range("x", "z").unwrap(), None);
  }

  // ── avg ───────────────────────────────────────────────────────────────────────

  #[test]
  fn avg_prefix_basic() {
      let engine = new_engine(&temp_dir("ap_basic"));
      engine.set("cpu:a", "1.0").unwrap();
      engine.set("cpu:b", "3.0").unwrap();
      assert_eq!(engine.avg_prefix("cpu:").unwrap(), Some(2.0));
  }

  #[test]
  fn avg_prefix_single_key_equals_value() {
      let engine = new_engine(&temp_dir("ap_single"));
      engine.set("cpu:a", "7.5").unwrap();
      assert_eq!(engine.avg_prefix("cpu:").unwrap(), Some(7.5));
  }

  #[test]
  fn avg_range_basic() {
      let engine = new_engine(&temp_dir("ar_basic"));
      engine.set("a", "2.0").unwrap();
      engine.set("b", "4.0").unwrap();
      assert_eq!(engine.avg_range("a", "b").unwrap(), Some(3.0));
  }

  // ── min/max ───────────────────────────────────────────────────────────────────

  #[test]
  fn min_prefix_basic() {
      let engine = new_engine(&temp_dir("mp_basic"));
      engine.set("cpu:a", "3.0").unwrap();
      engine.set("cpu:b", "1.0").unwrap();
      engine.set("cpu:c", "2.0").unwrap();
      assert_eq!(engine.min_prefix("cpu:").unwrap(), Some(1.0));
  }

  #[test]
  fn max_prefix_basic() {
      let engine = new_engine(&temp_dir("mx_basic"));
      engine.set("cpu:a", "3.0").unwrap();
      engine.set("cpu:b", "1.0").unwrap();
      engine.set("cpu:c", "2.0").unwrap();
      assert_eq!(engine.max_prefix("cpu:").unwrap(), Some(3.0));
  }

  #[test]
  fn min_max_identical_values() {
      let engine = new_engine(&temp_dir("mm_ident"));
      engine.set("cpu:a", "5.0").unwrap();
      engine.set("cpu:b", "5.0").unwrap();
      assert_eq!(engine.min_prefix("cpu:").unwrap(), Some(5.0));
      assert_eq!(engine.max_prefix("cpu:").unwrap(), Some(5.0));
  }

  #[test]
  fn min_range_basic() {
      let engine = new_engine(&temp_dir("mr_basic"));
      engine.set("a", "10.0").unwrap();
      engine.set("b", "2.0").unwrap();
      engine.set("c", "7.0").unwrap();
      assert_eq!(engine.min_range("a", "c").unwrap(), Some(2.0));
  }

  #[test]
  fn max_range_basic() {
      let engine = new_engine(&temp_dir("mxr_basic"));
      engine.set("a", "10.0").unwrap();
      engine.set("b", "2.0").unwrap();
      engine.set("c", "7.0").unwrap();
      assert_eq!(engine.max_range("a", "c").unwrap(), Some(10.0));
  }

  // ── multi-segment spread ──────────────────────────────────────────────────────

  #[test]
  fn sum_prefix_multi_segment() {
      // Small memtable forces flush to SSTables
      let dir = temp_dir("sp_multi");
      let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
      let engine = LsmEngine::new(&dir, "test", 64, strategy, 4096, true).unwrap();

      // Write enough keys to spill across multiple SSTables
      for i in 0..20 {
          engine.set(&format!("cpu:{:03}", i), &format!("{}.0", i)).unwrap();
      }

      let expected: f64 = (0..20).map(|i| i as f64).sum();
      assert_eq!(engine.sum_prefix("cpu:").unwrap(), Some(expected));
  }

  // ── differential equivalence ─────────────────────────────────────────────────

  #[test]
  fn sum_prefix_differential_equivalence() {
      let dir = temp_dir("sp_diff");
      let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
      let engine = LsmEngine::new(&dir, "test", 64, strategy, 4096, true).unwrap();

      for i in 0..10 {
          engine.set(&format!("k:{:02}", i), &format!("{}.0", i)).unwrap();
      }
      // Overwrite some
      engine.set("k:03", "30.0").unwrap();
      // Tombstone one
      engine.delete("k:05").unwrap();

      let via_prefix: f64 = engine
          .prefix("k:")
          .unwrap()
          .iter()
          .map(|(_, v)| v.parse::<f64>().unwrap())
          .sum();
      assert_eq!(engine.sum_prefix("k:").unwrap(), Some(via_prefix));
  }
  ```

- [ ] **Step 2: Run tests — expect compile error**

  ```
  cargo test --test lsm_aggregate 2>&1
  ```

  Expected: compile error — `LsmEngine` does not implement the 8 new `RangeScan` methods.

- [ ] **Step 3: Add `resolve_numeric` and the 8 `RangeScan` methods to `src/lsmengine.rs`**

  At the bottom of `src/lsmengine.rs`, **before** `impl RangeScan for LsmEngine` (or after it — `resolve_numeric` is a free function in the module), add:

  ```rust
  fn resolve_numeric(op: &str, pairs: Vec<(String, String)>) -> io::Result<Vec<f64>> {
      let mut nums = Vec::with_capacity(pairs.len());
      for (k, v) in pairs {
          match v.parse::<f64>() {
              Ok(n) => nums.push(n),
              Err(_) => {
                  return Err(io::Error::new(
                      io::ErrorKind::InvalidData,
                      format!("{op}: non-numeric value for key {k}"),
                  ));
              }
          }
      }
      Ok(nums)
  }
  ```

  Then inside `impl RangeScan for LsmEngine`, after `count_range`, add the 8 methods:

  ```rust
  fn sum_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
      let pairs = self.prefix(prefix)?;
      let nums = resolve_numeric("SUM", pairs)?;
      if nums.is_empty() { return Ok(None); }
      Ok(Some(nums.iter().sum()))
  }

  fn sum_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
      let pairs = self.range(start, end)?;
      let nums = resolve_numeric("SUM", pairs)?;
      if nums.is_empty() { return Ok(None); }
      Ok(Some(nums.iter().sum()))
  }

  fn avg_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
      let pairs = self.prefix(prefix)?;
      let nums = resolve_numeric("AVG", pairs)?;
      if nums.is_empty() { return Ok(None); }
      let s: f64 = nums.iter().sum();
      Ok(Some(s / nums.len() as f64))
  }

  fn avg_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
      let pairs = self.range(start, end)?;
      let nums = resolve_numeric("AVG", pairs)?;
      if nums.is_empty() { return Ok(None); }
      let s: f64 = nums.iter().sum();
      Ok(Some(s / nums.len() as f64))
  }

  fn min_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
      let pairs = self.prefix(prefix)?;
      let nums = resolve_numeric("MIN", pairs)?;
      if nums.is_empty() { return Ok(None); }
      Ok(Some(nums.iter().fold(f64::INFINITY, |acc, &x| acc.min(x))))
  }

  fn min_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
      let pairs = self.range(start, end)?;
      let nums = resolve_numeric("MIN", pairs)?;
      if nums.is_empty() { return Ok(None); }
      Ok(Some(nums.iter().fold(f64::INFINITY, |acc, &x| acc.min(x))))
  }

  fn max_prefix(&self, prefix: &str) -> io::Result<Option<f64>> {
      let pairs = self.prefix(prefix)?;
      let nums = resolve_numeric("MAX", pairs)?;
      if nums.is_empty() { return Ok(None); }
      Ok(Some(nums.iter().fold(f64::NEG_INFINITY, |acc, &x| acc.max(x))))
  }

  fn max_range(&self, start: &str, end: &str) -> io::Result<Option<f64>> {
      let pairs = self.range(start, end)?;
      let nums = resolve_numeric("MAX", pairs)?;
      if nums.is_empty() { return Ok(None); }
      Ok(Some(nums.iter().fold(f64::NEG_INFINITY, |acc, &x| acc.max(x))))
  }
  ```

- [ ] **Step 4: Run engine tests — expect pass**

  ```
  cargo test --test lsm_aggregate 2>&1
  ```

  Expected: all tests pass.

- [ ] **Step 5: Run full suite — no regressions**

  ```
  cargo test 2>&1
  ```

  Expected: all existing tests still pass.

- [ ] **Step 6: Commit**

  ```
  git add src/engine.rs src/lsmengine.rs tests/lsm_aggregate.rs
  git commit -m "#84 — implement resolve_numeric and 8 RangeScan aggregation methods"
  ```

---

## Task 5: Dispatch + end-to-end tests (`src/server/dispatch.rs`)

**Files:**
- Modify: `src/server/dispatch.rs`
- Modify: `tests/aggregate_command.rs`

- [ ] **Step 1: Add end-to-end dispatch tests to `tests/aggregate_command.rs`**

  Append to the existing file (after the CLI tests added in Task 2). The `rustikv::bffp` and `rustikv::cli` imports are already present from Task 2 — only add the new ones:

  ```rust
  use rustikv::engine::StorageEngine;
  use rustikv::kvengine::KVEngine;
  use rustikv::lsmengine::LsmEngine;
  use rustikv::server::{CompactionCfg, dispatch};
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
      path.push(format!("kv_agg_cmd_{}_{}", suffix, nanos));
      fs::create_dir_all(&path).unwrap();
      path.to_string_lossy().to_string()
  }

  fn lsm(suffix: &str) -> Arc<dyn StorageEngine> {
      let dir = temp_dir_d(suffix);
      let strategy = Box::new(SizeTiered::new(4, 32, 4096, true));
      Arc::new(LsmEngine::new(&dir, "seg", 1_048_576, strategy, 4096, true).unwrap())
  }

  fn kv(suffix: &str) -> Arc<dyn StorageEngine> {
      let dir = temp_dir_d(suffix);
      Arc::new(KVEngine::new(&dir, "seg", 1024 * 1024, FSyncStrategy::Never).unwrap())
  }

  fn no_compact() -> CompactionCfg {
      CompactionCfg { ratio: 0.0, max_segment: 0 }
  }

  fn ok_payload(db: &Arc<dyn StorageEngine>, cmd: Command) -> String {
      let stats = Arc::new(Stats::new());
      let resp = dispatch(cmd, db, &stats, &no_compact());
      let decoded = decode_response_frame(&resp).unwrap();
      assert!(matches!(decoded.status, ResponseStatus::Ok), "expected Ok, got {:?} payload={:?}", decoded.status as u8, decoded.payload);
      decoded.payload.into_iter().next().unwrap()
  }

  fn expect_not_found(db: &Arc<dyn StorageEngine>, cmd: Command) {
      let stats = Arc::new(Stats::new());
      let resp = dispatch(cmd, db, &stats, &no_compact());
      let decoded = decode_response_frame(&resp).unwrap();
      assert!(matches!(decoded.status, ResponseStatus::NotFound));
  }

  fn expect_error(db: &Arc<dyn StorageEngine>, cmd: Command) -> String {
      let stats = Arc::new(Stats::new());
      let resp = dispatch(cmd, db, &stats, &no_compact());
      let decoded = decode_response_frame(&resp).unwrap();
      assert!(matches!(decoded.status, ResponseStatus::Error));
      decoded.payload.into_iter().next().unwrap_or_default()
  }

  // --- BFFP round-trips ---

  #[test]
  fn sum_prefix_round_trips() {
      let frame = encode_command(Command::SumPrefix("cpu:".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::SumPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected SumPrefix"),
      }
  }

  #[test]
  fn sum_range_round_trips() {
      let frame = encode_command(Command::SumRange("a".to_string(), "z".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::SumRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected SumRange"),
      }
  }

  #[test]
  fn avg_prefix_round_trips() {
      let frame = encode_command(Command::AvgPrefix("cpu:".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::AvgPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected AvgPrefix"),
      }
  }

  #[test]
  fn avg_range_round_trips() {
      let frame = encode_command(Command::AvgRange("a".to_string(), "z".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::AvgRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected AvgRange"),
      }
  }

  #[test]
  fn min_prefix_round_trips() {
      let frame = encode_command(Command::MinPrefix("cpu:".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::MinPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected MinPrefix"),
      }
  }

  #[test]
  fn min_range_round_trips() {
      let frame = encode_command(Command::MinRange("a".to_string(), "z".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::MinRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected MinRange"),
      }
  }

  #[test]
  fn max_prefix_round_trips() {
      let frame = encode_command(Command::MaxPrefix("cpu:".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::MaxPrefix(p) => assert_eq!(p, "cpu:"),
          _ => panic!("expected MaxPrefix"),
      }
  }

  #[test]
  fn max_range_round_trips() {
      let frame = encode_command(Command::MaxRange("a".to_string(), "z".to_string()));
      match decode_input_frame(&frame).unwrap() {
          Command::MaxRange(s, e) => { assert_eq!(s, "a"); assert_eq!(e, "z"); }
          _ => panic!("expected MaxRange"),
      }
  }

  // --- Dispatch: correct values ---

  #[test]
  fn dispatch_sum_prefix_returns_correct_value() {
      let db = lsm("d_sp");
      db.set("cpu:a", "1.0").unwrap();
      db.set("cpu:b", "2.0").unwrap();
      db.set("mem:a", "99.0").unwrap();
      assert_eq!(ok_payload(&db, Command::SumPrefix("cpu:".to_string())), "3.0");
  }

  #[test]
  fn dispatch_sum_range_returns_correct_value() {
      let db = lsm("d_sr");
      db.set("a", "1.0").unwrap();
      db.set("b", "2.0").unwrap();
      db.set("c", "3.0").unwrap();
      assert_eq!(ok_payload(&db, Command::SumRange("a".to_string(), "b".to_string())), "3.0");
  }

  #[test]
  fn dispatch_avg_prefix_returns_correct_value() {
      let db = lsm("d_ap");
      db.set("cpu:a", "2.0").unwrap();
      db.set("cpu:b", "4.0").unwrap();
      assert_eq!(ok_payload(&db, Command::AvgPrefix("cpu:".to_string())), "3.0");
  }

  #[test]
  fn dispatch_avg_range_returns_correct_value() {
      let db = lsm("d_ar");
      db.set("a", "1.0").unwrap();
      db.set("b", "3.0").unwrap();
      assert_eq!(ok_payload(&db, Command::AvgRange("a".to_string(), "b".to_string())), "2.0");
  }

  #[test]
  fn dispatch_min_prefix_returns_correct_value() {
      let db = lsm("d_mp");
      db.set("cpu:a", "5.0").unwrap();
      db.set("cpu:b", "1.0").unwrap();
      assert_eq!(ok_payload(&db, Command::MinPrefix("cpu:".to_string())), "1.0");
  }

  #[test]
  fn dispatch_max_prefix_returns_correct_value() {
      let db = lsm("d_mx");
      db.set("cpu:a", "5.0").unwrap();
      db.set("cpu:b", "1.0").unwrap();
      assert_eq!(ok_payload(&db, Command::MaxPrefix("cpu:".to_string())), "5.0");
  }

  #[test]
  fn dispatch_min_range_returns_correct_value() {
      let db = lsm("d_mr");
      db.set("a", "3.0").unwrap();
      db.set("b", "1.0").unwrap();
      db.set("c", "2.0").unwrap();
      assert_eq!(ok_payload(&db, Command::MinRange("a".to_string(), "c".to_string())), "1.0");
  }

  #[test]
  fn dispatch_max_range_returns_correct_value() {
      let db = lsm("d_mxr");
      db.set("a", "3.0").unwrap();
      db.set("b", "1.0").unwrap();
      db.set("c", "2.0").unwrap();
      assert_eq!(ok_payload(&db, Command::MaxRange("a".to_string(), "c".to_string())), "3.0");
  }

  // --- Dispatch: empty match → NotFound ---

  #[test]
  fn dispatch_sum_prefix_empty_is_not_found() {
      let db = lsm("d_sp_nf");
      expect_not_found(&db, Command::SumPrefix("cpu:".to_string()));
  }

  #[test]
  fn dispatch_avg_range_inverted_is_not_found() {
      let db = lsm("d_ar_nf");
      db.set("a", "1.0").unwrap();
      expect_not_found(&db, Command::AvgRange("z".to_string(), "a".to_string()));
  }

  // --- Dispatch: non-numeric → Error ---

  #[test]
  fn dispatch_sum_prefix_non_numeric_is_error() {
      let db = lsm("d_sp_err");
      db.set("cpu:a", "bad").unwrap();
      let msg = expect_error(&db, Command::SumPrefix("cpu:".to_string()));
      assert!(msg.contains("SUM") && msg.contains("cpu:a"), "got: {msg}");
  }

  #[test]
  fn dispatch_min_range_non_numeric_is_error() {
      let db = lsm("d_mr_err");
      db.set("a", "bad").unwrap();
      let msg = expect_error(&db, Command::MinRange("a".to_string(), "z".to_string()));
      assert!(msg.contains("MIN"), "got: {msg}");
  }

  // --- Dispatch: KV engine returns not-supported ---

  #[test]
  fn dispatch_sum_prefix_kv_not_supported() {
      expect_error(&kv("kv_sp"), Command::SumPrefix("cpu:".to_string()));
  }

  #[test]
  fn dispatch_sum_range_kv_not_supported() {
      expect_error(&kv("kv_sr"), Command::SumRange("a".to_string(), "z".to_string()));
  }

  #[test]
  fn dispatch_avg_prefix_kv_not_supported() {
      expect_error(&kv("kv_ap"), Command::AvgPrefix("cpu:".to_string()));
  }

  #[test]
  fn dispatch_avg_range_kv_not_supported() {
      expect_error(&kv("kv_ar"), Command::AvgRange("a".to_string(), "z".to_string()));
  }

  #[test]
  fn dispatch_min_prefix_kv_not_supported() {
      expect_error(&kv("kv_mp"), Command::MinPrefix("cpu:".to_string()));
  }

  #[test]
  fn dispatch_min_range_kv_not_supported() {
      expect_error(&kv("kv_mr"), Command::MinRange("a".to_string(), "z".to_string()));
  }

  #[test]
  fn dispatch_max_prefix_kv_not_supported() {
      expect_error(&kv("kv_mxp"), Command::MaxPrefix("cpu:".to_string()));
  }

  #[test]
  fn dispatch_max_range_kv_not_supported() {
      expect_error(&kv("kv_mxr"), Command::MaxRange("a".to_string(), "z".to_string()));
  }

  // --- stats.reads bumped by exactly 1 ---

  #[test]
  fn aggregation_commands_bump_reads_by_one() {
      let db = lsm("d_stats");
      db.set("cpu:a", "1.0").unwrap();
      db.set("cpu:b", "2.0").unwrap();
      let stats = Arc::new(Stats::new());

      let cmds: Vec<Command> = vec![
          Command::SumPrefix("cpu:".to_string()),
          Command::SumRange("cpu:a".to_string(), "cpu:b".to_string()),
          Command::AvgPrefix("cpu:".to_string()),
          Command::AvgRange("cpu:a".to_string(), "cpu:b".to_string()),
          Command::MinPrefix("cpu:".to_string()),
          Command::MinRange("cpu:a".to_string(), "cpu:b".to_string()),
          Command::MaxPrefix("cpu:".to_string()),
          Command::MaxRange("cpu:a".to_string(), "cpu:b".to_string()),
      ];

      for cmd in cmds {
          let before = stats.reads.load(Ordering::Relaxed);
          dispatch(cmd, &db, &stats, &no_compact());
          let after = stats.reads.load(Ordering::Relaxed);
          assert_eq!(after - before, 1, "each aggregation command must bump reads by 1");
      }
  }
  ```

- [ ] **Step 2: Run to confirm dispatch tests fail**

  ```
  cargo test --test aggregate_command dispatch_ 2>&1
  ```

  Expected: compile error — dispatch arms not yet added.

- [ ] **Step 3: Add 8 dispatch arms to `src/server/dispatch.rs`**

  After the `Command::CountRange` arm (before the closing `}` of the `match cmd` block), add:

  ```rust
  Command::SumPrefix(prefix) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.sum_prefix(&prefix) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["SUM not supported by KV engine".to_string()]),
      }
  }
  Command::SumRange(start, end) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.sum_range(&start, &end) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["SUM not supported by KV engine".to_string()]),
      }
  }
  Command::AvgPrefix(prefix) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.avg_prefix(&prefix) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["AVG not supported by KV engine".to_string()]),
      }
  }
  Command::AvgRange(start, end) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.avg_range(&start, &end) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["AVG not supported by KV engine".to_string()]),
      }
  }
  Command::MinPrefix(prefix) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.min_prefix(&prefix) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["MIN not supported by KV engine".to_string()]),
      }
  }
  Command::MinRange(start, end) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.min_range(&start, &end) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["MIN not supported by KV engine".to_string()]),
      }
  }
  Command::MaxPrefix(prefix) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.max_prefix(&prefix) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["MAX not supported by KV engine".to_string()]),
      }
  }
  Command::MaxRange(start, end) => {
      match database.as_any().downcast_ref::<LsmEngine>() {
          Some(lsm) => match lsm.max_range(&start, &end) {
              Ok(Some(v)) => {
                  stats.reads.fetch_add(1, Ordering::Relaxed);
                  encode_frame(ResponseStatus::Ok, &[format!("{:?}", v)])
              }
              Ok(None) => encode_frame(ResponseStatus::NotFound, &[]),
              Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
          },
          None => encode_frame(ResponseStatus::Error, &["MAX not supported by KV engine".to_string()]),
      }
  }
  ```

- [ ] **Step 4: Run end-to-end tests — expect pass**

  ```
  cargo test --test aggregate_command 2>&1
  ```

  Expected: all tests pass.

- [ ] **Step 5: Run full suite — no regressions**

  ```
  cargo test 2>&1
  ```

  Expected: all tests pass.

- [ ] **Step 6: Clippy and fmt**

  ```
  cargo fmt
  cargo clippy -- -D warnings 2>&1
  ```

  Expected: zero warnings.

- [ ] **Step 7: Commit**

  ```
  git add src/server/dispatch.rs tests/aggregate_command.rs
  git commit -m "#84 — add 8 aggregation dispatch arms and end-to-end tests"
  ```

---

## Task 6: README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add 8 rows to the command table**

  In `README.md`, find the two `COUNT` rows (ops 15 and 16). After them, add:

  ```markdown
  | `SUM <prefix>` | 17 | **LSM only.** Returns the sum of live numeric values whose keys start with `<prefix>`. Returns a single float. Empty match returns "Not found". Non-numeric live value returns an error. Returns an error on the KV engine |
  | `SUM <start> <end>` | 18 | **LSM only.** Returns the sum of live numeric values in the inclusive range `[start, end]`. Returns "Not found" if no keys match or `start > end`. Returns an error on the KV engine |
  | `AVG <prefix>` | 19 | **LSM only.** Returns the average of live numeric values whose keys start with `<prefix>`. Returns "Not found" on empty match. Returns an error on the KV engine |
  | `AVG <start> <end>` | 20 | **LSM only.** Returns the average of live numeric values in the inclusive range `[start, end]`. Returns "Not found" if no keys match or `start > end`. Returns an error on the KV engine |
  | `MIN <prefix>` | 21 | **LSM only.** Returns the minimum of live numeric values whose keys start with `<prefix>`. Returns "Not found" on empty match. Returns an error on the KV engine |
  | `MIN <start> <end>` | 22 | **LSM only.** Returns the minimum of live numeric values in the inclusive range `[start, end]`. Returns "Not found" if no keys match or `start > end`. Returns an error on the KV engine |
  | `MAX <prefix>` | 23 | **LSM only.** Returns the maximum of live numeric values whose keys start with `<prefix>`. Returns "Not found" on empty match. Returns an error on the KV engine |
  | `MAX <start> <end>` | 24 | **LSM only.** Returns the maximum of live numeric values in the inclusive range `[start, end]`. Returns "Not found" if no keys match or `start > end`. Returns an error on the KV engine |
  ```

- [ ] **Step 2: Run full suite one last time**

  ```
  cargo fmt
  cargo clippy -- -D warnings 2>&1
  cargo test 2>&1
  ```

  Expected: all pass.

- [ ] **Step 3: Commit**

  ```
  git add README.md
  git commit -m "#84 — add SUM/AVG/MIN/MAX rows to README command table"
  ```

---

## Done

All tasks complete. The feature branch is ready for PR. Update `TASKS.md` to move `#84` to Closed Tasks and open a PR titled `#84 — Server-side aggregation: SUM/AVG/MIN/MAX (LSM only)`.
