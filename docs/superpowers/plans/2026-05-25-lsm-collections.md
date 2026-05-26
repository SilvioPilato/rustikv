# LSM Collections + Per-Collection Default TTL Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add LSM-only collections (independent named keyspaces) selectable per-connection via `USE`, created/dropped explicitly with a persisted catalog, each carrying an immutable default TTL applied to writes that don't specify one.

**Architecture:** A new `Collections` manager owns a name→`Arc<dyn StorageEngine>` registry plus a line-based catalog (`name\tdefault_ttl_secs`), sitting *above* an otherwise-unchanged `LsmEngine` (whose `db_name` already namespaces all its files). The server threads `Arc<Collections>` where it used to thread the single engine; `dispatch` resolves the connection's current collection per command. Default-TTL resolution happens in `dispatch` for WRITE/MSET and is passed into a widened `incr` for the create path.

**Tech Stack:** Rust 2024, Cargo, hand-rolled BFFP binary protocol, `mio-runtime` event loops. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-25-lsm-collections-design.md`

**Conventions (from AGENTS.md):**
- All new tests go in `tests/` — do NOT add inline `#[cfg(test)]` modules (cli.rs has a pre-existing one; leave it, add new CLI tests under `tests/`).
- Pre-commit, in order: `cargo fmt` → `cargo clippy -- -D warnings` (NO `--tests`) → `cargo test`.
- Hand-rolled over crates; follow existing patterns; keep it simple.
- Branch `86-lsm-collections` is already created off `main`. Commit frequently.

---

## File Structure

| File | Create/Modify | Responsibility |
|------|---------------|----------------|
| `src/collections.rs` | **Create** | `Collections` manager: registry, catalog load/persist, name validation, create/drop/get/list/default_ttl, startup+migration. |
| `src/lib.rs` | Modify | `pub mod collections;` |
| `src/bffp.rs` | Modify | Op codes 25–28; `Command::{Use,CreateCollection,DropCollection,ShowCollections}`; encode/decode arms. |
| `src/engine.rs` | Modify | Widen `StorageEngine::incr` to take `default_expiry_ms: Option<u64>`. |
| `src/lsmengine.rs` | Modify | `LsmShared.default_ttl_secs`; constructor params; `incr` create-path stamps `default_expiry_ms`. |
| `src/kvengine.rs` | Modify | `incr` signature widening; stamp `default_expiry_ms` on create. |
| `src/server/connection.rs` | Modify | Add `current_collection: String` to `Connection`. |
| `src/server/dispatch.rs` | Modify | New signature `(cmd, &Collections, &mut String current, &Arc<Stats>, &CompactionCfg)`; per-collection routing; control-command arms; default-TTL resolution. |
| `src/server/workerhandler.rs` | Modify | Hold `Arc<Collections>`; pass `&mut conn.current_collection`. |
| `src/server/worker.rs` | Modify | Thread `Arc<Collections>` instead of `Arc<dyn StorageEngine>`. |
| `src/server/lifecycle.rs` | Modify | `Server` holds `Arc<Collections>`. |
| `src/main.rs` | Modify | Build `Collections`; extract strategy-construction helper shared with the manager. |
| `src/cli.rs` | Modify | Parse arms for `USE`/`CREATE COLLECTION`/`DROP COLLECTION`/`SHOW COLLECTIONS`. |
| `src/settings.rs` | (read only) | Source of `db_name` (the default collection name). No change expected. |
| `tests/collections.rs` | **Create** | Manager unit tests. |
| `tests/collection_command.rs` | **Create** | Integration: BFFP round-trips, USE state, default-TTL behavior, KV not-supported. |
| `tests/cli_collections.rs` | **Create** | `parse_command` arms for the 4 new verbs. |
| `README.md` | Modify | Document the 4 commands + default-TTL semantics. |
| `TASKS.md` | Modify | Move `#86` Open→In Progress at start, →Closed before PR. |

**Dependency order rationale:** protocol + engine-signature changes first (they're leaf changes that later tasks compile against), then the manager (pure logic, easily TDD'd), then the server wiring that ties them together, then CLI/docs.

---

## Task 1: BFFP protocol — op codes, Command variants, encode/decode

**Files:**
- Modify: `src/bffp.rs`
- Test: `tests/collection_command.rs` (create; protocol section only in this task)

The four frames (mirroring existing shapes):
- `Use = 25` — single-name frame, identical shape to `Read`/`Prefix` (`| len | op | name_len(2) | name |`).
- `CreateCollection = 26` — name + optional ttl, mirroring `Write`'s flag/ttl handling: `| len | op | flags(1) | name_len(2) | name | [default_ttl(4) if flags & FLAG_HAS_TTL] |`.
- `DropCollection = 27` — single-name frame (like `Use`).
- `ShowCollections = 28` — no-arg frame (like `Stats`/`List`).

- [ ] **Step 1: Write failing round-trip tests**

Create `tests/collection_command.rs` with a protocol section. Use the existing round-trip pattern from `tests/count_command.rs` / `tests/prefix_command.rs` (encode → `decode_input_frame` → assert variant + fields):

```rust
use rustikv::bffp::{Command, decode_input_frame, encode_command};

fn roundtrip(cmd: Command) -> Command {
    decode_input_frame(&encode_command(cmd)).expect("decode")
}

#[test]
fn use_roundtrip() {
    match roundtrip(Command::Use("metrics".into())) {
        Command::Use(n) => assert_eq!(n, "metrics"),
        _ => panic!("expected Use"),
    }
}

#[test]
fn create_collection_roundtrip_with_ttl() {
    match roundtrip(Command::CreateCollection("metrics".into(), Some(86400))) {
        Command::CreateCollection(n, ttl) => {
            assert_eq!(n, "metrics");
            assert_eq!(ttl, Some(86400));
        }
        _ => panic!("expected CreateCollection"),
    }
}

#[test]
fn create_collection_roundtrip_no_ttl() {
    match roundtrip(Command::CreateCollection("logs".into(), None)) {
        Command::CreateCollection(n, ttl) => {
            assert_eq!(n, "logs");
            assert_eq!(ttl, None);
        }
        _ => panic!("expected CreateCollection"),
    }
}

#[test]
fn drop_collection_roundtrip() {
    match roundtrip(Command::DropCollection("metrics".into())) {
        Command::DropCollection(n) => assert_eq!(n, "metrics"),
        _ => panic!("expected DropCollection"),
    }
}

#[test]
fn show_collections_roundtrip() {
    assert!(matches!(roundtrip(Command::ShowCollections), Command::ShowCollections));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test collection_command`
Expected: FAIL to compile (`Command::Use` etc. don't exist).

- [ ] **Step 3: Implement protocol additions in `src/bffp.rs`**

1. Add to the `Command` enum (after `MaxRange`):
```rust
    Use(String),
    CreateCollection(String, Option<u32>),
    DropCollection(String),
    ShowCollections,
```
2. Add to `OpCode` enum and its `TryFrom<u8>`:
```rust
    Use = 25,
    CreateCollection = 26,
    DropCollection = 27,
    ShowCollections = 28,
```
(add `25 => Ok(OpCode::Use)`, … to the `try_from` match.)
3. `decode_input_frame`: add arms.
   - `Use` / `DropCollection`: `Ok(Command::Use(read_key(&mut cur)?))` (and Drop analogously).
   - `ShowCollections`: `Ok(Command::ShowCollections)`.
   - `CreateCollection`: mirror the `Write` arm's flag/ttl logic:
```rust
Ok(OpCode::CreateCollection) => {
    let flag = read_flags(&mut cur)?;
    let name = read_key(&mut cur)?;
    let ttl = if flag & FLAG_HAS_TTL != 0 { Some(read_ttl(&mut cur)?) } else { None };
    Ok(Command::CreateCollection(name, ttl))
}
```
4. `encode_command`: add arms.
   - `Use` / `DropCollection`: copy the `Prefix` arm verbatim, swapping the op code and variant.
   - `ShowCollections`: copy the `Stats` arm.
   - `CreateCollection`: copy the `Write` arm's structure but with a *name* (key_len/key) and the optional ttl, no value field:
```rust
Command::CreateCollection(name, ttl) => {
    let (flags, secs) = match ttl { Some(s) => (FLAG_HAS_TTL, Some(s)), None => (0u8, None) };
    let ttl_bytes = if secs.is_some() { TTL_LEN_SIZE } else { 0 };
    let total_len = (OP_CODE_SIZE + FLAGS_LEN + KEY_LEN_SIZE + name.len() + ttl_bytes) as u32;
    payload.write_all(&total_len.to_be_bytes()).unwrap();
    payload.write_all(&[OpCode::CreateCollection as u8]).unwrap();
    payload.write_all(&flags.to_be_bytes()).unwrap();
    payload.write_all(&(name.len() as u16).to_be_bytes()).unwrap();
    payload.write_all(name.as_bytes()).unwrap();
    if let Some(s) = secs { payload.write_all(&s.to_be_bytes()).unwrap(); }
    payload.into_inner()
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test collection_command`
Expected: PASS (5 protocol tests).

- [ ] **Step 5: Commit**

```bash
git add src/bffp.rs tests/collection_command.rs
git commit -m "#86 — BFFP op codes 25-28 for collection commands"
```

---

## Task 2: Widen `incr` to accept a default expiry

This is a cross-cutting signature change done early so the tree keeps compiling. The default-TTL *value* is wired in Task 7; here the engine just learns to accept and apply it.

**Files:**
- Modify: `src/engine.rs` (trait), `src/lsmengine.rs`, `src/kvengine.rs`, `src/server/dispatch.rs` (call site — pass `None` for now)
- Test: `tests/lsm_incr.rs`, `tests/kv_incr.rs` (extend)

- [ ] **Step 1: Write failing tests for default-on-create**

Add to `tests/lsm_incr.rs` (mirror its existing engine-construction helper):
```rust
#[test]
fn incr_stamps_default_expiry_on_create_lsm() {
    // new engine, default collection ttl handled by caller; here pass an explicit basis
    let (engine, _dir) = new_lsm(); // use this file's existing setup helper
    let in_1h = crate::now_ms_plus(3600); // or compute now_ms + 3_600_000 inline
    engine.incr("c", Some(in_1h)).unwrap();
    // value present now
    assert_eq!(engine.get("c").unwrap().map(|(_, v)| v), Some("1".into()));
    // bump preserves the (still-future) expiry, does not reset to None
    engine.incr("c", None).unwrap();
    assert_eq!(engine.get("c").unwrap().map(|(_, v)| v), Some("2".into()));
}

#[test]
fn incr_on_create_with_none_basis_has_no_expiry_lsm() {
    let (engine, _dir) = new_lsm();
    engine.incr("c", None).unwrap();
    assert_eq!(engine.get("c").unwrap().map(|(_, v)| v), Some("1".into()));
}
```
(Compute the absolute ms inline with `std::time::{SystemTime, UNIX_EPOCH}` if no helper exists. Add an analogous `..._kv` pair to `tests/kv_incr.rs`. Existing incr tests must be updated to the new arity — call `incr(key, None)`.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test lsm_incr --test kv_incr`
Expected: FAIL to compile (arity mismatch).

- [ ] **Step 3: Implement the signature change**

1. `src/engine.rs`: change the trait method to
```rust
    fn incr(&self, key: &str, default_expiry_ms: Option<u64>) -> io::Result<i64>;
```
2. `src/lsmengine.rs` `incr`: add the param; in the lookup branch, the `None` (absent) case must seed expiry from `default_expiry_ms` instead of `None`:
```rust
let (mut value, expiry_ms): (i64, Option<u64>) =
    match lookup(&active, immutable.as_ref(), strategy.as_ref(), key, now)? {
        Some((v, ttl)) => (v.parse::<i64>().map_err(...)?, ttl), // present: preserve existing
        None => (0i64, default_expiry_ms),                       // create: stamp default
    };
```
(The rest — overflow check, `apply_write(..., expiry_ms)` — is unchanged.)
3. `src/kvengine.rs` `incr`: same shape — preserve existing expiry on bump, use `default_expiry_ms` on create. (Inspect the existing KV `incr` lookup/append helpers and apply the identical create-vs-bump rule.)
4. `src/server/dispatch.rs` `Command::Incr` arm: temporarily call `database.incr(&key, None)` so it compiles. (Task 7 replaces `None` with the resolved default.)

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test lsm_incr --test kv_incr`
Expected: PASS.

- [ ] **Step 5: Full check + commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/engine.rs src/lsmengine.rs src/kvengine.rs src/server/dispatch.rs tests/lsm_incr.rs tests/kv_incr.rs
git commit -m "#86 — widen incr to accept default expiry (create-path stamping)"
```

---

## Task 3: `LsmEngine` learns its immutable default TTL

**Files:**
- Modify: `src/lsmengine.rs`, `src/main.rs` (callers pass `0` for now)
- Test: covered indirectly; no new test (constructor plumbing).

- [ ] **Step 1: Add the field + params**

In `LsmShared` add `default_ttl_secs: u32`. Add a `default_ttl_secs: u32` parameter to both `LsmEngine::new` and `LsmEngine::from_dir`, storing it in `LsmShared`. (It is read in a later task by nothing inside the engine except possibly future use; the spec keeps default resolution in dispatch, so this field exists mainly to let the manager record per-engine config and to keep the door open. If clippy flags it as unused, add `#[allow(dead_code)]` with a `// used by Collections / future per-engine resolution` comment, or expose a `pub fn default_ttl_secs(&self) -> u32` getter and use it in Task 6's manager. Prefer the getter.)

- [ ] **Step 2: Update EVERY `LsmEngine::new` / `from_dir` call site (this breaks the build until all are done)**

Adding a constructor parameter is a cross-cutting change: the tree will not compile (and `cargo test` cannot run) until every caller passes the new trailing `default_ttl_secs` argument. Pass `0` everywhere (today's behavior = no default). The complete call-site set, verified by grep:

- `src/main.rs` — the single `LsmEngine::from_dir(...)` in the `EngineType::Lsm` branch. (Transitional `0`; this construction is replaced wholesale in Task 6 when main builds `Collections`.)
- `src/bin/engbench.rs` — `LsmEngine::new(...)` around line 169.
- Test files (each constructs engines directly, often via a local helper — update the helper once per file): `tests/lsm_aggregate.rs`, `tests/aggregate_command.rs`, `tests/count_command.rs`, `tests/lsm_count.rs`, `tests/prefix_command.rs`, `tests/lsmengine.rs`, `tests/lsm_incr.rs`, `tests/incr_command.rs`, `tests/block_sstable.rs`, `tests/ttl_command.rs`, `tests/lsm_ttl_atomic.rs`, `tests/leveled.rs`, `tests/lsm_ttl.rs`, `tests/concurrency.rs`.

Re-grep before committing to be sure none were missed: `LsmEngine::(new|from_dir)` should appear only with the new trailing arg.

- [ ] **Step 3: Compile + commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/lsmengine.rs src/main.rs src/bin/engbench.rs tests/
git commit -m "#86 — LsmEngine carries immutable default_ttl_secs"
```

---

## Task 4: Collection-name validation (pure function)

**Files:**
- Create: `src/collections.rs` (start the module with just this function + `pub mod` wiring)
- Modify: `src/lib.rs`
- Test: `tests/collections.rs` (create)

- [ ] **Step 1: Write failing tests**

Create `tests/collections.rs`:
```rust
use rustikv::collections::is_valid_collection_name as valid;

#[test]
fn accepts_alnum_and_hyphen() {
    assert!(valid("metrics"));
    assert!(valid("cpu-load"));
    assert!(valid("a1B2-3"));
}

#[test]
fn rejects_empty_and_special() {
    assert!(!valid(""));
    assert!(!valid("foo_bar"));   // underscore collides with SSTable _L scheme
    assert!(!valid("foo.bar"));   // dot collides with .wal/.sst
    assert!(!valid("a/b"));
    assert!(!valid("a\\b"));
    assert!(!valid("a b"));       // space
    assert!(!valid("a\tb"));      // tab = catalog delimiter
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test collections`
Expected: FAIL to compile (module/function missing).

- [ ] **Step 3: Implement**

`src/lib.rs`: add `pub mod collections;` (alphabetical with siblings).
`src/collections.rs`:
```rust
/// A collection name is also the on-disk `db_name` / file prefix and a catalog
/// (tab-delimited) field, so it must avoid `_`/`_L` (SSTable filename scheme),
/// `.` (.wal/.sst extensions), path separators, whitespace, and tab.
pub fn is_valid_collection_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test collections`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lib.rs src/collections.rs tests/collections.rs
git commit -m "#86 — collection name validation"
```

---

## Task 5: Catalog load/persist (line-based file)

**Files:**
- Modify: `src/collections.rs`
- Test: `tests/collections.rs`

Catalog file: `<db_path>/collections.catalog`, lines `name\tdefault_ttl_secs`. Persist = write `collections.catalog.tmp` then rename. Load = parse lines into `Vec<(String, u32)>`; ignore blank lines and a stray `.tmp`.

- [ ] **Step 1: Write failing tests**

Add to `tests/collections.rs` (use `std::env::temp_dir()` + a unique subdir, or the same temp-dir helper other tests use — check `tests/lsmengine.rs` for the project's convention):
```rust
use rustikv::collections::{load_catalog, persist_catalog};

#[test]
fn catalog_roundtrips() {
    let dir = unique_tmp_dir(); // helper: create + return a fresh dir path
    persist_catalog(&dir, &[("segment".into(), 0), ("metrics".into(), 86400)]).unwrap();
    let mut got = load_catalog(&dir).unwrap();
    got.sort();
    assert_eq!(got, vec![("metrics".to_string(), 86400), ("segment".to_string(), 0)]);
}

#[test]
fn load_missing_catalog_returns_empty() {
    let dir = unique_tmp_dir();
    assert!(load_catalog(&dir).unwrap().is_empty());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test collections`
Expected: FAIL to compile.

- [ ] **Step 3: Implement in `src/collections.rs`**

```rust
use std::fs;
use std::io;
use std::path::Path;

const CATALOG_FILE: &str = "collections.catalog";
const CATALOG_TMP: &str = "collections.catalog.tmp";

pub fn load_catalog(db_path: &str) -> io::Result<Vec<(String, u32)>> {
    let path = Path::new(db_path).join(CATALOG_FILE);
    let contents = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for line in contents.lines() {
        if line.trim().is_empty() { continue; }
        let (name, ttl) = line
            .split_once('\t')
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad catalog line"))?;
        let ttl: u32 = ttl
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad catalog ttl"))?;
        out.push((name.to_string(), ttl));
    }
    Ok(out)
}

pub fn persist_catalog(db_path: &str, entries: &[(String, u32)]) -> io::Result<()> {
    let tmp = Path::new(db_path).join(CATALOG_TMP);
    let final_path = Path::new(db_path).join(CATALOG_FILE);
    let mut body = String::new();
    for (name, ttl) in entries {
        body.push_str(name);
        body.push('\t');
        body.push_str(&ttl.to_string());
        body.push('\n');
    }
    fs::write(&tmp, body)?;
    fs::rename(&tmp, &final_path)?; // atomic replace
    Ok(())
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test collections`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/collections.rs tests/collections.rs
git commit -m "#86 — line-based collection catalog load/persist"
```

---

## Task 6: `Collections` manager + startup/migration + strategy helper

**Files:**
- Modify: `src/collections.rs`, `src/main.rs` (extract `build_lsm_strategy` helper)
- Test: `tests/collections.rs`

This is the core. The manager builds and owns per-collection LSM engines and keeps the catalog in sync.

- [ ] **Step 1: Extract the strategy-construction helper from `main.rs`**

`main.rs` currently builds a `Box<dyn StorageStrategy>` inline from `Settings`. Extract a free function (in `main.rs` or, better, a small `pub fn` in `src/collections.rs` so the manager can call it):
```rust
pub struct LsmTemplate {
    pub storage_strategy: SettingsStorageStrategyKind, // mirror of settings enum (or pass needed fields)
    pub leveled_num_levels: usize,
    pub leveled_l0_threshold: usize,
    pub leveled_l1_max_bytes: u64,
    pub block_size_bytes: usize,
    pub block_compression_enabled: bool,
    pub max_memtable_bytes: usize,
}
```
and `fn build_strategy(db_path: &str, name: &str, t: &LsmTemplate) -> io::Result<Box<dyn StorageStrategy>>` that contains the existing `match settings.storage_strategy { SizeTiered => SizeTiered::load_from_dir(...), Leveled => Leveled::load_from_dir(...) }`. Keep the hardcoded `4, 32` size-tiered thresholds exactly as in current `main.rs`.

- [ ] **Step 2: Write failing manager tests**

Add to `tests/collections.rs`:
```rust
use rustikv::collections::Collections;

// Build a Collections rooted at a fresh tmp dir, default name "segment",
// using an LsmTemplate with size-tiered + lz77 (mirror main.rs defaults).
// Provide a test helper `lsm_collections(dir) -> Arc<Collections>`.

#[test]
fn default_collection_always_present() {
    let c = lsm_collections(&unique_tmp_dir());
    assert!(c.get("segment").is_some());
    assert_eq!(c.default_ttl("segment"), 0);
}

#[test]
fn create_then_get_and_list() {
    let c = lsm_collections(&unique_tmp_dir());
    c.create("metrics", 86400).unwrap();
    assert!(c.get("metrics").is_some());
    let mut names: Vec<_> = c.list().into_iter().collect();
    names.sort();
    assert_eq!(names, vec![("metrics".into(), 86400u32), ("segment".into(), 0)]);
}

#[test]
fn create_duplicate_errors() {
    let c = lsm_collections(&unique_tmp_dir());
    c.create("metrics", 0).unwrap();
    assert!(c.create("metrics", 0).is_err());
}

#[test]
fn create_invalid_name_errors() {
    let c = lsm_collections(&unique_tmp_dir());
    assert!(c.create("bad_name", 0).is_err());
}

#[test]
fn drop_default_errors() {
    let c = lsm_collections(&unique_tmp_dir());
    assert!(c.drop("segment").is_err());
}

#[test]
fn drop_removes_entry_and_files() {
    let dir = unique_tmp_dir();
    let c = lsm_collections(&dir);
    c.create("metrics", 0).unwrap();
    // write something so a wal exists; via the engine handle:
    c.get("metrics").unwrap().set("k", "v").unwrap();
    c.drop("metrics").unwrap();
    assert!(c.get("metrics").is_none());
    // metrics.wal / metrics_*.sst gone
    let leftover = std::fs::read_dir(&dir).unwrap()
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().starts_with("metrics"));
    assert!(!leftover);
}

#[test]
fn catalog_persists_across_reload() {
    let dir = unique_tmp_dir();
    { let c = lsm_collections(&dir); c.create("metrics", 86400).unwrap(); }
    let c2 = lsm_collections(&dir); // reloads catalog
    assert!(c2.get("metrics").is_some());
    assert_eq!(c2.default_ttl("metrics"), 86400);
}

#[test]
fn isolation_same_key_independent() {
    let c = lsm_collections(&unique_tmp_dir());
    c.create("a", 0).unwrap();
    c.create("b", 0).unwrap();
    c.get("a").unwrap().set("k", "1").unwrap();
    c.get("b").unwrap().set("k", "2").unwrap();
    assert_eq!(c.get("a").unwrap().get("k").unwrap().map(|(_, v)| v), Some("1".into()));
    assert_eq!(c.get("b").unwrap().get("k").unwrap().map(|(_, v)| v), Some("2".into()));
}

#[test]
fn migration_synthesizes_default_for_existing_db() {
    // Pre-create a segment_*.sst-style db by running an LsmEngine directly, no catalog,
    // then open Collections on that dir: "segment" must exist and see the data.
    // (Use the same helper that other lsm tests use to seed an engine, or set via handle.)
}
```

- [ ] **Step 3: Implement the manager**

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::engine::StorageEngine;
use crate::lsmengine::LsmEngine;

pub struct Collections {
    engines: RwLock<HashMap<String, Arc<dyn StorageEngine>>>,
    default_ttls: RwLock<HashMap<String, u32>>,
    db_path: String,
    default_name: String,
    template: Option<LsmTemplate>, // None under KV engine
}

impl Collections {
    /// LSM constructor: load catalog, build an engine per entry, ensure default exists,
    /// synthesize+persist a catalog on first run.
    pub fn load_lsm(db_path: &str, default_name: &str, template: LsmTemplate) -> io::Result<Arc<Self>> {
        let mut entries = load_catalog(db_path)?;
        if !entries.iter().any(|(n, _)| n == default_name) {
            entries.push((default_name.to_string(), 0)); // migration / first run
        }
        let mut engines: HashMap<String, Arc<dyn StorageEngine>> = HashMap::new();
        let mut ttls = HashMap::new();
        for (name, ttl) in &entries {
            let strategy = build_strategy(db_path, name, &template)?;
            let engine = LsmEngine::from_dir(
                db_path, name, template.max_memtable_bytes, strategy,
                template.block_size_bytes, template.block_compression_enabled, *ttl,
            )?;
            engines.insert(name.clone(), Arc::new(engine));
            ttls.insert(name.clone(), *ttl);
        }
        persist_catalog(db_path, &entries)?; // make first-run catalog durable
        Ok(Arc::new(Self {
            engines: RwLock::new(engines),
            default_ttls: RwLock::new(ttls),
            db_path: db_path.to_string(),
            default_name: default_name.to_string(),
            template: Some(template),
        }))
    }

    /// KV constructor: single default engine, collections unsupported.
    pub fn single(default_name: &str, engine: Arc<dyn StorageEngine>) -> Arc<Self> {
        let mut engines = HashMap::new();
        engines.insert(default_name.to_string(), engine);
        let mut ttls = HashMap::new();
        ttls.insert(default_name.to_string(), 0);
        Arc::new(Self {
            engines: RwLock::new(engines),
            default_ttls: RwLock::new(ttls),
            db_path: String::new(),
            default_name: default_name.to_string(),
            template: None,
        })
    }

    pub fn supports_collections(&self) -> bool { self.template.is_some() }
    pub fn default_name(&self) -> &str { &self.default_name }

    pub fn get(&self, name: &str) -> Option<Arc<dyn StorageEngine>> {
        self.engines.read().unwrap().get(name).cloned()
    }

    pub fn default_ttl(&self, name: &str) -> u32 {
        self.default_ttls.read().unwrap().get(name).copied().unwrap_or(0)
    }

    pub fn list(&self) -> Vec<(String, u32)> {
        self.default_ttls.read().unwrap().iter().map(|(n, t)| (n.clone(), *t)).collect()
    }

    pub fn create(&self, name: &str, default_ttl: u32) -> io::Result<()> {
        let template = self.template.as_ref()
            .ok_or_else(|| io::Error::other("collections not supported by KV engine"))?;
        if !is_valid_collection_name(name) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid collection name"));
        }
        let mut engines = self.engines.write().unwrap();
        let mut ttls = self.default_ttls.write().unwrap();
        if engines.contains_key(name) {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "collection exists"));
        }
        let strategy = build_strategy(&self.db_path, name, template)?;
        let engine = LsmEngine::new(
            &self.db_path, name, template.max_memtable_bytes, strategy,
            template.block_size_bytes, template.block_compression_enabled, default_ttl,
        )?;
        engines.insert(name.to_string(), Arc::new(engine));
        ttls.insert(name.to_string(), default_ttl);
        self.persist_locked(&ttls)
    }

    pub fn drop(&self, name: &str) -> io::Result<()> {
        if self.template.is_none() {
            return Err(io::Error::other("collections not supported by KV engine"));
        }
        if name == self.default_name {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "cannot drop the default collection"));
        }
        let mut engines = self.engines.write().unwrap();
        let mut ttls = self.default_ttls.write().unwrap();
        if engines.remove(name).is_none() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no such collection"));
        }
        ttls.remove(name);
        // dropping the Arc above joins the engine's flush thread (LsmEngine::Drop)
        delete_collection_files(&self.db_path, name)?;
        self.persist_locked(&ttls)
    }

    fn persist_locked(&self, ttls: &HashMap<String, u32>) -> io::Result<()> {
        let entries: Vec<(String, u32)> = ttls.iter().map(|(n, t)| (n.clone(), *t)).collect();
        persist_catalog(&self.db_path, &entries)
    }
}

/// Delete `<name>.wal` and `<name>_*.sst` (incl. level-encoded). Leaves other collections' files.
fn delete_collection_files(db_path: &str, name: &str) -> io::Result<()> {
    let wal = format!("{name}.wal");
    let sst_prefix = format!("{name}_");
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let fname = entry.file_name().to_string_lossy().to_string();
        if fname == wal || (fname.starts_with(&sst_prefix) && fname.ends_with(".sst")) {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
```
Note the **lock-order discipline**: `create`/`drop` hold the registry write lock only to mutate the maps and rewrite the catalog; they never call into an engine's own locks while holding the registry lock. `get` clones the `Arc` under a read lock and releases it before any engine op — exactly the spec's concurrency model.

`delete_collection_files` deletes after the registry entry is removed; an in-flight reader holding the `Arc` keeps the engine alive.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test collections`
Expected: PASS (all manager tests).

- [ ] **Step 5: Full check + commit**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
git add src/collections.rs src/main.rs tests/collections.rs
git commit -m "#86 — Collections manager: registry, catalog sync, startup/migration"
```

---

## Task 7: Wire the server to `Collections` + routing + default-TTL resolution

**Files:**
- Modify: `src/server/connection.rs`, `src/server/dispatch.rs`, `src/server/workerhandler.rs`, `src/server/worker.rs`, `src/server/lifecycle.rs`, `src/main.rs`
- Test: `tests/collection_command.rs` (extend with dispatch-level + default-TTL tests)

This is the integration task. Do it as one coherent change because the signatures are interlocked, but commit once green.

- [ ] **Step 1: Add per-connection state**

`src/server/connection.rs`: add `pub current_collection: String` to `Connection`; in `Connection::new` accept (or set) it. Easiest: add a parameter `default_collection: String` to `Connection::new` and store it. `WorkerHandler::on_wake` constructs connections — pass `self.collections.default_name().to_string()`.

- [ ] **Step 2: Change `WorkerHandler` to hold `Arc<Collections>`**

`workerhandler.rs`: replace `engine: Arc<dyn StorageEngine>` with `collections: Arc<Collections>`. In `on_event`, change the dispatch call:
```rust
for cmd in cmds {
    let res = dispatch(cmd, &self.collections, &mut conn.current_collection, &self.stats, &self.cfg);
    conn.enqueue_response(&res);
}
```
Update `WorkerHandler::new` signature accordingly. Update `Connection::new(stream, stats)` call to also pass the default collection name.

- [ ] **Step 3: Thread `Arc<Collections>` through `worker.rs` and `lifecycle.rs`**

`worker.rs`: `Worker::new` takes `collections: Arc<Collections>` instead of `engine`; pass to `WorkerHandler::new`. `lifecycle.rs`: `Server` holds `collections: Arc<Collections>`; `Server::new(collections, settings, stats)`; in `start`, `self.collections.clone()` into each `Worker::new`.

- [ ] **Step 4: Rewrite `dispatch` signature + routing + control arms + default-TTL**

New signature:
```rust
pub fn dispatch(
    cmd: Command,
    collections: &Collections,
    current: &mut String,
    stats: &Arc<Stats>,
    cfg: &CompactionCfg,
) -> Vec<u8>
```
For every existing **data** arm, first resolve the handle:
```rust
let Some(database) = collections.get(current) else {
    return encode_frame(ResponseStatus::Error, &[format!("no such collection '{current}'")]);
};
```
Then keep the arm body identical (it already uses `database`, `database.as_any().downcast_ref::<LsmEngine>()`, `maybe_trigger_compaction(database.clone(), ...)`, etc.). Resolve the handle once near the top for data ops; control ops below don't need it.

**Default-TTL resolution** — replace the existing `ttl_seconds.and_then(|s| if s==0 {None} else {Some(now+...)})` in the `Write` and `Mset` arms with collection-aware resolution. Add a helper:
```rust
/// effective absolute expiry (ms) given the wire TTL intent and the collection default (secs).
fn resolve_expiry(now_ms: u64, ttl_seconds: Option<u32>, default_secs: u32) -> Option<u64> {
    match ttl_seconds {
        None => if default_secs == 0 { None } else { Some(now_ms + default_secs as u64 * 1000) },
        Some(0) => None,                                  // explicit no-expiry overrides default
        Some(n) => Some(now_ms + n as u64 * 1000),
    }
}
```
- `Write`: `let ttl_ms = resolve_expiry(now_ms, ttl_seconds, collections.default_ttl(current));`
- `Mset`: map each item's `ttl_seconds` through `resolve_expiry(..., collections.default_ttl(current))`.
- `Incr`: compute the default basis and pass it in:
```rust
let default_secs = collections.default_ttl(current);
let default_expiry = if default_secs == 0 { None } else { Some(now_ms + default_secs as u64 * 1000) };
match database.incr(&key, default_expiry) { ... }
```

**New control arms:**
```rust
Command::Use(name) => {
    if collections.get(&name).is_some() {
        *current = name;
        encode_frame(ResponseStatus::Ok, &[])
    } else {
        encode_frame(ResponseStatus::Error, &[format!("no such collection '{name}'")])
    }
}
Command::CreateCollection(name, ttl) => {
    if !collections.supports_collections() {
        return encode_frame(ResponseStatus::Error, &["CREATE COLLECTION not supported by KV engine".into()]);
    }
    match collections.create(&name, ttl.unwrap_or(0)) {
        Ok(()) => encode_frame(ResponseStatus::Ok, &[]),
        Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
    }
}
Command::DropCollection(name) => {
    if !collections.supports_collections() {
        return encode_frame(ResponseStatus::Error, &["DROP COLLECTION not supported by KV engine".into()]);
    }
    match collections.drop(&name) {
        Ok(()) => encode_frame(ResponseStatus::Ok, &[]),
        Err(e) => encode_frame(ResponseStatus::Error, &[e.to_string()]),
    }
}
Command::ShowCollections => {
    if !collections.supports_collections() {
        return encode_frame(ResponseStatus::Error, &["SHOW COLLECTIONS not supported by KV engine".into()]);
    }
    let mut rows: Vec<String> = collections.list().into_iter()
        .map(|(n, t)| format!("{n}\t{t}")).collect();
    rows.sort();
    encode_frame(ResponseStatus::Ok, &rows)
}
```

- [ ] **Step 5: Rewrite `main.rs` to build `Collections`**

```rust
let stats = Arc::new(Stats::new());
let collections = match settings.engine {
    EngineType::KV => {
        let kv = /* existing KVEngine from_dir/new */;
        Collections::single(&settings.db_name, Arc::new(kv))
    }
    EngineType::Lsm => {
        let template = LsmTemplate { /* from settings, mirroring current values */ };
        Collections::load_lsm(&settings.db_file_path, &settings.db_name, template)?
    }
};
let server = Server::new(collections, settings, stats);
```
(Keep the KV branch's existing `from_dir`-or-`new` logic; just wrap the result in `Collections::single`.)

- [ ] **Step 6: Migrate EVERY existing direct-`dispatch` caller (this breaks the build until all are done)**

The new `dispatch` signature breaks six test files that call `dispatch(cmd, &engine, &stats, &cfg)` directly with the old 4-arg shape. They will not compile — and `cargo test` cannot run — until each is migrated. Verified call-site set: `tests/aggregate_command.rs`, `tests/count_command.rs`, `tests/prefix_command.rs`, `tests/incr_command.rs`, `tests/ttl_command.rs`, `tests/server_dispatch.rs`. (Several of these wrap `dispatch` in a local helper like `fn dispatch_cmd(...)` or `fn incr(...)` — migrate the helper once per file.)

Migration shim — wrap the existing engine in a single-collection manager and thread a current-collection string:
```rust
use rustikv::collections::Collections;
// engine: Arc<dyn StorageEngine> (LsmEngine or KVEngine) as before
let collections = Collections::single("segment", engine);
let mut current = "segment".to_string();
let res = dispatch(cmd, &collections, &mut current, &stats, &cfg);
```
`Collections::single` stores the `Arc<dyn StorageEngine>` unchanged, so the data arms' `downcast_ref::<LsmEngine>()` still works for the LSM-only tests, and `default_ttl("segment")` returns `0` so these tests see today's no-default behavior. Re-grep `dispatch\(` before committing to confirm only the new 5-arg shape remains (plus the def in `dispatch.rs` and the call in `workerhandler.rs`).

- [ ] **Step 7: Write failing dispatch + default-TTL tests**

Extend `tests/collection_command.rs`. These are integration tests over the running server binary (mirror `tests/ttl_command.rs` / `tests/count_command.rs` setup — spawn the server, read the address, connect, send frames). Cover:
- `USE` an existing collection then `WRITE`/`READ` lands there; same key in default collection is independent.
- `USE` unknown collection → Error, current unchanged (subsequent READ still hits default).
- `CREATE COLLECTION metrics default-ttl 1` then `WRITE k v` (no TTL flag) → key expires after ~1s (sleep + READ NotFound); `WRITE k v` with explicit `ttl=0` (via `WRITETTL`? no — needs the raw frame with `Some(0)`; encode `Command::Write(k, v, Some(0))` directly) → never expires.
- `INCR` on a fresh key in a `default-ttl 1` collection expires; on an existing key preserves.
- KV engine: `CREATE`/`USE`/`DROP`/`SHOW COLLECTIONS` all return Error "not supported".
- `SHOW COLLECTIONS` lists default + created with their TTLs.
- `DROP COLLECTION` then `READ` in it → Error "no such collection".

- [ ] **Step 8: Run + verify**

Run: `cargo test --test collection_command`
Expected: PASS.

- [ ] **Step 9: Full suite**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
```
Expected: entire suite green. This must include (a) the six migrated direct-`dispatch` test files from Step 6, and (b) the TCP integration tests in `tests/ping.rs` / `tests/server_*.rs`, which spawn the server binary and exercise the new wiring end-to-end. Green here proves the default-collection path is transparent to existing clients.

- [ ] **Step 10: Commit**

```bash
git add src/server/ src/main.rs tests/
git commit -m "#86 — route dispatch per-collection; default-TTL resolution; control commands"
```

---

## Task 8: CLI client parse arms

**Files:**
- Modify: `src/cli.rs`
- Test: `tests/cli_collections.rs` (create — new tests go in `tests/`, not the inline module)

- [ ] **Step 1: Write failing tests**

Create `tests/cli_collections.rs`:
```rust
use rustikv::bffp::Command;
use rustikv::cli::{ParseResult, parse_command};

fn cmd(line: &str) -> Command {
    match parse_command(line) { ParseResult::Cmd(c) => c, _ => panic!("expected Cmd for {line}") }
}
fn invalid(line: &str) -> bool { matches!(parse_command(line), ParseResult::InvalidInput(_)) }

#[test]
fn use_parses() {
    assert!(matches!(cmd("USE metrics"), Command::Use(n) if n == "metrics"));
    assert!(invalid("USE"));
    assert!(invalid("USE a b"));
}

#[test]
fn create_collection_parses_with_and_without_ttl() {
    assert!(matches!(cmd("CREATE COLLECTION metrics"), Command::CreateCollection(n, None) if n == "metrics"));
    assert!(matches!(cmd("CREATE COLLECTION metrics default-ttl 86400"),
        Command::CreateCollection(n, Some(86400)) if n == "metrics"));
    assert!(invalid("CREATE COLLECTION"));
    assert!(invalid("CREATE COLLECTION m default-ttl"));
    assert!(invalid("CREATE COLLECTION m default-ttl abc"));
    assert!(invalid("CREATE FOO m"));
}

#[test]
fn drop_collection_parses() {
    assert!(matches!(cmd("DROP COLLECTION metrics"), Command::DropCollection(n) if n == "metrics"));
    assert!(invalid("DROP COLLECTION"));
    assert!(invalid("DROP FOO m"));
}

#[test]
fn show_collections_parses() {
    assert!(matches!(cmd("SHOW COLLECTIONS"), Command::ShowCollections));
    assert!(invalid("SHOW FOO"));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test cli_collections`
Expected: FAIL (no arms).

- [ ] **Step 3: Implement arms in `parse_command`**

`USE` is a top-level verb. `CREATE`/`DROP`/`SHOW` are two-word verbs (`words[0]` + `words[1]`), so handle them by matching on `words[0]` then inspecting `words[1]`:
```rust
"USE" => if words.len() == 2 {
    ParseResult::Cmd(Command::Use(words[1].to_string()))
} else {
    ParseResult::InvalidInput("Usage: USE <collection>".to_string())
},
"CREATE" => match words.get(1).map(|s| s.to_uppercase()) {
    Some(ref w) if w == "COLLECTION" => match words.len() {
        3 => ParseResult::Cmd(Command::CreateCollection(words[2].to_string(), None)),
        5 if words[3].eq_ignore_ascii_case("default-ttl") => match words[4].parse::<u32>() {
            Ok(secs) => ParseResult::Cmd(Command::CreateCollection(words[2].to_string(), Some(secs))),
            Err(_) => ParseResult::InvalidInput("Invalid default-ttl seconds".to_string()),
        },
        _ => ParseResult::InvalidInput("Usage: CREATE COLLECTION <name> [default-ttl <secs>]".to_string()),
    },
    _ => ParseResult::InvalidInput("Usage: CREATE COLLECTION <name> [default-ttl <secs>]".to_string()),
},
"DROP" => match words.get(1).map(|s| s.to_uppercase()) {
    Some(ref w) if w == "COLLECTION" && words.len() == 3 =>
        ParseResult::Cmd(Command::DropCollection(words[2].to_string())),
    _ => ParseResult::InvalidInput("Usage: DROP COLLECTION <name>".to_string()),
},
"SHOW" => match words.get(1).map(|s| s.to_uppercase()) {
    Some(ref w) if w == "COLLECTIONS" && words.len() == 2 => ParseResult::Cmd(Command::ShowCollections),
    _ => ParseResult::InvalidInput("Usage: SHOW COLLECTIONS".to_string()),
},
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --test cli_collections`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cli.rs tests/cli_collections.rs
git commit -m "#86 — rustikli parse arms for collection commands"
```

---

## Task 9: README + final verification

**Files:**
- Modify: `README.md`, `TASKS.md`

- [ ] **Step 1: Update `README.md`**

Add the four commands to the command table (with op codes 25–28), an `LSM-only` note, and a short "Collections & default TTL" subsection: `USE`, `CREATE COLLECTION <name> [default-ttl <secs>]`, `DROP COLLECTION <name>`, `SHOW COLLECTIONS`; the default-collection backward-compat note; and the default-TTL resolution rule (no-flag → default, `0` → never, `N` → N).

- [ ] **Step 2: Full pre-commit suite**

```bash
cargo fmt && cargo clippy -- -D warnings && cargo test
```
Expected: all green.

- [ ] **Step 3: Move `#86` to Closed in `TASKS.md`**

Per AGENTS.md, write the `## #86` closed entry summarizing what shipped (collections, per-collection default TTL, catalog, the 4 commands, default-collection migration), referencing the spec and this plan. Add the PR link after opening the PR.

- [ ] **Step 4: Commit**

```bash
git add README.md TASKS.md
git commit -m "#86 — document collections + default TTL; close task"
```

---

## Verification Checklist (whole feature)

- [ ] Existing suite passes unchanged (default-collection transparency).
- [ ] `USE` switches collection for subsequent commands on one connection; isolation holds.
- [ ] Unknown-collection ops error without changing current selection.
- [ ] `CREATE`/`DROP`/`SHOW COLLECTIONS` work on LSM; all error on KV.
- [ ] Name validation rejects `_`/`.`/`/`/whitespace/tab/empty.
- [ ] Catalog round-trips across restart; first-run migration adopts existing `segment_*` files.
- [ ] Default-TTL table holds for WRITE, MSET, and INCR-on-create; explicit `0` overrides; `default-ttl 0` ≡ today.
- [ ] `DROP` deletes only the target collection's files; in-flight readers unaffected.
- [ ] `cargo fmt` clean, `cargo clippy -- -D warnings` clean (no `--tests`), full `cargo test` green.

## Notes / Deferred (from spec Future Work)

- Per-collection compaction strategy/block settings, `ALTER COLLECTION`, per-collection STATS, TWCS (#77), KV collections — all out of scope here.
