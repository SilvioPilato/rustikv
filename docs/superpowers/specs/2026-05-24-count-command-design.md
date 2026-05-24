# COUNT command — design (#51)

## Summary

Add a `COUNT` TCP command that returns the *number* of live keys matching a
query, **without materializing values**. **LSM-only**, mirroring `RANGE` (#48)
and `PREFIX` (#50) at every layer; the KV engine returns an error. This is the
fourth step of the telemetry use-case path (TTL → INCR → PREFIX → **COUNT** →
aggregation): a lightweight cardinality check — how many series live under a
prefix, or within a time window — without pulling values to the client.

Two variants, disambiguated by arity:

- `COUNT <prefix>` — count live keys where `key.starts_with(prefix)`.
- `COUNT <start> <end>` — count live keys in the inclusive range `[start, end]`.

## Semantics

- A key is counted iff it **matches** the query **and** is **live** under the
  three-tier merge: newest write wins across active memtable > immutable
  memtable > newer SSTable > older SSTable, with tombstoned and expired keys
  excluded. Liveness rules are identical to `RANGE`/`PREFIX`.
- **Empty prefix counts all live keys.** Consistent with `PREFIX` (#50) and the
  cross-industry empty-prefix convention; every string `starts_with("")`. Not
  rejected at the parse layer.
- **`start > end` returns `0`**, mirroring `RANGE` returning an empty result for
  an inverted interval.
- The result is a single **non-negative integer**, encoded as a one-element
  `Ok` frame (`count.to_string()`), exactly the shape `INCR` uses to return a
  number.
- KV engine: returns `"COUNT not supported by KV engine"`, exactly as `RANGE`
  and `PREFIX` do.

## Layers (each mirrors the existing `RANGE`/`PREFIX` implementation)

### Protocol — `src/bffp.rs`
- `OpCode::CountPrefix = 15` (frame shaped like `Prefix`: `op | key_len | key`).
- `OpCode::CountRange = 16` (frame shaped like `Range`:
  `op | start_len | start | end_len | end`).
  Two op codes reuse the existing "op + strings" framing exactly, with no new
  framing concept (no mode/discriminant byte). 11=Range, 12=Ttl, 13=Incr,
  14=Prefix are already taken; 15/16 are the next free codes.
- `Command::CountPrefix(String)` and `Command::CountRange(String, String)`.
- Decode arms: `CountPrefix` mirrors the `Prefix` decode at `bffp.rs:174`;
  `CountRange` mirrors the `Range` decode at `bffp.rs:163`.
- Encode arms: `CountPrefix` mirrors the `Prefix` encode at `bffp.rs:436`;
  `CountRange` mirrors the `Range` encode at `bffp.rs:395`.

### CLI parse — `src/cli.rs`
- One `"COUNT"` arm:
  - arity 2 (`COUNT <prefix>`) → `Command::CountPrefix(prefix)` — empty prefix
    accepted (valid "match all").
  - arity 3 (`COUNT <start> <end>`) → `Command::CountRange(start, end)`.
  - any other arity → usage string,
    e.g. `"usage: COUNT <prefix> | COUNT <start> <end>"`.
- Modeled on the `"RANGE"` arm (`cli.rs:85`) and the `"PREFIX"` arm.

### Engine — `src/engine.rs` + `src/lsmengine.rs`
- Add two methods to the existing `RangeScan` trait (COUNT is a range-scan
  specialization, so it shares the trait rather than introducing a parallel one):
  - `fn count_prefix(&self, prefix: &str) -> io::Result<usize>;`
  - `fn count_range(&self, start: &str, end: &str) -> io::Result<usize>;`
- `impl RangeScan for LsmEngine` gains both, each reusing the **exact three-tier
  merge** from `prefix`/`range` (`lsmengine.rs:516`, `:583`): SSTable segments →
  immutable memtable → active memtable, newest-wins, with expiry and tombstone
  handling identical to the value-returning versions.

### Count-only accumulation (the one piece that differs from `range`/`prefix`)
- Instead of a `BTreeMap<String, String>`, the merge accumulates into a
  **`BTreeSet<String>`** of live keys:
  - a live value `insert`s the key,
  - a tombstone or expired record `remove`s the key,
  exactly mirroring the `b_map.insert` / `b_map.remove` branches in
  `range`/`prefix`, but tracking only keys.
- **Values are never read into memory.** Where `range`/`prefix` do
  `b_map.insert(record.key, record.value)`, COUNT does `set.insert(record.key)`
  and drops `record.value`.
- The method returns `set.len()`.
- `count_prefix` reuses `prefix`'s pruning (`prefix_successor` →
  `iter_files_for_range(prefix, successor)`, fall back to `iter_all` when no
  finite successor) and `starts_with` correctness filter.
- `count_range` reuses `range`'s pruning (`iter_files_for_range(start, end)`)
  and `start <= key <= end` correctness filter, including the `start > end` →
  early-return-`0` guard.

### Dispatch — `src/server/dispatch.rs`
- `Command::CountPrefix(prefix)` arm modeled on `Command::Prefix`
  (`dispatch.rs:247`): `database.as_any().downcast_ref::<LsmEngine>()`, call
  `lsm.count_prefix(&prefix)`, return `encode_frame(Ok, &[count.to_string()])`;
  `None` branch returns the KV-not-supported error.
- `Command::CountRange(start, end)` arm modeled on `Command::Range`
  (`dispatch.rs:188`), calling `lsm.count_range(&start, &end)`.
- Each successful call bumps `stats.reads` by **1** (a single count query returns
  no records to the client; unlike `RANGE`/`PREFIX` which bump by the pair count).

## Invariants

These are the properties the implementation must preserve; the test suite below
exists to pin each one.

1. **Membership is exactly the matching, live key set.** A key is counted iff
   (prefix variant) `key.starts_with(prefix)` / (range variant)
   `start <= key <= end`, **and** the key is live (not tombstoned, not expired)
   under the merge. No optimization may add or drop a key relative to this
   definition.

2. **Pruning is sound and never changes the count.** Skipping an SSTable via
   `iter_files_for_range` is purely a performance hint — running COUNT with
   pruning disabled (scan every segment, filter) must yield the identical count.
   Inherited unchanged from `range`/`prefix` (their invariant 2/3).

3. **Newest write wins across the three tiers.** For any key, liveness reflects
   the most recent write in precedence order: active > immutable > newer SSTable
   > older SSTable. A tombstone or expired record in a higher-precedence tier
   shadows a live value in a lower one (the key is not counted), exactly as in
   `range`/`prefix`.

4. **Each key is counted at most once.** The merge accumulates into a
   `BTreeSet`, so a key present in multiple tiers/segments contributes exactly
   one to the count.

5. **Differential equivalence with `range`/`prefix`.** For any engine state:
   `count_prefix(p) == prefix(p).len()` and
   `count_range(s, e) == range(s, e).len()`. This is the tightest correctness
   statement — COUNT counts precisely what RANGE/PREFIX would return.

6. **Expiry is evaluated against a single `now_ms` captured once per call**, so a
   key cannot be live in one tier and expired in another within the same call.

7. **COUNT is read-only.** It takes only read locks and never mutates engine
   state, segments, or memtables.

8. **Edge cases degenerate cleanly.** Empty prefix counts every live key
   (no finite successor → all segments scanned). `start > end` returns `0`
   without scanning.

## Tests — `tests/`

Mirroring the `range`/`prefix` suites:

- `lsm_count` (engine-level), both variants:
  - basic matches; non-matching keys excluded;
  - expired keys excluded; tombstoned keys excluded;
  - newest-wins across active/immutable memtable and SSTables (a tombstone or
    expiry in a higher tier drops the count);
  - **empty prefix counts all live keys**; no-match returns `0`;
  - **`start > end` returns `0`**;
  - prefix equal to a full key counts that key; `starts_with` boundary
    (prefix `cpu:` must not count `cpuz`-style non-matches);
  - multi-segment spread of matching keys (forces the merge across several
    SSTables).
- **Differential test (invariant 5):** build an engine with keys scattered
  across multiple SSTables/memtables (mix of live, tombstoned, expired) and
  assert `count_prefix(p) == prefix(p).len()` and
  `count_range(s,e) == range(s,e).len()` over a spread of queries.
- `count_command` (end-to-end over BFFP framing) for **both** op codes
  (`CountPrefix` and `CountRange`). Include an assertion that a COUNT call bumps
  `stats.reads` by exactly **1** (not by the matched-key count) — this pins the
  deliberate divergence from `RANGE`/`PREFIX` so a planner can't copy their
  dispatch arm verbatim.
- CLI parse tests for the `COUNT` arm: arity 2 → `CountPrefix`, arity 3 →
  `CountRange`, wrong arity → usage string.
- KV engine returns the not-supported error for both variants.

## Docs

Add `COUNT` rows to the README command table:
- `COUNT <prefix>` (op 15) and `COUNT <start> <end>` (op 16), LSM-only.

## Out of scope

- Server-side aggregation (SUM/AVG/MIN/MAX over a range) — the next step on the
  telemetry path, its own task.
- KV-engine COUNT support.
- Pagination / streaming (COUNT returns a single integer, so this is moot).
