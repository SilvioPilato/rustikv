# PREFIX command — design (#50)

## Summary

Add a `PREFIX <prefix>` TCP command returning all live key-value pairs whose
keys start with the given string. **LSM-only**, mirroring `RANGE` (#48) at every
layer; the KV engine returns an error. This is the third step of the telemetry
use-case path (TTL → INCR → **PREFIX** → COUNT → aggregation) and makes `RANGE`
ergonomic for querying a whole metric namespace (e.g. `cpu:host1:`) without
knowing exact key bounds.

## Semantics

- `PREFIX <prefix>` returns every live `(key, value)` pair where
  `key.starts_with(prefix)`, newest write wins, expired and tombstoned keys
  excluded.
- **Empty prefix returns all live keys.** This is the cross-industry convention
  (AWS S3 `ListObjects` empty `Prefix`, etcd `WithPrefix` on empty key,
  DynamoDB `begins_with(sk, "")`, RocksDB prefix iterators) and is internally
  consistent — every string `starts_with("")`. The empty prefix is *not*
  rejected at the parse layer.
- KV engine: returns `"PREFIX not supported by KV engine"`, exactly as `RANGE`
  does.

## Layers (each mirrors the existing `RANGE` implementation)

### Protocol — `src/bffp.rs`
- `OpCode::Prefix = 14` (11=Range, 12=Ttl, 13=Incr already taken).
- `Command::Prefix(String)` — single-string frame shaped like `Read`/`Incr`
  (`op | key_len | key`).
- Encode/decode arms following the `Range` arms at `bffp.rs:160` (decode) and
  `bffp.rs:391` (encode).

### CLI parse — `src/cli.rs`
- `"PREFIX"` arm with arity 2 (`PREFIX <prefix>`); wrong arity → usage string.
  Modeled on the `"RANGE"` arm at `cli.rs:85`. Empty prefix is accepted (valid
  "match all").

### Engine — `src/engine.rs` + `src/lsmengine.rs`
- Add `fn prefix(&self, prefix: &str) -> io::Result<Vec<(String, String)>>` to
  the existing `RangeScan` trait (PREFIX is a range-scan specialization, so it
  shares the trait rather than introducing a parallel `PrefixScan`).
- `impl RangeScan for LsmEngine` gains `prefix`, reusing the exact three-tier
  merge from `range` (`lsmengine.rs:516`): SSTable segments → immutable memtable
  → active memtable, newest-wins, with expiry and tombstone handling identical
  to `range`.

### Scan bounds (the one piece that differs from `range`)
- **Start** of the scan is `prefix`.
- **Correctness filter** is `record.key.starts_with(prefix)` — this replaces
  range's `key < start || key > end` check and is the single source of truth for
  what matches.
- **Segment pruning** still goes through `iter_files_for_range(start, end)`,
  which skips SSTables whose `[min, max]` cannot overlap. We pass
  `end = prefix_successor(prefix)` — the smallest string strictly greater than
  every string starting with `prefix`, computed by incrementing the last UTF-8
  scalar of the prefix. If the prefix is empty, or ends in a scalar with no
  successor (`char::MAX`), fall back to an unbounded upper sentinel so the scan
  considers all segments. The successor is purely a pruning hint; the
  `starts_with` filter guarantees correctness regardless.
- Note on the spec's original `[prefix, prefix\xff]` phrasing: keys are Rust
  `String` (UTF-8), so appending a raw `0xFF` byte is invalid UTF-8 and will not
  compile. The `starts_with` + lexicographic-successor approach is the correct
  UTF-8-safe equivalent.

### Dispatch — `src/server/dispatch.rs`
- `Command::Prefix(prefix)` arm modeled on `Command::Range` at
  `dispatch.rs:188`: `database.as_any().downcast_ref::<LsmEngine>()`, flatten
  result `(k, v)` pairs into the response payload, bump `stats.reads` by the
  pair count, and return the KV-not-supported error on the `None` branch.

## Invariants

These are the properties the implementation must preserve; the test suite below
exists to pin each one.

1. **`starts_with` is the sole source of truth for membership.** A pair `(k, v)`
   appears in the result if and only if `k.starts_with(prefix)` *and* `k` is live
   (not tombstoned, not expired) under the merge. No optimization may add or drop
   a key relative to this definition.

2. **Pruning is sound and never changes the result set.** Skipping an SSTable via
   `iter_files_for_range(prefix, successor)` is purely a performance hint. A
   segment with key range `[min, max]` may be skipped only when it provably
   contains no matching key — i.e. `max < prefix` or `min >= successor`. Because
   every matching key `m` satisfies `prefix <= m < successor`, any skipped
   segment cannot hold an `m`. Equivalently: running PREFIX with pruning disabled
   (scan every segment, filter by `starts_with`) must yield byte-identical
   results.

3. **`successor(prefix)` is the least string strictly greater than every string
   that starts with `prefix`.** Computed by incrementing the last UTF-8 scalar of
   `prefix` (skipping the surrogate gap `U+D800..=U+DFFF` — `char::from_u32`
   returns `None` there); if the last scalar is `char::MAX`, drop it and increment
   the preceding scalar, recursively. If `prefix` is empty or consists entirely of
   `char::MAX`, **no finite successor exists** → use the unbounded upper sentinel
   so every segment is considered. Property: for all `k`, `k.starts_with(prefix)`
   ⇒ `prefix <= k < successor` (when a finite successor exists).

4. **Newest write wins across the three tiers.** For any key, the value reflects
   the most recent write in precedence order: active memtable > immutable memtable
   > newer SSTable > older SSTable. A tombstone or expired record in a
   higher-precedence tier shadows a live value in a lower one (the key is absent
   from the result), exactly as in `range`.

5. **Each key appears at most once.** The merge accumulates into a `BTreeMap`
   keyed by key, so the result contains no duplicates regardless of how many
   tiers/segments hold the key.

6. **Output is sorted ascending by key**, inherited from the `BTreeMap` merge —
   same guarantee `range` already provides.

7. **Expiry is evaluated against a single `now_ms` captured once per call**, so a
   key cannot appear live in one tier and expired in another within the same
   PREFIX call.

8. **PREFIX is read-only.** It takes only read locks and never mutates engine
   state, segments, or memtables.

9. **Empty prefix degenerates cleanly to "all live keys."** Invariants 1–8 hold
   unchanged: every key `starts_with("")`, no finite successor exists so all
   segments are scanned, and the result is every live key in sorted order.

## Tests — `tests/`
Mirroring the range test suites:
- `lsm_prefix` (engine-level): basic matches; non-matching keys excluded;
  expired keys excluded; tombstoned keys excluded; newest-wins across
  active/immutable memtable and SSTables; **empty prefix returns all live
  keys**; no-match returns empty; a prefix equal to a full key matches that key;
  prefix that is a strict substring boundary (e.g. prefix `cpu:` should not
  match `cpuz`... — verify `starts_with` semantics, not `<=` range semantics).
  A prefix one char shorter/longer than stored keys; multi-segment spread of
  matching keys (forces the merge across several SSTables).
- **Pruning soundness (invariant 2):** a differential test that builds an engine
  with matching keys scattered across multiple SSTables and asserts the result is
  identical whether or not segment pruning skipped any file — e.g. by comparing
  `prefix(p)` against a brute-force `list_keys().filter(starts_with).get(...)`
  reference over the same live set.
- **Successor edge cases (invariant 3):** prefix ending in `char::MAX`; prefix
  whose last scalar borders the surrogate gap (`U+D7FF` → must step to `U+E000`);
  multi-byte UTF-8 prefix (e.g. `"é"`, emoji); all-`char::MAX` prefix and empty
  prefix both fall back to the unbounded sentinel and return all matching keys.
- `prefix_command` (end-to-end over BFFP framing).
- cli parse tests for the `PREFIX` arm (correct arity, wrong arity).
- KV engine returns the not-supported error.

## Docs
Add the `PREFIX` (op 14) row to the README command table.

## Out of scope
- COUNT (#51), server-side aggregation, and KV-engine prefix support.
- Pagination / streaming of large result sets (full result returned in one
  frame, same as `RANGE`).
