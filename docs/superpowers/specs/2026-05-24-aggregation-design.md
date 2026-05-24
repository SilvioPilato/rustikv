# Aggregation commands — design (#84)

## Summary

Add four LSM-only TCP commands — `SUM`, `AVG`, `MIN`, `MAX` — that reduce the
**live numeric values** matching a query to a single scalar. This is the final
step of the telemetry use-case path (TTL → INCR → PREFIX → COUNT →
**aggregation**): clients stop pulling raw values to aggregate client-side; the
engine resolves the live key set, parses values as `f64`, and returns one number.

Each command has two variants, disambiguated by arity:
- `<CMD> <prefix>` — aggregate live keys where `key.starts_with(prefix)`.
- `<CMD> <start> <end>` — aggregate live keys in the inclusive range `[start, end]`.

The KV engine returns a not-supported error for all 8 op codes, exactly as
`RANGE`/`PREFIX`/`COUNT` do.

## Semantics

- **Live value resolution** reuses `RANGE`/`PREFIX`'s existing three-tier merge
  (SSTables → immutable memtable → active memtable, newest-wins, tombstone and
  expiry-aware). Only surviving live values are parsed; a non-numeric value that
  is shadowed, tombstoned, or expired never surfaces and must never cause a parse
  error.
- **Numeric type**: `f64` (IEEE 754 double). Each surviving live value is parsed
  with `str::parse::<f64>()`.
- **Parse failure**: any parse failure on a live value errors the whole query.
  Error message: `"<OP>: non-numeric value for key <k>"` (e.g.
  `"SUM: non-numeric value for key cpu:host1"`). This mirrors INCR-style strict
  semantics.
- **Reductions** over the resolved numeric set V:
  - `SUM = Σ V`
  - `AVG = Σ V / |V|`
  - `MIN = min V`
  - `MAX = max V`
- **Empty match** (no matching live keys, or `start > end`) returns the
  "Not found" response — for all four commands — reusing GET's missing-key path.
  This mirrors SQL `NULL` semantics; `0` or `NaN` would be misleading.
- **Result format**: `format!("{:?}", value)` — always includes the decimal point
  (`"1.0"`, `"1.5"`), consistent with the `f64` type.
- **`stats.reads`** is bumped by 1 per call, same as COUNT.

## Layers

### Protocol — `src/bffp.rs`

8 new op codes following COUNT (15/16):

```
SumPrefix=17, SumRange=18
AvgPrefix=19, AvgRange=20
MinPrefix=21, MinRange=22
MaxPrefix=23, MaxRange=24
```

8 new `Command` variants:
```rust
SumPrefix(String), SumRange(String, String),
AvgPrefix(String), AvgRange(String, String),
MinPrefix(String), MinRange(String, String),
MaxPrefix(String), MaxRange(String, String),
```

Frame shapes reuse the two existing layouts with no new framing concept:
- Single-arg (`*Prefix`): `op | key_len(2) | prefix` — identical to `CountPrefix`.
- Two-arg (`*Range`): `op | start_len(2) | start | end_len(2) | end` — identical
  to `CountRange`.

Each new `Command` variant needs a decode arm in `decode_input_frame` and an
encode arm in `encode_command`, both trivially cloned from the COUNT arms.

### CLI — `src/cli.rs`

Four new arms in `parse_command`, each arity-dispatching like `COUNT`:

```
"SUM"  => arity 2 → SumPrefix(prefix),  arity 3 → SumRange(start, end)
"AVG"  => arity 2 → AvgPrefix(prefix),  arity 3 → AvgRange(start, end)
"MIN"  => arity 2 → MinPrefix(prefix),  arity 3 → MinRange(start, end)
"MAX"  => arity 2 → MaxPrefix(prefix),  arity 3 → MaxRange(start, end)
```

Any other arity → usage string, e.g. `"Usage: SUM <prefix> | SUM <start> <end>"`.

### Engine trait — `src/engine.rs`

Extend `RangeScan` with 8 new methods (return `Option<f64>` so `None` maps to
"Not found" without inventing a sentinel value):

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

### Engine implementation — `src/lsmengine.rs`

One private free function keeps the fold logic single-sourced:

```rust
fn resolve_numeric(op: &str, pairs: Vec<(String, String)>) -> io::Result<Vec<f64>>
```

- Iterates `pairs`, parsing each value with `str::parse::<f64>()`.
- On the first parse failure returns
  `Err(io::Error::new(InvalidData, "<op>: non-numeric value for key <k>"))`.
- Returns `Ok(vec![])` when `pairs` is empty (callers check for empty → `None`).

Each of the 8 `RangeScan` impl methods:
1. Calls `self.prefix(p)?` or `self.range(s, e)?` to obtain the merged live pairs.
2. Calls `resolve_numeric(op, pairs)?`.
3. Returns `Ok(None)` if the vec is empty.
4. Otherwise computes and returns `Ok(Some(result))`:
   - `SUM`: `nums.iter().sum()`
   - `AVG`: `let s: f64 = nums.iter().sum(); Ok(Some(s / nums.len() as f64))`
   - `MIN`: `nums.iter().cloned().fold(f64::INFINITY, f64::min)`
   - `MAX`: `nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max)`

### Dispatch — `src/server/dispatch.rs`

8 new arms, all modeled on `CountPrefix`/`CountRange`:

1. Downcast: `database.as_any().downcast_ref::<LsmEngine>()`.
2. Call the appropriate trait method.
3. `Ok(Some(v))` → `encode_frame(Ok, &[format!("{:?}", v)])` + `stats.reads += 1`.
4. `Ok(None)` → `encode_frame(NotFound, &[])`.
5. `Err(e)` → `encode_frame(Error, &[e.to_string()])`.
6. `None` downcast → `encode_frame(Error, &["SUM not supported by KV engine"])` (and equivalent for AVG/MIN/MAX).

## Invariants

1. **Only live values are parsed.** A non-numeric value that is tombstoned or
   expired in the engine never reaches `resolve_numeric` and must never cause a
   parse error.

2. **Any parse failure on a live value fails the whole query.** There is no
   partial result.

3. **Empty match returns Not found, not zero or NaN.** `start > end` is treated
   as empty.

4. **Newest-wins merge is identical to `RANGE`/`PREFIX`.** The aggregation result
   equals the corresponding `RANGE`/`PREFIX` result folded — this is the
   differential equivalence invariant.

5. **Result is formatted as `f64`** (`"{:?}"`) — always includes the decimal
   point.

6. **`stats.reads` increments by exactly 1 per call**, regardless of matched-key
   count.

## Tests

### `tests/lsm_aggregate.rs` (engine-level)

- Basic prefix and range match returns the correct SUM/AVG/MIN/MAX.
- Empty match returns `Ok(None)`.
- `start > end` returns `Ok(None)`.
- Non-numeric live value returns `Err` with the correct message.
- Non-numeric value that is tombstoned or expired does NOT error (shadowed
  values never surface into `resolve_numeric`).
- Newest-wins: a tombstone in a higher tier removes a key that would otherwise
  be non-numeric — no error.
- Multi-segment spread: values spread across SSTables + immutable + active
  memtable all fold correctly.
- Differential equivalence: for SUM, assert
  `sum_prefix(p) == prefix(p).iter().map(|(_,v)| v.parse::<f64>().unwrap()).sum()`
  over a mixed-state engine.
- AVG over a single key equals that key's parsed value.
- MIN/MAX over identical values returns that value.

### `tests/aggregate_command.rs` (end-to-end over BFFP)

- Round-trip for all 8 op codes.
- Each successful call bumps `stats.reads` by exactly 1.
- Empty match returns `NotFound` status.
- Non-numeric live value returns `Error` status with the correct message.
- KV engine returns the not-supported error for all 8 commands.
- CLI parse: arity 2 → prefix variant, arity 3 → range variant, wrong arity →
  usage string (for all four commands).

## README

Add 8 rows to the command table after the COUNT rows:

| Command | Op | Engine | Description |
|---|---|---|---|
| `SUM <prefix>` | 17 | LSM | Sum of live numeric values under prefix |
| `SUM <start> <end>` | 18 | LSM | Sum of live numeric values in range |
| `AVG <prefix>` | 19 | LSM | Average of live numeric values under prefix |
| `AVG <start> <end>` | 20 | LSM | Average of live numeric values in range |
| `MIN <prefix>` | 21 | LSM | Minimum of live numeric values under prefix |
| `MIN <start> <end>` | 22 | LSM | Minimum of live numeric values in range |
| `MAX <prefix>` | 23 | LSM | Maximum of live numeric values under prefix |
| `MAX <start> <end>` | 24 | LSM | Maximum of live numeric values in range |

Empty match or inverted range returns "Not found". A non-numeric live value
errors the whole query.

## Out of scope

- Percentiles / streaming histograms (task #36).
- MIN/MAX returning the matching key rather than the value.
- KV-engine support.
- Grouping / downsampling / GROUP BY.
