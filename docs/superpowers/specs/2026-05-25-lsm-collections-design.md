# LSM collections + per-collection default TTL — design (#86)

## Summary

Add **collections** (RocksDB-style column families) to the LSM engine: multiple
independent keyspaces inside one server, each with its own memtable, WAL, SSTable
set, compaction strategy instance, and an immutable **default TTL**. This realises
the originally-planned `#75` (collections) and `#76` (per-collection default TTL)
from the TTL design's Future Work (`docs/superpowers/specs/2026-05-10-ttl-design.md`),
and is the retention-by-default story that doc deliberately chose over a
server-wide `--default-ttl` flag.

Collections are **LSM-only**, matching `RANGE`/`PREFIX`/`COUNT`/aggregation. The
KV engine returns a not-supported error for every collection command.

Clients address a collection with connection-scoped selection — a new `USE
<collection>` command sets the active collection for that connection; all
subsequent commands operate on it. A built-in **default collection** (named from
the existing `--name`, e.g. `segment`) is active on every new connection, so
clients that never issue `USE` — and all existing tests, the gateway, and the
benchmarks — behave exactly as today.

The central architectural realisation: **`LsmEngine` + its `db_name` is already
"one collection."** It self-contains its memtable, WAL, strategy, and block
config, all namespaced by `db_name`. So `LsmEngine` is left structurally
unchanged; a new manager layer (`Collections`) owns a registry of `LsmEngine`
handles keyed by name and routes each command. The new concepts — registry,
catalog, routing, control commands — live in the manager and the dispatch layer,
not in the storage internals.

## Commands

Four new TCP commands, op codes 25–28 (following aggregation's 17–24):

| Command | Arity | Effect |
|---|---|---|
| `USE <name>` | 1 | Set the connection's active collection. Unknown name → `Error`. |
| `CREATE COLLECTION <name> [default-ttl <secs>]` | 1–2 | Register a collection with its default TTL (omitted ⇒ `0`). Duplicate / invalid name → `Error`. |
| `DROP COLLECTION <name>` | 1 | Remove a collection and delete its files. Dropping the default → `Error`. |
| `SHOW COLLECTIONS` | 0 | List `name\tdefault_ttl_secs` for every collection. |

All existing data commands (`READ`/`WRITE`/`DELETE`/`MGET`/`MSET`/`RANGE`/
`PREFIX`/`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`INCR`/`TTL`/`EXISTS`/`LIST`/`COMPACT`)
operate on the connection's **current collection**. `STATS` stays server-wide.

## Semantics

### Collection lifecycle

- **Default collection**: always exists, named `settings.db_name`. Active on every
  new connection. Cannot be dropped.
- **Create**: validates the name, errors if it already exists, builds a fresh
  `LsmEngine` (`db_name = name`, inheriting the server's strategy / block / memtable
  config) with the supplied default TTL, inserts it into the registry, and
  persists the catalog.
- **Use**: validates the name against the registry; on success sets the
  connection's `current_collection`; on failure returns `Error: no such collection
  '<name>'` and leaves the current collection unchanged.
- **Drop**: errors on the default or an unknown name. Otherwise removes the
  registry entry (dropping the `Arc<LsmEngine>`, which joins its background flush
  thread via `Drop`), deletes the collection's `<name>.wal` and `<name>_*.sst`
  files, and rewrites the catalog. A connection still pointing at the dropped
  collection gets "no such collection" on its next data op until it `USE`s another.

### Name validation

A collection name **is** the `db_name`, which **is** the on-disk file prefix.
`SSTable::parse` splits filenames on `_` and `_L{n}`, and the catalog is
tab-delimited, so names are restricted to:

```
[A-Za-z0-9-]+      (one or more ASCII letters, digits, or hyphens)
```

Rejected: empty, and any name containing `_`, `.`, `/`, `\`, whitespace, or tab.
This prevents filename-parsing ambiguity (e.g. a name like `foo_L2` colliding with
the level-encoded SSTable scheme) and catalog corruption. `CREATE` with an invalid
name → `Error`.

### Default TTL resolution

`Command::Write(k, v, Option<u32>)` and `Mset` items preserve a three-way TTL
distinction on the wire (via `FLAG_HAS_TTL`): `None` = no flag, `Some(0)` =
explicit zero, `Some(N)` = explicit N. dispatch resolves the **effective expiry**
against the current collection's default `D` (seconds):

| Client sent | Effective expiry |
|---|---|
| `None` (no flag) | `D > 0` → `now_ms + D·1000`; `D == 0` → none |
| `Some(0)` | none (explicit override — never expires) |
| `Some(N > 0)` | `now_ms + N·1000` |

dispatch then calls the existing `set_with_ttl` / `mset_with_ttl` with a resolved
`Option<u64>`. **`LsmEngine`'s write path is unchanged** — it still receives a
fully-resolved absolute expiry or `None`. A collection with `default-ttl 0`
therefore behaves byte-for-byte as the engine does today.

**This replaces, not extends, the current dispatch logic.** Today dispatch
collapses `Some(0)` to `None` (`ttl_seconds.and_then(|s| if s == 0 { None } …)`
in `dispatch.rs`), so `None` and `Some(0)` are indistinguishable downstream. The
new resolution must make them diverge: `None` consults the collection default
while `Some(0)` stays an explicit no-expiry. An implementer who preserves the old
collapse would silently break the `None` → default case — the regression suite
guards this only for `default-ttl 0` collections.

### INCR and the default

`INCR` carries no TTL argument and atomically creates-or-bumps under the engine's
write lock, so the default must be resolved before dispatch knows whether the key
exists. dispatch computes the **default expiry basis** (`now_ms + D·1000`, or
`None` when `D == 0`) and passes it into a widened
`incr(key, default_expiry_ms: Option<u64>)`:

- key **absent** → created at `1`, stamped with `default_expiry_ms`.
- key **present** → bumped, **existing expiry preserved** (today's behaviour,
  unchanged).

The engine receives a resolved value, so it still holds no catalog knowledge. The
only standing exception to "engine knows nothing about defaults" is an immutable
`default_ttl_secs` carried on each `LsmEngine` purely to support this path; every
other write resolves its default in dispatch.

### Engine scope

KV mode runs a single `KVEngine` as the default collection. `USE`, `CREATE
COLLECTION`, `DROP COLLECTION`, and `SHOW COLLECTIONS` return the same
"not supported by KV engine" error that `RANGE`/`PREFIX`/etc. do. The registry
never grows beyond the default under KV.

### COMPACT and STATS

- `COMPACT` acts on the **current collection** — compacts only that collection's
  SSTables, via the existing `LsmEngine::compact`.
- `STATS` stays **server-wide**: the `Stats` struct is shared atomic counters
  (reads, writes, active_connections, compaction state) that are genuinely server
  properties, not per-collection. Per-collection observability is deferred (see
  Future Work).

## Layers

### Protocol — `src/bffp.rs`

Four new op codes and `Command` variants:

```
Use = 25                 frame: | len | op | name_len(2) | name |
CreateCollection = 26    frame: | len | op | flags(1) | name_len(2) | name | [default_ttl(4) if flags & HAS_TTL] |
DropCollection = 27      frame: | len | op | name_len(2) | name |
ShowCollections = 28     frame: | len | op |
```

`CreateCollection` reuses the `FLAG_HAS_TTL` convention so `default-ttl` is
optional on the wire (absent ⇒ `0`). The other three follow existing single-key /
no-arg frame shapes. `encode_command` / `decode_input_frame` gain matching arms.
No change to the existing 24 frames.

### Manager — `src/collections.rs` (new)

```rust
pub struct Collections {
    engines: RwLock<HashMap<String, Arc<LsmEngine>>>, // live registry
    default_ttls: RwLock<HashMap<String, u32>>,        // name -> default ttl secs (catalog)
    db_path: String,
    default_name: String,                              // the un-droppable default
    template: LsmConfig,                               // strategy kind, block + memtable settings
}
```

Methods: `load_or_init(...)` (startup + migration), `create(name, default_ttl)`,
`drop(name)`, `get(name) -> Option<Arc<LsmEngine>>`, `default_ttl(name) -> u32`,
`list() -> Vec<(String, u32)>`, and private `persist_catalog()`. `create` / `drop`
take the write lock and rewrite the catalog atomically; `get` / `default_ttl` take
the read lock. Building a new collection's strategy reuses the same construction
logic as `main.rs` (extracted into a small helper so both paths share it).

The manager knows nothing about wire framing; `LsmEngine` knows nothing about
other collections; dispatch knows nothing about file layout.

### Engine — `src/lsmengine.rs`

- `LsmShared` gains an immutable `default_ttl_secs: u32`; `new` / `from_dir` take
  it as a parameter.
- `incr` signature widens to `incr(&self, key: &str, default_expiry_ms:
  Option<u64>)`; the create branch stamps `default_expiry_ms`. (The
  `StorageEngine::incr` trait method widens accordingly; KV applies the same
  create-vs-bump rule.)

### Connection / dispatch — `src/server/`

- `Connection` gains `current_collection: String`, initialised to the default name
  on connect (`connection.rs`).
- `dispatch` takes `&Collections` and `&mut String` (the connection's current
  collection) instead of `&Arc<dyn StorageEngine>`. Data arms resolve
  `collections.get(current)?` then run unchanged (including the
  `downcast_ref::<LsmEngine>()` for range/agg). New arms handle the four control
  commands; the `Use` arm mutates `*current` after validation.
- `WorkerHandler` holds `Arc<Collections>` instead of `Arc<dyn StorageEngine>` and
  passes `&mut conn.current_collection` into `dispatch`.

### CLI — `src/cli.rs`, `src/bin/rustikli.rs`

Parse arms for `USE`, `CREATE COLLECTION`, `DROP COLLECTION`, `SHOW COLLECTIONS`.

## Catalog & startup

- **File**: `<db_path>/collections.catalog`, one `name\tdefault_ttl_secs` line per
  collection. Rewritten wholesale (write `collections.catalog.tmp`, then rename)
  on every `CREATE` / `DROP`.
- **Startup (LSM)**: read the catalog; for each line build
  `LsmEngine::from_dir(db_path, name, …, default_ttl)`, loading that name's
  existing `*.sst` / `*.wal` as today.
- **Migration / first run**: if no catalog exists, synthesise one with a single
  entry — the default collection (`settings.db_name`, ttl `0`). Pre-existing
  `segment_*` files load into it untouched. Upgrading an existing db needs no
  manual step.
- The default collection is always ensured present in the registry even if the
  catalog omits it.
- **Crash recovery**: a stray `collections.catalog.tmp` left by a crash mid-rename
  is ignored and overwritten on the next catalog write; startup reads only
  `collections.catalog`. (The committed catalog is always the last successfully
  renamed file, so the `.tmp` carries no authoritative state.)

## Concurrency

`CREATE` / `DROP` are serialised by the registry write lock. Data ops take a read
lock only long enough to clone the `Arc<LsmEngine>`, then operate on the handle
lock-free of the registry. A reader holding an `Arc` keeps the engine alive even
if a concurrent `DROP` removes the registry entry and deletes files — the
worst-case window (read from a file being deleted) is the same one that already
exists between compaction and reads today. No new lock-ordering constraints are
introduced inside `LsmEngine`.

## Testing

Following the `#50` / `#51` / `#84` pattern — manager-level unit tests plus command
integration tests, all under `tests/`.

- **`tests/collections.rs`** (manager): create / drop / get / list; duplicate-create
  error; drop-default error; drop removes files + catalog line; name validation
  accepts `[A-Za-z0-9-]+` and rejects `_` / `.` / `/` / tab / empty; catalog
  round-trip (write → reload → identical registry); first-run migration
  synthesises the default and adopts existing `segment_*` files; isolation (same
  key in two collections is independent).
- **`tests/collection_command.rs`** (integration): BFFP round-trip for op codes
  25–28; `USE` switches connection state across subsequent commands on one
  connection; unknown-collection errors; `SHOW COLLECTIONS` output; KV returns
  not-supported for all four.
- **Default-TTL tests**: the resolution table (no-flag → default, `0` → none,
  `N` → N) on WRITE and MSET; `default-ttl 0` collection behaves as today; `INCR`
  stamps default on create and preserves expiry on bump (extend `lsm_incr`); a
  collection's default does not leak into another collection.
- **Regression**: the existing suite must pass unchanged — proving the
  default-collection path is transparent to current clients, the gateway, and
  `kvbench` / `redis-compare`.

## Decisions locked

| Decision | Choice | Rationale |
|---|---|---|
| Engine scope | LSM-only | Matches RANGE/PREFIX/COUNT/aggregation; telemetry uses LSM. |
| Routing | Connection-scoped `USE` | Cheapest protocol change; one new op + one `Connection` field; teaches session state. |
| Lifecycle | Explicit `CREATE` / `DROP` + catalog | Clean home for per-collection config; teaches catalog/manifest management. |
| Backward compat | Implicit default collection | Existing data, tests, and gateway work unchanged; no migration. |
| Catalog format | Line-based text file | Matches the project's hand-rolled, zero-dep ethos; human-readable. |
| Default-TTL rule | Default unless overridden; `0` = no expiry | Uses the `FLAG_HAS_TTL` distinction already on the wire. |
| INCR + default | Apply on create, preserve on bump | Consistent with WRITE; counters expire like everything else in the collection. |
| Default TTL mutability | Immutable (set at `CREATE` only) | No `ALTER`; simpler. Changing it later is future work. |
| COMPACT scope | Current collection | Per-collection compaction; avoids heavyweight all-collection rewrites. |
| STATS scope | Server-wide | `Stats` is shared atomic counters; per-collection stats is a separate change. |
| Architecture | Registry above an unchanged `LsmEngine` | Least invasive to load-bearing engine code; isolates new concepts. |

## Out of scope / Future Work

- **Per-collection compaction strategy / block settings** — new collections inherit
  the server's CLI-configured settings. (`#75` floated per-collection config; scoped
  here to default TTL.)
- **`ALTER COLLECTION`** — default TTL is immutable once set.
- **Per-collection STATS / observability** — `STATS` stays server-wide; filed as a
  follow-up.
- **Time-Window Compaction Strategy (`#77`)** — depends on this landing.
- **KV-engine collections** — LSM-only by design.
