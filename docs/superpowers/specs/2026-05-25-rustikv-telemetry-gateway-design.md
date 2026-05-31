# rustikv Telemetry Gateway — Design

**Date:** 2026-05-25
**Status:** Approved for planning
**Spans:** a new repo (`rustikv-telemetry-gateway`) + a docs refresh in `rustikv` (task #85)

## Problem

rustikv's telemetry feature path is complete (TTL → INCR → PREFIX → COUNT →
SUM/AVG/MIN/MAX, all on the LSM engine). We want to actually exercise it with
real metrics from a Raspberry Pi and visualize them. Two gaps block this:

1. **Nothing speaks BFFP.** rustikv's wire protocol is a custom binary
   length-prefixed framing. No off-the-shelf collector can write to it and no
   visualization tool can read from it.
2. The use-case doc (`docs/telemetry-store-experiment.md`) is stale — it still
   lists the now-shipped features as "missing".

## Goal

A small standalone **telemetry gateway** that bridges standard tooling to
rustikv in both directions:

- **Ingest:** accept Graphite plaintext from any collector (collectd, Telegraf,
  statsd-graphite) and write it into rustikv.
- **Serve:** expose an HTTP/JSON query endpoint that Grafana charts via the
  Infinity datasource, pushing time-window rollups *server-side* into rustikv's
  aggregation ops.

Plus: refresh the rustikv telemetry doc (task #85) with worked examples of this
end-to-end flow.

## Non-goals (YAGNI)

- No new datasource contract (use Grafana's generic Infinity JSON datasource).
- No StatsD/INCR path — Graphite plaintext samples are points (gauges); every
  sample is a `WRITE`. INCR stays available for a future counter path.
- No multi-Pi fan-in initially (single inbound connection; thread-per-connection
  is a later, easy extension).
- No async runtime — synchronous std::net, matching rustikv's hand-rolled ethos.
- No changes to the rustikv crate — `bffp` is already `pub` via `lib.rs`.

## Architecture

```
Raspberry Pi                          gateway host                       rustikv host
┌────────────┐  Graphite plaintext  ┌─────────────────────┐  BFFP/TCP  ┌──────────┐
│ collectd / │  metric value ts\n   │ rustikv-telemetry-  │  MSET+TTL  │ rustikv  │
│ Telegraf   │ ───── TCP:2003 ────▶ │ gateway (Rust)      │ ──:6666──▶ │  (LSM)   │
└────────────┘                      │                     │            └────┬─────┘
                                    │  HTTP /query  ◀─────┼── Grafana       │ RANGE/
┌────────────┐  HTTP/JSON           │  (Infinity DS)      │ ────────────────┘ AVG/MIN/MAX
│  Grafana   │ ◀─── :8080 ───────── │                     │
└────────────┘                      └─────────────────────┘
```

One Rust binary, two listeners:
- **Ingest listener** (TCP, default `:2003`) — Graphite plaintext in.
- **Query listener** (HTTP, default `:8080`) — JSON out for Grafana.

Both translate to/from rustikv over a BFFP/TCP connection, reusing the `rustikv`
crate's `bffp::{Command, encode_command, decode_response_frame, ResponseStatus}`.
The client read-loop mirrors `src/bin/rustikli.rs`: write frame → read 4-byte
big-endian length → read body → `decode_response_frame`.

**Dependency:** `rustikv = { git = "https://github.com/SilvioPilato/rustikv.git" }`,
pinned by rev/tag. Decoupled, matches how rustikv itself pulls `mio-runtime`.

## Key schema

Graphite line `metric.path value timestamp` maps to:

```
key:   <metric.path>:<zero-padded-epoch-seconds(10)>     value: <numeric value>
e.g.   pi.cpu.user:0001748169600                          12.5
```

Fixed-width zero-padding is load-bearing: rustikv's `RANGE`/aggregation are
**lexicographic**, so the timestamp segment must sort lexicographically the same
way it sorts numerically. 10 digits covers epoch seconds through year 2286.
Consequences:
- `PREFIX pi.cpu.user:` → all samples of a metric.
- `RANGE pi.cpu.user:<from> pi.cpu.user:<to>` → that metric's samples in a window.
- `AVG/MIN/MAX/SUM <from-key> <to-key>` → server-side rollup over the window.

The gateway does no taxonomy of its own — it passes the collector's metric path
through verbatim, staying vendor-neutral.

## Ingest path

1. Read newline-delimited Graphite lines off the inbound TCP connection.
2. Parse each `name value ts`; skip + log malformed lines (don't drop the batch).
3. Buffer into a batch; flush on **N lines** or **T ms**, whichever first
   (defaults e.g. 200 lines / 1000 ms).
4. Encode one `Command::Mset` of `(key, value, Some(ttl))` tuples; send over BFFP.
5. Decode the response; on `Error`/connection drop, log and reconnect with backoff.

**Retention:** every write carries a TTL (default 24h, configurable) so the store
self-trims and the experiment stays bounded — exercising the TTL feature.

## Query path

```
GET /query?metric=<name>&from=<epoch>&to=<epoch>&agg=raw|avg|min|max|sum&step=<sec>
→  200 [ { "time": <epoch_ms>, "value": <f64> }, ... ]
```

- `agg=raw` → one `RANGE <metric>:<from> <metric>:<to>`; parse the flat
  key/value list into points; Grafana downsamples client-side.
- `agg=avg|min|max|sum` with `step` → split `[from,to]` into `step`-sized buckets
  and issue **one `AvgRange`/`MinRange`/`MaxRange`/`SumRange` per bucket**,
  returning one rolled-up point per bucket. This is the demonstration's point:
  downsampling is pushed server-side into rustikv's aggregation ops.
- Empty/`Not found` bucket → omitted from the series (or `null`), not an error.
- Errors (bad params, rustikv `Error`) → HTTP 4xx/5xx with a JSON message.

**Grafana:** the Infinity datasource consumes this JSON shape directly as a time
series — no custom datasource contract to maintain. The repo ships a sample
collectd config (Pi → Graphite out) and a sample Grafana panel/dashboard JSON.

## Components / units

| Unit | Responsibility | Depends on |
|------|----------------|------------|
| `rustikv_client` | Persistent BFFP/TCP conn: send `Command`, read+decode response, reconnect | `rustikv::bffp`, std::net |
| `graphite` | Parse Graphite plaintext lines → `(metric, value, ts)` | — |
| `schema` | `(metric, ts)` ↔ rustikv key (zero-pad / parse) | — |
| `ingest` | TCP listener, batching, flush policy, → `rustikv_client` MSET | graphite, schema, rustikv_client |
| `query` | HTTP listener, parse params, bucket fan-out, → JSON | schema, rustikv_client |
| `config` | CLI/env: ports, rustikv addr, TTL, batch/flush, bucket caps | — |

Each unit is independently testable: parsing and schema are pure functions;
`rustikv_client` is integration-tested against a live rustikv; ingest/query are
tested end-to-end.

## Error handling

- Malformed Graphite line → log + skip that line, keep the batch.
- rustikv connection drop → reconnect with bounded backoff; buffer or drop
  in-flight batch (drop is acceptable for telemetry; document the choice).
- Non-numeric aggregation result (rustikv errors on non-numeric live value) →
  surface as HTTP 5xx with the rustikv message; this shouldn't happen since the
  gateway only writes numeric values, but it's a real rustikv failure mode.
- Query window too large × small step → cap bucket count to avoid unbounded
  fan-out; return 400 if exceeded.

## Testing

- **Unit:** Graphite line parsing (valid, malformed, negative/float values);
  schema round-trip and zero-pad width; bucket boundary math.
- **Integration:** spin up rustikv (LSM) on a temp dir; ingest a known set of
  lines; assert via `/query?agg=raw` that points round-trip; assert
  `agg=avg/min/max` buckets match hand-computed values; assert TTL is set.
- **Contract:** a test that encodes a `Command::Write/Mset` and asserts the byte
  layout, guarding against silent BFFP drift across crate bumps.

## Deliverable 2: rustikv doc refresh (task #85)

Update `docs/telemetry-store-experiment.md` (rustikv repo, docs-only):
- Move SUM/AVG/MIN/MAX and COUNT from "missing"/"nice to have" into the "How it
  fits telemetry today" table as shipped.
- Update the "Suggested path" diagram to show the full path complete.
- Add worked end-to-end examples referencing this gateway → Grafana flow:
  timestamped-key `MSET` writes, `RANGE`/`PREFIX` windowing, `COUNT` cardinality,
  `AVG`/`MIN`/`MAX` over a window.

Follows rustikv's task workflow (branch `85-…`, PR, move task to Closed).

## Open follow-ups (not in this effort)

- Thread-per-connection ingest for multiple Pis.
- Optional StatsD/INCR path for true counters.
- #82 (size-tiered tombstone GC) if short-TTL reclamation proves too lazy under load.
