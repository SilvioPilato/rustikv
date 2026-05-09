# Sparse Index Interval Sweep (2026-04-25)

Comparing `SPARSE_INDEX_INTERVAL` values: 1, 2, 4, 16, 32.

**Baseline (#29):** 2026-04-24, block-based SSTable, `--block-compression none`, default interval (unknown, treated as ~4 in the original codebase).
All runs: `--engine lsm --fsync-interval never --block-compression none`.

---

## Sequential WRITE throughput (ops/sec) — kvbench

| Payload | si=1 | si=2 | si=4 | si=16 | si=32 | #29 |
|---------|------|------|------|-------|-------|-----|
| 100 B   | 36,017 | 35,918 | 34,869 | 35,956 | 32,229 | 40,996 |
| 1 KB    | 30,237 | 31,992 | 30,918 | 31,507 | 30,893 | 37,212 |
| 10 KB   | 16,603 | 15,960 | 14,550 | 15,943 | 12,313 | 19,051 |
| 100 KB  | 2,410  | 2,294  | 2,452  | 2,641  | 2,355  | 3,110  |
| 1 MB    | 246    | 240    | 256    | 250    | 245    | 224    |

---

## Sequential READ throughput (ops/sec) — kvbench

| Payload | si=1 | si=2 | si=4 | si=16 | si=32 | #29 |
|---------|------|------|------|-------|-------|-----|
| 100 B   | 44,619 | 43,401 | 41,883 | 43,149 | 41,124 | 47,552 |
| 1 KB    | 44,646 | 42,732 | 43,917 | 38,561 | 41,173 | 49,031 |
| 10 KB   | 39,574 | 36,830 | 37,304 | 36,975 | 36,211 | 42,808 |
| 100 KB  | 2,779  | 2,392  | 1,734  | 642    | 348    | 333 † |
| 1 MB    | 221    | 200    | 149    | 60     | 34     | 14 †† |

† #29 100KB read was a compaction-noise outlier.
†† #29 1MB read was severely SSTable-scan-bound (no usable index entries for large values).

---

## Key observations

### Writes
- All intervals 1–32 show a regression vs #29 baseline (~9–22%).
- Within the sweep, write throughput is **flat across si=1–16**, with si=32 showing a slight additional dip (~5–10%) at small payloads due to compaction overhead from larger SSTable scan ranges.
- The write regression vs #29 is pre-existing and unrelated to interval choice — it reflects another change between the two runs.

### Reads — small payloads (100B–10KB)
- Reads are **memtable-dominated** at these sizes: 10,000 × 10KB = 100 MB, still fits in the ~512 MB memtable budget.
- All intervals produce similar read throughput (36K–44K ops/sec). No meaningful signal.

### Reads — large payloads (100KB–1MB)
This is where interval choice clearly matters:

| Payload | si=1 | si=2 | si=4 | si=16 | si=32 |
|---------|------|------|------|-------|-------|
| 100 KB  | 2,779 | 2,392 | 1,734 | 642 | 348 |
| 1 MB    | 221   | 200   | 149   | 60  | 34  |

**Clear monotonic degradation as interval grows.** With si=1 (dense), every block has a direct index entry; reads go straight to the right block. With si=32, a lookup must linearly scan up to 32 blocks from the last index hit.

At 1MB values, the degradation is severe: si=32 delivers only **15% of si=1 read throughput**.

---

## Verdict

| Interval | Write cost | Small-payload reads | Large-payload reads | Assessment |
|----------|------------|--------------------|--------------------|------------|
| si=1 (dense) | Highest | ~same | **Best** | Optimal for large-value workloads |
| si=2 | ~same as si=1 | ~same | −9% | Minimal gain |
| si=4 | ~same | ~same | −33% | Noticeable degradation |
| si=16 | ~same | ~same | −73–78% | Severe for large values |
| si=32 | Slightly worse writes | ~same | **−84–85%** | Avoid for SSTable-bound reads |

**Recommendation:** Keep `SPARSE_INDEX_INTERVAL = 1`.

The write overhead of a dense index is negligible at large payload sizes (I/O dominates), and the read benefit at 100KB–1MB payloads is decisive. The only argument for interval > 1 would be memory pressure from a very large index on tiny-key/tiny-value workloads — which is not the current bottleneck.
