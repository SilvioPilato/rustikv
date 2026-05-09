# Sparse Index Interval = 1 Benchmark (2026-04-25)

**Baseline (#29):** 2026-04-24, block-based SSTable, `--block-compression none`. ([raw runs](../2026-04-24/))
**Current (si1):** 2026-04-25, sparse index interval set to 1 (dense index — every key gets an index entry). `--block-compression none`.

---

## 1. Sequential — no misses (10,000 keys, 100 B values)

### kvbench (TCP)

| Phase | #29 bcnone | si1 | Delta |
|---|---|---|---|
| WRITE | 40,996 | 36,017 | −12% |
| READ | 47,552 | 44,373 | −7% |

### engbench (in-process)

| Engine | si1 WRITE | si1 READ |
|---|---|---|
| rustikv-lsm | 300,793 | 4,842,615 |
| rustikv-kv | 146,595 | 39,765 |
| sled | 421,359 | 2,803,633 |

---

## 2. Concurrent — 4 writers / 8 readers (10,000 keys, 100 B values)

| Metric | #29 | si1 | Delta |
|---|---|---|---|
| WRITE | ~83K | 83,394 | 0% |
| READ | ~141K | 140,715 | 0% |
| AGGREGATE | 183,546 | 166,787 | **−9%** |

---

## 3. Payload scaling — sequential, LSM / fsync=never / bcnone

### WRITE throughput (ops/sec)

| Value size | #29 bcnone | si1 | Delta |
|---|---|---|---|
| 100 B | 40,996 | 37,172 | −9% |
| 1 KB | 37,212 | 30,237 | **−19%** |
| 10 KB | 19,051 | 16,603 | −13% |
| 100 KB | 3,110 | 2,410 | −23% |

### READ throughput (ops/sec)

| Value size | #29 bcnone | si1 | Delta |
|---|---|---|---|
| 100 B | 47,552 | 44,619 | −6% |
| 1 KB | 49,031 | 44,646 | −9% |
| 10 KB | 42,808 | 39,574 | −8% |
| 100 KB | 333 † | 2,779 | n/a |

† #29 100KB read was flagged as a compaction-noise outlier; not interpretable.

---

## 4. engbench payload scaling (in-process, no TCP)

### WRITE throughput (ops/sec)

| Value size | rustikv-lsm | rustikv-kv | sled |
|---|---|---|---|
| 100 B | 300,793 | 146,595 | 421,359 |
| 1 KB | 160,553 | 76,739 | 326,206 |
| 10 KB | 33,431 | 18,737 | 79,290 |
| 100 KB | 4,163 | 2,337 | 1,640 |

### READ throughput (ops/sec)

| Value size | rustikv-lsm | rustikv-kv | sled |
|---|---|---|---|
| 100 B | 4,842,615 | 39,765 | 2,803,633 |
| 1 KB | 3,307,534 | 38,186 | 2,903,516 |
| 10 KB | 24,393 | 22,746 | 2,828,854 |
| 100 KB | 3,393 | 4,742 | 2,237,136 |

---

## 5. Summary

| Dimension | si1 vs #29 bcnone | Verdict |
|---|---|---|
| Sequential writes ≤ 100B | −9–12% | Regression |
| Sequential reads ≤ 100B | −6–7% | Minor regression |
| Write throughput 1KB–100KB | −13–23% | **Noticeable regression** |
| Read throughput 1KB–10KB | −8–9% | Minor regression |
| Concurrent aggregate | −9% | Regression |

**Cause:** Dense sparse index (interval=1) increases SSTable flush cost — every key emits an index entry instead of every N-th key. This adds write overhead proportional to key count. Read benefits (direct index lookup) are not visible at these workload sizes because small payloads mostly stay in the memtable.

**Next step:** Test with a dataset larger than the memtable (>10M bytes) to see if dense indexing improves read latency for SSTable-bound lookups.
