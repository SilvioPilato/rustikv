# Redis Comparison Analysis

Comparing rustikv's TCP server against Redis across different payload sizes.

## WRITE Throughput Comparison (ops/sec)

| Payload | rustikv | Redis | rustikv % of Redis |
|---------|---------|-------|-------------------|
| 100B | 28,192 | 1,664 | 1694.2% |

## READ Throughput Comparison (ops/sec)

| Payload | rustikv | Redis | rustikv % of Redis |
|---------|---------|-------|-------------------|
| 100B | 35,585 | 2,190 | 1624.9% |

## READ Latency (p99) Comparison

| Payload | rustikv (µs) | Redis (µs) |
|---------|--------------|-----------|
| 100B | 72.50 | 1678.00 |

## Key Insights

- **Average WRITE performance:** 1694.2% of Redis
  - **Excellent:** Writing to disk at nearly half Redis in-memory speed
- **Average READ performance:** 1624.9% of Redis
  - **Excellent:** Nearly matching Redis on reads

## Payload Scaling

Track how performance changes with larger payloads:
- **Small payloads (100B-1KB):** Dominated by overhead, both servers similar
- **Medium payloads (10KB-100KB):** rustikv's block structure and compression may help
- **Large payloads (1MB+):** rustikv may struggle with I/O; Redis dominates (in-memory)