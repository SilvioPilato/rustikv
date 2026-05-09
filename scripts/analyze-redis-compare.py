#!/usr/bin/env python3
"""
Analyze redis-compare benchmark results.

Parses redis-compare-*.txt files from docs/benchmark-runs/2026-04-25/
and generates a comparison report showing rustikv performance vs Redis.
"""

import re
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

@dataclass
class PhaseStats:
    """Statistics for a single phase (WRITE or READ)."""
    server: str
    phase: str
    count: int
    total_ms: float
    throughput: float
    latency_min_us: float
    latency_mean_us: float
    latency_p99_us: float
    latency_max_us: float

def parse_duration_to_ms(duration_str: str) -> float:
    """Convert duration string like '25.123ms' to float milliseconds."""
    duration_str = duration_str.strip()
    if duration_str.endswith('ms'):
        return float(duration_str[:-2])
    elif duration_str.endswith('s'):
        return float(duration_str[:-1]) * 1000
    elif duration_str.endswith('µs'):
        return float(duration_str[:-2]) / 1000
    else:
        return float(duration_str)

def parse_latency_duration(duration_str: str) -> float:
    """Convert latency duration to microseconds (µs)."""
    duration_str = duration_str.strip()
    # Remove microsecond symbol variations
    if 'ns' in duration_str:
        val = float(duration_str.split('ns')[0].strip())
        return val / 1000
    elif 'ms' in duration_str:
        val = float(duration_str.split('ms')[0].strip())
        return val * 1000
    else:  # Assume microseconds
        # Try to extract numeric value
        import re
        match = re.search(r'[\d.]+', duration_str)
        if match:
            return float(match.group())
    return float(duration_str)

def parse_benchmark_file(filepath: Path) -> Dict[str, PhaseStats]:
    """Parse a redis-compare output file and extract stats."""
    stats = {}

    with open(filepath, 'r') as f:
        content = f.read()

    # Pattern: === [server] PHASE (count ops) ===
    phase_pattern = r'=== \[(\w+)\] (\w+) \((\d+) ops\) ===\n.*?Total:\s+([^\n]+)\n.*?Throughput:\s+([\d,]+) ops/sec\n.*?Latency\s+min=([^\s]+)\s+mean=([^\s]+)\s+p99=([^\s]+)\s+max=([^\s]+)'

    for match in re.finditer(phase_pattern, content, re.DOTALL):
        server = match.group(1)
        phase = match.group(2)
        count = int(match.group(3))
        total_str = match.group(4)
        throughput_str = match.group(5).replace(',', '')
        min_lat = match.group(6)
        mean_lat = match.group(7)
        p99_lat = match.group(8)
        max_lat = match.group(9)

        total_ms = parse_duration_to_ms(total_str)
        throughput = float(throughput_str)

        key = f"{server}_{phase}"
        stats[key] = PhaseStats(
            server=server,
            phase=phase,
            count=count,
            total_ms=total_ms,
            throughput=throughput,
            latency_min_us=parse_latency_duration(min_lat),
            latency_mean_us=parse_latency_duration(mean_lat),
            latency_p99_us=parse_latency_duration(p99_lat),
            latency_max_us=parse_latency_duration(max_lat),
        )

    return stats

def extract_payload_size(filename: str) -> str:
    """Extract payload size from filename like 'redis-compare-100B.txt'."""
    match = re.search(r'redis-compare-([^.]+)\.txt', filename)
    return match.group(1) if match else "unknown"

def main():
    benchmark_dir = Path("docs/benchmark-runs/2026-04-25")

    if not benchmark_dir.exists():
        print(f"Error: {benchmark_dir} not found")
        sys.exit(1)

    # Find all redis-compare-*.txt files
    result_files = sorted(benchmark_dir.glob("redis-compare-*.txt"))

    if not result_files:
        print(f"No redis-compare-*.txt files found in {benchmark_dir}")
        sys.exit(1)

    print(f"Found {len(result_files)} benchmark files")
    print()

    # Parse all results
    all_results = {}
    for filepath in result_files:
        payload_size = extract_payload_size(filepath.name)
        print(f"Parsing {filepath.name}...", end=" ")
        try:
            stats = parse_benchmark_file(filepath)
            all_results[payload_size] = stats
            print(f"OK ({len(stats)} phases)")
        except Exception as e:
            print(f"✗ Error: {e}")

    print()

    # Generate comparison tables
    output_lines = [
        "# Redis Comparison Analysis",
        "",
        "Comparing rustikv's TCP server against Redis across different payload sizes.",
        "",
    ]

    # WRITE comparison
    output_lines.append("## WRITE Throughput Comparison (ops/sec)")
    output_lines.append("")
    output_lines.append("| Payload | rustikv | Redis | rustikv % of Redis |")
    output_lines.append("|---------|---------|-------|-------------------|")

    for payload_size in sorted(all_results.keys(), key=lambda x: (x.count(x[0]), int(re.search(r'\d+', x).group()))):
        stats = all_results[payload_size]
        if f"rustikv_WRITE" in stats and f"redis_WRITE" in stats:
            rustikv_write = stats["rustikv_WRITE"].throughput
            redis_write = stats["redis_WRITE"].throughput
            pct = (rustikv_write / redis_write * 100) if redis_write > 0 else 0
            output_lines.append(f"| {payload_size} | {rustikv_write:,.0f} | {redis_write:,.0f} | {pct:.1f}% |")

    output_lines.append("")

    # READ comparison
    output_lines.append("## READ Throughput Comparison (ops/sec)")
    output_lines.append("")
    output_lines.append("| Payload | rustikv | Redis | rustikv % of Redis |")
    output_lines.append("|---------|---------|-------|-------------------|")

    for payload_size in sorted(all_results.keys(), key=lambda x: (x.count(x[0]), int(re.search(r'\d+', x).group()))):
        stats = all_results[payload_size]
        if f"rustikv_READ" in stats and f"redis_READ" in stats:
            rustikv_read = stats["rustikv_READ"].throughput
            redis_read = stats["redis_READ"].throughput
            pct = (rustikv_read / redis_read * 100) if redis_read > 0 else 0
            output_lines.append(f"| {payload_size} | {rustikv_read:,.0f} | {redis_read:,.0f} | {pct:.1f}% |")

    output_lines.append("")

    # Latency comparison (p99 for each phase)
    output_lines.append("## READ Latency (p99) Comparison")
    output_lines.append("")
    output_lines.append("| Payload | rustikv (µs) | Redis (µs) |")
    output_lines.append("|---------|--------------|-----------|")

    for payload_size in sorted(all_results.keys(), key=lambda x: (x.count(x[0]), int(re.search(r'\d+', x).group()))):
        stats = all_results[payload_size]
        if f"rustikv_READ" in stats and f"redis_READ" in stats:
            rustikv_p99 = stats["rustikv_READ"].latency_p99_us
            redis_p99 = stats["redis_READ"].latency_p99_us
            output_lines.append(f"| {payload_size} | {rustikv_p99:.2f} | {redis_p99:.2f} |")

    output_lines.append("")
    output_lines.append("## Key Insights")
    output_lines.append("")

    # Calculate insights
    write_percentages = []
    read_percentages = []

    for payload_size, stats in all_results.items():
        if f"rustikv_WRITE" in stats and f"redis_WRITE" in stats:
            rustikv_write = stats["rustikv_WRITE"].throughput
            redis_write = stats["redis_WRITE"].throughput
            pct = (rustikv_write / redis_write * 100) if redis_write > 0 else 0
            write_percentages.append(pct)

        if f"rustikv_READ" in stats and f"redis_READ" in stats:
            rustikv_read = stats["rustikv_READ"].throughput
            redis_read = stats["redis_READ"].throughput
            pct = (rustikv_read / redis_read * 100) if redis_read > 0 else 0
            read_percentages.append(pct)

    if write_percentages:
        avg_write_pct = sum(write_percentages) / len(write_percentages)
        output_lines.append(f"- **Average WRITE performance:** {avg_write_pct:.1f}% of Redis")
        if avg_write_pct > 40:
            output_lines.append("  - **Excellent:** Writing to disk at nearly half Redis in-memory speed")
        elif avg_write_pct > 20:
            output_lines.append("  - **Good:** Reasonable performance given durability overhead")
        else:
            output_lines.append("  - **Investigate:** Consider profiling for compaction/fsync bottlenecks")

    if read_percentages:
        avg_read_pct = sum(read_percentages) / len(read_percentages)
        output_lines.append(f"- **Average READ performance:** {avg_read_pct:.1f}% of Redis")
        if avg_read_pct > 80:
            output_lines.append("  - **Excellent:** Nearly matching Redis on reads")
        elif avg_read_pct > 50:
            output_lines.append("  - **Good:** Solid read performance, likely memtable hits")
        else:
            output_lines.append("  - **Investigate:** Low read throughput suggests cache misses or contention")

    output_lines.append("")
    output_lines.append("## Payload Scaling")
    output_lines.append("")
    output_lines.append("Track how performance changes with larger payloads:")
    output_lines.append("- **Small payloads (100B-1KB):** Dominated by overhead, both servers similar")
    output_lines.append("- **Medium payloads (10KB-100KB):** rustikv's block structure and compression may help")
    output_lines.append("- **Large payloads (1MB+):** rustikv may struggle with I/O; Redis dominates (in-memory)")

    # Write to file
    output_file = benchmark_dir / "redis-comparison-analysis.md"
    with open(output_file, 'w') as f:
        f.write('\n'.join(output_lines))

    print(f"OK: Analysis saved to {output_file}")
    print()
    print('\n'.join(output_lines))

if __name__ == "__main__":
    main()
