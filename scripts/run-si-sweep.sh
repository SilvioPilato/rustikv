#!/usr/bin/env bash
# run-si-sweep.sh — benchmark SPARSE_INDEX_INTERVAL across multiple values
# Usage: bash scripts/run-si-sweep.sh
set -euo pipefail

INTERVALS=(2 4 16 32)
DB_PATH=/c/Users/Silvio/bench-db
SSTABLE=src/sstable.rs
DATE_DIR=docs/benchmark-runs/2026-04-25

kill_server() {
    powershell -Command "Get-Process rustikv -ErrorAction SilentlyContinue | Stop-Process -Force" 2>/dev/null || true
    sleep 1
}

wait_for_server() {
    for i in $(seq 1 30); do
        if nc -z 127.0.0.1 6666 2>/dev/null; then return 0; fi
        sleep 0.5
    done
    echo "ERROR: server did not start in time" >&2
    exit 1
}

for SI in "${INTERVALS[@]}"; do
    echo ""
    echo "========================================"
    echo "  SPARSE_INDEX_INTERVAL = $SI"
    echo "========================================"

    # Patch the constant
    sed -i "s/^const SPARSE_INDEX_INTERVAL: usize = .*/const SPARSE_INDEX_INTERVAL: usize = $SI;/" "$SSTABLE"
    grep "SPARSE_INDEX_INTERVAL" "$SSTABLE" | head -1

    # Build
    echo "Building..."
    cargo build --release --bin rustikv --bin kvbench --bin engbench 2>&1 | tail -3

    OUTDIR="$DATE_DIR/sparse-index-$SI"
    mkdir -p "$OUTDIR"

    # Start fresh server
    kill_server
    rm -rf "$DB_PATH" && mkdir -p "$DB_PATH"
    cargo run --release -- "$DB_PATH" --engine lsm --fsync-interval never --block-compression none &
    SERVER_PID=$!
    echo "Server PID: $SERVER_PID"
    wait_for_server
    echo "Server ready."

    # --- seq-nomiss (10k keys, 100B) ---
    echo "Running seq-nomiss..."
    cargo run --release --bin kvbench -- -n 10000 -s 100 2>/dev/null \
        > "$OUTDIR/seq-nomiss-lsm-never.txt"
    cat "$OUTDIR/seq-nomiss-lsm-never.txt"

    # --- payload scaling (kvbench) ---
    for SZ in 100 1024 10240 102400 1000000; do
        LABEL=$(python3 -c "
s=$SZ
if s < 1024: print(f'{s}B')
elif s < 1048576: print(f'{s//1024}KB')
else: print(f'{s//1000000}MB')
")
        echo "Running kvbench pay-$LABEL..."
        N=10000
        if [ "$SZ" -ge 1000000 ]; then N=500; fi
        if [ "$SZ" -ge 10240 ]; then N=2000; fi
        cargo run --release --bin kvbench -- -n $N -s $SZ 2>/dev/null \
            > "$OUTDIR/pay-${LABEL}-lsm-never-bcnone.txt"
        grep "Throughput" "$OUTDIR/pay-${LABEL}-lsm-never-bcnone.txt"
    done

    # --- engbench (in-process, 500 ops, 1MB) ---
    echo "Running engbench 1MB..."
    cargo run --release --bin engbench -- --count 500 --value-size 1000000 --engines lsm 2>/dev/null \
        >> "$OUTDIR/pay-1MB-lsm-never-bcnone.txt"

    kill_server
    echo "Done si=$SI"
done

echo ""
echo "All intervals complete. Results in $DATE_DIR/sparse-index-{2,4,16,32}/"
