#!/usr/bin/env bash
# Benchmark: LSM + LZ77 + SPARSE_INDEX_INTERVAL=1
# Fresh DB per run, proper server lifecycle management on Windows
set -euo pipefail

RUSTIKV="./target/release/rustikv.exe"
KVBENCH="./target/release/kvbench.exe"
OUT_DIR="docs/benchmark-runs/2026-04-25"
GIT_HASH=$(git rev-parse --short HEAD)
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

mkdir -p "$OUT_DIR"
OUT_FILE="$OUT_DIR/sparse-index-1-bclz77.txt"

{
echo "# sparse-index-1-bclz77"
echo "# server: rustikv --engine lsm --fsync-interval never -bc lz77 (SPARSE_INDEX_INTERVAL=1)"
echo "# bench:  kvbench sequential, fresh DB per run"
echo "# date:   $DATE"
echo "# git:    $GIT_HASH"
echo ""
} | tee "$OUT_FILE"

kill_server() {
    local pid=$1
    # Windows: use taskkill
    taskkill.exe /PID "$pid" /F >/dev/null 2>&1 || true
    # Wait for port 6666 to be free (up to 10s)
    local tries=0
    while netstat -ano 2>/dev/null | grep -q ":6666 "; do
        tries=$((tries+1))
        [ $tries -ge 20 ] && break
        sleep 0.5
    done
}

wait_for_server() {
    local tries=0
    while ! "$KVBENCH" -n 1 -s 1 >/dev/null 2>&1; do
        tries=$((tries+1))
        [ $tries -ge 30 ] && { echo "ERROR: server did not start"; exit 1; }
        sleep 0.5
    done
}

# payload: count value_size label
PAYLOADS=(
    "10000 100 100B"
    "10000 1024 1KB"
    "2000 10240 10KB"
    "1000 102400 100KB"
    "500 1048576 1MB"
)

for spec in "${PAYLOADS[@]}"; do
    count=$(echo $spec | awk '{print $1}')
    vsize=$(echo $spec | awk '{print $2}')
    label=$(echo $spec | awk '{print $3}')

    echo "===== $label ($count keys, ${vsize}B values) =====" | tee -a "$OUT_FILE"

    for run in 1 2 3; do
        DB_DIR="/tmp/bench-si1-${label}-r${run}"
        rm -rf "$DB_DIR"
        mkdir -p "$DB_DIR"

        "$RUSTIKV" "$DB_DIR" --engine lsm --fsync-interval never -bc lz77 &
        SERVER_PID=$!
        wait_for_server

        {
        echo "--- run $run ---"
        "$KVBENCH" -n "$count" -s "$vsize"
        echo "=== DB size (bytes) ==="
        # Get disk usage via PowerShell (reliable on Windows)
        powershell.exe -NoProfile -Command "(Get-ChildItem -Recurse '$DB_DIR' -File | Measure-Object -Property Length -Sum).Sum"
        } | tee -a "$OUT_FILE"

        kill_server $SERVER_PID
    done

    echo "" | tee -a "$OUT_FILE"
done

echo "Done. Results in $OUT_FILE"
