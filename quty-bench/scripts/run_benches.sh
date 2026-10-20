#!/usr/bin/env bash
set -euo pipefail

# Configuration
ADDR="${1:-127.0.0.1:6379}"
HOST="${ADDR%%:*}"
PORT="${ADDR##*:}"

CLIENT_BIN="./target/release/quty-cli"
BENCH_BIN="./target/release/quty-bench"
LOG_FILE="bench_results.txt"

# Resolve client binary fallback
if [[ ! -x "$CLIENT_BIN" ]]; then
    if [[ -x "./target/release/quty-client" ]]; then
        CLIENT_BIN="./target/release/quty-client"
    else
        CLIENT_BIN="cargo run -q -p quty-client --"
    fi
fi

if [[ ! -x "$BENCH_BIN" ]]; then
    echo "Building benchmark harness in release mode..."
    cargo build --release -p quty-bench -p quty-client
fi

# Helper: send command via client
send_cmd() {
    $CLIENT_BIN -H "$HOST" -p "$PORT" "$@"
}

# Verify server connectivity
echo "Connecting to Quty server at $ADDR..."
if ! send_cmd PING > /dev/null 2>&1; then
    echo "Error: Unable to connect to server at $ADDR. Is quty-server running?" >&2
    exit 1
fi
echo "Server connection verified."
echo "Benchmark output will be saved to: $LOG_FILE"
echo "" > "$LOG_FILE"

# Helper: reset database and run benchmark
run_bench() {
    local title="$1"
    shift
    local pre_populate="${PRE_POPULATE:-0}"

    echo "=================================================================" | tee -a "$LOG_FILE"
    echo "  $title" | tee -a "$LOG_FILE"
    echo "=================================================================" | tee -a "$LOG_FILE"

    echo "Resetting server state..." | tee -a "$LOG_FILE"
    send_cmd RESET | tee -a "$LOG_FILE"
    sleep 0.5

    # If the benchmark is read-only (GET), populate keys first so reads are cache hits
    if [[ "$pre_populate" -eq 1 ]]; then
        echo "Pre-populating keys for read benchmark..." | tee -a "$LOG_FILE"
        $BENCH_BIN -a "$ADDR" -c 50 -p 16 -d 2 -w set --val-size 64 > /dev/null 2>&1
        sleep 0.5
    fi

    echo "Executing: $BENCH_BIN $*" | tee -a "$LOG_FILE"
    $BENCH_BIN "$@" 2>&1 | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    sleep 1
}

# -----------------------------------------------------------------------------
# 1. Baseline CRUD Throughput
# -----------------------------------------------------------------------------

# 1A. 100% Writes (SET)
run_bench "1A: 100% Writes (SET)" \
    -a "$ADDR" -c 50 -p 16 -d 10 -w set --val-size 64

# 1B. 100% Reads (GET) - pre-populates keys after reset
PRE_POPULATE=1 run_bench "1B: 100% Reads (GET)" \
    -a "$ADDR" -c 50 -p 16 -d 10 -w get --val-size 64

# 1C. Mixed (80% GET / 20% SET)
run_bench "1C: Mixed (80% Read / 20% Write)" \
    -a "$ADDR" -c 50 -p 16 -d 10 -w mixed --val-size 64

# -----------------------------------------------------------------------------
# 2. Access Distribution Skew (Zipfian)
# -----------------------------------------------------------------------------

# 2A. Mixed workload with 80/20 Zipfian access skew
run_bench "2A: Mixed Workload (Zipfian 80/20 Skew)" \
    -a "$ADDR" -c 50 -p 16 -d 10 -w mixed --zipf --val-size 64

# -----------------------------------------------------------------------------
# 3. Concurrency & Pipeline Sensitivity
# -----------------------------------------------------------------------------

# 3A. Low Pipelining / Latency Sensitive (pipeline = 1)
run_bench "3A: Pipeline Depth = 1 (Latency Focused)" \
    -a "$ADDR" -c 50 -p 1 -d 10 -w mixed --val-size 64

# 3B. High Pipelining (pipeline = 32)
run_bench "3B: Pipeline Depth = 32 (Throughput Focused)" \
    -a "$ADDR" -c 50 -p 32 -d 10 -w mixed --val-size 64

# -----------------------------------------------------------------------------
# 4. Transactional Guarantees (Atomic Batch vs Rollback)
# -----------------------------------------------------------------------------

# 4A. Atomic Batch Commits (3 operations per batch on single shard prefix)
run_bench "4A: Atomic Batch Commits (Single Shard)" \
    -a "$ADDR" -c 30 -p 8 -d 10 -w batch --val-size 64

# 4B. Atomic Batch Rollbacks (Engineered ADD collisions triggering undo unwinds)
run_bench "4B: Atomic Batch Rollbacks (RAII Stack Unwind)" \
    -a "$ADDR" -c 30 -p 8 -d 10 -w batch-rollback --val-size 64

echo "Benchmark suite complete. Output saved to $LOG_FILE."