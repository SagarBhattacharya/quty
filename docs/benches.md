# Performance & Benchmarking Analysis

This document details the observed latency, throughput, and scalability profiles of `quty-server`, measured using the `quty-bench` load-testing harness.

## 1. System Configuration

All benchmarks were executed locally on the following hardware and software configuration.

| Component        | Details                        |
|------------------|--------------------------------|
| Operating System | Fedora Linux 44                |
| Kernel           | `7.2.8-200.fc44.x86_64`        |
| CPU              | AMD Ryzen 5 5500U              |
| CPU Topology     | 6 cores / 12 threads           |
| Memory           | 8 GB RAM                       |
| Swap             | 8 GB                           |
| Rust             | `1.99.0`                       |
| Cargo            | `1.99.0`                       |
| Memory Allocator | Jemalloc (`tikv-jemallocator`) |

### Server Configuration

| Parameter                 | Value                        |
|---------------------------|------------------------------|
| Network Interface         | Loopback TCP (`127.0.0.1`)   |
| Key-Space Shards          | 64                           |
| Shard Routing             | `ahash` with bitwise masking |
| Memory Ceiling            | 1 GB                         |
| TCP Write Flush Threshold | 128 KB                       |

### Test Methodology

- **Warmup Phase:** A 2-second unmeasured load phase before every run to prime OS TCP windows, warm CPU caches, and preallocate hash table buckets.
- **Database Reset:** Server state was reset using the `RESET` command before each scenario, with prepopulation performed where required.
- **Measurement Resolution:** Microsecond-resolution HDR Histogram tracking across concurrent client connections.
- **Test Duration:** Approximately 10 seconds of continuous measurement per workload profile.

## 2. Benchmark Results Summary

The following table summarizes observed throughput and latency percentiles across the benchmark matrix.

| Scenario | Workload                         | Target Spec | Duration |  Total Ops | Throughput (ops/sec) |    p50 |    p90 |    p99 |    p99.9 |      Max |
|----------|----------------------------------|-------------|---------:|-----------:|---------------------:|-------:|-------:|-------:|---------:|---------:|
| 1A       | 100% Writes (`SET`)              | P=16, C=50  |  10.00 s | 22,522,384 |         2,251,994.08 |  19 µs |  35 µs |  57 µs |    82 µs |   235 µs |
| 1B       | 100% Reads (`GET`)               | P=16, C=50  |  10.00 s | 19,203,600 |         1,920,139.68 |  23 µs |  42 µs |  67 µs |    93 µs |   208 µs |
| 1C       | Mixed (80% Read / 20% Write)     | P=16, C=50  |  10.00 s | 18,672,416 |         1,867,099.17 |  24 µs |  43 µs |  68 µs |    98 µs |   293 µs |
| 2A       | Mixed (Zipfian 80/20 Skew)       | P=16, C=50  |  10.01 s | 17,490,656 |         1,747,944.06 |  25 µs |  46 µs |  75 µs |   111 µs |   299 µs |
| 3A       | Pipeline Depth = 1 (Latency)     | P=1, C=50   |  10.00 s |  1,638,786 |           163,864.11 | 266 µs | 542 µs | 791 µs | 1,136 µs | 15.77 ms |
| 3B       | Pipeline Depth = 32 (Throughput) | P=32, C=50  |  10.00 s | 25,857,504 |         2,585,439.29 |  17 µs |  31 µs |  50 µs |    80 µs |   276 µs |
| 4A       | Atomic Batch Commits             | P=8, C=30   |  10.00 s |  7,943,792 |           794,299.51 |  32 µs |  59 µs | 101 µs |   156 µs |   428 µs |
| 4B       | Atomic Batch Rollbacks           | P=8, C=30   |  10.00 s |  8,203,000 |           820,223.84 |  31 µs |  58 µs |  99 µs |   153 µs |   565 µs |

**Legend:** `P` = pipeline depth; `C` = concurrent clients. Latency values are reported in microseconds except where explicitly indicated.

## 3. Invariant & Architecture Analysis

### 3.1 Lock Sharding and Concurrency

Across read-only and write-only workloads, the 64-shard architecture delivers high throughput under concurrent load.

**100% SET (Scenario 1A)**

- Throughput: 2.25 million operations/sec.
- Median latency: 19 µs.
- p99 latency: 57 µs.

Shard writes operate within independently locked memory domains, allowing concurrent workers to mutate keys in different shards without contending on a single global lock.

**100% GET (Scenario 1B)**

- Throughput: 1.92 million operations/sec.
- Median latency: 23 µs.
- p99 latency: 67 µs.

Read recency updates use relaxed atomic clock stores through `AtomicU32`, allowing reads to share `parking_lot::RwLock` read locks without requiring exclusive access for each recency update.

### 3.2 Pipeline Depth vs. Latency Trade-Offs

Comparing Scenarios 3A, 1C, and 3B illustrates the impact of TCP pipelining and syscall overhead.

**Unpipelined Execution (P=1, Scenario 3A)**

- Throughput: 163.8K operations/sec.
- p50 latency: 266 µs.
- p99 latency: 791 µs.

Each operation requires an individual request-response exchange, increasing the relative cost of network I/O and syscall transitions.

**Moderate Pipelining (P=16, Scenario 1C)**

- Throughput: 1.86 million operations/sec.
- Median latency: 24 µs.
- Throughput improvement: approximately 11.4× over unpipelined execution.

**Deep Pipelining (P=32, Scenario 3B)**

- Throughput: 2.58 million operations/sec.
- Median latency: 17 µs.
- p99 latency: 50 µs.

Amortizing syscall and transport overhead across multiple commands allows the Tokio event loop and socket buffers to process requests more efficiently.

These results demonstrate the throughput benefits of pipelining under this test configuration; they do not independently establish that the server is operating at memory-bandwidth limits.

### 3.3 Transaction & Rollback Invariants

Scenarios 4A and 4B measure single-shard multi-operation atomic batches.

**Batch Commits (Scenario 4A)**

- Batch throughput: 794.2K batch requests/sec.
- Operations per batch: 3 (`SET`, `SET`, `GET`).
- Equivalent throughput: approximately 2.38 million sub-operations/sec.
- p99 latency: 101 µs.

Each batch targets keys sharing a routing prefix and executes within a single shard.

**Batch Rollbacks (Scenario 4B)**

- Rollback throughput: 820.2K batch requests/sec.
- p99 latency: 99 µs.

Deliberately engineered `ADD` collisions trigger short-circuit failures on the final operation, causing the `RollbackGuard` to unwind the undo stack in reverse order.

Stack-allocated inverse logs using `SmallVec<UndoRecord, 16>` are intended to minimize allocation overhead during transaction execution and rollback.

The measured throughput and latency are comparable between the two scenarios. These results alone do not establish that rollback incurs no measurable penalty; a controlled comparison with repeated trials and uncertainty estimates would be needed to support that conclusion.

## 4. Reproducing the Benchmarks

### 4.1 Start the Server

Build the optimized server and launch it with the benchmark configuration:

```bash
cargo build --release -p quty-server

./target/release/quty-server \
    --bind 127.0.0.1:6379 \
    --shards 64 \
    --max-memory 1GB
```

### 4.2 Run the Automated Benchmark Suite

In another terminal, execute the benchmark runner:

```bash
./quty-bench/scripts/run_benches.sh
```

The benchmark harness runs the configured workload scenarios and measures throughput and latency under concurrent load.

## 5. Results Interpretation

The observed results highlight three primary characteristics of `quty-server` under the tested conditions:

1. **High concurrent throughput:** Independent shard locks allow parallel operations without requiring a single global storage lock.
2. **Pipelining efficiency:** Increasing pipeline depth substantially improves throughput by amortizing per-request transport and syscall overhead.
3. **Transaction processing:** Atomic batch commits and rollbacks sustain high request rates while preserving the intended single-shard transaction model.

These measurements are specific to the listed hardware, software, server configuration, and benchmark methodology. They should be treated as local benchmark results rather than universal performance guarantees. Comparisons with other cache engines should use identical workloads, concurrency levels, data distributions, and measurement procedures.
