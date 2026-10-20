# Performance & Benchmarking Analysis

This document summarizes the observed throughput and latency of `quty-server` across read, write, mixed, skewed, pipelined, and transactional workloads. Results are specific to the hardware, software, configuration, and test methodology described below.

## 1. System Configuration

### Hardware and software

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

### Server configuration

| Parameter                 | Value                        |
|---------------------------|------------------------------|
| Network Interface         | Loopback TCP (`127.0.0.1`)   |
| Key-Space Shards          | 64                           |
| Shard Routing             | `ahash` with bitwise masking |
| Memory Ceiling            | 1 GB                         |
| TCP Write Flush Threshold | 128 KB                       |

### Test methodology

- **Warmup:** A 2-second unmeasured load phase before each run.
- **Database reset:** The server's `RESET` command was used between scenarios, with prepopulation performed where required.
- **Measurement:** Microsecond-resolution HDR Histogram measurements across concurrent client connections.
- **Duration:** Approximately 10 seconds of measurement per workload.
- **Environment:** Client and server ran locally over loopback TCP.

These results describe a local benchmark environment. Loopback measurements exclude the network latency and variability associated with remote clients.

## 2. Benchmark Results

The following table summarizes the measured results across the benchmark matrix.

| Scenario | Workload                     | Pipeline / Clients | Duration | Total Requests | Throughput (requests/s) |    p50 |    p90 |    p99 |    p99.9 |      Max |
|----------|------------------------------|--------------------|---------:|---------------:|------------------------:|-------:|-------:|-------:|---------:|---------:|
| 1A       | 100% Writes (`SET`)          | P=16, C=50         |  10.00 s |     23,446,752 |               2,344,423 |  18 µs |  35 µs |  57 µs |    92 µs |   854 µs |
| 1B       | 100% Reads (`GET`)           | P=16, C=50         |  10.00 s |     20,899,472 |               2,089,741 |  21 µs |  39 µs |  64 µs |   106 µs |   798 µs |
| 1C       | Mixed (80% Read / 20% Write) | P=16, C=50         |  10.00 s |     20,339,488 |               2,033,806 |  21 µs |  40 µs |  65 µs |   100 µs |   259 µs |
| 2A       | Mixed (Zipfian 80/20 Skew)   | P=16, C=50         |  10.00 s |     19,136,608 |               1,913,596 |  22 µs |  42 µs |  71 µs |   112 µs |   844 µs |
| 3A       | Unpipelined (P=1)            | P=1, C=50          |  10.00 s |      1,701,570 |                 170,139 | 250 µs | 518 µs | 791 µs | 1,887 µs | 13.42 ms |
| 3B       | Deep Pipelining (P=32)       | P=32, C=50         |  10.00 s |     28,074,464 |               2,807,208 |  15 µs |  28 µs |  51 µs |    90 µs |   822 µs |
| 4A       | Atomic Batch Commits         | P=8, C=30          |  10.00 s |      8,441,832 |                 844,106 |  30 µs |  57 µs |  98 µs |   162 µs |   658 µs |
| 4B       | Atomic Batch Rollbacks       | P=8, C=30          |  10.00 s |      9,152,648 |                 915,175 |  28 µs |  52 µs |  88 µs |   139 µs |   472 µs |

**Legend:** P = pipeline depth; C = concurrent clients. Latency values are in microseconds unless otherwise indicated. For batch scenarios, throughput counts batch requests, not individual operations inside each batch.

## 3. Performance Analysis

### 3.1 Read and write throughput

**Write-heavy workload**

The `SET` workload reached approximately **2.34 million requests/s**, with p50 latency of 18 µs and p99 latency of 57 µs.

**Read-heavy workload**

The `GET` workload reached approximately **2.09 million requests/s**, with p50 latency of 21 µs and p99 latency of 64 µs.

These results are consistent with an architecture designed to reduce contention through independently locked storage shards. They do not, by themselves, isolate the contribution of sharding from the effects of pipelining, client concurrency, allocator behavior, or other implementation details.

Read recency tracking uses relaxed atomic updates through `AtomicU32`, allowing the relevant recency update to avoid requiring an exclusive lock solely for that update. This design choice may help reduce read-side contention.

### 3.2 Mixed and skewed workloads

The mixed workload reached approximately **2.03 million requests/s**, with p99 latency of 65 µs.

The Zipfian-skew workload reached approximately **1.91 million requests/s**, with p99 latency of 71 µs.

The lower throughput and higher tail latency under skew are consistent with increased contention when a disproportionate share of operations targets a smaller subset of keys. The results alone do not quantify how much of the difference is caused by shard contention versus other workload effects.

### 3.3 Pipeline depth and throughput

The unpipelined workload reached approximately **170,139 requests/s**, compared with approximately **2.81 million requests/s** at pipeline depth 32.

| Pipeline depth | Throughput | p50 latency | p99 latency |
|---:|---:|---:|---:|
| 1 | 170,139 requests/s | 250 µs | 791 µs |
| 16 | 2,033,806 requests/s | 21 µs | 65 µs |
| 32 | 2,807,208 requests/s | 15 µs | 51 µs |

The measured throughput at pipeline depth 32 is approximately **16.5×** the unpipelined result. This demonstrates a substantial throughput benefit from pipelining in this test configuration.

Pipelining amortizes per-request transport and syscall overhead across multiple requests. However, pipeline depth also changes the timing and batching characteristics of the workload, so the latency figures should not be interpreted as a direct comparison of isolated single-request response times under otherwise identical conditions.

These results do not independently establish that the server is memory-bandwidth-bound or has reached its maximum throughput.

### 3.4 Atomic batch commits and rollbacks

The batch workloads use three operations per batch for the commit scenario (`SET`, `SET`, `GET`) and deliberately trigger a failure on the final operation in the rollback scenario.

| Scenario        |  Batch throughput | p50 latency | p99 latency |
|-----------------|------------------:|------------:|------------:|
| Batch commits   | 844,106 batches/s |       30 µs |       98 µs |
| Batch rollbacks | 915,175 batches/s |       28 µs |       88 µs |

With three operations per committed batch, the commit result corresponds to approximately **2.53 million sub-operations/s**, assuming all three operations are counted as one sub-operation each.

The rollback path uses a `RollbackGuard` and an undo stack backed by `SmallVec<UndoRecord, 16>`, intended to reduce allocation overhead for small transactions.

The observed rollback throughput is higher than commit throughput in this run. That does **not** establish that rollback is intrinsically faster or incurs no performance penalty: the workloads perform different amounts of work. Repeated trials and comparable transaction paths would be required to draw that conclusion.

The results apply to the tested single-shard batch model and should not be interpreted as evidence of cross-shard transaction support.

## 4. Reproducing the Benchmarks

### 4.1 Build and start the server

Build the optimized server:

```bash
cargo build --release -p quty-server
```

Start it with the benchmark configuration:

```bash
./target/release/quty-server \
    --bind 127.0.0.1:6379 \
    --shards 64 \
    --max-memory 1GB
```

Confirm that the server's supported command-line arguments match the flags above before running the suite.

### 4.2 Run the benchmark suite

In another terminal, execute:

```bash
./quty-bench/scripts/run_benches.sh
```

The runner executes the configured workload scenarios and reports throughput and latency measurements.

For meaningful comparisons:

- Use the same server build, machine, and configuration.
- Keep concurrency, pipeline depth, key distribution, and payload sizes consistent.
- Ensure the database reset and any prepopulation finish before measurement begins.
- Run each scenario multiple times and compare medians across runs.
- Record failures and incomplete runs rather than treating partial results as successful benchmarks.
- Verify that latency percentiles represent per-request measurements rather than an aggregate pipeline duration divided by pipeline depth.

## 5. Limitations

These results should be interpreted with the following limitations in mind:

1. **Local-only testing:** Loopback TCP does not represent remote network latency or network congestion.
2. **Hardware specificity:** Results depend on the CPU, memory, operating system, runtime, allocator, and server configuration.
3. **Workload dependence:** Key distribution, operation mix, pipeline depth, and concurrency materially affect results.
4. **Short measurement windows:** Approximately 10 seconds per workload may not reveal long-running behavior, sustained memory pressure, or periodic latency spikes.
5. **Statistical uncertainty:** A single reported result does not establish run-to-run variance or reproducibility.
6. **No universal comparison:** These measurements cannot establish superiority over other cache engines without equivalent workloads, configurations, and measurement procedures.

## 6. Summary

The benchmark suite demonstrates high local request throughput for `quty-server` under concurrent, pipelined workloads. The highest measured throughput was approximately **2.81 million requests/s** at pipeline depth 32. The read, write, and mixed workloads also exceeded two million requests/s in the reported runs.

Pipelining produced the largest observed throughput difference, while skewed access and batch execution provided additional information about the system's behavior under different workload patterns.

These figures are local benchmark observations, not universal performance guarantees. Repeated trials, verified per-request latency measurements, and controlled comparisons are necessary for stronger performance claims.
