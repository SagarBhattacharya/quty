# Performance & Benchmarking

This document reports the observed throughput and latency of `quty-server` across write-heavy, read-heavy, mixed, skewed, pipelined, and transactional workloads. All measurements were collected locally using the `quty-bench` harness.

The results are workload-specific measurements, not universal performance guarantees.

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
| Memory Budget             | 1 GB                         |
| TCP Write Flush Threshold | 128 KB                       |

### Test methodology

- **Warmup:** A 2-second unmeasured load phase before each workload.
- **Database reset:** The server's `RESET` command was used between scenarios. Read workloads were prepopulated where required.
- **Measurement:** HDR Histogram latency tracking across concurrent client connections.
- **Measurement duration:** 10 seconds per workload, following the warmup phase.
- **Key-space:** 100,000 keys.
- **Value size:** 64 bytes.
- **Transport:** Local TCP loopback.
- **Trials:** One reported run per scenario.

Latency is measured from pipeline dispatch to the corresponding response being received and decoded. Requests in the same pipeline share a dispatch-start timestamp, so these measurements include client-side socket dispatch, server processing, and response delivery. They should not be interpreted as isolated server service time for each request.

Because each scenario was run once, the results do not quantify run-to-run variance or statistical uncertainty.

## 2. Benchmark Results

### Throughput and latency summary

| Scenario | Workload                     | Pipeline / Clients | Total Requests |        Throughput |    p50 |    p90 |    p99 |    p99.9 |      Max |
|----------|------------------------------|--------------------|---------------:|------------------:|-------:|-------:|-------:|---------:|---------:|
| 1A       | 100% Writes (`SET`)          | P=16, C=50         |     21,199,360 |   2,119,717 req/s |  19 µs |  39 µs |  81 µs |   203 µs |   966 µs |
| 1B       | 100% Reads (`GET`)           | P=16, C=50         |     21,608,128 |   2,160,607 req/s |  20 µs |  37 µs |  59 µs |   122 µs |   519 µs |
| 1C       | Mixed (80% Read / 20% Write) | P=16, C=50         |     21,506,784 |   2,150,267 req/s |  21 µs |  37 µs |  58 µs |    80 µs |   200 µs |
| 2A       | Mixed, Zipfian Skew          | P=16, C=50         |     20,850,624 |   2,084,772 req/s |  21 µs |  38 µs |  60 µs |    80 µs |   233 µs |
| 3A       | Mixed, Unpipelined           | P=1, C=50          |      1,770,001 |     176,980 req/s | 245 µs | 513 µs | 724 µs | 1,009 µs | 17.58 ms |
| 3B       | Mixed, Deep Pipelining       | P=32, C=50         |     30,479,296 |   3,047,592 req/s |  14 µs |  25 µs |  42 µs |    76 µs |   607 µs |
| 4A       | Atomic Batch Commits         | P=8, C=30          |      8,642,528 | 864,156 batches/s |  29 µs |  55 µs |  96 µs |   186 µs | 1,091 µs |
| 4B       | Atomic Batch Rollbacks       | P=8, C=30          |      6,455,736 | 645,512 batches/s |  35 µs |  80 µs | 196 µs |   463 µs | 3,179 µs |

**Legend:** P = pipeline depth; C = concurrent clients. Latencies are in microseconds unless otherwise indicated. Throughput counts requests for ordinary workloads and batch requests for transactional workloads.

## 3. Results Analysis

### 3.1 Read and write workloads

The write-only workload reached approximately **2.12 million requests/s**, with a median latency of 19 µs and p99 latency of 81 µs.

The read-only workload reached approximately **2.16 million requests/s**, with a median latency of 20 µs and p99 latency of 59 µs.

These results demonstrate that Quty sustained more than two million requests per second in both read-heavy and write-heavy workloads under the tested local configuration.

The observed difference between read and write throughput should not be treated as a general performance ranking. It is specific to this run, workload, and machine.

Quty's sharded storage design allows operations on different shards to proceed under independent locks. Read recency tracking also uses relaxed atomic updates, avoiding an exclusive lock solely for the recency update. The benchmark results are consistent with this design supporting concurrent workloads, but they do not isolate the performance contribution of any single implementation choice.

### 3.2 Mixed and skewed workloads

The mixed workload reached approximately **2.15 million requests/s**, with p50 latency of 21 µs and p99 latency of 58 µs.

The Zipfian-skew workload reached approximately **2.08 million requests/s**, with p50 latency of 21 µs and p99 latency of 60 µs.

The skewed workload produced slightly lower throughput than the uniformly distributed mixed workload. A skewed key distribution can increase contention when operations concentrate on a smaller subset of keys. However, one run per scenario is insufficient to establish a statistically meaningful difference or attribute it exclusively to contention.

### 3.3 Pipeline depth and latency

The unpipelined mixed workload reached approximately **176,980 requests/s**. At pipeline depth 32, throughput increased to approximately **3.05 million requests/s**.

| Pipeline depth |      Throughput |    p50 |    p99 |
|---------------:|----------------:|-------:|-------:|
|              1 |   176,980 req/s | 245 µs | 724 µs |
|             16 | 2,150,267 req/s |  21 µs |  58 µs |
|             32 | 3,047,592 req/s |  14 µs |  42 µs |

Pipeline depth 32 achieved approximately **17.2× the throughput** of the unpipelined workload in these runs.

Pipelining amortizes request and response transport overhead across multiple commands and reduces the frequency of request-response synchronization. It substantially improves throughput under this test configuration.

The latency measurements also change with pipeline depth. Because requests within a pipeline share a dispatch-start timestamp, the reported latency reflects the dispatch-to-response interval for the pipeline's requests. It is not a measurement of isolated server execution time.

These measurements do not independently establish that Quty is memory-bandwidth-bound or operating at its maximum possible throughput.

### 3.4 Atomic batch transactions

The commit workload reached approximately **864,156 batch requests/s**, with p50 latency of 29 µs and p99 latency of 96 µs.

The rollback workload reached approximately **645,512 batch requests/s**, with p50 latency of 35 µs and p99 latency of 196 µs.

| Scenario               |        Throughput |   p50 |    p99 |
|------------------------|------------------:|------:|-------:|
| Atomic batch commits   | 864,156 batches/s | 29 µs |  96 µs |
| Atomic batch rollbacks | 645,512 batches/s | 35 µs | 196 µs |

The rollback workload was slower and showed higher tail latency in this run. This is consistent with the additional work involved in undoing mutations after a failed transaction, but repeated controlled trials are required to quantify the overhead reliably.

Quty uses an undo log and a rollback guard to restore state when a batch operation fails. The implementation uses `SmallVec<UndoRecord, 16>` to keep small undo logs inline when possible.

These results cover the tested batch workloads and should not be interpreted as evidence of cross-shard transaction support.

Batch throughput counts completed batch requests, not the number of individual operations inside each batch. Do not compare it directly with ordinary request throughput without accounting for the number and type of operations per batch.

## 4. Reproducing the Benchmarks

### 4.1 Build the release binaries

From the workspace root:

```bash
cargo build --release --workspace
```

### 4.2 Start the server

Start Quty with the same configuration used for the measurements:

```bash
./target/release/quty-server \
    --bind 127.0.0.1:6379 \
    --shards 64 \
    --max-memory 1GB
```

Verify that the command-line options match the current server configuration before running the benchmark suite.

### 4.3 Run the benchmark suite

In a second terminal, run:

```bash
./quty-bench/scripts/run_benches.sh
```

The script runs the configured workload matrix, resets the database between scenarios, performs warmup, and records throughput and latency statistics.

### 4.4 Benchmarking recommendations

For more reliable comparisons:

- Run each workload multiple times and report the median throughput across runs.
- Keep hardware, build profile, shard count, memory budget, payload sizes, concurrency, and pipeline depth consistent.
- Verify that database reset and workload prepopulation finish before measurement begins.
- Record failed or incomplete runs rather than reporting partial results as successful benchmarks.
- Avoid running unrelated CPU- or disk-intensive tasks during measurement.
- Use a separate benchmark environment when comparing Quty with other servers.
- Compare equivalent workloads and operation semantics when evaluating other cache engines.

## 5. Limitations

1. **Local transport:** Loopback TCP does not represent remote network latency, packet loss, or network congestion.
2. **Single run per scenario:** The reported results do not show variance or confidence intervals.
3. **Short duration:** Ten seconds of measurement may not reveal long-running memory pressure, periodic latency spikes, or sustained performance changes.
4. **Workload dependence:** Key distribution, value size, concurrency, and pipeline depth influence throughput and latency.
5. **Memory accounting:** The configured memory budget tracks key and value payload bytes rather than total process memory, including allocator and data-structure overhead.
6. **No direct competitor comparison:** These measurements do not establish that Quty is faster than another cache engine without equivalent configurations and test procedures.

## 6. Summary

In the reported local runs, Quty achieved:

- **3.05 million requests/s** for a mixed workload at pipeline depth 32.
- **2.16 million requests/s** for a read-only workload.
- **2.12 million requests/s** for a write-only workload.
- **2.15 million requests/s** for a mixed workload at pipeline depth 16.
- **864K batch requests/s** for atomic batch commits.

The strongest observed throughput result came from deep pipelining. The results also show the effect of workload shape and transaction behavior on throughput and tail latency.

These measurements are intended to document Quty's observed behavior on a specific machine and configuration. They are not universal performance guarantees. Repeated trials and controlled comparisons are needed for stronger performance conclusions.
