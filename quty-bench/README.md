# quty-bench

A high-concurrency load-testing and latency-profiling harness for the `quty` in-memory cache server.

---

## Overview

`quty-bench` drives synthetic workloads against a running `quty-server` instance over TCP. It measures sustained operational throughput and records microsecond-level latency distributions using an HDR Histogram.

### Features

- **Workload Generators:** Evaluate `Set` (writes), `Get` (reads), `Mixed` (80/20 read-heavy), `Ttl`, `Batch` (atomic multi-key commits), and `BatchRollback` (aborted transactions unwinding RAII undo stacks).
- **Access Distributions:** Supports uniform random key access and 80/20 power-law skew using a Zipfian distribution generator.
- **Warmup Phase Isolation:** Configurable pre-run warmup period (`--warmup-secs`) to prime OS socket buffers and allocate hash table buckets before capturing measured metrics.
- **Coordinated Worker Start:** Synchronized worker release to simulate immediate, high-concurrency client load.
- **ASCII Reporting:** Renders formatted summary tables covering total operations, throughput (ops/sec), and latency percentiles (p50, p90, p99, p99.9, and maximum).

---

## Installation & Build

Build the benchmark binary in release mode:

```bash
cargo build --release -p quty-bench
```

The resulting binary will be available at:

```text
target/release/quty-bench
```

## CLI Options

```text
Usage: quty-bench [OPTIONS]

Options:
  -a, --addr <ADDR>              Target server address [default: 127.0.0.1:6379]
  -c, --concurrency <COUNT>      Concurrent TCP client connections [default: 50]
  -p, --pipeline <DEPTH>         Commands dispatched per TCP batch frame [default: 16]
  -d, --duration <SECS>          Measured phase duration in seconds [default: 10]
      --warmup-secs <SECS>       Unmeasured pre-warm duration [default: 2]
  -w, --workload <WORKLOAD>      Workload type [default: mixed]
                                 [values: set, get, mixed, ttl, batch, batch-rollback]
  -k, --keys <COUNT>             Key-space cardinality [default: 100000]
      --val-size <BYTES>         Value payload size in bytes [default: 64]
      --zipf                     Enable 80/20 Zipfian key access skew
  -h, --help                     Print help
  -V, --version                  Print version
```

## Usage Examples

### 1. Standard Mixed Workload

Run a mixed read/write workload with 50 concurrent clients and a pipeline depth of 16.

```bash
./target/release/quty-bench \
    -a 127.0.0.1:6379 \
    -c 50 \
    -p 16 \
    -d 10 \
    -w mixed \
    --val-size 64
```

### 2. Zipfian Skew on Hot Keys

Simulate workloads in which a small subset of keys receives a disproportionate share of requests.

```bash
./target/release/quty-bench \
    -a 127.0.0.1:6379 \
    -c 50 \
    -p 16 \
    -d 10 \
    -w mixed \
    --zipf
```

### 3. Atomic Multi-Operation Batch Performance

Measure throughput and latency for atomic batch transactions.

```bash
./target/release/quty-bench \
    -a 127.0.0.1:6379 \
    -c 30 \
    -p 8 \
    -d 10 \
    -w batch
```

### 4. Transaction Abort & Rollback Overhead

Measure transaction rollback performance when batch operations fail.

```bash
./target/release/quty-bench \
    -a 127.0.0.1:6379 \
    -c 30 \
    -p 8 \
    -d 10 \
    -w batch-rollback
```

## Output Metrics

`quty-bench` records latency distributions in an HDR Histogram configured to cover \(1\,\mu s\) to \(60\,s\) at three significant figures of precision.

The reported metrics include:

| Metric           | Description                                                                |
|------------------|----------------------------------------------------------------------------|
| Benchmark Target | Selected workload, pipeline depth, concurrency, and Zipfian configuration. |
| Total Operations | Number of operations completed during the measured phase.                  |
| Duration         | Actual measurement duration.                                               |
| Throughput       | Completed operations per second.                                           |
| p50 Latency      | Median operation latency.                                                  |
| p90 Latency      | Latency below which 90% of recorded operations fall.                       |
| p99 Latency      | Latency below which 99% of recorded operations fall.                       |
| p99.9 Latency    | Latency below which 99.9% of recorded operations fall.                     |
| Max Latency      | Highest recorded operation latency.                                        |

### Sample Output

The following example illustrates a mixed workload with a pipeline depth of 16 and 50 concurrent clients.

```text
╭──────────────────────┬────────────────────────────────╮
│ Metric               ┆ Value                          │
╞══════════════════════╪════════════════════════════════╡
│ Benchmark Target     ┆ Mixed (P=16, C=50, Zipf=false) │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Total Operations     ┆ 18672416                       │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Duration             ┆ 10.00 s                        │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Throughput           ┆ 1867099.17 ops/sec             │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ p50 (Median) Latency ┆ 24 µs                          │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ p90 Latency          ┆ 43 µs                          │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ p99 Latency          ┆ 68 µs                          │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ p99.9 Latency        ┆ 98 µs                          │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Max Latency          ┆ 293 µs                         │
╰──────────────────────┴────────────────────────────────╯
```

### Interpreting Results

- **Throughput** indicates the rate at which the server processes operations under the selected workload.
- **Latency percentiles** reveal typical response times and tail-latency behavior under concurrent load.
- **Pipeline depth** influences how much transport and syscall overhead can be amortized across requests.
- **Zipfian access skew** helps evaluate performance when a subset of keys becomes disproportionately popular.
- **Batch workloads** measure the cost of atomic transaction execution, including rollback when operations fail.

For meaningful comparisons, keep the hardware, server configuration, key-space cardinality, value sizes, concurrency, pipeline depth, warmup duration, and measurement duration consistent.

