# Quty

[![CI](https://github.com/SagarBhattacharya/quty/actions/workflows/ci.yml/badge.svg)](https://github.com/SagarBhattacharya/quty/actions/workflows/ci.yml)
[![Docs](https://github.com/SagarBhattacharya/quty/actions/workflows/docs.yml/badge.svg)](https://sagarbhattacharya.github.io/quty/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-performance, sharded in-memory cache server written in Rust, featuring deterministic memory accounting, sampled LRU eviction, and RAII-guaranteed atomic batch transactions.

---

## Key Features

- **Sharded Concurrency:** Power-of-two key-space partitioning with independent `parking_lot::RwLock` shards to reduce cross-core lock contention.
- **Concurrent Reads:** `GET` operations use shared read locks, with recency clocks updated through relaxed atomic stores.
- **Atomic Batch Transactions:** Single-shard atomic sequences (`BATCH`) backed by an inverse undo stack (`RollbackGuard`). Precondition failures and capacity limits trigger automatic LIFO rollback.
- **Sampled LRU Eviction:** Uniform candidate sampling through a dense key index, maintaining O(1) amortized `swap_remove` operations without full-map iteration.
- **Payload-Byte Budgeting:** Tracks key and value bytes. Data-structure and
	allocator overhead are excluded, so the configured budget is not a hard
	process-memory or RSS ceiling.
- **Custom Binary Protocol:** Compact 20-byte frame header supporting pipelined, stream-oriented I/O.
- **Jemalloc Integration:** Uses `tikv-jemallocator` as the memory allocator to help manage heap fragmentation under sustained write workloads.

## Benchmarks

Quty includes a benchmark harness for measuring throughput and latency across read, write, mixed, skewed, pipelined, and transactional workloads.

### Observed Results

Measured locally on Fedora Linux 44 using an AMD Ryzen 5 5500U (6 cores / 12 threads), Rust 1.99.0, 64 shards, and loopback TCP. Each workload used a 10-second measurement period following a 2-second warmup.

| Workload                       | Pipeline Depth | Clients |     Throughput |    p50 |    p99 |
|--------------------------------|---------------:|--------:|---------------:|-------:|-------:|
| SET (100% writes)              |             16 |      50 |    2.12M req/s |  19 µs |  81 µs |
| GET (100% reads)               |             16 |      50 |    2.16M req/s |  20 µs |  59 µs |
| Mixed (80% reads / 20% writes) |             16 |      50 |    2.15M req/s |  21 µs |  58 µs |
| Zipfian-skewed mixed workload  |             16 |      50 |    2.08M req/s |  21 µs |  60 µs |
| Unpipelined mixed workload     |              1 |      50 |     177K req/s | 245 µs | 724 µs |
| Deep-pipelined mixed workload  |             32 |      50 |    3.05M req/s |  14 µs |  42 µs |
| Atomic batch commits           |              8 |      30 | 864K batches/s |  29 µs |  96 µs |
| Atomic batch rollbacks         |              8 |      30 | 646K batches/s |  35 µs | 196 µs |

The highest measured throughput was approximately **3.05 million requests/s** with pipeline depth 32. In these runs, that was approximately **17.2× the throughput** of the unpipelined mixed workload.

Latency is measured from pipeline dispatch to response receipt and decoding. Requests in the same pipeline share a start timestamp, so these figures include client dispatch, server processing, and response delivery rather than representing isolated server service time.

These are single-run, local benchmark results, not universal performance guarantees. Results depend on hardware, workload, concurrency, and configuration. Batch throughput counts batch requests rather than individual operations inside each batch.

See [`docs/benches.md`](docs/benches.md) for the full methodology, results, limitations, and reproduction instructions.

---

## Architecture & Workspace Layout

Quty separates protocol framing, client access, storage execution, and performance testing into independent workspace crates.

```text
.
├── quty-proto/     # Wire framing, 20-byte codec, and command AST
├── quty-client/    # Async client driver, CLI tool, and interactive REPL
├── quty-server/    # Sharded cache engine, TCP server, and transaction runtime
├── quty-bench/     # Load-testing harness and latency-profiling tool
└── docs/           # mdBook documentation published to GitHub Pages
```

### Architectural Principles

- **Partitioned Synchronization:** Independent shards reduce contention by isolating key-space operations.
- **Bounded Memory Management:** Explicit payload accounting and eviction policies enforce configured memory limits.
- **Transactional Safety:** Batch operations use inverse mutations to restore prior state after failures.
- **Supervised Networking:** Tokio-based TCP handling supports graceful shutdown and connection lifecycle management.

---

## Quickstart

### Build from Source

Prerequisites: Rust and Cargo compatible with the workspace's configured toolchain.

```bash
cargo build --release
```

Binaries are generated under `target/release/`:

| Binary        | Purpose                                  |
|---------------|------------------------------------------|
| `quty-server` | Cache daemon                             |
| `quty-cli`    | Command-line client and interactive REPL |
| `quty-bench`  | Load generator and latency profiler      |

### Run the Server

Launch the server with 64 shards and a 1 GB memory limit:

```bash
./target/release/quty-server \
    --bind 127.0.0.1:6379 \
    --shards 64 \
    --max-memory 1GB
```

### Start the Interactive CLI

In another terminal:

```bash
./target/release/quty-cli -H 127.0.0.1 -p 6379
```

Example session:

```text
Connected to quty server at 127.0.0.1:6379
127.0.0.1:6379> SET user:1:name "Alice"
OK (102 µs)

127.0.0.1:6379> GET user:1:name
"Alice" (68 µs)

127.0.0.1:6379> BATCH (SET user:1:role "admin", GET user:1:role)
2 items (115 µs)
  1) OK
  2) "admin"

127.0.0.1:6379> STATS
── Server Telemetry ───────────────────────
uptime_seconds:        42
total_connections:     5
active_connections:    1
total_keys:            2
used_memory_bytes:     36 (0.00 MB)
get_hits:              1
get_misses:            0
evictions:             0
────────────────────────────────────────────
(79 µs)
```

---

## Command Reference

| Command   | Arguments           | Behavior                                                                                                                |
|-----------|---------------------|-------------------------------------------------------------------------------------------------------------------------|
| `PING`    | None                | Checks TCP connection liveness.                                                                                         |
| `SET`     | `<key> <val>`       | Unconditional key-value upsert.                                                                                         |
| `GET`     | `<key>`             | Retrieves a value and updates the LRU recency clock.                                                                    |
| `ADD`     | `<key> <val>`       | Stores a value only if the key does not exist or has expired.                                                           |
| `REPLACE` | `<key> <val>`       | Updates a value only if the key currently exists.                                                                       |
| `DEL`     | `<key>`             | Deletes a key and reclaims accounted payload memory.                                                                    |
| `EXISTS`  | `<key>`             | Checks key presence without altering LRU order.                                                                         |
| `TTL`     | `<key>`             | Returns the remaining lifespan in milliseconds (`-1` if persistent).                                                    |
| `EXPIRE`  | `<key> <ms>`        | Sets an expiration deadline in milliseconds.                                                                            |
| `PERSIST` | `<key>`             | Clears the expiration deadline, keeping the key persistent.                                                             |
| `BATCH`   | `(cmd1, cmd2, ...)` | Executes an atomic transaction on a single shard, with rollback on error.                                               |
| `STATS`   | None                | Retrieves a telemetry snapshot.                                                                                         |
| `RESET`   | None                | Atomically clears all shards and resets hit, miss, and eviction counters. Uptime and connection counters are preserved. |

---

## Container Deployment

Build the container image using Docker or Podman:

```bash
podman build -t quty-server:latest .
```

Run the containerized server:

```bash
podman run -d \
    --name quty \
    -p 6379:6379 \
    quty-server:latest
```

The server listens on the configured container address and port. Ensure the container's bind address permits external connections through the published port.

---

## Testing & Verification

Run the following checks from the workspace root.

### Formatting

```bash
cargo fmt --all -- --check
```

### Strict Linting

```bash
cargo clippy --workspace --all-targets -- -D warnings -W clippy::pedantic
```

### Unit and Integration Tests

```bash
cargo test --workspace
```

### Benchmarks

Build the benchmark harness:

```bash
cargo build --release -p quty-bench
```

See [`docs/benches.md`](docs/benches.md) for benchmark workloads, configuration, and reproduction instructions.

---

## License

Quty is licensed under the [MIT License](LICENSE).