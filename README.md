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
- **Deterministic Memory Accounting:** Tracks key and value payload bytes, with self-eviction protection to prevent overwrites from evicting their own entries.
- **Custom Binary Protocol:** Compact 20-byte frame header supporting pipelined, stream-oriented I/O.
- **Jemalloc Integration:** Uses `tikv-jemallocator` as the memory allocator to help manage heap fragmentation under sustained write workloads.

## Performance Overview

Benchmarked over loopback TCP using `quty-bench`, with 64 shards and 50 concurrent connections. Results are from the documented benchmark environment and workload configurations.

| Workload                         | Pipeline |                                      Throughput | p50 Latency | p99 Latency | Max Latency |
|----------------------------------|---------:|------------------------------------------------:|------------:|------------:|------------:|
| **100% Writes (`SET`)**          |       16 |                               2,251,994 ops/sec |       19 µs |       57 µs |      235 µs |
| **100% Reads (`GET`)**           |       16 |                               1,920,139 ops/sec |       23 µs |       67 µs |      208 µs |
| **Mixed (80% Read / 20% Write)** |       16 |                               1,867,099 ops/sec |       24 µs |       68 µs |      293 µs |
| **Mixed (Zipfian Skew)**         |       16 |                               1,747,944 ops/sec |       25 µs |       75 µs |      299 µs |
| **Pipelined Throughput**         |       32 |                               2,585,439 ops/sec |       17 µs |       50 µs |      276 µs |
| **Atomic Batch Commits**         |        8 | 794,299 batches/sec (~2.38M sub-operations/sec) |       32 µs |      101 µs |      428 µs |
| **Batch Abort Rollbacks**        |        8 |                             820,223 batches/sec |       31 µs |       99 µs |      565 µs |

Detailed benchmark methodology and reproduction steps are documented in [`docs/benches.md`](docs/benches.md).

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

| Command   | Arguments           | Behavior                                                                  |
|-----------|---------------------|---------------------------------------------------------------------------|
| `PING`    | None                | Checks TCP connection liveness.                                           |
| `SET`     | `<key> <val>`       | Unconditional key-value upsert.                                           |
| `GET`     | `<key>`             | Retrieves a value and updates the LRU recency clock.                      |
| `ADD`     | `<key> <val>`       | Stores a value only if the key does not exist or has expired.             |
| `REPLACE` | `<key> <val>`       | Updates a value only if the key currently exists.                         |
| `DEL`     | `<key>`             | Deletes a key and reclaims accounted payload memory.                      |
| `EXISTS`  | `<key>`             | Checks key presence without altering LRU order.                           |
| `TTL`     | `<key>`             | Returns the remaining lifespan in milliseconds (`-1` if persistent).      |
| `EXPIRE`  | `<key> <ms>`        | Sets an expiration deadline in milliseconds.                              |
| `PERSIST` | `<key>`             | Clears the expiration deadline, keeping the key persistent.               |
| `BATCH`   | `(cmd1, cmd2, ...)` | Executes an atomic transaction on a single shard, with rollback on error. |
| `STATS`   | None                | Retrieves a telemetry snapshot.                                           |
| `RESET`   | None                | Flushes all partition shards and clears metrics.                          |

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