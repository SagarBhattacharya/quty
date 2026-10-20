# quty-server

A high-performance, multithreaded in-memory cache server featuring lock-isolated key-space sharding, sampled LRU eviction, and single-shard atomic batch transactions with RAII rollback safety.

## Architecture Highlights

- **Key-Space Partitioning:** Automatic power-of-two sharding using `ahash`, with per-shard `parking_lot::RwLock` instances to minimize cross-thread contention.
- **Deterministic Memory Model:** Explicit accounting for key and value payload bytes, with inline headroom eviction and background watermark enforcement.
- **O(1) Dense Key Sampling:** Memory-efficient candidate selection via an indexed `Vec<Box<str>>`, maintaining O(1) amortized `swap_remove` operations.
- **Unified Mutation Engine:** A single execution pipeline for both standalone commands and batch transactions.
- **RAII Transaction Rollback:** Atomic multi-operation batches on shared key prefixes, with rollback on precondition failures or worker-thread panics.
- **Hybrid Expiration:** Passive lazy evaluation on reads, coupled with a non-blocking active background sweep task.
- **Supervised Network Daemon:** Non-blocking Tokio TCP listener supporting graceful shutdown on both `SIGINT` and `SIGTERM`.

## Installation & Build

### Prerequisites

- Rust 1.85+ (Edition 2024).
- LLVM / Clang for `tikv-jemallocator` on Linux and macOS.

### Building from Source

```bash
cargo build --release -p quty-server
```

The resulting optimized binary is available at:

```text
target/release/quty-server
```

## CLI Configuration & Flags

```text
Usage: quty-server [OPTIONS]

Options:
  -b, --bind <BIND_ADDR>
          Bind socket address [default: 0.0.0.0:6379]

  -s, --shards <SHARDS>
          Number of concurrent key-space shards (must be a power of two)
          [default: 64]

  -m, --max-memory <MAX_MEMORY>
          Maximum aggregate memory limit (e.g., "512MB", "2GB", "1048576")
          [default: 512MB]

      --high-watermark <HIGH_WATERMARK>
          High watermark ratio to trigger maintenance eviction (0.01 - 1.0)
          [default: 0.80]

      --low-watermark <LOW_WATERMARK>
          Low watermark ratio to stop eviction sweep (0.01 - 1.0)
          [default: 0.70]

      --sweep-interval-ms <SWEEP_INTERVAL_MS>
          Background sweep interval in milliseconds [default: 100]

      --initial-shard-capacity <INITIAL_SHARD_CAPACITY>
          Initial bucket capacity pre-allocated per shard [default: 8192]

      --eviction-burst-limit <EVICTION_BURST_LIMIT>
          Maximum keys evicted per burst before yielding the shard write lock
          [default: 128]

      --flush-threshold-bytes <FLUSH_THRESHOLD_BYTES>
          Network write buffer flush watermark in bytes [default: 131072]

      --shutdown-timeout-secs <SHUTDOWN_TIMEOUT_SECS>
          Graceful shutdown timeout in seconds [default: 3]

  -h, --help
          Print help

  -V, --version
          Print version
```

## Running the Daemon

### Standard Startup

Launch the server with 64 shards and a 1 GB memory ceiling:

```bash
./target/release/quty-server \
    --bind 127.0.0.1:6379 \
    --shards 64 \
    --max-memory 1GB
```

### Production Setup with Structured Logging

Configure structured logging through `RUST_LOG` and start the server with 128 shards and a 4 GB memory limit:

```bash
RUST_LOG=info,quty_server=debug ./target/release/quty-server \
    --bind 0.0.0.0:6379 \
    --shards 128 \
    --max-memory 4GB \
    --high-watermark 0.85 \
    --low-watermark 0.70
```

## Wire Protocol Semantics

`quty-server` communicates over a binary protocol backed by a fixed 20-byte frame header.

```text
+-------------------+-------------------+-------------------+-------------------+
|  Magic (2B: 0x5154)| Version (1B: 0x01)| Flags (1B: Type)  | Status Code (2B)  |
+-------------------+-------------------+-------------------+-------------------+
|   Reserved (2B)   |                 Request ID (8B, Big-Endian)               |
+-------------------+-------------------+-------------------+-------------------+
|                            Payload Length (4B, Big-Endian)                    |
+-------------------------------------------------------------------------------+
|                            Payload Data (Variable)                            |
+-------------------------------------------------------------------------------+
```

## Supported Command Verbs

| Command   | Arguments           | Semantics                                                                             |
|-----------|---------------------|---------------------------------------------------------------------------------------|
| `PING`    | None                | Liveness check. Returns `StatusCode::Ok`.                                             |
| `GET`     | `<key>`             | Fetches cached binary payload and updates the LRU recency clock.                      |
| `SET`     | `<key> <val>`       | Unconditional storage upsert.                                                         |
| `ADD`     | `<key> <val>`       | Inserts payload only if the key is missing or expired; otherwise returns `KeyExists`. |
| `REPLACE` | `<key> <val>`       | Updates payload only if the key is active; otherwise returns `KeyNotFound`.           |
| `DEL`     | `<key>`             | Removes the key and immediately reclaims accounted payload memory.                    |
| `EXISTS`  | `<key>`             | Checks key presence without altering the LRU access clock.                            |
| `TTL`     | `<key>`             | Queries remaining lifespan in milliseconds (`-1` if persistent).                      |
| `EXPIRE`  | `<key> <ms>`        | Attaches or updates the expiration deadline.                                          |
| `PERSIST` | `<key>`             | Removes the expiration deadline, keeping the key persistent.                          |
| `BATCH`   | `(cmd1, cmd2, ...)` | Executes an atomic sequence targeting keys with identical prefixes.                   |
| `STATS`   | None                | Retrieves cache hit/miss, memory, connection, and eviction telemetry counters.        |
| `RESET`   | None                | Wipes all partition shards and resets accounting counters.                            |
