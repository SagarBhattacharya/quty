# quty-client

An asynchronous client driver, command-line tool, and interactive REPL for the Quty in-memory cache server.

## Features

- **Tokio Async Driver:** Monotonic request sequence ID tracking, TCP connection state poisoning prevention, and zero-allocation frame encoding.
- **Bounded Memory Model:** Automatic buffer capacity compaction to prevent uncollected memory bloat after processing large payloads.
- **Interactive Terminal REPL:** Persistent command history via `~/.quty_history`, colorized output following system conventions, and execution latency reporting.
- **Atomic Batch Transactions:** Client-side parsing and prefix-based cross-shard verification for `BATCH (cmd1, cmd2, ...)`.
- **Zero-Roundtrip Input Guards:** Local pre-validation ensures keys stay within 64 KB and values within 32 MB before network dispatch.

## Installation & CLI Setup

### Build and Install

```bash
cargo install --path quty-client
```

This installs the `quty-cli` binary to your Cargo binary directory.

### Command-Line Arguments

```text
Usage: quty-cli [OPTIONS] [RAW_COMMAND]...

Arguments:
  [RAW_COMMAND]...  Raw command arguments. If omitted, launches interactive REPL.

Options:
  -H, --host <HOST>  Server hostname or IP address [default: 127.0.0.1]
  -p, --port <PORT>  Server port [default: 6379]
  -h, --help         Print help
  -V, --version      Print version
```

## Usage Modes

### 1. Interactive REPL Mode

Launch the REPL without command parameters:

```bash
quty-cli -H 127.0.0.1 -p 6379
```

#### Session Example

```text
Connected to quty server at 127.0.0.1:6379
Type exit or help to disconnect.

127.0.0.1:6379> SET user:100:profile "Alice"
OK (118 µs)

127.0.0.1:6379> GET user:100:profile
"Alice" (82 µs)

127.0.0.1:6379> BATCH (SET user:100:name "Alice", SET user:100:age 30, GET user:100:name)
3 items (215 µs)
  1) OK
  2) OK
  3) "Alice"

127.0.0.1:6379> STATS
── Server Telemetry ───────────────────────
uptime_seconds:        412
total_connections:     14
active_connections:    1
total_keys:            2
used_memory_bytes:     184 (0.00 MB)
get_hits:              18
get_misses:            2
evictions:             0
────────────────────────────────────────────
(94 µs)
```

### 2. One-Off Scripting Mode

Execute standalone commands from shell scripts and automation workflows.

```bash
# Returns exit code 0 on operational completion or a soft miss ((nil)).
quty-cli -p 6379 SET config:mode "production"
quty-cli -p 6379 GET config:mode

# Returns exit code 1 on hard errors, such as capacity limits,
# failed preconditions, or syntax errors.
quty-cli -p 6379 BATCH (SET u:1 a, SET s:1 b)
```

## SDK Integration

Add `quty-client` to your application's `Cargo.toml`:

```toml
[dependencies]
quty-client = { path = "../quty-client" }
tokio = { version = "1.0", features = ["rt-multi-thread", "macros"] }
```

### Async Client Example

```rust
use quty_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect("127.0.0.1:6379").await?;

    // Basic storage operation
    client.set("user:1", "active_payload").await?;

    let res = client.get("user:1").await?;
    println!(
        "Response: {:?} in {:?}",
        res.response.payload,
        res.elapsed
    );

    // Atomic batch transaction
    use quty_proto::Operation;

    let ops = vec![
        Operation::Set {
            key: "sess:1".into(),
            value: "tok_1".into(),
        },
        Operation::Expire {
            key: "sess:1".into(),
            ttl_ms: 3600_000,
        },
    ];

    let batch_res = client.batch(ops).await?;
    println!("Batch finished in {:?}", batch_res.elapsed);

    Ok(())
}
```

## Supported Commands

| Command          | Arguments           | Semantics                                                           |
|------------------|---------------------|---------------------------------------------------------------------|
| `SET`            | `<key> <val>`       | Unconditional storage upsert.                                       |
| `GET`            | `<key>`             | Read cached binary payload; updates the LRU clock.                  |
| `ADD`            | `<key> <val>`       | Store payload only if the key does not already exist.               |
| `REPLACE`        | `<key> <val>`       | Update payload only if the key currently exists.                    |
| `DEL` / `DELETE` | `<key>`             | Delete an entry and free accounted heap space.                      |
| `EXISTS`         | `<key>`             | Check key presence without altering LRU order.                      |
| `TTL`            | `<key>`             | Fetch remaining lifespan in milliseconds (`-1` if persistent).      |
| `EXPIRE`         | `<key> <ms>`        | Attach or update a millisecond TTL deadline.                        |
| `PERSIST`        | `<key>`             | Remove the TTL deadline, keeping the key persistent.                |
| `BATCH`          | `(cmd1, cmd2, ...)` | Execute an atomic multi-operation sequence with rollback isolation. |
| `PING`           | None                | Check TCP connection liveness.                                      |
| `STATS`          | None                | Retrieve the eight-field server telemetry table.                    |
| `RESET`          | None                | Flush all shards and clear metrics.                                 |
