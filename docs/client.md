# Client Driver & CLI Guide (`quty-client`)

This document details the architecture, design choices, error-handling semantics, and execution modes of `quty-client`.

## 1. Architectural Design

`quty-client` fulfills two architectural functions:

1. **Asynchronous Library Driver:** A lightweight, non-blocking network client built on `tokio` and `bytes` for application integration.
2. **Operational Terminal Client (`quty-cli`):** An interactive REPL and scriptable CLI tool for server administrators and DevOps workflows.

### Architecture Overview

```text
               +--------------------------------------+
               |            quty-cli CLI              |
               | (One-shot Mode & Interactive REPL)   |
               +------------------+-------------------+
                                  |
               +------------------v-------------------+
               |         quty_client::Client          |
               | (Socket Pooling, Correlation, Timeout)|
               +------------------+-------------------+
                                  |
               +------------------v-------------------+
               |             quty-proto               |
               | (20-byte Frame Codec, Zero-Copy AST) |
               +------------------+-------------------+
                                  |
                           [TCP Transport]
```

## 2. Network Loop & Invariant Safety

### 2.1 Monotonic Request Correlation

The client implements a strict request-response correlation check. For every command dispatched over the wire:

1. An internal `request_id_counter: u64` increments monotonically.
2. The request ID is encoded into bytes `8..16` of the 20-byte header.
3. Upon receiving a frame, the client verifies that the response request ID matches the expected request ID.

```rust
if response.header.request_id != expected_req_id {
    return Err(RequestCorrelationError);
}
```

This catches socket desynchronization, interleaving anomalies, or corrupted frames immediately.

### 2.2 Connection Poisoning Guard

Network protocols operating over raw TCP streams can become desynchronized if an I/O timeout, frame parsing error, or unexpected connection drop occurs mid-payload.

To eliminate subtle downstream data corruption, `quty-client` implements connection poisoning:

- If `Client::execute` encounters an I/O error or decoding failure, `self.is_poisoned` is set to `true`.
- Subsequent attempts to reuse the client instance immediately fail with `io::ErrorKind::BrokenPipe`.
- Callers must establish a fresh TCP connection via `Client::connect()` rather than continuing on a potentially desynchronized byte stream.

This prevents the client from interpreting stale or incomplete response bytes as responses to subsequent requests.

### 2.3 Memory Capacity Compaction

When reading large values (up to 32 MB), the underlying `read_buf: BytesMut` grows to hold incoming frames. To prevent sustained memory bloat, `Client` monitors the buffer's capacity after execution.

```rust
if self.read_buf.is_empty() && self.read_buf.capacity() > 1024 * 1024 {
    self.read_buf = BytesMut::with_capacity(64 * 1024);
}
```

Whenever the buffer is completely drained and its allocated capacity exceeds 1 MB, the client replaces the oversized buffer with a new buffer initialized with a baseline capacity of 64 KB.

## 3. Shell Automation & Exit Code Semantics

When running in one-off mode (for example, `quty-cli -p 6379 GET key:1`), the process exit status follows conventional systems-operation semantics.

### Exit Code `0` — Success

- **Operational completions:** `StatusCode::Ok`, `StatusCode::Created`, and `StatusCode::NoContent`.
- **Cache misses:** `StatusCode::KeyNotFound` and `StatusCode::KeyExpired`.

A cache miss is treated as a successful query evaluation. This ensures shell scripts do not fail prematurely when querying optional keys.

### Exit Code `1` — Failure

- Network I/O failures, connection refusals, or timeouts.
- Syntax and tokenization errors.
- Hard engine errors, including:
	- `PreconditionFailed`
	- `StorageFull` (out-of-memory or capacity limit)
	- `CrossShardViolation`
	- `BatchShortCircuited`
	- Protocol violations

### Scripting Example

```bash
#!/usr/bin/env bash
set -e

# Conditional execution: fails the script only if a real hard error occurs.
quty-cli -p 6379 SET user:100:active "true"

# Checking existence in automation.
STATUS=$(quty-cli -p 6379 EXISTS user:100:active)
echo "Query completed: $STATUS"
```

## 4. Interactive REPL Architecture

The interactive REPL is powered by `rustyline` and provides the following capabilities.

### Command Persistence

Commands are saved to `~/.quty_history` during execution and reloaded when the REPL starts.

### Lexical Parser

The tokenizer supports whitespace-separated arguments alongside quoted string literals, allowing values to contain spaces.

Example:

```text
SET user:100:profile "Alice Smith"
```

### Cross-Shard Batch Guard

When parsing `BATCH (...)`, `quty-client` validates `is_single_shard()` before serializing the request.

Cross-shard batches fail immediately in the client terminal with actionable guidance, without sending invalid traffic to the server.

This client-side validation complements the server's own shard-boundary enforcement and avoids unnecessary network round trips for invalid batches.
