# Server Engine & Storage Architecture (`quty-server`)

This document details the internal design, concurrency guarantees, memory allocation, and transaction pipeline of the `quty-server` caching daemon.

## 1. Concurrency & Partitioning Architecture

To eliminate global mutex bottlenecks under high read/write concurrency, `quty-server` splits its keyspace into \(N\) independent, power-of-two partition shards.

### Architecture Overview

```text
                              Incoming Request
                                     |
                                     v
                     Hash Partition: ahash(routing_key) & (N - 1)
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
         v                           v                           v
  Shard Lock [0]              Shard Lock [1]              Shard Lock [N-1]
+-----------------+         +-----------------+         +-----------------+
| parking_lot     |         | parking_lot     |         | parking_lot     |
| RwLock<Shard>   |         | RwLock<Shard>   |         | RwLock<Shard>   |
+--------+--------+         +--------+--------+         +--------+--------+
         |                           |                           |
         v                           v                           v
+-----------------+         +-----------------+         +-----------------+
|  ShardStorage   |         |  ShardStorage   |         |  ShardStorage   |
|  - AHashMap     |         |  - AHashMap     |         |  - AHashMap     |
|  - Vec<Box<str>>|         |  - Vec<Box<str>>|         |  - Vec<Box<str>>|
|  - Memory Bytes |         |  - Memory Bytes |         |  - Memory Bytes |
+-----------------+         +-----------------+         +-----------------+
```

### Partition Properties

- **Bitwise Masking:** When `num_shards` is a power of two, routing resolves via `hash & (num_shards - 1)`, avoiding CPU division instructions.
- **Shared Read Concurrency:** Read operations (`GET`, `EXISTS`, `TTL`) acquire a shared read lock using `shard.read()`.
- **Atomic Clock Updates Under Shared Lock:** Recency tracking is managed through `Entry.last_accessed: AtomicU32`. A `GET` request updates the LRU clock using relaxed atomic stores without acquiring an exclusive write lock.

## 2. Memory Accounting & Sampled LRU Eviction

Memory management enforces deterministic bounds calculated strictly from key length and value payload length.

### Data Structures (`Shard`)

Each shard maintains two synchronized data structures:

1. `entries: AHashMap<Box<str>, Entry>` — the primary key-value mapping, storing payload slices, optional deadlines, and clock metadata.
2. `keys: Vec<Box<str>>` — a dense vector containing active keys for uniform candidate sampling.

### Data Structure Relationship

```text
Vec<Box<str>>:     [ "user:1", "user:2", "user:3", ... ]
                                ^
                                |
                                | key_index: 1
                                |
AHashMap:          "user:2" => Entry {
                                value,
                                expires_at,
                                last_accessed,
                                key_index: 1
                            }
```

### O(1) Swap-Remove Invariant

When an entry is removed from the shard:

1. The entry is removed from `entries`.
2. The key at index `entry.key_index` is removed from `keys` via `keys.swap_remove(idx)`.
3. If an element from the tail of the vector is moved into the vacated position, its corresponding entry in `entries` has its `key_index` updated to `idx`.

This guarantees that candidate sampling remains O(1), without requiring bucket iteration or allowing tombstone leakage in the dense key vector.

### Eviction Pipeline (`LruPolicy`)

- **Inline Headroom Eviction:** When a write requires more memory than is available, `LruPolicy::evict_headroom` samples up to `DEFAULT_SAMPLE_SIZE` (5) keys, selects the one with the highest idle duration, and removes it. This repeats until the required headroom is reclaimed or the attempt limit is reached.
- **Self-Eviction Shield:** Before checking headroom for an update to an existing key, the mutation pipeline invokes `shard.touch(key, clock)`. This resets its idle duration, preventing an active key from evicting itself during an in-place update.
- **Background Maintenance Sweep:** Every `sweep_interval_ms`, a background worker samples keys to reclaim expired TTL entries. If memory exceeds `high_watermark`, it evicts candidates in bursts of `eviction_burst_limit` until memory drops below `low_watermark`.

## 3. Transaction Execution & Rollback Pipeline

Batch operations execute atomically within a single shard using an inverse mutation log.

### Transaction Flow

```text
                  Batch Request [cmd0, cmd1, cmd2]
                                 |
                                 v
                   Verify Prefix Alignment (is_single_shard)
                                 |
                                 v
                    Acquire Shard Write Lock
                                 |
                                 v
                    Spawn RollbackGuard (RAII)
                                 |
                 +---------------+---------------+
                 |                               |
                 v                               v
          execute_mutation(op0)           execute_mutation(op1)
                 |                               |
                 v                               v
          Record UndoRecord                Record UndoRecord
          [UndoRecord::Restore]            [UndoRecord::Delete]
                 |                               |
                 +---------------+---------------+
                                 |
                                 v
                 Condition Failure on op2 (KeyExists)
                                 |
                                 v
                     Early Return: Status Abort
                                 |
                                 v
                     RollbackGuard Drops
                                 |
                                 v
                     Unroll Undo Stack (LIFO)
                     - Undo op1: Delete newly inserted key
                     - Undo op0: Restore displaced entry
```

### Inverse Undo Actions (`UndoRecord`)

- `Delete { key }`: Undoes a newly inserted key.
- `Restore { key, entry }`: Restores an overwritten or deleted entry to its exact prior state.
- `RevertExpiry { key, previous_deadline }`: Restores a previously configured expiration deadline.

### Transaction Invariants

1. **Eviction Prohibition:** Atomic batches execute with `allow_inline_eviction = false`. If a batch operation exceeds capacity, it fails immediately with `StatusCode::StorageFull` and triggers rollback rather than evicting existing keys.
2. **Panic Safety:** If an unhandled panic occurs in a worker thread during batch processing, Rust's stack unwinding executes `RollbackGuard::drop()`, ensuring that shard state and memory accounting remain consistent.

## 4. Supervised Network Architecture

```text
                           TcpListener (0.0.0.0:6379)
                                       |
                                       v
                       accept_loop (TaskTracker Supervised)
                                       |
                +----------------------+----------------------+
                |                                             |
                v                                             v
        Connection Task 1                             Connection Task 2
   (Socket + 64KB Read/Write)                    (Socket + 64KB Read/Write)
                |                                             |
                v                                             v
        CancellationToken                             CancellationToken
        (Shared Shutdown)                             (Shared Shutdown)
```

### Signal Handling & Graceful Drain

- **Multi-Signal Detection:** The runtime listens concurrently for `SIGINT` (Ctrl+C) and `SIGTERM` (container orchestrators).
- **Connection Draining:** Upon receiving a shutdown signal, `cancel_token.cancel()` notifies all active connection loops.
- **TaskTracker Barrier:** The server closes its listener, allows in-flight operations to finish writing responses, and waits up to `shutdown_timeout_secs` before forcing process exit.
- **Buffer Compaction:** Connections reclaim excess buffer capacity when processing large payloads expands internal buffers beyond 1 MB.

## 5. Memory Accounting

Quty accounts for the byte lengths of keys and values. This is a payload-byte
budget, not a strict process-memory ceiling. Hash-map buckets, dense-index
storage, entry metadata, allocator overhead, and other process allocations
are excluded.

The configured budget is divided among shards. A shard may reach its budget
even when other shards have unused capacity.

### Eviction Behavior

Before evicting entries to reclaim headroom, the eviction policy determines
whether the selected candidates can free enough accounted bytes. If the
capacity requirement cannot be satisfied, the operation fails without
removing those candidates.

## 6. RESET Semantics

`RESET` acquires all shard write locks in ascending shard-index order, clears
the shard contents, and resets hit, miss, and eviction counters. Uptime,
total connections, and active-connection counts are preserved.