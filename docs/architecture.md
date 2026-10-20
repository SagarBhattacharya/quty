# Architecture & Design

`quty` decouples storage partitioning, mutation execution, and networking into clean workspace boundaries.

## System Architecture

```text
               +----------------------------------------+
               |               Client App               |
               +-------------------+--------------------+
                                   |
                           TCP (20-byte Frame)
                                   |
                                   v
               +----------------------------------------+
               |          quty-server (Tokio)           |
               | - Multi-signal Supervisor              |
               | - Compacting Connection Read/Write     |
               +-------------------+--------------------+
                                   |
         +-------------------------+-------------------------+
         | Routing: ahash(key.split_once(':')) & (shards - 1) |
         +-------------------------+-------------------------+
                                   |
           +-----------------------+-----------------------+
           |                       |                       |
           v                       v                       v
    [Shard 0 (RwLock)]      [Shard 1 (RwLock)]      [Shard N-1 (RwLock)]
    +-----------------+     +-----------------+     +-----------------+
    | AHashMap        |     | AHashMap        |     | AHashMap        |
    | Dense Key Index |     | Dense Key Index |     | Dense Key Index |
    | Memory Accounting|    | Memory Accounting|    | Memory Accounting|
    +-----------------+     +-----------------+     +-----------------+
           |
           +----> LruPolicy (Stateless Sampled LRU)
           |
           +----> RollbackGuard (RAII Batch Transactions)
```

## Architectural Principles

1. **Zero Global Mutexes:** Key-space partitioning across \(N\) shards distributes work across CPU cores while isolating synchronization to individual shards.

2. **Deterministic Payload Accounting:** Shard memory tracking accounts for key and value slice byte lengths, keeping accounted memory usage within defined operational boundaries.

3. **No Phantom Eviction in Transactions:** Transactions cannot trigger inline LRU eviction. If a transaction exceeds available memory capacity, it aborts and restores the prior state rather than silently dropping existing keys.

4. **Panic Isolation:** Worker panics unwind through RAII transaction guards, applying inverse mutations in last-in, first-out (LIFO) order to restore the shard's prior state.

