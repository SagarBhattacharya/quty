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

1. **Shard-Scoped Synchronization:** Most operations synchronize on the shard
	 they access. Operations such as `RESET` acquire all shard locks in a
	 consistent order.

2. **Payload-Byte Budgeting:** Memory accounting tracks key and value byte
	 lengths. It excludes hash-table/index capacity, entry metadata, allocator
	 overhead, and other process allocations; it is not a strict RSS ceiling.
	 The budget is divided among shards.

3. **Transactional Safety:** Single-shard batches use inverse mutations to
	 restore prior state when a transaction fails.

