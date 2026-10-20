# Overview

**Quty** is a multithreaded, in-memory cache daemon written in Rust. It provides sub-millisecond key-value operations, deterministic payload memory budgeting, and single-shard atomic batch transactions with guaranteed rollback invariants.

## Core Capabilities

- **Partitioned Key-Space:** Power-of-two sharding with independent
	`parking_lot::RwLock` instances reduces contention by isolating most
	operations to individual shards.
- **Payload-Byte Budgeting:** Accounts for key and value bytes, excluding
	data-structure and allocator overhead. The budget is not a process-RSS limit.
- **Sampled LRU Eviction:** Uses a dense key index for candidate sampling
	without scanning the entire hash map.
- **Atomic Batch Transactions:** Single-shard batches use an undo log to
	restore prior state when an operation fails.
- **Binary Wire Protocol:** A fixed 20-byte header supports incremental
	decoding and bounded request and response payloads.