# Overview

**Quty** is a multithreaded, in-memory cache daemon written in Rust. It provides sub-millisecond key-value operations, deterministic payload memory budgeting, and single-shard atomic batch transactions with guaranteed rollback invariants.

## Core Capabilities

* **Partitioned Key-Space:** Automatic power-of-two sharding (`ahash`) with independent `parking_lot::RwLock` instances to eliminate global lock contention across worker threads.
* **Deterministic Memory Budgeting:** Explicit memory accounting tracking key and value byte lengths, backed by inline headroom reclamation and background maintenance sweeps.
* **Stateless Sampled LRU:** Memory-efficient candidate selection via an indexed dense key vector maintaining $O(1)$ amortized `swap_remove` operations with zero bucket iteration stalls.
* **Atomic Batch Transactions:** Single-shard atomic sequences (`BATCH (cmd1, cmd2, ...)`) with RAII-managed undo stacks. If an operation fails a precondition or causes a capacity breach, state is safely rolled back in LIFO order.
* **Binary Wire Protocol:** Compact 20-byte fixed frame header supporting pipelined streaming I/O and zero-allocation frame decoding.