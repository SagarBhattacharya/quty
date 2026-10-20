//! Sharded, concurrent in-memory caching engine with atomic transactions.
//!
//! Provides lock-isolated partition sharding, Sampled LRU eviction,
//! prefix-based transaction routing, and streaming TCP connection handlers.

pub mod config;
pub mod database;
pub mod net;

pub use config::{CliArgs, EngineConfig, ServerConfig};
pub use database::Engine;
pub use net::Server;
