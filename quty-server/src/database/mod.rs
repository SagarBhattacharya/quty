//! Sharded cache engine facade, dispatch router, and background maintenance.

mod entry;
mod exec;
mod lru;
mod shard;
mod stats;
mod tx;

use crate::config::EngineConfig;
use ahash::RandomState;
use exec::{execute_mutation, execute_read};
use lru::LruPolicy;
use parking_lot::RwLock;
use quty_proto::{
	BatchErrorPayload, Command, Operation, ResponsePayload, StatsPayload, StatusCode, SubResult,
};
use shard::Shard;
use stats::EngineStats;
use std::sync::Arc;
use std::time::Instant;
use tx::{RollbackGuard, TxContext};

/// Maximum operations permitted in a single atomic batch transaction.
pub const MAX_BATCH_SIZE: usize = 64;

/// High-concurrency sharded storage engine.
pub struct Engine {
	shards: Vec<RwLock<Shard>>,
	hasher: RandomState,
	stats: EngineStats,
	lru: LruPolicy,
}

impl Engine {
	/// Initializes all partition shards with pre-allocated capacities.
	///
	/// # Panics
	///
	/// Panics if `config.num_shards == 0`.
	#[must_use]
	pub fn new(config: EngineConfig) -> Arc<Self> {
		assert!(config.num_shards > 0, "Shard count must be greater than 0");
		let per_shard_bytes = (config.max_memory_bytes / config.num_shards).max(1);

		let mut shards = Vec::with_capacity(config.num_shards);
		for _ in 0..config.num_shards {
			shards.push(RwLock::new(Shard::new(
				per_shard_bytes,
				config.initial_shard_capacity,
			)));
		}

		Arc::new(Self {
			shards,
			hasher: RandomState::new(),
			stats: EngineStats::new(),
			lru: LruPolicy::new(config.eviction_burst_limit),
		})
	}

	/// Dispatches any wire command to its appropriate execution path.
	pub fn execute(&self, command: Command) -> (StatusCode, ResponsePayload) {
		match command {
			Command::Ping => (StatusCode::Ok, ResponsePayload::Null),
			Command::Stats => (StatusCode::Ok, ResponsePayload::Stats(self.collect_stats())),
			Command::Reset => {
				self.reset_all();
				(StatusCode::NoContent, ResponsePayload::Null)
			}
			Command::Single(op) => self.execute_single(&op),
			Command::Batch(ops) => self.execute_batch(&ops),
		}
	}

	fn execute_single(&self, op: &Operation) -> (StatusCode, ResponsePayload) {
		let idx = self.shard_index(op.routing_key());
		let now = Instant::now();
		let clock = self.stats.lru_clock_100ms();

		if op.is_mutation() {
			let mut shard = self.shards[idx].write();
			let mut ctx = TxContext::direct();
			let (status, payload, evicted) =
				execute_mutation(&mut shard, &self.lru, op, &mut ctx, now, clock);
			if evicted > 0 {
				self.stats.inc_evictions(evicted);
			}
			(status, payload)
		} else {
			let shard = self.shards[idx].read();
			let (status, payload, is_hit) = execute_read(&shard, op, now, clock);

			if matches!(op, Operation::Get { .. }) {
				if is_hit {
					self.stats.inc_hits();
				} else {
					self.stats.inc_misses();
				}
			}

			(status, payload)
		}
	}

	fn execute_batch(&self, ops: &[Operation]) -> (StatusCode, ResponsePayload) {
		if ops.is_empty() {
			return (StatusCode::EmptyBatch, ResponsePayload::Null);
		}

		if ops.len() > MAX_BATCH_SIZE {
			return (StatusCode::BadRequest, ResponsePayload::Null);
		}

		let first_prefix = ops[0].routing_key();
		for op in &ops[1..] {
			if op.routing_key() != first_prefix {
				return (StatusCode::CrossShardViolation, ResponsePayload::Null);
			}
		}

		let target_idx = self.shard_index(first_prefix);
		let mut shard = self.shards[target_idx].write();
		let now = Instant::now();
		let clock = self.stats.lru_clock_100ms();

		let mut guard = RollbackGuard::new(&mut shard, ops.len());
		let mut results = Vec::with_capacity(ops.len());
		let mut batch_hits = 0;
		let mut batch_misses = 0;

		for (idx, op) in ops.iter().enumerate() {
			let (active_shard, undo_log) = guard.split_mut();
			let mut ctx = TxContext::transactional(undo_log);

			let (status, payload, evicted) =
				execute_mutation(active_shard, &self.lru, op, &mut ctx, now, clock);

			if matches!(op, Operation::Get { .. }) {
				if status == StatusCode::Ok {
					batch_hits += 1;
				} else if status == StatusCode::KeyNotFound
					|| status == StatusCode::KeyExpired {
					batch_misses += 1;
				}
			}

			if status.should_abort_batch() {
				// Batch failed - do not commit or record hit stats for the aborted batch
				return (
					StatusCode::BatchShortCircuited,
					ResponsePayload::BatchError(BatchErrorPayload {
						failed_index: u32::try_from(idx).unwrap_or(0),
						failure_code: status,
					}),
				);
			}

			if evicted > 0 {
				self.stats.inc_evictions(evicted);
			}

			results.push(SubResult { status, payload });
		}

		guard.commit();

		// Commit telemetry stats on batch success
		for _ in 0..batch_hits {
			self.stats.inc_hits();
		}
		for _ in 0..batch_misses {
			self.stats.inc_misses();
		}

		(StatusCode::Ok, ResponsePayload::Array(results))
	}

	#[inline]
	#[allow(clippy::cast_possible_truncation)]
	fn shard_index(&self, routing_key: &str) -> usize {
		let num_shards = self.shards.len();
		let hash = self.hasher.hash_one(routing_key) as usize;
		if num_shards.is_power_of_two() {
			hash & (num_shards - 1)
		} else {
			hash % num_shards
		}
	}

	fn reset_all(&self) {
		for shard_lock in &self.shards {
			let mut shard = shard_lock.write();
			shard.clear();
		}
	}

	/// Records a newly accepted client TCP connection.
	#[inline]
	pub fn on_client_connected(&self) {
		self.stats.on_client_connected();
	}

	/// Records a client TCP socket disconnect.
	#[inline]
	pub fn on_client_disconnected(&self) {
		self.stats.on_client_disconnected();
	}

	/// Collects metrics snapshot across all partition shards.
	#[must_use]
	pub fn collect_stats(&self) -> StatsPayload {
		let mut total_keys: u64 = 0;
		let mut used_memory_bytes: u64 = 0;

		for shard_lock in &self.shards {
			let shard = shard_lock.read();
			total_keys += shard.len() as u64;
			used_memory_bytes += shard.used_memory_bytes() as u64;
		}

		StatsPayload {
			uptime_seconds: self.stats.uptime_secs(),
			total_connections: self.stats.total_connections(),
			active_connections: self.stats.curr_connections(),
			total_keys,
			used_memory_bytes,
			get_hits: self.stats.get_hits(),
			get_misses: self.stats.get_misses(),
			evictions: self.stats.get_evictions(),
		}
	}

	/// Background task: purges expired keys and enforces memory watermarks.
	pub fn run_maintenance_sweep(
		&self,
		high_watermark_ratio: f64,
		low_watermark_ratio: f64,
	) -> (usize, usize) {
		let now = Instant::now();
		let clock = self.stats.lru_clock_100ms();
		let mut total_expired = 0;
		let mut total_evicted = 0;

		for shard_lock in &self.shards {
			{
				let mut shard = shard_lock.write();
				let purged = self.lru.sweep_expired(&mut shard, 16, now);
				total_expired += purged;
			}

			loop {
				let mut shard = shard_lock.write();
				let capacity = shard.max_memory_bytes();
				#[allow(
					clippy::cast_precision_loss,
					clippy::cast_sign_loss,
					clippy::cast_possible_truncation
				)]
				let high_bytes = (capacity as f64 * high_watermark_ratio) as usize;
				#[allow(
					clippy::cast_precision_loss,
					clippy::cast_sign_loss,
					clippy::cast_possible_truncation
				)]
				let low_bytes = (capacity as f64 * low_watermark_ratio) as usize;

				if shard.used_memory_bytes() <= high_bytes {
					break;
				}

				let evicted = self.lru.evict_to_target(&mut shard, low_bytes, clock);
				total_evicted += evicted;

				if evicted == 0 || shard.used_memory_bytes() <= low_bytes {
					break;
				}
			}
		}

		if total_evicted > 0 {
			self.stats.inc_evictions(total_evicted);
		}
		(total_expired, total_evicted)
	}
}

#[cfg(test)]
mod tests {
	use super::Engine;
	use crate::config::EngineConfig;
	use bytes::Bytes;
	use quty_proto::{Command, Operation, ResponsePayload, StatusCode};

	#[test]
	fn test_batch_cross_shard_routing_guard() {
		let config = EngineConfig {
			num_shards: 16,
			max_memory_bytes: 1024 * 1024,
			..Default::default()
		};
		let engine = Engine::new(config);

		// Batch containing operations with differing routing prefixes
		let ops = vec![
			Operation::Set {
				key: "tenantA:key1".into(),
				value: Bytes::from_static(b"1"),
			},
			Operation::Set {
				key: "tenantB:key2".into(),
				value: Bytes::from_static(b"2"),
			},
		];

		let (status, _) = engine.execute(Command::Batch(ops));
		assert_eq!(status, StatusCode::CrossShardViolation);
	}

	#[test]
	fn test_batch_single_prefix_routing_success() {
		let config = EngineConfig {
			num_shards: 16,
			max_memory_bytes: 1024 * 1024,
			..Default::default()
		};
		let engine = Engine::new(config);

		// Batch where all keys share the exact same routing prefix
		let ops = vec![
			Operation::Set {
				key: "order:1001:items".into(),
				value: Bytes::from_static(b"item_data"),
			},
			Operation::Set {
				key: "order:1001:total".into(),
				value: Bytes::from_static(b"42"),
			},
		];

		let (status, payload) = engine.execute(Command::Batch(ops));
		assert_eq!(status, StatusCode::Ok);
		if let ResponsePayload::Array(results) = payload {
			assert_eq!(results.len(), 2);
			assert_eq!(results[0].status, StatusCode::Created);
			assert_eq!(results[1].status, StatusCode::Created);
		} else {
			panic!("Expected Array payload");
		}
	}

	#[test]
	fn test_stats_and_telemetry_isolation() {
		let engine = Engine::new(EngineConfig::default());

		engine.on_client_connected();
		engine.on_client_connected();
		engine.on_client_disconnected();

		// Trigger one hit and one miss
		engine.execute(Command::Single(Operation::Set {
			key: "item:1".into(),
			value: Bytes::from_static(b"val"),
		}));
		engine.execute(Command::Single(Operation::Get {
			key: "item:1".into(),
		}));
		engine.execute(Command::Single(Operation::Get {
			key: "item:missing".into(),
		}));

		let stats = engine.collect_stats();
		assert_eq!(stats.total_connections, 2);
		assert_eq!(stats.active_connections, 1);
		assert_eq!(stats.get_hits, 1);
		assert_eq!(stats.get_misses, 1);
		assert_eq!(stats.total_keys, 1);
	}

	#[test]
	fn test_background_maintenance_sweep() {
		let config = EngineConfig {
			num_shards: 2,
			max_memory_bytes: 200, // 100 bytes per shard
			..Default::default()
		};
		let engine = Engine::new(config);

		// Populate 3 entries on shard for prefix "user" (3 * 21B = 63B, which is 63% full)
		// 63% is safely above the 50% high watermark and below the 100% capacity wall
		let routing_prefix = "user";
		for i in 0..3 {
			engine.execute(Command::Single(Operation::Set {
				key: format!("{routing_prefix}:{i}"),
				value: Bytes::from(vec![0xAA; 15]), // 6B key + 15B val = 21B per entry
			}));
		}

		let before_stats = engine.collect_stats();
		assert_eq!(before_stats.used_memory_bytes, 63);
		assert_eq!(
			before_stats.evictions, 0,
			"No inline evictions should occur during setup"
		);

		// Run sweep with 50% high watermark (50B) and 30% low watermark (30B)
		let (_expired, evicted) = engine.run_maintenance_sweep(0.50, 0.30);
		assert!(
			evicted > 0,
			"Maintenance sweep must evict keys above watermark"
		);

		let after_stats = engine.collect_stats();
		assert!(
			after_stats.used_memory_bytes <= 30,
			"Memory must drop down to low watermark target"
		);
		assert_eq!(after_stats.evictions, evicted as u64);
	}

	#[test]
	fn test_batch_updates_hit_and_miss_statistics() {
		let engine = Engine::new(EngineConfig {
			num_shards: 4,
			max_memory_bytes: 1024 * 1024,
			initial_shard_capacity: 64,
			eviction_burst_limit: 16,
		});

		// Populate a test key
		let _ = engine.execute(Command::Single(Operation::Set {
			key: "test:k1".to_string(),
			value: Bytes::from_static(b"value1"),
		}));

		let initial_stats = engine.collect_stats();
		assert_eq!(initial_stats.get_hits, 0);
		assert_eq!(initial_stats.get_misses, 0);

		// Execute batch: 1 Hit (test:k1), 1 Miss (test:missing)
		let batch_cmd = Command::Batch(vec![
			Operation::Get { key: "test:k1".to_string() },
			Operation::Get { key: "test:missing".to_string() },
		]);

		let (status, _) = engine.execute(batch_cmd);
		assert_eq!(status, StatusCode::Ok);

		let stats_after = engine.collect_stats();
		assert_eq!(stats_after.get_hits, 1, "Batch GET should record a cache hit");
		assert_eq!(stats_after.get_misses, 1, "Batch GET should record a cache miss");
	}
}
