mod entry;
mod shard;
mod rollback;

#[cfg(test)]
mod shard_tests;
#[cfg(test)]
mod rollback_tests;
#[cfg(test)]
mod engine_tests;
#[cfg(test)]
mod execute_tests;
mod stats;

use crate::db::shard::{CapacityExceeded, Shard};
use crate::db::stats::EngineStats;
use ahash::RandomState;
use parking_lot::RwLock;
use quty_proto::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use crate::config::EngineConfig;

pub const MAX_BATCH_SIZE: usize = 64;

pub struct Engine {
	shards: Vec<RwLock<Shard>>,
	hasher: RandomState,
	stats: EngineStats,
}

impl Engine {
	pub fn new(config: EngineConfig) -> Arc<Self> {
		assert!(config.num_shards > 0, "Shard count must be greater than 0");
		let per_shard_bytes = config.max_memory_bytes / config.num_shards;

		let mut shards = Vec::with_capacity(config.num_shards);
		for _ in 0..config.num_shards {
			shards.push(RwLock::new(Shard::new(
				per_shard_bytes,
				config.eviction_burst_limit,
				config.initial_shard_capacity,
			)));
		}

		Arc::new(Self {
			shards,
			hasher: RandomState::new(),
			stats: EngineStats::new(),
		})
	}

	// -------------------------------------------------------------------------
	// Public Engine Facade (The ONLY entry point called by the net layer)
	// -------------------------------------------------------------------------

	/// Dispatches any transport or storage command to its appropriate handler.
	pub fn execute(&self, command: Command) -> (StatusCode, ResponsePayload) {
		match command {
			Command::Ping => (StatusCode::Ok, ResponsePayload::Null),
			Command::Stats => (
				StatusCode::Ok,
				ResponsePayload::Stats(self.collect_stats()),
			),
			Command::Reset => {
				self.reset_all();
				(StatusCode::NoContent, ResponsePayload::Null)
			}
			Command::Single(op) => self.execute_single(op),
			Command::Batch(ops) => self.execute_batch(ops),
		}
	}

	// -------------------------------------------------------------------------
	// Internal Routing & Dispatch
	// -------------------------------------------------------------------------

	fn execute_single(&self, op: Operation) -> (StatusCode, ResponsePayload) {
		let idx = self.shard_index(op.routing_key());
		let now = Instant::now();
		let clock = self.current_lru_clock();

		if op.is_readonly() {
			let shard = self.shards[idx].read();
			let (status, payload, is_hit) = self.execute_read_op(&shard, &op, now, clock);

			if matches!(op, Operation::Get { .. }) {
				if is_hit {
					self.stats.inc_hits();
				} else {
					self.stats.inc_misses();
				}
			}

			(status, payload)
		} else {
			let mut shard = self.shards[idx].write();
			self.execute_write_op(&mut shard, &op, now, clock)
		}
	}

	fn execute_batch(&self, ops: Vec<Operation>) -> (StatusCode, ResponsePayload) {
		if ops.is_empty() {
			return (StatusCode::EmptyBatch, ResponsePayload::Null);
		}

		if ops.len() > MAX_BATCH_SIZE {
			return (StatusCode::BadRequest, ResponsePayload::Null);
		}

		let target_idx = self.shard_index(ops[0].routing_key());
		for op in &ops[1..] {
			if self.shard_index(op.routing_key()) != target_idx {
				return (StatusCode::CrossShardViolation, ResponsePayload::Null);
			}
		}

		let mut shard = self.shards[target_idx].write();
		let now = Instant::now();
		let clock = self.current_lru_clock();

		let mut guard = rollback::RollbackGuard::new(&mut shard, ops.len());
		let mut results = Vec::with_capacity(ops.len());

		for (idx, op) in ops.iter().enumerate() {
			let (status, payload) = guard.execute_op(op, now, clock);

			// Only hard errors (e.g., KeyExists on ADD, StorageFull, etc.) abort the batch
			if status.should_abort_batch() {
				return (
					StatusCode::BatchShortCircuited,
					ResponsePayload::BatchError(BatchErrorPayload {
						failed_index: idx as u16,
						failure_code: status,
					}),
				);
			}

			results.push(SubResult { status, payload });
		}

		guard.commit();
		(StatusCode::Ok, ResponsePayload::Array(results))
	}

	#[inline]
	fn shard_index(&self, routing_key: &str) -> usize {
		(self.hasher.
			hash_one(routing_key) as usize
		) % self.shards.len()
	}

	#[inline]
	fn current_lru_clock(&self) -> u32 {
		self.stats.uptime_secs() as u32
	}

	fn reset_all(&self) {
		for shard_lock in &self.shards {
			let mut shard = shard_lock.write();
			shard.clear();
		}
	}

	// -------------------------------------------------------------------------
	// Private Execution Primitives
	// -------------------------------------------------------------------------

	fn execute_read_op(
		&self,
		shard: &Shard,
		op: &Operation,
		now: Instant,
		clock: u32,
	) -> (StatusCode, ResponsePayload, bool) {
		match op {
			Operation::Get { key } => {
				match shard.get_value(key, now, clock) {
					Some(value) => (StatusCode::Ok, ResponsePayload::Raw(value), true),
					None => {
						let status = match shard.entry(key) {
							Some(entry) if entry.is_expired(now) => StatusCode::KeyExpired,
							_ => StatusCode::KeyNotFound,
						};
						(status, ResponsePayload::Null, false)
					}
				}
			}
			Operation::Exists { key } => {
				let exists = shard.entry(key).map(|e| !e.is_expired(now)).unwrap_or(false);
				(StatusCode::Ok, ResponsePayload::Bool(exists), exists)
			}
			Operation::Ttl { key } => {
				let entry = match shard.entry(key) {
					Some(e) => e,
					None => return (StatusCode::KeyNotFound, ResponsePayload::Null, false),
				};

				match entry.expires_at {
					None => (StatusCode::Ok, ResponsePayload::Int64(-1), true),
					Some(deadline) => {
						if now >= deadline {
							(StatusCode::KeyNotFound, ResponsePayload::Null, false)
						} else {
							let ms = deadline.duration_since(now).as_millis() as i64;
							(StatusCode::Ok, ResponsePayload::Int64(ms), true)
						}
					}
				}
			}
			// Invariant: Writing operation routed to read handler is an internal programming error.
			mutating_op => unreachable!(
				"Internal invariant violation: Mutating op {:?} dispatched to read handler",
				mutating_op
			),
		}
	}

	pub(crate) fn execute_write_op(
		&self,
		shard: &mut Shard,
		op: &Operation,
		now: Instant,
		clock: u32,
	) -> (StatusCode, ResponsePayload) {
		match op {
			Operation::Set { key, value } => {
				match shard.insert(key.clone().into_boxed_str(), value.clone(), None, clock) {
					Ok((Some(old), evicted)) => {
						if evicted > 0 {
							self.stats.inc_evictions(evicted);
						}
						if !old.is_expired(now) {
							(StatusCode::Ok, ResponsePayload::Null)
						} else {
							(StatusCode::Created, ResponsePayload::Null)
						}
					}
					Ok((None, evicted)) => {
						if evicted > 0 {
							self.stats.inc_evictions(evicted);
						}
						(StatusCode::Created, ResponsePayload::Null)
					}
					Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
				}
			}
			Operation::Add { key, value } => {
				let is_active = shard.entry(key).map(|e| !e.is_expired(now));
				match is_active {
					Some(true) => (StatusCode::KeyExists, ResponsePayload::Null),
					Some(false) => {
						shard.remove(key);
						match shard.insert(key.clone().into_boxed_str(), value.clone(), None, clock) {
							Ok((_, evicted)) => {
								if evicted > 0 {
									self.stats.inc_evictions(evicted);
								}
								(StatusCode::Created, ResponsePayload::Null)
							},
							Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
						}
					}
					None => match shard.insert(key.clone().into_boxed_str(), value.clone(), None, clock) {
						Ok((_, evicted)) => {
							if evicted > 0 {
								self.stats.inc_evictions(evicted);
							}
							(StatusCode::Created, ResponsePayload::Null)
						},
						Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
					},
				}
			}
			Operation::Replace { key, value } => {
				let is_active = shard.entry(key).map(|e| !e.is_expired(now));
				match is_active {
					Some(true) => {
						match shard.insert(key.clone().into_boxed_str(), value.clone(), None, clock) {
							Ok((_, evicted)) => {
								if evicted > 0 {
									self.stats.inc_evictions(evicted);
								}
								(StatusCode::Ok, ResponsePayload::Null)
							},
							Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
						}
					}
					Some(false) => {
						shard.remove(key);
						(StatusCode::KeyNotFound, ResponsePayload::Null)
					}
					None => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
			Operation::Delete { key } => {
				match shard.remove(key) {
					Some(entry) if !entry.is_expired(now) => (StatusCode::NoContent, ResponsePayload::Null),
					_ => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
			Operation::Expire { key, ttl_ms } => {
				let deadline = now.checked_add(Duration::from_millis(*ttl_ms)).unwrap_or(now);
				match shard.update_expiry(key, Some(deadline), now) {
					Some(_) => (StatusCode::NoContent, ResponsePayload::Null),
					None => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
			Operation::Persist { key } => {
				match shard.update_expiry(key, None, now) {
					Some(Some(_)) => (StatusCode::NoContent, ResponsePayload::Null),
					Some(None) => (StatusCode::PreconditionFailed, ResponsePayload::Null),
					None => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
			// Reads executed within a write lock (e.g. within an atomic Batch)
			read_op @ (Operation::Get { .. } | Operation::Exists { .. } | Operation::Ttl { .. }) => {
				let (status, payload, _) = self.execute_read_op(shard, read_op, now, clock);
				(status, payload)
			}
		}
	}

	// -------------------------------------------------------------------------
	// Public Connection & Stats API
	// -------------------------------------------------------------------------

	#[inline]
	pub fn on_client_connected(&self) {
		self.stats.on_client_connected();
	}

	#[inline]
	pub fn on_client_disconnected(&self) {
		self.stats.on_client_disconnected();
	}

	pub fn collect_stats(&self) -> StatsPayload {
		let mut total_keys: u64 = 0;
		let mut used_memory_bytes: u64 = 0;

		for shard_lock in &self.shards {
			let shard = shard_lock.read();
			total_keys += shard.len() as u64;
			used_memory_bytes += shard.used_memory_bytes() as u64;
		}

		StatsPayload {
			uptime_secs: self.stats.uptime_secs(),
			total_connections: self.stats.total_connections(),
			curr_connections: self.stats.curr_connections(),
			get_hits: self.stats.get_hits(),
			get_misses: self.stats.get_misses(),
			evictions: self.stats.get_evictions(),
			total_keys, used_memory_bytes
		}
	}

	/// Background task sweep: purges expired keys and enforces memory watermarks.
	///
	/// - `high_watermark_ratio`: e.g., 0.80 (trigger eviction at 80% usage)
	/// - `low_watermark_ratio`: e.g., 0.70 (evict down to 70% usage)
	pub fn run_maintenance_sweep(
		&self,
		high_watermark_ratio: f64,
		low_watermark_ratio: f64,
	) -> (usize, usize) {
		let now = Instant::now();
		let clock = self.current_lru_clock();
		let mut total_expired = 0;
		let mut total_evicted = 0;

		for shard_lock in &self.shards {
			{
				let mut shard = shard_lock.write();
				let purged = shard.sweep_expired(16, now);
				total_expired += purged;
			}

			loop {
				let mut shard = shard_lock.write();
				let capacity = shard.max_memory_bytes();
				let high_bytes = (capacity as f64 * high_watermark_ratio) as usize;
				let low_bytes = (capacity as f64 * low_watermark_ratio) as usize;

				if shard.used_memory_bytes() <= high_bytes {
					break;
				}

				let evicted = shard.evict_to_target(low_bytes, clock);
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