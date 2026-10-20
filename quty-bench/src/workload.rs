//! Workload generation profiles, key-space distributions, and transaction generators.

use bytes::Bytes;
use quty_proto::{Command, Operation};
use rand::distr::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rand_distr::Zipf;

/// Supported benchmark workload execution profiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum WorkloadType {
	/// 100% SET operations (unconditional write throughput).
	Set,
	/// 100% GET operations (in-memory read throughput under shared locks).
	Get,
	/// 80% GET, 20% SET (standard read-heavy caching distribution).
	Mixed,
	/// 100% SET with EXPIRE (tests expiration deadline management).
	Ttl,
	/// Atomic multi-key batch transactions committing successfully on a single shard.
	Batch,
	/// Atomic batch transactions engineered to abort and roll back state via RAII undo stack.
	BatchRollback,
}

/// Generates synthetic keys, values, and protocol commands conforming to shard boundaries.
pub struct KeyGenerator {
	rng: SmallRng,
	key_count: usize,
	zipf: Option<Zipf<f64>>,
	uniform: Uniform<usize>,
	payload_buffer: Bytes,
}

impl KeyGenerator {
	/// Constructs a new [`KeyGenerator`].
	#[must_use]
	pub fn new(key_count: usize, use_zipf: bool, val_size_bytes: usize, seed: u64) -> Self {
		let zipf = if use_zipf {
			#[allow(clippy::cast_precision_loss)]
			Some(Zipf::new(key_count as f64, 1.05).expect("Invalid Zipf parameters"))
		} else {
			None
		};

		let uniform = Uniform::new(0, key_count).expect("Invalid key range");
		let payload_buffer = Bytes::from(vec![0x42u8; val_size_bytes]);

		Self {
			rng: SmallRng::seed_from_u64(seed),
			key_count,
			zipf,
			uniform,
			payload_buffer,
		}
	}

	#[inline]
	fn next_key_index(&mut self) -> usize {
		if let Some(ref zipf) = self.zipf {
			let sample = zipf.sample(&mut self.rng);
			#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
			(sample as usize).saturating_sub(1).min(self.key_count - 1)
		} else {
			self.uniform.sample(&mut self.rng)
		}
	}

	/// Generates the next command according to the chosen benchmark profile.
	pub fn next_command(&mut self, workload: WorkloadType) -> Command {
		let key_idx = self.next_key_index();
		// Route across 64 uniform prefix shards: e.g. "s00", "s01", ... "s63"
		let shard_id = key_idx % 64;
		let key = format!("s{shard_id:02}:item:{key_idx:08}");

		match workload {
			WorkloadType::Set => Command::Single(Operation::Set {
				key,
				value: self.payload_buffer.clone(),
			}),
			WorkloadType::Get => Command::Single(Operation::Get { key }),
			WorkloadType::Mixed => {
				if self.rng.random_bool(0.80) {
					Command::Single(Operation::Get { key })
				} else {
					Command::Single(Operation::Set {
						key,
						value: self.payload_buffer.clone(),
					})
				}
			}
			WorkloadType::Ttl => {
				let ttl_key = format!("s{shard_id:02}:ttl:{key_idx:08}");
				Command::Single(Operation::Set {
					key: ttl_key,
					value: self.payload_buffer.clone(),
				})
			}
			WorkloadType::Batch => {
				// Single-shard invariant: all sub-operations share prefix `s{shard_id:02}:`
				let k1 = format!("s{shard_id:02}:tx:{key_idx:08}:a");
				let k2 = format!("s{shard_id:02}:tx:{key_idx:08}:b");
				Command::Batch(vec![
					Operation::Set {
						key: k1.clone(),
						value: self.payload_buffer.clone(),
					},
					Operation::Set {
						key: k2,
						value: self.payload_buffer.clone(),
					},
					Operation::Get { key: k1 },
				])
			}
			WorkloadType::BatchRollback => {
				// Engineered conflict: 3rd operation (ADD) hits an already-created key
				let k1 = format!("s{shard_id:02}:ab:{key_idx:08}:x");
				let k2 = format!("s{shard_id:02}:ab:{key_idx:08}:y");
				Command::Batch(vec![
					Operation::Set {
						key: k1.clone(),
						value: self.payload_buffer.clone(),
					},
					Operation::Set {
						key: k2,
						value: self.payload_buffer.clone(),
					},
					Operation::Add {
						key: k1,
						value: self.payload_buffer.clone(),
					},
				])
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashSet;

	#[test]
	fn test_ttl_workload_distributes_across_shards() {
		let mut key_gen = KeyGenerator::new(10_000, false, 64, 12345);
		let mut observed_shards = HashSet::new();

		// Sample 1,000 commands and verify routing prefixes
		for _ in 0..1_000 {
			if let Command::Single(op) = key_gen.next_command(WorkloadType::Ttl) {
				let routing_prefix = op.routing_key();
				assert!(
					routing_prefix.starts_with('s'),
					"Expected shard prefix 'sXX', got: {routing_prefix}"
				);
				assert_ne!(
					routing_prefix, "ttl",
					"Routing key must not be 'ttl' (would cause single-shard contention)"
				);
				observed_shards.insert(routing_prefix.to_string());
			} else {
				panic!("Expected Command::Single for WorkloadType::Ttl");
			}
		}

		// 1,000 samples across 64 shards should hit all or nearly all shards
		assert!(
			observed_shards.len() >= 60,
			"Expected distribution across shards, but only observed {} shards",
			observed_shards.len()
		);
	}
}