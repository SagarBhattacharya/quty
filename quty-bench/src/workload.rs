use bytes::Bytes;
use quty_proto::{Command, Operation};
use rand::distr::Distribution;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};
use rand_distr::Zipf;

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum WorkloadType {
	/// 100% SET operations
	Set,
	/// 100% GET operations (assumes keys are populated)
	Get,
	/// 80% GET, 20% SET (standard read-heavy cache profile)
	Mixed,
}

pub struct KeyGenerator {
	rng: SmallRng,
	key_count: usize,
	zipf: Option<Zipf<f64>>,
	payload_buffer: Bytes,
}

impl KeyGenerator {
	pub fn new(key_count: usize, use_zipf: bool, val_size_bytes: usize, seed: u64) -> Self {
		let zipf = if use_zipf {
			Some(Zipf::new(key_count as f64, 1.05)
				.expect("Invalid Zipf parameters"))
		} else {
			None
		};

		let payload_buffer = Bytes::from(vec![0x42u8; val_size_bytes]);

		Self {
			rng: SmallRng::seed_from_u64(seed),
			key_count,
			zipf,
			payload_buffer,
		}
	}

	/// Generates a key index based on distribution choice.
	#[inline]
	fn next_key_index(&mut self) -> usize {
		if let Some(ref zipf) = self.zipf {
			let sample = zipf.sample(&mut self.rng);
			(sample as usize).saturating_sub(1).min(self.key_count - 1)
		} else {
			self.rng.random_range(0..self.key_count)
		}
	}

	/// Creates next command according to workload ratio.
	pub fn next_command(&mut self, workload: WorkloadType) -> Command {
		let key_idx = self.next_key_index();
		// Distribute across 64 distinct routing prefixes: e.g. "k00:12345", "k63:12345"
		let shard_prefix = key_idx % 64;
		let key = format!("k{:02}:{:08}", shard_prefix, key_idx);

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
		}
	}
}