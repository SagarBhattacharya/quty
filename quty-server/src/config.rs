//! Server configuration parameters, CLI flag definitions, and memory parser.

use clap::Parser;
use std::net::SocketAddr;

/// Command-line argument specifications for `quty-server`.
#[derive(Parser, Debug, Clone)]
#[command(
	name = "quty-server",
	author = "Sagar Bhattacharya",
	version = "0.1.0",
	about = "High-performance sharded in-memory cache server with atomic batch transactions"
)]
pub struct CliArgs {
	/// Bind socket address
	#[arg(short = 'b', long = "bind", default_value = "0.0.0.0:6379")]
	pub bind_addr: SocketAddr,

	/// Number of concurrent key-space shards (must be a power of two)
	#[arg(short = 's', long = "shards", default_value_t = 64)]
	pub shards: usize,

	/// Maximum memory limit (e.g., "512MB", "2GB", "1048576")
	#[arg(short = 'm', long = "max-memory", default_value = "512MB")]
	pub max_memory: String,

	/// High watermark ratio to trigger maintenance eviction (0.10 - 0.95)
	#[arg(long = "high-watermark", default_value_t = 0.80)]
	pub high_watermark: f64,

	/// Low watermark ratio to stop eviction sweep (0.10 - 0.90)
	#[arg(long = "low-watermark", default_value_t = 0.70)]
	pub low_watermark: f64,

	/// Background sweep interval in milliseconds
	#[arg(long = "sweep-interval-ms", default_value_t = 100)]
	pub sweep_interval_ms: u64,

	/// Initial bucket capacity pre-allocated per shard
	#[arg(long = "initial-shard-capacity", default_value_t = 8192)]
	pub initial_shard_capacity: usize,

	/// Maximum keys evicted per burst before yielding the shard write lock
	#[arg(long = "eviction-burst-limit", default_value_t = 128)]
	pub eviction_burst_limit: usize,

	/// Network write buffer flush watermark in bytes
	#[arg(long = "flush-threshold-bytes", default_value_t = 128 * 1024)]
	pub flush_threshold_bytes: usize,

	/// Graceful shutdown timeout in seconds
	#[arg(long = "shutdown-timeout-secs", default_value_t = 3)]
	pub shutdown_timeout_secs: u64,
}

/// Consolidated runtime configuration for the Quty server daemon.
#[derive(Debug, Clone)]
pub struct ServerConfig {
	/// Network socket address to bind TCP listener.
	pub bind_addr: SocketAddr,
	/// Storage engine tuning and shard configuration.
	pub engine: EngineConfig,
	/// Sweep frequency for expired keys and capacity maintenance.
	pub sweep_interval_ms: u64,
	/// Memory ratio threshold that activates eviction.
	pub high_watermark: f64,
	/// Memory ratio target where eviction stops.
	pub low_watermark: f64,
	/// Outbound socket write buffer capacity before flushing.
	pub flush_threshold_bytes: usize,
	/// Maximum duration allowed for active connections to drain on shutdown.
	pub shutdown_timeout_secs: u64,
}

impl Default for ServerConfig {
	fn default() -> Self {
		Self {
			bind_addr: SocketAddr::from(([127, 0, 0, 1], 6379)),
			engine: EngineConfig::default(),
			sweep_interval_ms: 100,
			high_watermark: 0.80,
			low_watermark: 0.70,
			flush_threshold_bytes: 128 * 1024,
			shutdown_timeout_secs: 3,
		}
	}
}

/// Operational parameters for partition sharding and memory thresholds.
#[derive(Debug, Clone, Copy)]
pub struct EngineConfig {
	/// Number of independent key-space shards.
	pub num_shards: usize,
	/// Total aggregate memory ceiling in bytes across all shards.
	pub max_memory_bytes: usize,
	/// Pre-allocated hash table entry capacity per shard.
	pub initial_shard_capacity: usize,
	/// Maximum number of records evicted in a single critical section.
	pub eviction_burst_limit: usize,
}

impl Default for EngineConfig {
	fn default() -> Self {
		Self {
			num_shards: 64,
			max_memory_bytes: 512 * 1024 * 1024,
			initial_shard_capacity: 8192,
			eviction_burst_limit: 128,
		}
	}
}

impl ServerConfig {
	/// Validates and constructs [`ServerConfig`] from parsed command-line arguments.
	///
	/// # Errors
	///
	/// Returns an error string if:
	/// * `shards` is zero or not a power of two.
	/// * `max_memory` contains invalid numerical values or suffixes.
	/// * Watermark ratios are outside `(0.0, 1.0]`.
	/// * `low_watermark` is greater than or equal to `high_watermark`.
	pub fn from_args(args: &CliArgs) -> Result<Self, String> {
		let max_memory_bytes = parse_memory_string(&args.max_memory)?;

		if args.shards == 0 || !args.shards.is_power_of_two() {
			return Err("Shard count must be non-zero and a power of two (e.g. 16, 32, 64)".into());
		}

		if !(0.01..=1.0).contains(&args.high_watermark) {
			return Err("High watermark must be within the range (0.01, 1.0]".into());
		}

		if !(0.01..=1.0).contains(&args.low_watermark) {
			return Err("Low watermark must be within the range (0.01, 1.0]".into());
		}

		if args.low_watermark >= args.high_watermark {
			return Err("Low watermark must be strictly less than high watermark".into());
		}

		Ok(Self {
			bind_addr: args.bind_addr,
			sweep_interval_ms: args.sweep_interval_ms,
			high_watermark: args.high_watermark,
			low_watermark: args.low_watermark,
			flush_threshold_bytes: args.flush_threshold_bytes,
			shutdown_timeout_secs: args.shutdown_timeout_secs,
			engine: EngineConfig {
				num_shards: args.shards,
				max_memory_bytes,
				initial_shard_capacity: args.initial_shard_capacity,
				eviction_burst_limit: args.eviction_burst_limit,
			},
		})
	}
}

/// Parses human-readable memory strings (e.g., `"512MB"`, `"2GB"`, `"1048576"`) into bytes.
///
/// Supported units (case-insensitive): `B`, `K`, `KB`, `M`, `MB`, `G`, `GB`.
///
/// # Errors
///
/// Returns an error string if the format is invalid, unit suffix is unrecognized,
/// or parsed size overflows the system architecture word size (`usize`).
pub fn parse_memory_string(input: &str) -> Result<usize, String> {
	let trimmed = input.trim();
	if trimmed.is_empty() {
		return Err("Memory limit cannot be empty".into());
	}

	let (num_part, unit_part) = match trimmed.find(|c: char| c.is_alphabetic()) {
		Some(idx) => (&trimmed[..idx], &trimmed[idx..]),
		None => (trimmed, ""),
	};

	let base: usize = num_part
		.parse()
		.map_err(|_| format!("Invalid memory numeric value: '{num_part}'"))?;

	let unit = unit_part.to_ascii_uppercase();
	let multiplier: usize = match unit.as_str() {
		"" | "B" => 1,
		"K" | "KB" => 1024,
		"M" | "MB" => 1024 * 1024,
		"G" | "GB" => 1024 * 1024 * 1024,
		other => return Err(format!("Unknown memory unit suffix: '{other}'")),
	};

	base
		.checked_mul(multiplier)
		.ok_or_else(|| format!("Memory limit '{input}' exceeds maximum addressable usize"))
}
