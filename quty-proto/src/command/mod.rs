mod opcode;
mod operation;

pub use opcode::Opcode;
pub use operation::Operation;

/// Top-level decoded client request_old dispatched by the net connection loop.
///
/// Encapsulates transport-level administrative operations and storage-targeted
/// requests (both isolated single operations and compound atomic batches).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
	// -------------------------------------------------------------------------
	// Transport & Diagnostics (Zero Shard Locking)
	// -------------------------------------------------------------------------

	/// Connection health check and roundtrip latency validation.
	///
	/// Returns immediately without touching cache partitions.
	Ping,

	/// Aggregate engine runtime metrics (hits, misses, memory, uptime).
	///
	/// Evaluated across lock-free atomic counters.
	Stats,

	/// Drops all keys and zeroes payload allocations across all shards.
	///
	/// Iterates through every shard and takes exclusive write locks sequentially.
	Reset,

	// -------------------------------------------------------------------------
	// Storage Engine Actions
	// -------------------------------------------------------------------------

	/// A standalone key-value operation targeting a single shard.
	Single(Operation),

	/// An ordered sequence of operations executed atomically under a single shard lock.
	///
	/// # Sharding Invariant
	/// Every operation inside `Batch` **must** produce the exact same `routing_key()`
	/// prefix hash. If an operation targets a diverging shard, the engine rejects
	/// the batch with [`StatusCode::CrossShardViolation`] prior to execution.
	Batch(Vec<Operation>),
}

impl Command {
	/// Maps the top-level datatype variant to its frozen 1-byte wire opcode.
	#[inline]
	pub fn opcode(&self) -> Opcode {
		match self {
			Self::Ping => Opcode::Ping,
			Self::Stats => Opcode::Stats,
			Self::Reset => Opcode::Reset,
			Self::Single(op) => op.opcode(),
			Self::Batch(_) => Opcode::Batch,
		}
	}
}