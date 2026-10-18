#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StatusCode {
	// =========================================================================
	// Success (0x00 - 0x0F)
	// =========================================================================
	/// Generic success; payload contains requested data (Get, Ttl, Stats, Ping).
	Ok                  = 0x00,

	/// Key was newly created (Add, or Set on nonexistent key).
	Created             = 0x01,

	/// Operation completed successfully with no response body (Delete, Expire, Persist, Reset).
	NoContent           = 0x02,

	// =========================================================================
	// Client Failures & Precondition Errors (0x10 - 0x2F)
	// =========================================================================
	/// Malformed binary frame, invalid flags, or truncated header.
	BadRequest          = 0x10,

	/// Key does not exist in the shard (Get, Delete, Replace, Expire, Persist).
	KeyNotFound         = 0x11,

	/// Key already exists (Conditional Add failed).
	KeyExists           = 0x12,

	/// Key was found but its TTL has expired (distinguishes lazy expiration from never-existed).
	KeyExpired          = 0x13,

	/// Unrecognized request_old opcode.
	UnknownOpcode       = 0x14,

	/// Key exceeds maximum allowed length of 64 KB (65,536 bytes).
	KeyTooLarge         = 0x15,

	/// Value exceeds maximum allowed length of 32 MB (33,554,432 bytes).
	ValueTooLarge       = 0x16,

	/// Target key cannot accept this operation (e.g., trying to persist a non-expiring key).
	PreconditionFailed  = 0x17,

	// =========================================================================
	// Batch & Routing Specific Errors (0x30 - 0x3F)
	// =========================================================================
	/// Sub-keys in a Batch route to different shards (prefix mismatch).
	CrossShardViolation = 0x30,

	/// Nested Batch opcode detected within a Batch frame (strictly forbidden).
	NestedBatchNotAllowed = 0x31,

	/// Batch execution aborted midway due to a sub-request_old error.
	BatchShortCircuited = 0x32,

	/// Empty batch received (sub-request_old count is 0).
	EmptyBatch          = 0x33,

	// =========================================================================
	// Server Invariants & System Failures (0x40 - 0x4F)
	// =========================================================================
	/// Out of memory: shard reached memory ceiling and Sampled LRU eviction failed to free space.
	StorageFull         = 0x40,

	/// Server is saturated or shed connection under heavy backpressure.
	ServerBusy          = 0x41,

	/// Unrecoverable internal failure (e.g. poisoned lock, unhandled panic).
	InternalError       = 0x50,
}

impl TryFrom<u8> for StatusCode {
	type Error = u8;

	#[inline]
	fn try_from(val: u8) -> Result<Self, Self::Error> {
		match val {
			0x00 => Ok(Self::Ok),
			0x01 => Ok(Self::Created),
			0x02 => Ok(Self::NoContent),
			0x10 => Ok(Self::BadRequest),
			0x11 => Ok(Self::KeyNotFound),
			0x12 => Ok(Self::KeyExists),
			0x13 => Ok(Self::KeyExpired),
			0x14 => Ok(Self::UnknownOpcode),
			0x15 => Ok(Self::KeyTooLarge),
			0x16 => Ok(Self::ValueTooLarge),
			0x17 => Ok(Self::PreconditionFailed),
			0x30 => Ok(Self::CrossShardViolation),
			0x31 => Ok(Self::NestedBatchNotAllowed),
			0x32 => Ok(Self::BatchShortCircuited),
			0x33 => Ok(Self::EmptyBatch),
			0x40 => Ok(Self::StorageFull),
			0x41 => Ok(Self::ServerBusy),
			0x50 => Ok(Self::InternalError),
			unknown => Err(unknown),
		}
	}
}