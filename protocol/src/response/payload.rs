use bytes::Bytes;
use crate::response::status::StatusCode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadType {
	/// No payload data follows (e.g. NoContent, Ping/Pong, simple booleans via StatusCode).
	Null    = 0x00,

	/// 1-byte boolean: 0x00 = false, 0x01 = true.
	Bool    = 0x01,

	/// 8-byte big-endian signed integer (`i64`). Used for TTL, counters, epochs.
	Int64   = 0x02,

	/// Validated UTF-8 text string (4-byte length prefix + bytes). Used for errors/diagnostics.
	String  = 0x03,

	/// Arbitrary binary blob (4-byte length prefix + raw bytes). Used for cached values.
	Raw     = 0x04,

	/// Ordered array of homogeneous or heterogeneous typed elements (2-byte count + elements).
	Array   = 0x05,

	/// Fixed-layout binary telemetry metrics block (exactly 64 bytes, zero map-parsing overhead).
	Stats   = 0x06,
}

impl TryFrom<u8> for PayloadType {
	type Error = u8;

	#[inline]
	fn try_from(val: u8) -> Result<Self, Self::Error> {
		match val {
			0x00 => Ok(Self::Null),
			0x01 => Ok(Self::Bool),
			0x02 => Ok(Self::Int64),
			0x03 => Ok(Self::String),
			0x04 => Ok(Self::Raw),
			0x05 => Ok(Self::Array),
			0x06 => Ok(Self::Stats),
			unknown => Err(unknown),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct StatsPayload {
	pub uptime_secs: u64,          // Uptime in seconds since engine launch
	pub total_connections: u64,    // Monotonically increasing connection count
	pub curr_connections: u64,     // Active client TCP sockets
	pub get_hits: u64,             // Successful cache reads
	pub get_misses: u64,           // Cache read misses (expired or missing)
	pub total_keys: u64,           // Aggregate active keys across all shards
	pub used_memory_bytes: u64,    // Total payload bytes currently held in RAM
	pub evictions: u64,            // Total keys evicted via Sampled LRU
}

/// Typed response payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponsePayload {
	Null,
	Bool(bool),
	Int64(i64),
	String(String),
	Raw(Bytes),
	Array(Vec<SubResult>),
	Stats(StatsPayload),
}

impl ResponsePayload {
	#[inline]
	pub fn payload_type(&self) -> PayloadType {
		match self {
			Self::Null => PayloadType::Null,
			Self::Bool(_) => PayloadType::Bool,
			Self::Int64(_) => PayloadType::Int64,
			Self::String(_) => PayloadType::String,
			Self::Raw(_) => PayloadType::Raw,
			Self::Array(_) => PayloadType::Array,
			Self::Stats(_) => PayloadType::Stats,
		}
	}
}

/// An individual sub-command result inside a Batch array response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubResult {
	pub status: StatusCode,
	pub payload: ResponsePayload,
}