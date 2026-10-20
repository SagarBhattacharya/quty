//! Response framing, status codes, payload variants, and deserialization codecs.
//!
//! This module defines the server-to-client response layout for Quty.
//! Every server reply starts with a fixed 20-byte [`ResponseHeader`] followed
//! by a variable-length [`ResponsePayload`].

use crate::constants::{PROTOCOL_MAGIC, PROTOCOL_VERSION_V1, RESPONSE_HEADER_SIZE};
use crate::to_u32_len;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Operational outcome and error discriminator returned in every response header.
///
/// Status codes are partitioned into:
/// - **Success** (`0x00..=0x02`): Non-error completions.
/// - **Soft Errors** (`0x10..=0x11`): Read misses (do not abort atomic batches).
/// - **Hard Errors** (`0x12..=0x44`): Precondition failures, capacity walls, and protocol errors (abort batches and trigger rollback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StatusCode {
	/// Command processed successfully (`0x00`).
	Ok = 0x00,
	/// Record newly created and inserted into storage (`0x01`).
	Created = 0x01,
	/// Modification applied without returning payload data (`0x02`).
	NoContent = 0x02,
	/// Targeted key does not exist or has already expired (`0x10`).
	KeyNotFound = 0x10,
	/// Targeted key reached its TTL deadline and was lazily purged (`0x11`).
	KeyExpired = 0x11,
	/// Conditional insertion (`ADD`) rejected because key is already present (`0x12`).
	KeyExists = 0x12,
	/// Conditional update (`REPLACE`) rejected because key is absent (`0x13`).
	PreconditionFailed = 0x13,
	/// Storage limit reached; eviction could not reclaim required bytes (`0x20`).
	StorageFull = 0x20,
	/// Key size exceeded the 64 KB limit (`0x30`).
	KeyTooLarge = 0x30,
	/// Value size exceeded the 32 MB limit (`0x31`).
	ValueTooLarge = 0x31,
	/// Multi-key batch contained keys resolving to disparate shards (`0x32`).
	CrossShardViolation = 0x32,
	/// Batch command contained zero executable operations (`0x33`).
	EmptyBatch = 0x33,
	/// Batch aborted prior to completion due to a hard error on a sub-operation (`0x34`).
	BatchShortCircuited = 0x34,
	/// Malformed framing, invalid magic, or unparseable payload structure (`0x40`).
	BadRequest = 0x40,
	/// Unhandled engine invariant violation or internal failure (`0x41`).
	InternalError = 0x41,
	/// Unassigned opcode or command identifier encountered (`0x42`).
	UnknownOpcode = 0x42,
	/// Attempted to nest a batch command inside an existing batch (`0x43`).
	NestedBatchNotAllowed = 0x43,
	/// Server is saturated or undergoing graceful shutdown drain (`0x44`).
	ServerBusy = 0x44,
}

impl StatusCode {
	/// Returns `true` if the status code represents an operational success.
	#[inline]
	#[must_use]
	pub const fn is_success(&self) -> bool {
		matches!(self, Self::Ok | Self::Created | Self::NoContent)
	}

	/// Returns `true` if the status code is a non-aborting cache miss (`KeyNotFound` or `KeyExpired`).
	#[inline]
	#[must_use]
	pub const fn is_soft_error(&self) -> bool {
		matches!(self, Self::KeyNotFound | Self::KeyExpired)
	}

	/// Returns `true` if the status represents a hard failure condition.
	#[inline]
	#[must_use]
	pub const fn is_hard_error(&self) -> bool {
		!self.is_success() && !self.is_soft_error()
	}

	/// Returns `true` if this status encountered in a batch should trigger transaction rollback.
	#[inline]
	#[must_use]
	pub const fn should_abort_batch(&self) -> bool {
		self.is_hard_error()
	}
}

impl TryFrom<u8> for StatusCode {
	type Error = u8;

	/// Attempts to parse an 8-bit wire identifier into a [`StatusCode`].
	///
	/// # Errors
	///
	/// Returns the unrecognized byte value if it does not map to a recognized status code.
	fn try_from(val: u8) -> Result<Self, Self::Error> {
		match val {
			0x00 => Ok(Self::Ok),
			0x01 => Ok(Self::Created),
			0x02 => Ok(Self::NoContent),
			0x10 => Ok(Self::KeyNotFound),
			0x11 => Ok(Self::KeyExpired),
			0x12 => Ok(Self::KeyExists),
			0x13 => Ok(Self::PreconditionFailed),
			0x20 => Ok(Self::StorageFull),
			0x30 => Ok(Self::KeyTooLarge),
			0x31 => Ok(Self::ValueTooLarge),
			0x32 => Ok(Self::CrossShardViolation),
			0x33 => Ok(Self::EmptyBatch),
			0x34 => Ok(Self::BatchShortCircuited),
			0x40 => Ok(Self::BadRequest),
			0x41 => Ok(Self::InternalError),
			0x42 => Ok(Self::UnknownOpcode),
			0x43 => Ok(Self::NestedBatchNotAllowed),
			0x44 => Ok(Self::ServerBusy),
			other => Err(other),
		}
	}
}

/// 1-byte wire identifier representing the payload serialization type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PayloadType {
	/// Zero-length empty payload (`0x00`).
	Null = 0x00,
	/// 1-byte boolean flag (`0x01`).
	Bool = 0x01,
	/// 8-byte big-endian signed integer (`0x02`).
	Int64 = 0x02,
	/// UTF-8 encoded text string (`0x03`).
	String = 0x03,
	/// Opaque binary byte sequence (`0x04`).
	Raw = 0x04,
	/// Fixed 64-byte server statistics struct (`0x05`).
	Stats = 0x05,
	/// Array of sub-results from an atomic batch transaction (`0x06`).
	Array = 0x06,
	/// Failure index and abort status for a short-circuited batch (`0x07`).
	BatchErr = 0x07,
}

impl TryFrom<u8> for PayloadType {
	type Error = u8;

	/// Attempts to parse an 8-bit wire identifier into a [`PayloadType`].
	///
	/// # Errors
	///
	/// Returns the unrecognized byte value if it does not map to a recognized payload type.
	fn try_from(val: u8) -> Result<Self, Self::Error> {
		match val {
			0x00 => Ok(Self::Null),
			0x01 => Ok(Self::Bool),
			0x02 => Ok(Self::Int64),
			0x03 => Ok(Self::String),
			0x04 => Ok(Self::Raw),
			0x05 => Ok(Self::Stats),
			0x06 => Ok(Self::Array),
			0x07 => Ok(Self::BatchErr),
			other => Err(other),
		}
	}
}

/// The result of an individual sub-operation inside an atomic batch response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubResult {
	/// Individual completion status for this operation.
	pub status: StatusCode,
	/// Output payload for this operation.
	pub payload: ResponsePayload,
}

/// Failure metadata returned when an atomic batch transaction short-circuits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchErrorPayload {
	/// Zero-based index of the sub-operation that caused the abort.
	pub failed_index: u32,
	/// The hard status code that triggered the abort.
	pub failure_code: StatusCode,
}

/// Fixed 64-byte telemetry payload returned by the `STATS` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsPayload {
	/// Server process uptime in seconds.
	pub uptime_seconds: u64,
	/// Lifetime total TCP connections accepted.
	pub total_connections: u64,
	/// Active open client connections.
	pub active_connections: u64,
	/// Total live unexpired keys across all shards.
	pub total_keys: u64,
	/// Total tracked payload and metadata memory in bytes.
	pub used_memory_bytes: u64,
	/// Total successful cache hits.
	pub get_hits: u64,
	/// Total cache lookups resulting in misses.
	pub get_misses: u64,
	/// Total entries evicted due to memory pressure.
	pub evictions: u64,
}

impl StatsPayload {
	/// Encodes all 8 counter fields directly into `dst` as 64-bit big-endian integers.
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u64(self.uptime_seconds);
		dst.put_u64(self.total_connections);
		dst.put_u64(self.active_connections);
		dst.put_u64(self.total_keys);
		dst.put_u64(self.used_memory_bytes);
		dst.put_u64(self.get_hits);
		dst.put_u64(self.get_misses);
		dst.put_u64(self.evictions);
	}

	/// Decodes a 64-byte statistics payload from the buffer.
	///
	/// # Errors
	///
	/// Returns [`ResponseDecodeError::MalformedPayload`] if fewer than 64 bytes remain.
	pub fn decode(src: &mut BytesMut) -> Result<Self, ResponseDecodeError> {
		if src.remaining() < 64 {
			return Err(ResponseDecodeError::MalformedPayload);
		}
		Ok(Self {
			uptime_seconds: src.get_u64(),
			total_connections: src.get_u64(),
			active_connections: src.get_u64(),
			total_keys: src.get_u64(),
			used_memory_bytes: src.get_u64(),
			get_hits: src.get_u64(),
			get_misses: src.get_u64(),
			evictions: src.get_u64(),
		})
	}
}

/// Variable-length response payload variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponsePayload {
	/// Empty response body (0 bytes).
	Null,
	/// 1-byte boolean representation (`0x01` = true, `0x00` = false).
	Bool(bool),
	/// 64-bit signed integer (TTL ms, existence counter, or timestamp).
	Int64(i64),
	/// UTF-8 encoded text string.
	String(String),
	/// Arbitrary binary bytes representing cached values.
	Raw(Bytes),
	/// Fixed 64-byte engine performance metrics.
	Stats(StatsPayload),
	/// Itemized results of an atomic batch sequence.
	Array(Vec<SubResult>),
	/// Transaction abort diagnostic information.
	BatchError(BatchErrorPayload),
}

impl ResponsePayload {
	/// Returns the 1-byte [`PayloadType`] wire discriminator for this payload.
	#[inline]
	#[must_use]
	pub const fn payload_type(&self) -> PayloadType {
		match self {
			Self::Null => PayloadType::Null,
			Self::Bool(_) => PayloadType::Bool,
			Self::Int64(_) => PayloadType::Int64,
			Self::String(_) => PayloadType::String,
			Self::Raw(_) => PayloadType::Raw,
			Self::Stats(_) => PayloadType::Stats,
			Self::Array(_) => PayloadType::Array,
			Self::BatchError(_) => PayloadType::BatchErr,
		}
	}

	/// Computes the exact wire length in bytes without performing allocations.
	#[must_use]
	pub fn encoded_len(&self) -> usize {
		match self {
			Self::Null => 0,
			Self::Bool(_) => 1,
			Self::Int64(_) => 8,
			Self::String(s) => s.len(),
			Self::Raw(bytes) => bytes.len(),
			Self::Stats(_) => 64,     // 8 * u64
			Self::BatchError(_) => 5, // failed_index (u32: 4B) + failure_code (u8: 1B)
			Self::Array(items) => {
				let mut len = 4; // u32 item count
				for item in items {
					// status (1B) + payload_type (1B) + item_len (4B) + payload
					len += 1 + 1 + 4 + item.payload.encoded_len();
				}
				len
			}
		}
	}

	/// Serializes payload contents directly into the buffer.
	pub fn encode(&self, dst: &mut BytesMut) {
		match self {
			Self::Null => {}
			Self::Bool(b) => dst.put_u8(u8::from(*b)),
			Self::Int64(v) => dst.put_i64(*v),
			Self::String(s) => dst.put_slice(s.as_bytes()),
			Self::Raw(bytes) => dst.put_slice(bytes),
			Self::Stats(stats) => stats.encode(dst),
			Self::BatchError(err) => {
				dst.put_u32(err.failed_index);
				dst.put_u8(err.failure_code as u8);
			}
			Self::Array(items) => {
				dst.put_u32(to_u32_len(items.len()));
				for item in items {
					dst.put_u8(item.status as u8);
					dst.put_u8(item.payload.payload_type() as u8);
					dst.put_u32(to_u32_len(item.payload.encoded_len()));
					item.payload.encode(dst);
				}
			}
		}
	}

	/// Deserializes a payload based on the declared [`PayloadType`].
	///
	/// # Errors
	///
	/// Returns [`ResponseDecodeError`] if payload length is malformed, unrecognized
	/// status/type codes are encountered, or strings contain invalid UTF-8.
	pub fn decode(
		payload_type: PayloadType,
		src: &mut BytesMut,
	) -> Result<Self, ResponseDecodeError> {
		match payload_type {
			PayloadType::Null => Ok(Self::Null),
			PayloadType::Bool => {
				if src.remaining() < 1 {
					return Err(ResponseDecodeError::MalformedPayload);
				}
				Ok(Self::Bool(src.get_u8() != 0))
			}
			PayloadType::Int64 => {
				if src.remaining() < 8 {
					return Err(ResponseDecodeError::MalformedPayload);
				}
				Ok(Self::Int64(src.get_i64()))
			}
			PayloadType::String => {
				let s = std::str::from_utf8(&src[..])
					.map_err(|_| ResponseDecodeError::InvalidUtf8)?
					.to_owned();
				src.clear();
				Ok(Self::String(s))
			}
			PayloadType::Raw => Ok(Self::Raw(src.split().freeze())),
			PayloadType::Stats => Ok(Self::Stats(StatsPayload::decode(src)?)),
			PayloadType::BatchErr => {
				if src.remaining() < 5 {
					return Err(ResponseDecodeError::MalformedPayload);
				}
				let failed_index = src.get_u32();
				let code_raw = src.get_u8();
				let failure_code = StatusCode::try_from(code_raw)
					.map_err(|_| ResponseDecodeError::UnknownStatusCode(code_raw))?;
				Ok(Self::BatchError(BatchErrorPayload {
					failed_index,
					failure_code,
				}))
			}
			PayloadType::Array => {
				if src.remaining() < 4 {
					return Err(ResponseDecodeError::MalformedPayload);
				}
				let count = src.get_u32() as usize;
				let mut results = Vec::with_capacity(count);

				for _ in 0..count {
					if src.remaining() < 6 {
						return Err(ResponseDecodeError::MalformedPayload);
					}
					let status_raw = src.get_u8();
					let status = StatusCode::try_from(status_raw)
						.map_err(|_| ResponseDecodeError::UnknownStatusCode(status_raw))?;

					let pt_raw = src.get_u8();
					let pt = PayloadType::try_from(pt_raw)
						.map_err(|_| ResponseDecodeError::UnknownPayloadType(pt_raw))?;

					let item_len = src.get_u32() as usize;
					if src.remaining() < item_len {
						return Err(ResponseDecodeError::MalformedPayload);
					}

					let mut item_bytes = src.split_to(item_len);
					let payload = Self::decode(pt, &mut item_bytes)?;
					results.push(SubResult { status, payload });
				}
				Ok(Self::Array(results))
			}
		}
	}
}

/// Fixed 20-byte header preceding every outbound server response frame.
///
/// # Binary Wire Layout (Network Byte Order / Big-Endian)
///
/// ```text
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                     Magic Identifier (0x51555459)             |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |        Version (0x0001)       |  Status (u8)  | PayloadT (u8) |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                   Request ID (Bytes 0..=3)                    |
/// |                   Request ID (Bytes 4..=7)                    |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                  Payload Length in Bytes (u32)                |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
	/// Protocol magic bytes. Must equal [`PROTOCOL_MAGIC`] (`0x51555459` / `"QUTY"`).
	pub magic: u32,
	/// Protocol version. Must equal [`PROTOCOL_VERSION_V1`] (`0x0001`).
	pub version: u16,
	/// Overall execution status code.
	pub status: StatusCode,
	/// Declared payload encoding format.
	pub payload_type: PayloadType,
	/// Correlated request sequence ID echoed from the client request.
	pub request_id: u64,
	/// Length in bytes of the payload trailing this 20-byte header.
	pub payload_len: u32,
}

impl ResponseHeader {
	/// Serializes the header into `dst` using big-endian byte order.
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u32(self.magic);
		dst.put_u16(self.version);
		dst.put_u8(self.status as u8);
		dst.put_u8(self.payload_type as u8);
		dst.put_u64(self.request_id);
		dst.put_u32(self.payload_len);
	}

	/// Deserializes a 20-byte response header from the buffer.
	///
	/// # Errors
	///
	/// Returns [`ResponseDecodeError`] if buffer is incomplete, magic or version
	/// are invalid, or status/payload enum codes are unknown.
	pub fn decode(src: &mut BytesMut) -> Result<Self, ResponseDecodeError> {
		if src.remaining() < RESPONSE_HEADER_SIZE {
			return Err(ResponseDecodeError::IncompleteHeader);
		}

		let magic = src.get_u32();
		if magic != PROTOCOL_MAGIC {
			return Err(ResponseDecodeError::InvalidMagic(magic));
		}

		let version = src.get_u16();
		if version != PROTOCOL_VERSION_V1 {
			return Err(ResponseDecodeError::UnsupportedVersion(version));
		}

		let raw_status = src.get_u8();
		let status = StatusCode::try_from(raw_status)
			.map_err(|_| ResponseDecodeError::UnknownStatusCode(raw_status))?;

		let raw_pt = src.get_u8();
		let payload_type =
			PayloadType::try_from(raw_pt).map_err(|_| ResponseDecodeError::UnknownPayloadType(raw_pt))?;

		let request_id = src.get_u64();
		let payload_len = src.get_u32();

		Ok(Self {
			magic,
			version,
			status,
			payload_type,
			request_id,
			payload_len,
		})
	}
}

/// A complete server response frame containing header metadata and payload body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
	/// The fixed 20-byte frame header.
	pub header: ResponseHeader,
	/// The body payload corresponding to the operation outcome.
	pub payload: ResponsePayload,
}

impl Response {
	/// Constructs a new [`Response`], computing `payload_len` immediately.
	#[must_use]
	pub fn new(request_id: u64, status: StatusCode, payload: ResponsePayload) -> Self {
		let payload_len = to_u32_len(payload.encoded_len());
		Self {
			header: ResponseHeader {
				magic: PROTOCOL_MAGIC,
				version: PROTOCOL_VERSION_V1,
				status,
				payload_type: payload.payload_type(),
				request_id,
				payload_len,
			},
			payload,
		}
	}

	/// Encodes header and payload directly into `dst` without intermediate heap allocations.
	pub fn encode(&self, dst: &mut BytesMut) {
		let mut header = self.header;
		header.payload_len = to_u32_len(self.payload.encoded_len());
		header.encode(dst);
		self.payload.encode(dst);
	}

	/// Resumable streaming decoder for incoming server response frames.
	///
	/// Reads from `src` without consuming partial bytes:
	/// * If fewer bytes than a full frame are available, leaves `src` untouched and returns `Ok(None)`.
	/// * If a full frame is present, consumes exactly one frame and returns `Ok(Some(response))`.
	///
	/// # Errors
	///
	/// Returns [`ResponseDecodeError`] if wire bytes violate framing invariants or bounds.
	pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ResponseDecodeError> {
		if src.len() < RESPONSE_HEADER_SIZE {
			return Ok(None);
		}

		let payload_len = u32::from_be_bytes([src[16], src[17], src[18], src[19]]) as usize;
		let total_frame_len = RESPONSE_HEADER_SIZE + payload_len;

		if src.len() < total_frame_len {
			return Ok(None);
		}

		let header = ResponseHeader::decode(src)?;
		let mut payload_bytes = src.split_to(payload_len);
		let payload = ResponsePayload::decode(header.payload_type, &mut payload_bytes)?;

		Ok(Some(Self { header, payload }))
	}
}

/// Errors that can occur when decoding a server response frame from the wire.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ResponseDecodeError {
	/// Insufficient bytes to construct a 20-byte [`ResponseHeader`].
	#[error("Header bytes incomplete")]
	IncompleteHeader,
	/// Magic identifier did not match [`PROTOCOL_MAGIC`].
	#[error("Invalid magic byte: 0x{0:08X}")]
	InvalidMagic(u32),
	/// Frame version did not match [`PROTOCOL_VERSION_V1`].
	#[error("Unsupported protocol version: {0}")]
	UnsupportedVersion(u16),
	/// Unassigned status code identifier encountered.
	#[error("Unknown status code: 0x{0:02X}")]
	UnknownStatusCode(u8),
	/// Unassigned payload type identifier encountered.
	#[error("Unknown payload type: 0x{0:02X}")]
	UnknownPayloadType(u8),
	/// Payload length or structural delimiters were malformed.
	#[error("Payload frame format was malformed")]
	MalformedPayload,
	/// String payload failed valid UTF-8 conversion.
	#[error("String payload contained invalid UTF-8 bytes")]
	InvalidUtf8,
}
