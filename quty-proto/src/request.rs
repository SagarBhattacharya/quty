//! Request framing, fixed headers, and deserialization state machines.
//!
//! This module handles the encoding and decoding of inbound client requests.
//! Every request begins with a fixed 20-byte [`RequestHeader`] followed by
//! an optional variable-length command payload.

use crate::command::{Command, CommandType, Operation, OperationDecodeError};
use crate::constants::{
	MAX_BATCH_OPERATIONS, MAX_PAYLOAD_SIZE, PROTOCOL_MAGIC, PROTOCOL_VERSION_V1, REQUEST_HEADER_SIZE,
};
use crate::to_u32_len;
use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

/// Fixed 20-byte header preceding every inbound client request frame.
///
/// # Binary Wire Layout (Network Byte Order / Big-Endian)
///
/// ```text
///  0                   1                   2                   3
///  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                     Magic Identifier (0x51555459)             |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |        Version (0x0001)       |  Cmd Type (u8)|   Flags (u8)  |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                   Request ID (Bytes 0..=3)                    |
/// |                   Request ID (Bytes 4..=7)                    |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// |                  Payload Length in Bytes (u32)                |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
	/// Protocol magic bytes. Must equal [`PROTOCOL_MAGIC`] (`0x51555459` / `"QUTY"`).
	pub magic: u32,
	/// Protocol version. Must equal [`PROTOCOL_VERSION_V1`] (`0x0001`).
	pub version: u16,
	/// High-level command category discriminator.
	pub command_type: CommandType,
	/// Reserved configuration flags (currently unused; must be `0x00`).
	pub flags: u8,
	/// Client-assigned opaque sequence identifier, mirrored back in the response.
	pub request_id: u64,
	/// Length in bytes of the command payload trailing this 20-byte header.
	pub payload_len: u32,
}

impl RequestHeader {
	/// Serializes the header into the destination buffer in big-endian byte order.
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u32(self.magic);
		dst.put_u16(self.version);
		dst.put_u8(self.command_type as u8);
		dst.put_u8(self.flags);
		dst.put_u64(self.request_id);
		dst.put_u32(self.payload_len);
	}

	/// Deserializes a 20-byte request header from the buffer.
	///
	/// # Errors
	///
	/// * [`RequestDecodeError::IncompleteHeader`]: Fewer than 20 bytes remain.
	/// * [`RequestDecodeError::InvalidMagic`]: Magic bytes do not match `'QUTY'`.
	/// * [`RequestDecodeError::UnsupportedVersion`]: Protocol version is not `1`.
	/// * [`RequestDecodeError::UnknownCommandType`]: Command type identifier is unrecognized.
	pub fn decode(src: &mut BytesMut) -> Result<Self, RequestDecodeError> {
		if src.remaining() < REQUEST_HEADER_SIZE {
			return Err(RequestDecodeError::IncompleteHeader);
		}

		let magic = src.get_u32();
		if magic != PROTOCOL_MAGIC {
			return Err(RequestDecodeError::InvalidMagic(magic));
		}

		let version = src.get_u16();
		if version != PROTOCOL_VERSION_V1 {
			return Err(RequestDecodeError::UnsupportedVersion(version));
		}

		let raw_type = src.get_u8();
		let command_type = CommandType::try_from(raw_type)
			.map_err(|_| RequestDecodeError::UnknownCommandType(raw_type))?;

		let flags = src.get_u8();
		let request_id = src.get_u64();
		let payload_len = src.get_u32();

		Ok(Self {
			magic,
			version,
			command_type,
			flags,
			request_id,
			payload_len,
		})
	}
}

/// A parsed client request containing tracking metadata and the target command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
	/// Unique correlation identifier for pipelining and multiplexing.
	pub request_id: u64,
	/// The executable command payload.
	pub command: Command,
}

impl Request {
	/// Constructs a new [`Request`] with the specified request ID and command.
	pub fn new(request_id: u64, command: Command) -> Self {
		Self {
			request_id,
			command,
		}
	}

	/// Serializes this request directly into `dst` without intermediate heap allocations.
	pub fn encode(&self, dst: &mut BytesMut) {
		let header = RequestHeader {
			magic: PROTOCOL_MAGIC,
			version: PROTOCOL_VERSION_V1,
			command_type: self.command.command_type(),
			flags: 0,
			request_id: self.request_id,
			payload_len: to_u32_len(self.command.encoded_len()),
		};

		header.encode(dst);
		self.command.encode(dst);
	}

	/// Resumable streaming decoder for incoming client frames.
	///
	/// Reads from `src` without consuming partial bytes:
	/// * If fewer bytes than a full frame are available, leaves `src` untouched and returns `Ok(None)`.
	/// * If a full frame is present, consumes exactly one frame and returns `Ok(Some(request))`.
	///
	/// # Errors
	///
	/// Returns [`RequestDecodeError`] if header fields, magic identifiers, or
	/// command payload formats violate wire protocol specifications.
	pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, RequestDecodeError> {
		if src.len() < REQUEST_HEADER_SIZE {
			return Ok(None);
		}

		let flags = u8::from_be_bytes([src[7]]);
		if flags != 0 {
			return Err(RequestDecodeError::InvalidFlags(flags));
		}

		// Peek payload length from bytes 16..20 without advancing buffer cursor
		let payload_len = u32::from_be_bytes([src[16], src[17], src[18], src[19]]) as usize;
		if payload_len > MAX_PAYLOAD_SIZE {
			return Err(RequestDecodeError::PayloadTooLarge(
				payload_len,
				MAX_PAYLOAD_SIZE,
			));
		}

		let total_frame_len = REQUEST_HEADER_SIZE + payload_len;
		if src.len() < total_frame_len {
			return Ok(None);
		}

		let header = RequestHeader::decode(src)?;
		let mut payload = src.split_to(payload_len);

		let command = match header.command_type {
			CommandType::Ping => {
				if payload_len != 0 {
					return Err(RequestDecodeError::UnexpectedPayload);
				}
				Command::Ping
			}
			CommandType::Stats => {
				if payload_len != 0 {
					return Err(RequestDecodeError::UnexpectedPayload);
				}
				Command::Stats
			}
			CommandType::Reset => {
				if payload_len != 0 {
					return Err(RequestDecodeError::UnexpectedPayload);
				}
				Command::Reset
			}
			CommandType::Single => {
				let op = Operation::decode(&mut payload)?;
				if payload.has_remaining() {
					return Err(RequestDecodeError::TrailingBytes);
				}
				Command::Single(op)
			}
			CommandType::Batch => {
				if payload.remaining() < 4 {
					return Err(RequestDecodeError::MalformedPayload);
				}
				let op_count = payload.get_u32() as usize;
				if op_count > MAX_BATCH_OPERATIONS {
					return Err(RequestDecodeError::BatchTooLarge(op_count));
				}

				let mut ops = Vec::with_capacity(op_count);
				for _ in 0..op_count {
					let op = Operation::decode(&mut payload)?;
					ops.push(op);
				}

				if payload.has_remaining() {
					return Err(RequestDecodeError::TrailingBytes);
				}
				Command::Batch(ops)
			}
		};

		Ok(Some(Self {
			request_id: header.request_id,
			command,
		}))
	}
}

/// Errors that can occur when decoding a client request frame from the wire.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RequestDecodeError {
	/// Insufficient bytes to construct a 20-byte [`RequestHeader`].
	#[error("Header bytes incomplete")]
	IncompleteHeader,
	/// Magic identifier did not match [`PROTOCOL_MAGIC`].
	#[error("Invalid protocol magic byte: 0x{0:08X}")]
	InvalidMagic(u32),
	/// Frame version did not match [`PROTOCOL_VERSION_V1`].
	#[error("Unsupported protocol version: {0}")]
	UnsupportedVersion(u16),
	/// Unassigned command type identifier encountered.
	#[error("Unknown command type: 0x{0:02X}")]
	UnknownCommandType(u8),
	/// Payload length or structural delimiters were malformed.
	#[error("Payload frame format was malformed")]
	MalformedPayload,
	/// Batch command exceeded [`MAX_BATCH_OPERATIONS`].
	#[error("Batch operations count ({0}) exceeded limit")]
	BatchTooLarge(usize),
	/// Underlying sub-operation decoding failure.
	#[error("{0}")]
	OpError(#[from] OperationDecodeError),
	/// Flags must be 0x00 for Quty Protocol v1
	#[error("Reserved flag bits must be 0x00, got: 0x{0:02X}")]
	InvalidFlags(u8),
	/// Payload exceeded [`MAX_PAYLOAD_SIZE`]
	#[error("Payload size ({0} B) exceeds maximum limit ({1} B)")]
	PayloadTooLarge(usize, usize),
	/// Unexpected Payload (returned by PING/STATS/RESET if any payload present)
	#[error("Unexpected payload present for zero-argument command")]
	UnexpectedPayload,
	/// Reported payload length does not match consumed payload
	#[error("Payload length mismatch: unparsed trailing bytes remaining")]
	TrailingBytes,
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::constants::MAX_BATCH_OPERATIONS;
	use bytes::BytesMut;

	fn make_test_batch_bytes(count: usize) -> BytesMut {
		let mut buf = BytesMut::new();
		let mut ops = Vec::with_capacity(count);
		for i in 0..count {
			ops.push(Operation::Get {
				key: format!("s00:k:{i}"),
			});
		}

		let request = Request::new(1, Command::Batch(ops));
		request.encode(&mut buf);
		buf
	}

	#[test]
	fn test_batch_boundary_allowed() {
		// Exactly at max limit: 64 operations
		let mut valid_buf = make_test_batch_bytes(MAX_BATCH_OPERATIONS);
		let decoded = Request::decode(&mut valid_buf);
		assert!(
			decoded.is_ok(),
			"Batch of exactly MAX_BATCH_SIZE should decode successfully"
		);
	}

	#[test]
	fn test_batch_boundary_exceeded_rejected_early() {
		// One over the limit: 65 operations
		let mut oversized_buf = make_test_batch_bytes(MAX_BATCH_OPERATIONS + 1);
		let decoded = Request::decode(&mut oversized_buf);
		assert!(
			decoded.is_err(),
			"Batch of MAX_BATCH_SIZE + 1 must be rejected during protocol decoding"
		);
	}

	#[test]
	fn test_reject_oversized_payload_len_early() {
		let mut buf = BytesMut::new();
		buf.put_u32(PROTOCOL_MAGIC);
		buf.put_u16(PROTOCOL_VERSION_V1);
		buf.put_u8(CommandType::Single as u8);
		buf.put_u8(0); // flags
		buf.put_u64(1); // req_id
		buf.put_u32(100 * 1024 * 1024); // 100 MB > 64 MB limit

		let err = Request::decode(&mut buf).expect_err("Must reject oversized payload");
		assert!(matches!(err, RequestDecodeError::PayloadTooLarge(..)));
	}

	#[test]
	fn test_reject_nonzero_reserved_flags() {
		let mut buf = BytesMut::new();
		buf.put_u32(PROTOCOL_MAGIC);
		buf.put_u16(PROTOCOL_VERSION_V1);
		buf.put_u8(CommandType::Ping as u8);
		buf.put_u8(0xFF); // Invalid non-zero flag
		buf.put_u64(1);
		buf.put_u32(0);

		let err = Request::decode(&mut buf).expect_err("Must reject non-zero flags");
		assert!(matches!(err, RequestDecodeError::InvalidFlags(0xFF)));
	}

	#[test]
	fn test_reject_unexpected_payload_for_ping() {
		let mut buf = BytesMut::new();
		buf.put_u32(PROTOCOL_MAGIC);
		buf.put_u16(PROTOCOL_VERSION_V1);
		buf.put_u8(CommandType::Ping as u8);
		buf.put_u8(0);
		buf.put_u64(1);
		buf.put_u32(8); // Non-zero payload for PING
		buf.put_u64(0xDEAD_BEEF);

		let err = Request::decode(&mut buf).expect_err("PING with payload must fail");
		assert_eq!(err, RequestDecodeError::UnexpectedPayload);
	}

	#[test]
	fn test_reject_trailing_bytes_in_single_op() {
		let mut buf = BytesMut::new();
		let op = Operation::Get { key: "k:1".into() };
		let req = Request::new(1, Command::Single(op));
		req.encode(&mut buf);

		// Artificially inflate payload_len by 4 bytes and add trailing junk
		let real_len = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
		buf[16..20].copy_from_slice(&(real_len + 4).to_be_bytes());
		buf.put_u32(0xCAFE_BABE);

		let err = Request::decode(&mut buf).expect_err("Trailing bytes must fail");
		assert_eq!(err, RequestDecodeError::TrailingBytes);
	}
}
