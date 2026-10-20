//! Command models, storage operations, wire opcodes, and serialization codecs.
//!
//! This module defines the client-to-server command syntax tree for Quty,
//! covering single operations, atomic batches, server telemetry inspection,
//! and node health checks.

use crate::constants::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use crate::to_u32_len;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// High-level command category transmitted in the fixed 20-byte request header.
///
/// Determines how the server's framing decoder parses the variable-length payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CommandType {
	/// A single storage engine operation (e.g., `GET`, `SET`).
	Single = 0x01,
	/// An atomic batch transaction containing multiple operations with rollback guarantees.
	Batch = 0x02,
	/// Connection heartbeat check; responds with `StatusCode::Ok` and null payload.
	Ping = 0x03,
	/// Requests server telemetry counters and memory metrics.
	Stats = 0x04,
	/// Flushes all cached entries and resets statistics across all engine shards.
	Reset = 0x05,
}

impl TryFrom<u8> for CommandType {
	type Error = u8;

	/// Attempts to parse an 8-bit wire identifier into a [`CommandType`].
	///
	/// # Errors
	///
	/// Returns the unrecognized byte value if it does not match a known command type.
	fn try_from(val: u8) -> Result<Self, Self::Error> {
		match val {
			0x01 => Ok(Self::Single),
			0x02 => Ok(Self::Batch),
			0x03 => Ok(Self::Ping),
			0x04 => Ok(Self::Stats),
			0x05 => Ok(Self::Reset),
			other => Err(other),
		}
	}
}

/// 1-byte wire identifier representing a specific key-value storage operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
	/// Read the binary value associated with a key (`0x01`).
	Get = 0x01,
	/// Unconditionally write or overwrite a key-value pair (`0x02`).
	Set = 0x02,
	/// Conditionally store a key-value pair only if the key does not already exist (`0x03`).
	Add = 0x03,
	/// Conditionally update a key-value pair only if the key is already present (`0x04`).
	Replace = 0x04,
	/// Delete a key and its associated value (`0x05`).
	Delete = 0x05,
	/// Check for key existence without updating LRU or returning data (`0x06`).
	Exists = 0x06,
	/// Retrieve remaining time-to-live in milliseconds for a key (`0x07`).
	Ttl = 0x07,
	/// Set or update the expiration deadline for an existing key (`0x08`).
	Expire = 0x08,
	/// Remove any expiration deadline, persisting the key indefinitely (`0x09`).
	Persist = 0x09,
}

impl TryFrom<u8> for Opcode {
	type Error = u8;

	/// Attempts to parse an 8-bit wire identifier into an [`Opcode`].
	///
	/// # Errors
	///
	/// Returns the unrecognized byte value if it does not match a known opcode.
	fn try_from(val: u8) -> Result<Self, Self::Error> {
		match val {
			0x01 => Ok(Self::Get),
			0x02 => Ok(Self::Set),
			0x03 => Ok(Self::Add),
			0x04 => Ok(Self::Replace),
			0x05 => Ok(Self::Delete),
			0x06 => Ok(Self::Exists),
			0x07 => Ok(Self::Ttl),
			0x08 => Ok(Self::Expire),
			0x09 => Ok(Self::Persist),
			other => Err(other),
		}
	}
}

/// A discrete key-value operation executed against the storage engine.
///
/// Can be issued standalone via [`Command::Single`] or grouped inside [`Command::Batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
	/// Fetch the value associated with `key`.
	Get {
		/// Lookup key target.
		key: String,
	},
	/// Store `value` at `key`, overwriting any prior entry.
	Set {
		/// Storage key target.
		key: String,
		/// Binary payload bytes.
		value: Bytes,
	},
	/// Insert `value` at `key` only if `key` does not exist.
	Add {
		/// Storage key target.
		key: String,
		/// Binary payload bytes.
		value: Bytes,
	},
	/// Update `key` with `value` only if `key` is currently present.
	Replace {
		/// Storage key target.
		key: String,
		/// Binary payload bytes.
		value: Bytes,
	},
	/// Remove `key` and free associated payload memory.
	Delete {
		/// Target key to delete.
		key: String,
	},
	/// Verify whether `key` is present and unexpired.
	Exists {
		/// Target key to check.
		key: String,
	},
	/// Query remaining lifespan of `key` in milliseconds.
	Ttl {
		/// Target key to inspect.
		key: String,
	},
	/// Attach or update an expiration timer on `key`.
	Expire {
		/// Target key to set TTL on.
		key: String,
		/// Expiration window in milliseconds from now.
		ttl_ms: u64,
	},
	/// Strip any active expiration deadline from `key`, preventing eviction by TTL.
	Persist {
		/// Target key to make persistent.
		key: String,
	},
}

impl Operation {
	/// Returns the 1-byte wire [`Opcode`] corresponding to this operation.
	#[inline]
	pub const fn opcode(&self) -> Opcode {
		match self {
			Self::Get { .. } => Opcode::Get,
			Self::Set { .. } => Opcode::Set,
			Self::Add { .. } => Opcode::Add,
			Self::Replace { .. } => Opcode::Replace,
			Self::Delete { .. } => Opcode::Delete,
			Self::Exists { .. } => Opcode::Exists,
			Self::Ttl { .. } => Opcode::Ttl,
			Self::Expire { .. } => Opcode::Expire,
			Self::Persist { .. } => Opcode::Persist,
		}
	}

	/// Returns a borrowed slice of the key targeted by this operation.
	#[inline]
	pub fn key(&self) -> &str {
		match self {
			Self::Get { key }
			| Self::Set { key, .. }
			| Self::Add { key, .. }
			| Self::Replace { key, .. }
			| Self::Delete { key }
			| Self::Exists { key }
			| Self::Ttl { key }
			| Self::Expire { key, .. }
			| Self::Persist { key } => key.as_str(),
		}
	}

	/// Indicates whether this operation modifies storage engine state.
	///
	/// Read operations (`GET`, `EXISTS`, `TTL`) return `false`.
	/// Write and lifecycle operations (`SET`, `ADD`, `DELETE`, etc.) return `true`.
	#[inline]
	pub const fn is_mutation(&self) -> bool {
		!matches!(
			self,
			Self::Get { .. } | Self::Exists { .. } | Self::Ttl { .. }
		)
	}

	/// Returns the routing key for this operation according to prefix sharding rules.
	#[inline]
	#[must_use]
	pub fn routing_key(&self) -> &str {
		routing_key(self.key())
	}

	/// Encodes this sub-operation directly into a destination buffer.
	///
	/// # Wire Layout
	///
	/// ```text
	/// [Opcode: 1B][Key Length: 4B][Key Bytes: UTF-8]
	/// Optional:
	///   - Values: [Value Length: 4B][Value Bytes]
	///   - Expire: [TTL: 8B big-endian]
	/// ```
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u8(self.opcode() as u8);
		let key_bytes = self.key().as_bytes();
		dst.put_u32(to_u32_len(key_bytes.len()));
		dst.put_slice(key_bytes);

		match self {
			Self::Set { value, .. } | Self::Add { value, .. } | Self::Replace { value, .. } => {
				dst.put_u32(to_u32_len(value.len()));
				dst.put_slice(value);
			}
			Self::Expire { ttl_ms, .. } => {
				dst.put_u64(*ttl_ms);
			}
			_ => {}
		}
	}

	/// Returns the exact number of bytes this operation will occupy on the wire.
	#[inline]
	pub fn encoded_len(&self) -> usize {
		// opcode (1B) + key_len (4B) + key_bytes
		let base = 1 + 4 + self.key().len();
		match self {
			Self::Set { value, .. } | Self::Add { value, .. } | Self::Replace { value, .. } => {
				// val_len (4B) + value_bytes
				base + 4 + value.len()
			}
			Self::Expire { .. } => {
				// ttl_ms (8B)
				base + 8
			}
			_ => base,
		}
	}

	/// Decodes an operation from the wire payload buffer.
	///
	/// # Errors
	///
	/// Returns [`OperationDecodeError`] if the buffer ends prematurely, the opcode
	/// is unassigned, key/value limits are exceeded, or key bytes contain invalid UTF-8.
	pub fn decode(src: &mut BytesMut) -> Result<Self, OperationDecodeError> {
		let raw_opcode = src
			.try_get_u8()
			.map_err(|_| OperationDecodeError::UnexpectedEof)?;
		let opcode =
			Opcode::try_from(raw_opcode).map_err(|_| OperationDecodeError::UnknownOpcode(raw_opcode))?;

		let key_len = src
			.try_get_u32()
			.map_err(|_| OperationDecodeError::UnexpectedEof)? as usize;

		if key_len > MAX_KEY_SIZE {
			return Err(OperationDecodeError::KeyTooLarge(key_len));
		}
		if src.remaining() < key_len {
			return Err(OperationDecodeError::UnexpectedEof);
		}

		let key_str =
			std::str::from_utf8(&src[..key_len]).map_err(|_| OperationDecodeError::InvalidUtf8)?;
		let key = key_str.to_owned();
		src.advance(key_len);

		match opcode {
			Opcode::Get => Ok(Operation::Get { key }),
			Opcode::Delete => Ok(Operation::Delete { key }),
			Opcode::Exists => Ok(Operation::Exists { key }),
			Opcode::Ttl => Ok(Operation::Ttl { key }),
			Opcode::Persist => Ok(Operation::Persist { key }),
			Opcode::Expire => {
				if src.remaining() < 8 {
					return Err(OperationDecodeError::UnexpectedEof);
				}
				let ttl_ms = src.get_u64();
				Ok(Operation::Expire { key, ttl_ms })
			}
			Opcode::Set | Opcode::Add | Opcode::Replace => {
				if src.remaining() < 4 {
					return Err(OperationDecodeError::UnexpectedEof);
				}
				let val_len = src.get_u32() as usize;
				if val_len > MAX_VALUE_SIZE {
					return Err(OperationDecodeError::ValueTooLarge(val_len));
				}
				if src.remaining() < val_len {
					return Err(OperationDecodeError::UnexpectedEof);
				}
				let value = src.split_to(val_len).freeze();

				match opcode {
					Opcode::Set => Ok(Operation::Set { key, value }),
					Opcode::Add => Ok(Operation::Add { key, value }),
					Opcode::Replace => Ok(Operation::Replace { key, value }),
					_ => unreachable!(),
				}
			}
		}
	}
}

/// Top-level command executable by the Quty server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
	/// An isolated single operation on the key-space.
	Single(Operation),
	/// An atomic multi-key sequence guaranteed to execute or roll back together.
	Batch(Vec<Operation>),
	/// Server liveliness check (`PING`).
	Ping,
	/// Requests internal engine and partition statistics (`STATS`).
	Stats,
	/// Flushes all entries and clears partition telemetry (`RESET`).
	Reset,
}

impl Command {
	/// Returns the [`CommandType`] marker representing this command in frame headers.
	#[inline]
	pub const fn command_type(&self) -> CommandType {
		match self {
			Self::Single(_) => CommandType::Single,
			Self::Batch(_) => CommandType::Batch,
			Self::Ping => CommandType::Ping,
			Self::Stats => CommandType::Stats,
			Self::Reset => CommandType::Reset,
		}
	}

	/// Validates whether all operations in a batch share identical shard routing keys.
	///
	/// For non-batch commands, this always returns `true`.
	/// For [`Command::Batch`], returns `true` if all sub-operations target the exact
	/// same routing prefix, or if the batch contains 0 or 1 operation.
	///
	/// # Examples
	///
	/// ```
	/// use bytes::Bytes;
	/// use quty_proto::{Command, Operation};
	///
	/// let valid_batch = Command::Batch(vec![
	///     Operation::Set { key: "user:1".into(), value: Bytes::from_static(b"a") },
	///     Operation::Get { key: "user:2".into() },
	/// ]);
	/// assert!(valid_batch.is_single_shard());
	///
	/// let cross_shard_batch = Command::Batch(vec![
	///     Operation::Set { key: "user:1".into(), value: Bytes::from_static(b"a") },
	///     Operation::Set { key: "session:1".into(), value: Bytes::from_static(b"b") },
	/// ]);
	/// assert!(!cross_shard_batch.is_single_shard());
	/// ```
	#[inline]
	#[must_use]
	pub fn is_single_shard(&self) -> bool {
		match self {
			Self::Batch(ops) => {
				let mut iter = ops.iter();
				if let Some(first) = iter.next() {
					let target = first.routing_key();
					iter.all(|op| op.routing_key() == target)
				} else {
					true
				}
			}
			_ => true,
		}
	}

	/// Computes the exact payload length of this command in bytes.
	#[inline]
	pub fn encoded_len(&self) -> usize {
		match self {
			Self::Single(op) => op.encoded_len(),
			Self::Batch(ops) => {
				let mut len = 4; // op_count (u32: 4B)
				for op in ops {
					len += op.encoded_len();
				}
				len
			}
			Self::Ping | Self::Stats | Self::Reset => 0,
		}
	}

	/// Encodes the command payload into the provided buffer.
	pub fn encode(&self, dst: &mut BytesMut) {
		match self {
			Self::Single(op) => op.encode(dst),
			Self::Batch(ops) => {
				dst.put_u32(to_u32_len(ops.len()));
				for op in ops {
					op.encode(dst);
				}
			}
			Self::Ping | Self::Stats | Self::Reset => {}
		}
	}
}

/// Extracts the routing key prefix used for key-space sharding.
///
/// In Quty, routing partition locality is determined by the substring
/// preceding the first colon (`:`). If the key contains no colon, the entire
/// key is used as the routing identifier.
///
/// # Examples
///
/// ```
/// use quty_proto::routing_key;
///
/// assert_eq!(routing_key("user:1000:profile"), "user");
/// assert_eq!(routing_key("session:xyz"), "session");
/// assert_eq!(routing_key("global_counter"), "global_counter");
/// ```
#[inline]
#[must_use]
pub fn routing_key(key: &str) -> &str {
	match key.split_once(':') {
		Some((prefix, _)) => prefix,
		None => key,
	}
}

/// Errors that can occur while decoding an [`Operation`] from the wire.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum OperationDecodeError {
	/// Buffer exhausted before reading required operation fields.
	#[error("Buffer ended prematurely while decoding operation")]
	UnexpectedEof,
	/// Encountered an unrecognized 1-byte opcode identifier.
	#[error("Unknown operation opcode: 0x{0:02X}")]
	UnknownOpcode(u8),
	/// Deserialized key size exceeds [`MAX_KEY_SIZE`].
	#[error("Key size ({0} B) exceeded limit")]
	KeyTooLarge(usize),
	/// Deserialized value size exceeds [`MAX_VALUE_SIZE`].
	#[error("Value size ({0} B) exceeded limit")]
	ValueTooLarge(usize),
	/// Key bytes failed valid UTF-8 string conversion.
	#[error("Key string contained invalid UTF-8 bytes")]
	InvalidUtf8,
}
