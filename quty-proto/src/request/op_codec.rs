use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::command::{Opcode, Operation};
use crate::constants::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use crate::RequestDecodeError;

impl Operation {
	/// Decodes a storage operation payload based on the given wire opcode.
	pub fn decode(opcode: Opcode, buf: &mut BytesMut) -> Result<Self, RequestDecodeError> {
		match opcode {
			Opcode::Get => Ok(Self::Get {
				key: Self::decode_key(buf)?,
			}),
			Opcode::Exists => Ok(Self::Exists {
				key: Self::decode_key(buf)?,
			}),
			Opcode::Ttl => Ok(Self::Ttl {
				key: Self::decode_key(buf)?,
			}),
			Opcode::Delete => Ok(Self::Delete {
				key: Self::decode_key(buf)?,
			}),
			Opcode::Persist => Ok(Self::Persist {
				key: Self::decode_key(buf)?,
			}),
			Opcode::Set => Ok(Self::Set {
				key: Self::decode_key(buf)?,
				value: Self::decode_value(buf)?,
			}),
			Opcode::Add => Ok(Self::Add {
				key: Self::decode_key(buf)?,
				value: Self::decode_value(buf)?,
			}),
			Opcode::Replace => Ok(Self::Replace {
				key: Self::decode_key(buf)?,
				value: Self::decode_value(buf)?,
			}),
			Opcode::Expire => {
				let key = Self::decode_key(buf)?;
				if buf.remaining() < 8 {
					return Err(RequestDecodeError::Incomplete);
				}
				let ttl_ms = buf.get_u64();
				Ok(Self::Expire { key, ttl_ms })
			}
			_ => Err(RequestDecodeError::InvalidPayload(
				"Opcode is not a valid shard storage operation".into(),
			)),
		}
	}

	/// Encodes the operation's fields directly into `dst`.
	pub fn encode(&self, dst: &mut BytesMut) {
		match self {
			Self::Get { key }
			| Self::Exists { key }
			| Self::Ttl { key }
			| Self::Delete { key }
			| Self::Persist { key } => {
				Self::encode_key(key, dst);
			}
			Self::Set { key, value }
			| Self::Add { key, value }
			| Self::Replace { key, value } => {
				Self::encode_key(key, dst);
				Self::encode_value(value, dst);
			}
			Self::Expire { key, ttl_ms } => {
				Self::encode_key(key, dst);
				dst.put_u64(*ttl_ms);
			}
		}
	}

	#[inline]
	pub fn decode_key(buf: &mut BytesMut) -> Result<String, RequestDecodeError> {
		if buf.remaining() < 2 {
			return Err(RequestDecodeError::Incomplete);
		}
		let key_len = buf.get_u16() as usize;
		if key_len > MAX_KEY_SIZE {
			return Err(RequestDecodeError::KeyTooLarge(key_len));
		}
		if buf.remaining() < key_len {
			return Err(RequestDecodeError::Incomplete);
		}
		let key_bytes = buf.split_to(key_len);
		String::from_utf8(key_bytes.to_vec()).map_err(|_| RequestDecodeError::InvalidUtf8)
	}

	#[inline]
	pub fn decode_value(buf: &mut BytesMut) -> Result<Bytes, RequestDecodeError> {
		if buf.remaining() < 4 {
			return Err(RequestDecodeError::Incomplete);
		}
		let val_len = buf.get_u32() as usize;
		if val_len > MAX_VALUE_SIZE {
			return Err(RequestDecodeError::ValueTooLarge(val_len));
		}
		if buf.remaining() < val_len {
			return Err(RequestDecodeError::Incomplete);
		}
		Ok(buf.split_to(val_len).freeze())
	}

	#[inline]
	pub fn encode_key(key: &str, dst: &mut BytesMut) {
		dst.put_u16(key.len() as u16);
		dst.extend_from_slice(key.as_bytes());
	}

	#[inline]
	pub fn encode_value(value: &Bytes, dst: &mut BytesMut) {
		dst.put_u32(value.len() as u32);
		dst.extend_from_slice(value);
	}
}