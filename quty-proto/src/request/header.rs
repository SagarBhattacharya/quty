use bytes::{Buf, BufMut, BytesMut};
use crate::command::Opcode;
use crate::constants::*;
use crate::RequestDecodeError;

/// Fixed 8-byte request_old frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
	pub request_id: u16,
	pub opcode: Opcode,
	pub flags: u8,
	pub payload_len: u32,
}

impl RequestHeader {
	#[inline]
	pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, RequestDecodeError> {
		if src.len() < HEADER_SIZE {
			return Ok(None);
		}

		let opcode_byte = src[2];
		let opcode = Opcode::try_from(opcode_byte)
			.map_err(RequestDecodeError::UnknownOpcode)?;

		// Peek without consuming until total frame presence is confirmed
		let mut peek = &src[..HEADER_SIZE];
		let request_id = peek.get_u16();
		let _ = peek.get_u8(); // opcode
		let flags = peek.get_u8();
		let payload_len = peek.get_u32();

		if payload_len as usize > MAX_FRAME_SIZE {
			return Err(RequestDecodeError::FrameTooLarge(payload_len));
		}

		Ok(Some(RequestHeader {
			request_id,
			opcode,
			flags,
			payload_len,
		}))
	}

	#[inline]
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u16(self.request_id);
		dst.put_u8(self.opcode as u8);
		dst.put_u8(self.flags);
		dst.put_u32(self.payload_len);
	}
}