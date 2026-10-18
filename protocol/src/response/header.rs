use bytes::{Buf, BufMut, BytesMut};
use crate::constants::HEADER_SIZE;
use crate::response::payload::PayloadType;
use crate::response::status::StatusCode;
use crate::ResponseDecodeError;

/// Fixed 8-byte response frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
	pub request_id: u16,
	pub status: StatusCode,
	pub payload_type: PayloadType,
	pub payload_len: u32,
}

impl ResponseHeader {
	#[inline]
	pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ResponseDecodeError> {
		if src.len() < HEADER_SIZE {
			return Ok(None);
		}

		let status_byte = src[2];
		let status = StatusCode::try_from(status_byte)
			.map_err(ResponseDecodeError::UnknownStatusCode)?;

		let type_byte = src[3];
		let payload_type = PayloadType::try_from(type_byte)
			.map_err(ResponseDecodeError::UnknownPayloadType)?;

		let mut peek = &src[..HEADER_SIZE];
		let request_id = peek.get_u16();
		let _ = peek.get_u8(); // status
		let _ = peek.get_u8(); // payload_type
		let payload_len = peek.get_u32();

		Ok(Some(ResponseHeader {
			request_id,
			status,
			payload_type,
			payload_len,
		}))
	}

	#[inline]
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u16(self.request_id);
		dst.put_u8(self.status as u8);
		dst.put_u8(self.payload_type as u8);
		dst.put_u32(self.payload_len);
	}
}