use bytes::{Buf, BufMut, BytesMut};
use crate::constants::{HEADER_SIZE, STATS_PAYLOAD_SIZE};
use crate::response::header::ResponseHeader;
use crate::response::payload::{PayloadType, ResponsePayload, StatsPayload, SubResult};
use crate::response::Response;
use crate::response::status::StatusCode;
use crate::ResponseDecodeError;

impl Response {
	/// Decodes a complete Response frame from an incoming TCP buffer.
	pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ResponseDecodeError> {
		let header = match ResponseHeader::decode(src)? {
			Some(h) => h,
			None => return Ok(None),
		};

		let total_frame_len = HEADER_SIZE + header.payload_len as usize;
		if src.len() < total_frame_len {
			return Ok(None);
		}

		let mut frame = src.split_to(total_frame_len);
		frame.advance(HEADER_SIZE);

		let payload = match header.payload_type {
			PayloadType::Null => {
				if header.payload_len != 0 {
					return Err(ResponseDecodeError::PayloadLengthMismatch {
						expected: 0,
						found: header.payload_len as usize,
					});
				}
				ResponsePayload::Null
			}
			PayloadType::Bool => {
				if header.payload_len != 1 {
					return Err(ResponseDecodeError::PayloadLengthMismatch {
						expected: 1,
						found: header.payload_len as usize,
					});
				}
				ResponsePayload::Bool(frame.get_u8() != 0)
			}
			PayloadType::Int64 => {
				if header.payload_len != 8 {
					return Err(ResponseDecodeError::PayloadLengthMismatch {
						expected: 8,
						found: header.payload_len as usize,
					});
				}
				ResponsePayload::Int64(frame.get_i64())
			}
			PayloadType::String => {
				let s = String::from_utf8(frame.to_vec())
					.map_err(|_| ResponseDecodeError::InvalidUtf8)?;
				ResponsePayload::String(s)
			}
			PayloadType::Raw => ResponsePayload::Raw(frame.freeze()),
			PayloadType::Stats => {
				if header.payload_len != STATS_PAYLOAD_SIZE as u32 {
					return Err(ResponseDecodeError::PayloadLengthMismatch {
						expected: STATS_PAYLOAD_SIZE,
						found: header.payload_len as usize,
					});
				}
				ResponsePayload::Stats(StatsPayload::decode(&mut frame)?)
			}
			PayloadType::Array => {
				if frame.remaining() < 2 {
					return Err(ResponseDecodeError::Incomplete);
				}
				let count = frame.get_u16() as usize;
				let mut sub_results = Vec::with_capacity(count);

				for _ in 0..count {
					sub_results.push(SubResult::decode(&mut frame)?);
				}

				if frame.has_remaining() {
					return Err(ResponseDecodeError::InvalidPayload(
						"Unexpected trailing bytes in array response".into(),
					));
				}

				ResponsePayload::Array(sub_results)
			}
		};

		Ok(Some(Response {
			request_id: header.request_id,
			status: header.status,
			payload,
		}))
	}

	/// Serializes a complete Response frame into `dst`.
	pub fn encode(&self, dst: &mut BytesMut) {
		let payload_type = self.payload.payload_type();

		let header_offset = dst.len();
		dst.put_u16(self.request_id);
		dst.put_u8(self.status as u8);
		dst.put_u8(payload_type as u8);
		dst.put_u32(0); // Temporary length slot

		let payload_start = dst.len();

		match &self.payload {
			ResponsePayload::Null => {}
			ResponsePayload::Bool(b) => dst.put_u8(if *b { 1 } else { 0 }),
			ResponsePayload::Int64(v) => dst.put_i64(*v),
			ResponsePayload::String(s) => dst.extend_from_slice(s.as_bytes()),
			ResponsePayload::Raw(b) => dst.extend_from_slice(b),
			ResponsePayload::Stats(stats) => stats.encode(dst),
			ResponsePayload::Array(sub_results) => {
				dst.put_u16(sub_results.len() as u16);
				for sub in sub_results {
					sub.encode(dst);
				}
			}
		}

		let payload_len = (dst.len() - payload_start) as u32;
		dst[header_offset + 4..header_offset + 8].copy_from_slice(&payload_len.to_be_bytes());
	}
}

impl StatsPayload {
	#[inline]
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u64(self.uptime_secs);
		dst.put_u64(self.total_connections);
		dst.put_u64(self.curr_connections);
		dst.put_u64(self.get_hits);
		dst.put_u64(self.get_misses);
		dst.put_u64(self.total_keys);
		dst.put_u64(self.used_memory_bytes);
		dst.put_u64(self.evictions);
	}

	#[inline]
	pub fn decode(buf: &mut BytesMut) -> Result<Self, ResponseDecodeError> {
		if buf.remaining() < STATS_PAYLOAD_SIZE {
			return Err(ResponseDecodeError::Incomplete);
		}

		Ok(StatsPayload {
			uptime_secs: buf.get_u64(),
			total_connections: buf.get_u64(),
			curr_connections: buf.get_u64(),
			get_hits: buf.get_u64(),
			get_misses: buf.get_u64(),
			total_keys: buf.get_u64(),
			used_memory_bytes: buf.get_u64(),
			evictions: buf.get_u64(),
		})
	}
}



impl SubResult {
	pub fn encode(&self, dst: &mut BytesMut) {
		dst.put_u8(self.status as u8);
		dst.put_u8(self.payload.payload_type() as u8);

		// Reserve 4 bytes for length
		let len_offset = dst.len();
		dst.put_u32(0);
		let start = dst.len();

		match &self.payload {
			ResponsePayload::Null => {}
			ResponsePayload::Bool(b) => dst.put_u8(if *b { 1 } else { 0 }),
			ResponsePayload::Int64(v) => dst.put_u64(*v as u64),
			ResponsePayload::String(s) => dst.extend_from_slice(s.as_bytes()),
			ResponsePayload::Raw(b) => dst.extend_from_slice(b),
			ResponsePayload::Stats(stats) => stats.encode(dst),
			ResponsePayload::Array(_) => {
				panic!("Nested arrays inside SubResult are prohibited");
			}
		}

		let len = (dst.len() - start) as u32;
		dst[len_offset..len_offset + 4].copy_from_slice(&len.to_be_bytes());
	}

	pub fn decode(buf: &mut BytesMut) -> Result<Self, ResponseDecodeError> {
		if buf.remaining() < 6 {
			return Err(ResponseDecodeError::Incomplete);
		}

		let status = StatusCode::try_from(buf.get_u8())
			.map_err(ResponseDecodeError::UnknownStatusCode)?;
		let payload_type = PayloadType::try_from(buf.get_u8())
			.map_err(ResponseDecodeError::UnknownPayloadType)?;
		let len = buf.get_u32() as usize;

		if buf.remaining() < len {
			return Err(ResponseDecodeError::Incomplete);
		}

		let mut data_slice = buf.split_to(len);
		let payload = match payload_type {
			PayloadType::Null => {
				if len != 0 {
					return Err(ResponseDecodeError::PayloadLengthMismatch { expected: 0, found: len });
				}
				ResponsePayload::Null
			}
			PayloadType::Bool => {
				if len != 1 {
					return Err(ResponseDecodeError::PayloadLengthMismatch { expected: 1, found: len });
				}
				ResponsePayload::Bool(data_slice.get_u8() != 0)
			}
			PayloadType::Int64 => {
				if len != 8 {
					return Err(ResponseDecodeError::PayloadLengthMismatch { expected: 8, found: len });
				}
				ResponsePayload::Int64(data_slice.get_i64())
			}
			PayloadType::String => {
				let s = String::from_utf8(data_slice.to_vec())
					.map_err(|_| ResponseDecodeError::InvalidUtf8)?;
				ResponsePayload::String(s)
			}
			PayloadType::Raw => ResponsePayload::Raw(data_slice.freeze()),
			PayloadType::Stats => {
				if len != STATS_PAYLOAD_SIZE {
					return Err(ResponseDecodeError::PayloadLengthMismatch {
						expected: STATS_PAYLOAD_SIZE,
						found: len,
					});
				}
				ResponsePayload::Stats(StatsPayload::decode(&mut data_slice)?)
			}
			PayloadType::Array => {
				return Err(ResponseDecodeError::InvalidPayload("Nested Array prohibited".into()));
			}
		};

		Ok(SubResult { status, payload })
	}
}