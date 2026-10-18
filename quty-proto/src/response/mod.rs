mod codec;
mod status;
mod payload;
mod header;

pub use status::StatusCode;
pub use payload::{
	PayloadType,
	StatsPayload,
	SubResult,
	ResponsePayload,
	BatchErrorPayload
};
pub use header::ResponseHeader;

/// Complete decoded response frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
	pub request_id: u16,
	pub status: StatusCode,
	pub payload: ResponsePayload,
}

impl Response {
	pub fn new(
		request_id: u16,
		status: StatusCode,
		payload: ResponsePayload
	) -> Self {
		Self {
			request_id,
			status,
			payload,
		}
	}

	/// Convenience constructor for empty responses (NoContent, simple errors).
	pub fn empty(
		request_id: u16,
		status: StatusCode
	) -> Self {
		Self {
			request_id,
			status,
			payload: ResponsePayload::Null,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::*;
	use bytes::{BufMut, Bytes, BytesMut};

	#[test]
	fn test_response_header_alignment_and_field_offsets() {
		let header = ResponseHeader {
			request_id: 0xCAFE,
			status: StatusCode::KeyNotFound,
			payload_type: PayloadType::Null,
			payload_len: 0,
		};

		let mut buf = BytesMut::new();
		header.encode(&mut buf);

		assert_eq!(buf.len(), HEADER_SIZE);
		assert_eq!(&buf[0..2], &[0xCA, 0xFE]); // request_id
		assert_eq!(buf[2], StatusCode::KeyNotFound as u8);
		assert_eq!(buf[3], PayloadType::Null as u8);
		assert_eq!(&buf[4..8], &[0x00, 0x00, 0x00, 0x00]); // payload_len
	}

	#[test]
	fn test_stats_payload_fixed_size_and_endianness() {
		let original = StatsPayload {
			uptime_secs: 0x0102030405060708,
			total_connections: 10,
			curr_connections: 2,
			get_hits: 1000,
			get_misses: 20,
			total_keys: 500,
			used_memory_bytes: 1024 * 1024,
			evictions: 0,
		};

		let mut buf = BytesMut::new();
		original.encode(&mut buf);

		assert_eq!(buf.len(), STATS_PAYLOAD_SIZE);
		assert_eq!(&buf[0..8], &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);

		let decoded = StatsPayload::decode(&mut buf).unwrap();
		assert_eq!(original, decoded);
		assert_eq!(buf.len(), 0);
	}

	#[test]
	fn test_payload_length_mismatch_detection() {
		let mut buf = BytesMut::new();
		buf.put_u16(10);
		buf.put_u8(StatusCode::Ok as u8);
		buf.put_u8(PayloadType::Int64 as u8);
		buf.put_u32(4); // Int64 requires strictly 8 bytes
		buf.put_u32(12345);

		let err = Response::decode(&mut buf).unwrap_err();
		assert!(matches!(
        err,
        ResponseDecodeError::PayloadLengthMismatch { expected: 8, found: 4 }
    ));
	}

	#[test]
	fn test_batch_array_heterogeneous_sub_results() {
		let sub_results = vec![
			SubResult {
				status: StatusCode::Created,
				payload: ResponsePayload::Null,
			},
			SubResult {
				status: StatusCode::Ok,
				payload: ResponsePayload::Int64(-1),
			},
			SubResult {
				status: StatusCode::Ok,
				payload: ResponsePayload::Raw(Bytes::from_static(b"value_payload")),
			},
		];

		let resp = Response::new(42, StatusCode::Ok, ResponsePayload::Array(sub_results));
		let mut buf = BytesMut::new();
		resp.encode(&mut buf);

		let decoded = Response::decode(&mut buf).unwrap().unwrap();
		assert_eq!(resp, decoded);
		assert_eq!(buf.len(), 0);
	}

	#[test]
	fn test_prohibit_nested_array_in_sub_result() {
		let mut buf = BytesMut::new();
		buf.put_u8(StatusCode::Ok as u8);
		buf.put_u8(PayloadType::Array as u8); // Nested Array tag
		buf.put_u32(2);
		buf.put_u16(0);

		let err = SubResult::decode(&mut buf).unwrap_err();
		assert!(matches!(err, ResponseDecodeError::InvalidPayload(_)));
	}
}