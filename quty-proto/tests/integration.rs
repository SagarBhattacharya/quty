use bytes::{Bytes, BytesMut};
use quty_proto::*;

#[test]
fn test_request_single_operations_round_trip() {
	let test_ops = vec![
		Operation::Get {
			key: "user:100".into(),
		},
		Operation::Set {
			key: "session:abc".into(),
			value: Bytes::from_static(b"active_session_payload_data"),
		},
		Operation::Add {
			key: "lock:worker_1".into(),
			value: Bytes::from_static(b"acquired"),
		},
		Operation::Replace {
			key: "cached:config".into(),
			value: Bytes::from_static(b"new_config_payload"),
		},
		Operation::Delete {
			key: "temp:cache_key".into(),
		},
		Operation::Exists {
			key: "user:profile:200".into(),
		},
		Operation::Ttl {
			key: "expiring:token".into(),
		},
		Operation::Expire {
			key: "session:xyz".into(),
			ttl_ms: 3_600_000,
		},
		Operation::Persist {
			key: "permanent:flag".into(),
		},
	];

	for (idx, op) in test_ops.into_iter().enumerate() {
		let req_id = (idx as u64) + 1000;
		let original_req = Request::new(req_id, Command::Single(op));

		let mut buf = BytesMut::new();
		original_req.encode(&mut buf);

		let decoded = Request::decode(&mut buf)
			.expect("Decoding valid request must succeed")
			.expect("Complete frame must yield Request");

		assert_eq!(decoded, original_req);
		assert!(
			buf.is_empty(),
			"Buffer must be drained after full frame decode"
		);
	}
}

#[test]
fn test_request_batch_operations_round_trip() {
	let batch_ops = vec![
		Operation::Set {
			key: "u:1:name".into(),
			value: Bytes::from_static(b"Alice"),
		},
		Operation::Set {
			key: "u:1:role".into(),
			value: Bytes::from_static(b"admin"),
		},
		Operation::Get {
			key: "u:1:name".into(),
		},
		Operation::Expire {
			key: "u:1:session".into(),
			ttl_ms: 60_000,
		},
		Operation::Exists {
			key: "u:1:flags".into(),
		},
	];

	let original_req = Request::new(42, Command::Batch(batch_ops));
	let mut buf = BytesMut::new();
	original_req.encode(&mut buf);

	let decoded = Request::decode(&mut buf)
		.expect("Decoding valid batch must succeed")
		.expect("Complete frame must yield Request");

	assert_eq!(decoded, original_req);
	assert!(buf.is_empty());
}

#[test]
fn test_request_control_commands_round_trip() {
	let control_commands = vec![Command::Ping, Command::Stats, Command::Reset];

	for (idx, cmd) in control_commands.into_iter().enumerate() {
		let req = Request::new((idx as u64) + 500, cmd);
		let mut buf = BytesMut::new();
		req.encode(&mut buf);

		let decoded = Request::decode(&mut buf)
			.expect("Decoding control command must succeed")
			.expect("Complete frame must yield Request");

		assert_eq!(decoded, req);
		assert!(buf.is_empty());
	}
}

#[test]
fn test_response_all_payload_variants_round_trip() {
	let stats = StatsPayload {
		uptime_seconds: 12345,
		total_connections: 50,
		active_connections: 12,
		total_keys: 80000,
		used_memory_bytes: 10_485_760,
		get_hits: 500_000,
		get_misses: 2500,
		evictions: 150,
	};

	let test_responses = vec![
		Response::new(1, StatusCode::Ok, ResponsePayload::Null),
		Response::new(2, StatusCode::Ok, ResponsePayload::Bool(true)),
		Response::new(3, StatusCode::Ok, ResponsePayload::Bool(false)),
		Response::new(4, StatusCode::Ok, ResponsePayload::Int64(-42)),
		Response::new(5, StatusCode::Ok, ResponsePayload::Int64(3_600_000)),
		Response::new(
			6,
			StatusCode::Ok,
			ResponsePayload::String("Simple string result".into()),
		),
		Response::new(
			7,
			StatusCode::Created,
			ResponsePayload::Raw(Bytes::from_static(b"\x00\xFF\xAA\x55\xDE\xAD\xBE\xEF")),
		),
		Response::new(8, StatusCode::Ok, ResponsePayload::Stats(stats)),
		Response::new(
			9,
			StatusCode::BatchShortCircuited,
			ResponsePayload::BatchError(BatchErrorPayload {
				failed_index: 3,
				failure_code: StatusCode::KeyExists,
			}),
		),
		Response::new(
			10,
			StatusCode::Ok,
			ResponsePayload::Array(vec![
				SubResult {
					status: StatusCode::Created,
					payload: ResponsePayload::Null,
				},
				SubResult {
					status: StatusCode::Ok,
					payload: ResponsePayload::String("Alice".into()),
				},
				SubResult {
					status: StatusCode::KeyNotFound,
					payload: ResponsePayload::Null,
				},
				SubResult {
					status: StatusCode::Ok,
					payload: ResponsePayload::Bool(true),
				},
			]),
		),
	];

	for original_res in test_responses {
		let mut buf = BytesMut::new();
		original_res.encode(&mut buf);

		let decoded = Response::decode(&mut buf)
			.expect("Decoding valid response must succeed")
			.expect("Complete frame must yield Response");

		assert_eq!(decoded, original_res);
		assert!(
			buf.is_empty(),
			"Buffer must be drained after full frame decode"
		);
	}
}

#[test]
fn test_resumable_decoder_byte_by_byte_stream() {
	let req = Request::new(
		999,
		Command::Single(Operation::Set {
			key: "streaming:stream_test".into(),
			value: Bytes::from_static(b"buffered_chunks"),
		}),
	);

	let mut encoded = BytesMut::new();
	req.encode(&mut encoded);
	let total_bytes = encoded.len();

	let mut stream_buffer = BytesMut::new();

	// Push bytes one by one and assert decoder stays in Ok(None) until the very last byte
	for (i, byte) in encoded.iter().enumerate() {
		stream_buffer.extend_from_slice(&[*byte]);

		let decoded = Request::decode(&mut stream_buffer).expect("Must not error on partial frame");
		if i + 1 < total_bytes {
			assert!(
				decoded.is_none(),
				"Decoder must return None on partial frame (byte {}/{})",
				i + 1,
				total_bytes
			);
		} else {
			let complete_req = decoded.expect("Decoder must return frame on final byte");
			assert_eq!(complete_req, req);
			assert!(stream_buffer.is_empty());
		}
	}
}

#[test]
fn test_reject_invalid_magic_byte() {
	let mut buf = BytesMut::new();
	let header = RequestHeader {
		magic: 0xFF, // Invalid magic (not 'Q' / 0x51)
		version: PROTOCOL_VERSION_V1,
		command_type: CommandType::Ping,
		flags: 0,
		request_id: 1,
		payload_len: 0,
	};
	header.encode(&mut buf);

	let err = Request::decode(&mut buf).expect_err("Must reject invalid magic byte");
	assert_eq!(err, RequestDecodeError::InvalidMagic(0xFF));
}

#[test]
fn test_reject_unsupported_protocol_version() {
	let mut buf = BytesMut::new();
	let header = RequestHeader {
		magic: PROTOCOL_MAGIC,
		version: 0x02, // Unsupported future version
		command_type: CommandType::Ping,
		flags: 0,
		request_id: 1,
		payload_len: 0,
	};
	header.encode(&mut buf);

	let err = Request::decode(&mut buf).expect_err("Must reject unsupported version");
	assert_eq!(err, RequestDecodeError::UnsupportedVersion(0x02));
}

#[test]
fn test_reject_unknown_command_type() {
	let mut buf = BytesMut::new();
	buf.extend_from_slice(PROTOCOL_MAGIC.to_be_bytes().as_ref());
	buf.extend_from_slice(PROTOCOL_VERSION_V1.to_be_bytes().as_ref());
	buf.extend_from_slice(&[
		0xAA, // Unknown CommandType
		0x00, // flags
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // req_id = 1
		0x00, 0x00, 0x00, 0x00, // payload_len = 0
	]);

	let err = Request::decode(&mut buf).expect_err("Must reject unknown command type");
	assert_eq!(err, RequestDecodeError::UnknownCommandType(0xAA));
}

#[test]
fn test_reject_unknown_operation_opcode() {
	let mut buf = BytesMut::new();
	// RequestHeader for Single operation
	let header = RequestHeader {
		magic: PROTOCOL_MAGIC,
		version: PROTOCOL_VERSION_V1,
		command_type: CommandType::Single,
		flags: 0,
		request_id: 1,
		payload_len: 5,
	};
	header.encode(&mut buf);

	// Payload: [Opcode: 0xEE (unknown)], [KeyLen: 0]
	buf.extend_from_slice(&[0xEE, 0x00, 0x00, 0x00, 0x00]);

	let err = Request::decode(&mut buf).expect_err("Must reject unknown operation opcode");
	assert_eq!(
		err,
		RequestDecodeError::OpError(OperationDecodeError::UnknownOpcode(0xEE))
	);
}

#[test]
fn test_reject_key_exceeding_max_key_size() {
	let mut buf = BytesMut::new();
	let oversized_len = u32::try_from(MAX_KEY_SIZE + 1).expect("MAX_KEY_SIZE + 1 should fit in u32");

	let header = RequestHeader {
		magic: PROTOCOL_MAGIC,
		version: PROTOCOL_VERSION_V1,
		command_type: CommandType::Single,
		flags: 0,
		request_id: 1,
		payload_len: 5,
	};
	header.encode(&mut buf);

	// Payload: [Opcode: GET (0x01)], [KeyLen: MAX_KEY_SIZE + 1]
	buf.extend_from_slice(&[0x01]);
	buf.extend_from_slice(&oversized_len.to_be_bytes());

	let err = Request::decode(&mut buf).expect_err("Must reject key size exceeding 64 KB");
	assert_eq!(
		err,
		RequestDecodeError::OpError(OperationDecodeError::KeyTooLarge(MAX_KEY_SIZE + 1))
	);
}

#[test]
fn test_reject_invalid_utf8_key() {
	let mut buf = BytesMut::new();
	let header = RequestHeader {
		magic: PROTOCOL_MAGIC,
		version: PROTOCOL_VERSION_V1,
		command_type: CommandType::Single,
		flags: 0,
		request_id: 1,
		payload_len: 7,
	};
	header.encode(&mut buf);

	// Opcode: GET (0x01), KeyLen: 2, KeyBytes: [0xFF, 0xFE] (Invalid UTF-8)
	buf.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x02, 0xFF, 0xFE]);

	let err = Request::decode(&mut buf).expect_err("Must reject invalid UTF-8 in key bytes");
	assert_eq!(
		err,
		RequestDecodeError::OpError(OperationDecodeError::InvalidUtf8)
	);
}

#[test]
fn test_reject_batch_exceeding_max_batch_operations() {
	let mut buf = BytesMut::new();
	let header = RequestHeader {
		magic: PROTOCOL_MAGIC,
		version: PROTOCOL_VERSION_V1,
		command_type: CommandType::Batch,
		flags: 0,
		request_id: 1,
		payload_len: 4,
	};
	header.encode(&mut buf);

	let oversized_count =
		u32::try_from(MAX_BATCH_OPERATIONS + 1).expect("MAX_BATCH_OPERATIONS + 1 should fit in u32");
	buf.extend_from_slice(&oversized_count.to_be_bytes());

	let err = Request::decode(&mut buf).expect_err("Must reject batch operation count > limit");
	assert_eq!(
		err,
		RequestDecodeError::BatchTooLarge(MAX_BATCH_OPERATIONS + 1)
	);
}

#[test]
fn test_response_reject_invalid_magic_and_version() {
	let mut buf = BytesMut::new();
	let mut header = ResponseHeader {
		magic: 0x99,
		version: PROTOCOL_VERSION_V1,
		status: StatusCode::Ok,
		payload_type: PayloadType::Null,
		request_id: 1,
		payload_len: 0,
	};
	header.encode(&mut buf);

	let err = Response::decode(&mut buf).expect_err("Must reject invalid response magic");
	assert_eq!(err, ResponseDecodeError::InvalidMagic(0x99));

	buf.clear();
	header.magic = PROTOCOL_MAGIC;
	header.version = 0x05;
	header.encode(&mut buf);

	let err = Response::decode(&mut buf).expect_err("Must reject invalid response version");
	assert_eq!(err, ResponseDecodeError::UnsupportedVersion(0x05));
}
