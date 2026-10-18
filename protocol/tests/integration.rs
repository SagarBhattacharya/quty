use bytes::{Bytes, BytesMut};
use protocol::*;

#[test]
fn test_stream_fragmentation_resilience() {
	let req = Request {
		request_id: 1001,
		flags: 0,
		command: Command::Single(Operation::Set {
			key: "account:404:profile".into(),
			value: Bytes::from(vec![0x7A; 16384]), // 16 KB payload
		}),
	};

	let mut wire_bytes = BytesMut::new();
	req.encode(&mut wire_bytes);

	let mut stream = BytesMut::new();
	// Simulate TCP delivering data in erratic 256-byte chunks
	for chunk in wire_bytes.chunks(256) {
		stream.extend_from_slice(chunk);
		if stream.len() < wire_bytes.len() {
			let res = Request::decode(&mut stream).unwrap();
			assert!(res.is_none(), "Decoder must yield None until the final byte arrives");
		}
	}

	let decoded = Request::decode(&mut stream).unwrap().expect("Complete frame must parse");
	assert_eq!(decoded, req);
	assert_eq!(stream.len(), 0);
}

#[test]
fn test_bidirectional_pipelined_stream() {
	// 1. Client pipelines three consecutive requests
	let req1 = Request {
		request_id: 1,
		flags: 0,
		command: Command::Single(Operation::Set {
			key: "session:1".into(),
			value: Bytes::from_static(b"tok_a"),
		}),
	};
	let req2 = Request {
		request_id: 2,
		flags: 0,
		command: Command::Single(Operation::Get { key: "session:1".into() }),
	};
	let req3 = Request {
		request_id: 3,
		flags: 0,
		command: Command::Ping,
	};

	let mut client_send_buf = BytesMut::new();
	req1.encode(&mut client_send_buf);
	req2.encode(&mut client_send_buf);
	req3.encode(&mut client_send_buf);

	// 2. Server decodes each sequentially from single read buffer
	let server_req1 = Request::decode(&mut client_send_buf).unwrap().unwrap();
	let server_req2 = Request::decode(&mut client_send_buf).unwrap().unwrap();
	let server_req3 = Request::decode(&mut client_send_buf).unwrap().unwrap();
	assert_eq!(client_send_buf.len(), 0);

	// 3. Server generates and pipelines matching responses
	let resp1 = Response::empty(server_req1.request_id, StatusCode::Created);
	let resp2 = Response::new(
		server_req2.request_id,
		StatusCode::Ok,
		ResponsePayload::Raw(Bytes::from_static(b"tok_a")),
	);
	let resp3 = Response::empty(server_req3.request_id, StatusCode::Ok);

	let mut server_send_buf = BytesMut::new();
	resp1.encode(&mut server_send_buf);
	resp2.encode(&mut server_send_buf);
	resp3.encode(&mut server_send_buf);

	// 4. Client unpacks responses preserving correlation IDs
	let client_resp1 = Response::decode(&mut server_send_buf).unwrap().unwrap();
	let client_resp2 = Response::decode(&mut server_send_buf).unwrap().unwrap();
	let client_resp3 = Response::decode(&mut server_send_buf).unwrap().unwrap();

	assert_eq!(client_resp1.request_id, 1);
	assert_eq!(client_resp1.status, StatusCode::Created);

	assert_eq!(client_resp2.request_id, 2);
	assert_eq!(
		client_resp2.payload,
		ResponsePayload::Raw(Bytes::from_static(b"tok_a"))
	);

	assert_eq!(client_resp3.request_id, 3);
	assert_eq!(client_resp3.status, StatusCode::Ok);
	assert_eq!(server_send_buf.len(), 0);
}

#[test]
fn test_batch_execution_short_circuit_wire_representation() {
	// Simulates engine reporting a batch that failed at step 2 (0-indexed)
	let batch_results = vec![
		SubResult {
			status: StatusCode::Ok,
			payload: ResponsePayload::Null,
		},
		SubResult {
			status: StatusCode::Created,
			payload: ResponsePayload::Null,
		},
		SubResult {
			status: StatusCode::KeyExists, // Condition failure causing short-circuit
			payload: ResponsePayload::Null,
		},
	];

	let resp = Response::new(
		99,
		StatusCode::BatchShortCircuited,
		ResponsePayload::Array(batch_results),
	);

	let mut buf = BytesMut::new();
	resp.encode(&mut buf);

	let decoded = Response::decode(&mut buf).unwrap().unwrap();
	assert_eq!(decoded.status, StatusCode::BatchShortCircuited);

	if let ResponsePayload::Array(results) = decoded.payload {
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].status, StatusCode::Ok);
		assert_eq!(results[1].status, StatusCode::Created);
		assert_eq!(results[2].status, StatusCode::KeyExists);
	} else {
		panic!("Decoded response payload is not an Array");
	}
}

#[test]
fn test_routing_key_extraction_invariants() {
	let op_colon = Operation::Get { key: "users:profile:100".into() };
	let op_flat = Operation::Get { key: "session_token".into() };
	let op_empty_prefix = Operation::Get { key: ":leading_colon".into() };

	assert_eq!(op_colon.routing_key(), "users");
	assert_eq!(op_flat.routing_key(), "session_token");
	assert_eq!(op_empty_prefix.routing_key(), "");
}