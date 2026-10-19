use crate::db::Engine;
use quty_proto::{Request, Response};
use std::sync::Arc;

/// Dispatches a decoded network request into the engine facade and constructs
/// the corresponding protocol response frame.
#[inline]
pub fn handle_request(req: Request, engine: &Arc<Engine>) -> Response {
	let (status, payload) = engine.execute(req.command);
	Response::new(req.request_id, status, payload)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::db::EngineConfig;
	use bytes::Bytes;
	use quty_proto::{Command, Operation, ResponsePayload, StatusCode};

	#[test]
	fn test_handle_request_round_trip() {
		let engine = Engine::new(EngineConfig {
			num_shards: 4,
			max_memory_bytes: 1024 * 1024,
		});

		// 1. Dispatch SET request with request_id = 42
		let req1 = Request::new(
			42, 0,
			Command::Single(Operation::Set {
				key: "session:abc".into(),
				value: Bytes::from_static(b"tok_123"),
			}),
		);
		let res1 = handle_request(req1, &engine);
		assert_eq!(res1.request_id, 42);
		assert_eq!(res1.status, StatusCode::Created);

		// 2. Dispatch GET request with request_id = 43
		let req2 = Request::new(
			43, 0,
			Command::Single(Operation::Get {
				key: "session:abc".into(),
			}),
		);
		let res2 = handle_request(req2, &engine);
		assert_eq!(res2.request_id, 43);
		assert_eq!(res2.status, StatusCode::Ok);
		assert_eq!(
			res2.payload,
			ResponsePayload::Raw(Bytes::from_static(b"tok_123"))
		);
	}
}