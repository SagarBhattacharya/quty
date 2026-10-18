use crate::db::{Engine, EngineConfig};
use bytes::Bytes;
use quty_proto::{Command, Operation, ResponsePayload, StatusCode};

fn make_test_engine() -> std::sync::Arc<Engine> {
	Engine::new(EngineConfig {
		num_shards: 4,
		max_memory_bytes: 64 * 1024, // 64 KB total (16 KB per shard)
	})
}

#[test]
fn test_execute_transport_commands() {
	let engine = make_test_engine();

	// 1. Ping
	let (status, payload) = engine.execute(Command::Ping);
	assert_eq!(status, StatusCode::Ok);
	assert_eq!(payload, ResponsePayload::Null);

	// 2. Set an entry and verify Stats
	engine.execute(Command::Single(Operation::Set {
		key: "node:1".into(),
		value: Bytes::from_static(b"healthy"),
	}));

	let (status, payload) = engine.execute(Command::Stats);
	assert_eq!(status, StatusCode::Ok);
	if let ResponsePayload::Stats(stats) = payload {
		assert_eq!(stats.total_keys, 1);
		assert_eq!(stats.used_memory_bytes, 6 + 7); // "node:1" (6B) + "healthy" (7B)
	} else {
		panic!("Expected Stats payload");
	}

	// 3. Reset clears all shards
	let (status, payload) = engine.execute(Command::Reset);
	assert_eq!(status, StatusCode::NoContent);
	assert_eq!(payload, ResponsePayload::Null);

	let (_, stats_payload) = engine.execute(Command::Stats);
	if let ResponsePayload::Stats(stats) = stats_payload {
		assert_eq!(stats.total_keys, 0);
		assert_eq!(stats.used_memory_bytes, 0);
	}
}

#[test]
fn test_execute_single_crud_lifecycle() {
	let engine = make_test_engine();

	// 1. Initial Set -> Created
	let (status, _) = engine.execute(Command::Single(Operation::Set {
		key: "user:100".into(),
		value: Bytes::from_static(b"Alice"),
	}));
	assert_eq!(status, StatusCode::Created);

	// 2. Read Get -> Ok with Raw bytes
	let (status, payload) = engine.execute(Command::Single(Operation::Get {
		key: "user:100".into(),
	}));
	assert_eq!(status, StatusCode::Ok);
	assert_eq!(payload, ResponsePayload::Raw(Bytes::from_static(b"Alice")));

	// 3. Overwrite Set -> Ok
	let (status, _) = engine.execute(Command::Single(Operation::Set {
		key: "user:100".into(),
		value: Bytes::from_static(b"AliceUpdated"),
	}));
	assert_eq!(status, StatusCode::Ok);

	// 4. Add should fail with KeyExists
	let (status, _) = engine.execute(Command::Single(Operation::Add {
		key: "user:100".into(),
		value: Bytes::from_static(b"AliceDuplicate"),
	}));
	assert_eq!(status, StatusCode::KeyExists);

	// 5. Replace succeeds
	let (status, _) = engine.execute(Command::Single(Operation::Replace {
		key: "user:100".into(),
		value: Bytes::from_static(b"AliceReplaced"),
	}));
	assert_eq!(status, StatusCode::Ok);

	// 6. Delete removes entry -> NoContent
	let (status, _) = engine.execute(Command::Single(Operation::Delete {
		key: "user:100".into(),
	}));
	assert_eq!(status, StatusCode::NoContent);

	// 7. Get after Delete -> KeyNotFound
	let (status, _) = engine.execute(Command::Single(Operation::Get {
		key: "user:100".into(),
	}));
	assert_eq!(status, StatusCode::KeyNotFound);
}

#[test]
fn test_execute_single_expiry_and_ttl() {
	let engine = make_test_engine();

	engine.execute(Command::Single(Operation::Set {
		key: "session:xyz".into(),
		value: Bytes::from_static(b"active"),
	}));

	// Initial TTL on persistent entry is -1
	let (status, payload) = engine.execute(Command::Single(Operation::Ttl {
		key: "session:xyz".into(),
	}));
	assert_eq!(status, StatusCode::Ok);
	assert_eq!(payload, ResponsePayload::Int64(-1));

	// Set Expire for 5000ms
	let (status, _) = engine.execute(Command::Single(Operation::Expire {
		key: "session:xyz".into(),
		ttl_ms: 5000,
	}));
	assert_eq!(status, StatusCode::NoContent);

	// TTL should now be > 0 and <= 5000
	let (status, payload) = engine.execute(Command::Single(Operation::Ttl {
		key: "session:xyz".into(),
	}));
	assert_eq!(status, StatusCode::Ok);
	if let ResponsePayload::Int64(ms) = payload {
		assert!(ms > 0 && ms <= 5000);
	} else {
		panic!("Expected Int64 payload for TTL");
	}

	// Persist removes TTL
	let (status, _) = engine.execute(Command::Single(Operation::Persist {
		key: "session:xyz".into(),
	}));
	assert_eq!(status, StatusCode::NoContent);

	// Calling Persist again on persistent key fails PreconditionFailed
	let (status, _) = engine.execute(Command::Single(Operation::Persist {
		key: "session:xyz".into(),
	}));
	assert_eq!(status, StatusCode::PreconditionFailed);
}

#[test]
fn test_execute_batch_success() {
	let engine = make_test_engine();

	// Single shard prefix "acc"
	let ops = vec![
		Operation::Set {
			key: "acc:1".into(),
			value: Bytes::from_static(b"v1"),
		},
		Operation::Set {
			key: "acc:2".into(),
			value: Bytes::from_static(b"v2"),
		},
		Operation::Get {
			key: "acc:1".into(),
		},
	];

	let (status, payload) = engine.execute(Command::Batch(ops));
	assert_eq!(status, StatusCode::Ok);

	if let ResponsePayload::Array(results) = payload {
		assert_eq!(results.len(), 3);
		assert_eq!(results[0].status, StatusCode::Created);
		assert_eq!(results[1].status, StatusCode::Created);
		assert_eq!(results[2].status, StatusCode::Ok);
		assert_eq!(
			results[2].payload,
			ResponsePayload::Raw(Bytes::from_static(b"v1"))
		);
	} else {
		panic!("Expected Array payload for Batch");
	}
}

#[test]
fn test_execute_batch_short_circuit_and_rollback() {
	let engine = make_test_engine();

	// Baseline key
	engine.execute(Command::Single(Operation::Set {
		key: "tx:orig".into(),
		value: Bytes::from_static(b"initial"),
	}));

	let ops = vec![
		// 1. Mutate existing
		Operation::Set {
			key: "tx:orig".into(),
			value: Bytes::from_static(b"mutated"),
		},
		// 2. Create new
		Operation::Set {
			key: "tx:new".into(),
			value: Bytes::from_static(b"temp"),
		},
		// 3. Fails: Add on existing key
		Operation::Add {
			key: "tx:orig".into(),
			value: Bytes::from_static(b"conflict"),
		},
		// 4. Should never run
		Operation::Set {
			key: "tx:unreached".into(),
			value: Bytes::from_static(b"noop"),
		},
	];

	let (status, payload) = engine.execute(Command::Batch(ops));
	assert_eq!(status, StatusCode::BatchShortCircuited);

	if let ResponsePayload::BatchError(err) = payload {
		assert_eq!(err.failed_index, 2);
		assert_eq!(err.failure_code, StatusCode::KeyExists);
	} else {
		panic!("Expected Array payload");
	}

	// Verify state was completely rolled back
	let (_, get_orig) = engine.execute(Command::Single(Operation::Get {
		key: "tx:orig".into(),
	}));
	assert_eq!(
		get_orig,
		ResponsePayload::Raw(Bytes::from_static(b"initial"))
	);

	let (status_new, _) = engine.execute(Command::Single(Operation::Get {
		key: "tx:new".into(),
	}));
	assert_eq!(status_new, StatusCode::KeyNotFound);
}