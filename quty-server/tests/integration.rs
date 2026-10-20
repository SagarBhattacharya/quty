//! End-to-end integration test suite for the Quty cache server.

use bytes::Bytes;
use quty_client::Client;
use quty_proto::{Operation, StatusCode};
use quty_server::{EngineConfig, Server, ServerConfig};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

/// Spawns an isolated background server on an ephemeral OS-assigned port.
fn spawn_test_server(engine_config: EngineConfig) -> SocketAddr {
	// 1. Acquire an ephemeral port and IMMEDIATELY drop the probe socket
	// so the port is free for `Server::run()` to bind.
	let addr = {
		let probe =
			std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind ephemeral probe port");
		probe.local_addr().expect("Failed to get local address")
	};

	let server_config = ServerConfig {
		bind_addr: addr,
		engine: engine_config,
		sweep_interval_ms: 50,
		high_watermark: 0.80,
		low_watermark: 0.70,
		flush_threshold_bytes: 4096,
		shutdown_timeout_secs: 2,
	};

	let server = Server::new(server_config);
	tokio::spawn(async move {
		if let Err(e) = server.run().await {
			eprintln!("Test server runtime error on {addr}: {e}");
		}
	});

	addr
}

/// Connects to the test server with a fast polling retry loop.
async fn connect_test_client(addr: SocketAddr) -> Client {
	let start = std::time::Instant::now();
	let timeout = Duration::from_secs(3);

	while start.elapsed() < timeout {
		if let Ok(client) = Client::connect(addr).await {
			return client;
		}
		sleep(Duration::from_millis(5)).await;
	}

	panic!("Timed out waiting to connect to test server at {addr}");
}

#[tokio::test]
async fn test_client_crud_lifecycle_over_tcp() {
	let addr = spawn_test_server(EngineConfig {
		num_shards: 4,
		max_memory_bytes: 1024 * 1024,
		..Default::default()
	});

	let mut client = connect_test_client(addr).await;

	// 1. Ping
	let res = client.ping().await.expect("Ping failed");
	assert_eq!(res.response.header.status, StatusCode::Ok);

	// 2. Set key
	let res = client
		.set("session:100", "payload_abc")
		.await
		.expect("Set failed");
	assert_eq!(res.response.header.status, StatusCode::Created);

	// 3. Get key
	let res = client.get("session:100").await.expect("Get failed");
	assert_eq!(res.response.header.status, StatusCode::Ok);
	assert_eq!(
		res.response.payload,
		quty_proto::ResponsePayload::Raw(Bytes::from_static(b"payload_abc"))
	);

	// 4. Exists key
	let res = client.exists("session:100").await.expect("Exists failed");
	assert_eq!(res.response.header.status, StatusCode::Ok);
	assert_eq!(
		res.response.payload,
		quty_proto::ResponsePayload::Bool(true)
	);

	// 5. Delete key
	let res = client.delete("session:100").await.expect("Delete failed");
	assert_eq!(res.response.header.status, StatusCode::NoContent);

	// 6. Get deleted key returns KeyNotFound
	let res = client.get("session:100").await.expect("Get failed");
	assert_eq!(res.response.header.status, StatusCode::KeyNotFound);
}

#[tokio::test]
async fn test_ttl_and_expiration_lifecycle_over_tcp() {
	let addr = spawn_test_server(EngineConfig::default());
	let mut client = connect_test_client(addr).await;

	client.set("cache:ttl_item", "data").await.unwrap();

	// Initial TTL on persistent entry is -1
	let res = client.ttl("cache:ttl_item").await.unwrap();
	assert_eq!(res.response.payload, quty_proto::ResponsePayload::Int64(-1));

	// Attach 300ms TTL
	let res = client.expire("cache:ttl_item", 300).await.unwrap();
	assert_eq!(res.response.header.status, StatusCode::NoContent);

	// Query active TTL
	let res = client.ttl("cache:ttl_item").await.unwrap();
	if let quty_proto::ResponsePayload::Int64(ms) = res.response.payload {
		assert!(ms > 0 && ms <= 300);
	} else {
		panic!("Expected Int64 TTL payload");
	}

	// Await expiration deadline
	sleep(Duration::from_millis(350)).await;

	// Passive lazy-expiry check
	let res = client.get("cache:ttl_item").await.unwrap();
	assert!(res.response.header.status.is_soft_error());
}

#[tokio::test]
async fn test_atomic_batch_transaction_and_rollback_over_tcp() {
	let addr = spawn_test_server(EngineConfig::default());
	let mut client = connect_test_client(addr).await;

	// Pre-populate baseline state
	client.set("order:999:status", "pending").await.unwrap();

	// Batch where sub-operation 3 fails precondition (ADD on existing key)
	let batch = vec![
		Operation::Set {
			key: "order:999:status".into(),
			value: Bytes::from_static(b"processed"),
		},
		Operation::Set {
			key: "order:999:total".into(),
			value: Bytes::from_static(b"150"),
		},
		Operation::Add {
			key: "order:999:status".into(),
			value: Bytes::from_static(b"conflict"),
		},
	];

	let res = client.batch(batch).await.expect("Batch dispatch failed");
	assert_eq!(res.response.header.status, StatusCode::BatchShortCircuited);

	if let quty_proto::ResponsePayload::BatchError(err) = res.response.payload {
		assert_eq!(err.failed_index, 2);
		assert_eq!(err.failure_code, StatusCode::KeyExists);
	} else {
		panic!("Expected BatchError payload");
	}

	// Verify atomic rollback
	let res = client.get("order:999:status").await.unwrap();
	assert_eq!(
		res.response.payload,
		quty_proto::ResponsePayload::Raw(Bytes::from_static(b"pending"))
	);

	let res = client.get("order:999:total").await.unwrap();
	assert_eq!(res.response.header.status, StatusCode::KeyNotFound);
}

#[tokio::test]
async fn test_cross_shard_batch_violation_rejection() {
	let addr = spawn_test_server(EngineConfig::default());
	let mut client = connect_test_client(addr).await;

	let invalid_batch = vec![
		Operation::Set {
			key: "tenantA:key1".into(),
			value: Bytes::from_static(b"v1"),
		},
		Operation::Set {
			key: "tenantB:key2".into(),
			value: Bytes::from_static(b"v2"),
		},
	];

	let res = client.batch(invalid_batch).await.unwrap();
	assert_eq!(res.response.header.status, StatusCode::CrossShardViolation);
}

#[tokio::test]
async fn test_concurrent_clients_with_pipelining() {
	let addr = spawn_test_server(EngineConfig {
		num_shards: 16,
		max_memory_bytes: 4 * 1024 * 1024,
		..Default::default()
	});

	let worker_count = 4;
	let ops_per_worker = 50;
	let mut handles = Vec::with_capacity(worker_count);

	for worker_id in 0..worker_count {
		let server_addr = addr;
		handles.push(tokio::spawn(async move {
			let mut client = connect_test_client(server_addr).await;

			for i in 0..ops_per_worker {
				let key = format!("worker:{worker_id}:item:{i}");
				let val = format!("val_{worker_id}_{i}");

				let set_res = client
					.set(&key, Bytes::copy_from_slice(val.as_bytes()))
					.await
					.unwrap();
				assert_eq!(set_res.response.header.status, StatusCode::Created);

				let get_res = client.get(&key).await.unwrap();
				assert_eq!(get_res.response.header.status, StatusCode::Ok);
				assert_eq!(
					get_res.response.payload,
					quty_proto::ResponsePayload::Raw(Bytes::copy_from_slice(val.as_bytes()))
				);
			}
		}));
	}

	for handle in handles {
		handle.await.expect("Worker task panicked");
	}

	let mut admin_client = connect_test_client(addr).await;
	let stats_res = admin_client.stats().await.unwrap();
	if let quty_proto::ResponsePayload::Stats(stats) = stats_res.response.payload {
		assert_eq!(stats.total_keys, (worker_count * ops_per_worker) as u64);
		assert_eq!(stats.get_hits, (worker_count * ops_per_worker) as u64);
	} else {
		panic!("Expected Stats payload");
	}
}
