use bytes::Bytes;
use quty::db::{Engine, EngineConfig};
use quty::net::Connection;
use quty_client::Client;
use quty_proto::{BatchErrorPayload, Operation, ResponsePayload, StatusCode};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn spawn_test_server() -> (std::net::SocketAddr, CancellationToken) {
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let addr = listener.local_addr().unwrap();
	let cancel_token = CancellationToken::new();

	let engine = Engine::new(EngineConfig {
		num_shards: 16,
		max_memory_bytes: 10 * 1024 * 1024,
	});

	let loop_cancel = cancel_token.clone();
	tokio::spawn(async move {
		loop {
			tokio::select! {
				_ = loop_cancel.cancelled() => break,
				accept_res = listener.accept() => {
					if let Ok((socket, peer_addr)) = accept_res {
						let conn_engine = Arc::clone(&engine);
						let conn_cancel = loop_cancel.clone();

						tokio::spawn(async move {
							let conn = Connection::new(socket, peer_addr, conn_engine, conn_cancel);
							conn.run().await;
						});
					}
				}
			}
		}
	});

	(addr, cancel_token)
}

#[tokio::test]
async fn test_end_to_end_crud_lifecycle() {
	let (addr, _cancel) = spawn_test_server().await;
	let mut client = Client::connect(addr).await.unwrap();

	// 1. Ping
	let ping_res = client.ping().await.unwrap();
	assert_eq!(ping_res.response.status, StatusCode::Ok);
	assert_eq!(ping_res.response.payload, ResponsePayload::Null);

	// 2. Initial Set -> Created
	let set_res = client.set("user:101", "Alice").await.unwrap();
	assert_eq!(set_res.response.status, StatusCode::Created);

	// 3. Read Get -> Ok with Raw bytes
	let get_res = client.get("user:101").await.unwrap();
	assert_eq!(get_res.response.status, StatusCode::Ok);
	assert_eq!(
		get_res.response.payload,
		ResponsePayload::Raw(Bytes::from_static(b"Alice"))
	);

	// 4. Overwrite Set -> Ok
	let overwrite_res = client.set("user:101", "AliceUpdated").await.unwrap();
	assert_eq!(overwrite_res.response.status, StatusCode::Ok);

	// 5. Delete -> NoContent
	let del_res = client.delete("user:101").await.unwrap();
	assert_eq!(del_res.response.status, StatusCode::NoContent);

	// 6. Get after delete -> KeyNotFound
	let missing_res = client.get("user:101").await.unwrap();
	assert_eq!(missing_res.response.status, StatusCode::KeyNotFound);
}

#[tokio::test]
async fn test_end_to_end_ttl_and_persist() {
	let (addr, _cancel) = spawn_test_server().await;
	let mut client = Client::connect(addr).await.unwrap();

	client.set("cache:temp", "payload").await.unwrap();

	// Expire in 60,000 ms
	let exp_res = client.expire("cache:temp", 60_000).await.unwrap();
	assert_eq!(exp_res.response.status, StatusCode::NoContent);

	// Query TTL
	let ttl_res = client.ttl("cache:temp").await.unwrap();
	assert_eq!(ttl_res.response.status, StatusCode::Ok);
	if let ResponsePayload::Int64(ms) = ttl_res.response.payload {
		assert!(ms > 0 && ms <= 60_000);
	} else {
		panic!("Expected Int64 payload for TTL query");
	}

	// Persist removes expiry
	let persist_res = client.persist("cache:temp").await.unwrap();
	assert_eq!(persist_res.response.status, StatusCode::NoContent);

	// Calling Persist again on persistent key returns PreconditionFailed
	let re_persist = client.persist("cache:temp").await.unwrap();
	assert_eq!(
		re_persist.response.status,
		StatusCode::PreconditionFailed
	);
}

#[tokio::test]
async fn test_end_to_end_batch_transaction_rollback() {
	let (addr, _cancel) = spawn_test_server().await;
	let mut client = Client::connect(addr).await.unwrap();

	client.set("tenant:baseline", "stable").await.unwrap();

	// Batch with uniform routing prefix "tenant"
	let ops = vec![
		Operation::Set {
			key: "tenant:1".into(),
			value: Bytes::from_static(b"val1"),
		},
		Operation::Add {
			key: "tenant:baseline".into(), // Exists! Fails with KeyExists
			value: Bytes::from_static(b"conflict"),
		},
	];

	let batch_exec = client.batch(ops).await.unwrap();
	assert_eq!(
		batch_exec.response.status,
		StatusCode::BatchShortCircuited
	);
	assert_eq!(
		batch_exec.response.payload,
		ResponsePayload::BatchError(BatchErrorPayload {
			failed_index: 1,
			failure_code: StatusCode::KeyExists,
		})
	);

	// Verify tenant:1 was reverted and never persisted
	let rolled_back = client.get("tenant:1").await.unwrap();
	assert_eq!(rolled_back.response.status, StatusCode::KeyNotFound);

	// Verify baseline was not corrupted or modified
	let baseline = client.get("tenant:baseline").await.unwrap();
	assert_eq!(baseline.response.status, StatusCode::Ok);
	assert_eq!(
		baseline.response.payload,
		ResponsePayload::Raw(Bytes::from_static(b"stable"))
	);
}

#[tokio::test]
async fn test_end_to_end_cross_shard_batch_rejection() {
	let (addr, _cancel) = spawn_test_server().await;
	let mut client = Client::connect(addr).await.unwrap();

	// Batch with different routing keys
	let ops = vec![
		Operation::Set {
			key: "alpha:1".into(),
			value: Bytes::from_static(b"1"),
		},
		Operation::Set {
			key: "beta:2".into(),
			value: Bytes::from_static(b"2"),
		},
	];

	let res = client.batch(ops).await.unwrap();
	assert_eq!(res.response.status, StatusCode::CrossShardViolation);
}

#[tokio::test]
async fn test_end_to_end_graceful_shutdown() {
	let (addr, cancel) = spawn_test_server().await;
	let mut client = Client::connect(addr).await.unwrap();

	// Verify server processes initial command
	let res = client.set("shutdown:test", "val").await.unwrap();
	assert_eq!(res.response.status, StatusCode::Created);

	// Trigger graceful cancellation
	cancel.cancel();

	// Active connection should finish in-flight operations or clean up without hanging
	let get_res = client.get("shutdown:test").await;
	// Client either reads response or encounters clean EOF
	if let Ok(res) = get_res {
		assert_eq!(res.response.status, StatusCode::Ok);
	}
}