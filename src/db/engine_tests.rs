use crate::db::{Engine};
use crate::config::EngineConfig;
use bytes::Bytes;
use quty_proto::{Command, Operation, StatusCode};

#[test]
fn test_batch_cross_shard_routing_guard() {
	let config = EngineConfig {
		num_shards: 16,
		max_memory_bytes: 1024 * 1024,
		..Default::default()
	};
	let engine = Engine::new(config);

	// Batch containing operations with differing routing prefixes
	let ops = vec![
		Operation::Set {
			key: "tenantA:key1".into(),
			value: Bytes::from_static(b"1"),
		},
		Operation::Set {
			key: "tenantB:key2".into(),
			value: Bytes::from_static(b"2"),
		},
	];

	let (status, _) = engine.execute(Command::Batch(ops));
	assert_eq!(status, StatusCode::CrossShardViolation);
}

#[test]
fn test_stats_and_telemetry_isolation() {
	let engine = Engine::new(EngineConfig::default());

	engine.on_client_connected();
	engine.on_client_connected();
	engine.on_client_disconnected();

	// Trigger one hit and one miss
	engine.execute(Command::Single(Operation::Set {
		key: "item:1".into(),
		value: Bytes::from_static(b"val"),
	}));
	engine.execute(Command::Single(Operation::Get {
		key: "item:1".into(),
	}));
	engine.execute(Command::Single(Operation::Get {
		key: "item:missing".into(),
	}));

	let stats = engine.collect_stats();
	assert_eq!(stats.total_connections, 2);
	assert_eq!(stats.curr_connections, 1);
	assert_eq!(stats.get_hits, 1);
	assert_eq!(stats.get_misses, 1);
	assert_eq!(stats.total_keys, 1);
}

#[test]
fn test_background_maintenance_sweep() {
	let config = EngineConfig {
		num_shards: 2,
		max_memory_bytes: 200, // 100 bytes per shard
		..Default::default()
	};
	let engine = Engine::new(config);

	// Populate 3 entries on shard 0 (3 * 21B = 63B, which is 63% full)
	// 63% is safely above the 50% high watermark and below the 100% capacity wall
	let routing_prefix = "user";
	for i in 0..3 {
		engine.execute(Command::Single(Operation::Set {
			key: format!("{}:{}", routing_prefix, i),
			value: Bytes::from(vec![0xAA; 15]), // 6B key + 15B val = 21B per entry
		}));
	}

	let before_stats = engine.collect_stats();
	assert_eq!(before_stats.used_memory_bytes, 63);
	assert_eq!(before_stats.evictions, 0, "No inline evictions should occur during setup");

	// Run sweep with 50% high watermark (50B) and 30% low watermark (30B)
	let (_expired, evicted) = engine.run_maintenance_sweep(0.50, 0.30);
	assert!(evicted > 0, "Maintenance sweep must evict keys above watermark");

	let after_stats = engine.collect_stats();
	assert!(after_stats.used_memory_bytes <= 30, "Memory must drop down to low watermark target");
	assert_eq!(after_stats.evictions, evicted as u64);
}