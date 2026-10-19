use crate::db::rollback::RollbackGuard;
use crate::db::shard::Shard;
use bytes::Bytes;
use quty_proto::{Operation, StatusCode};
use std::time::Instant;

#[test]
fn test_rollback_on_failed_suboperation() {
	let mut shard = Shard::new(
		1024, 
		128, 
		8192
	);
	let now = Instant::now();
	let clock = 0;

	// Pre-populate baseline state
	shard
		.insert(
			"account:1".into(),
			Bytes::from_static(b"initial_val"),
			None,
			clock,
		)
		.unwrap();
	let initial_bytes = shard.used_memory_bytes();

	{
		let mut guard = RollbackGuard::new(&mut shard, 3);

		// Sub-op 1: Overwrite existing key (generates UndoAction::Restore)
		let op1 = Operation::Set {
			key: "account:1".into(),
			value: Bytes::from_static(b"modified_val"),
		};
		let (s1, _) = guard.execute_op(&op1, now, clock);
		assert_eq!(s1, StatusCode::Ok);

		// Sub-op 2: Add brand-new key (generates UndoAction::Delete)
		let op2 = Operation::Set {
			key: "account:2".into(),
			value: Bytes::from_static(b"brand_new"),
		};
		let (s2, _) = guard.execute_op(&op2, now, clock);
		assert_eq!(s2, StatusCode::Created);

		// Sub-op 3: Condition failure
		let op3 = Operation::Add {
			key: "account:1".into(), // Already exists -> fails
			value: Bytes::from_static(b"conflict"),
		};
		let (s3, _) = guard.execute_op(&op3, now, clock);
		assert_eq!(s3, StatusCode::KeyExists);

		// Drop without calling guard.commit() triggers rollback
	}

	// Verify shard reverted to exact pre-batch state
	assert_eq!(shard.len(), 1);
	assert_eq!(shard.used_memory_bytes(), initial_bytes);

	let restored_val = shard.get_value("account:1", now, clock).unwrap();
	assert_eq!(restored_val.as_ref(), b"initial_val");
	assert!(shard.get_value("account:2", now, clock).is_none());
}

#[test]
fn test_rollback_on_thread_panic_unwinding() {
	let mut shard = Shard::new(
		1024,
		128,
		8192
	);
	let now = Instant::now();
	let clock = 0;

	shard
		.insert("ledger:10".into(), Bytes::from_static(b"100"), None, clock)
		.unwrap();

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let mut guard = RollbackGuard::new(&mut shard, 2);
		guard.execute_op(
			&Operation::Set {
				key: "ledger:10".into(),
				value: Bytes::from_static(b"200"),
			},
			now,
			clock,
		);
		guard.execute_op(
			&Operation::Set {
				key: "ledger:11".into(),
				value: Bytes::from_static(b"300"),
			},
			now,
			clock,
		);

		panic!("Simulated worker thread crash!");
	}));

	assert!(result.is_err(), "Worker thread must unwind");

	// RAII drop must have executed during stack unwinding
	assert_eq!(shard.len(), 1);
	let val = shard.get_value("ledger:10", now, clock).unwrap();
	assert_eq!(val.as_ref(), b"100");
}