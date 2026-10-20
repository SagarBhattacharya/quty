//! Unified execution pipeline for both single operations and atomic batches.

use super::lru::LruPolicy;
use super::shard::{CapacityExceeded, Shard};
use super::tx::{TxContext, UndoRecord};
use bytes::Bytes;
use quty_proto::{Operation, ResponsePayload, StatusCode};
use std::time::{Duration, Instant};

/// Executes a read-only operation against an immutable shard reference.
#[must_use]
pub fn execute_read(
	shard: &Shard,
	op: &Operation,
	now: Instant,
	clock: u32,
) -> (StatusCode, ResponsePayload, bool) {
	match op {
		Operation::Get { key } => {
			if let Some(value) = shard.get_value(key, now, clock) {
				(StatusCode::Ok, ResponsePayload::Raw(value), true)
			} else {
				let status = match shard.entry(key) {
					Some(entry) if entry.is_expired(now) => StatusCode::KeyExpired,
					_ => StatusCode::KeyNotFound,
				};
				(status, ResponsePayload::Null, false)
			}
		}
		Operation::Exists { key } => {
			let exists = shard.is_key_active(key, now);
			(StatusCode::Ok, ResponsePayload::Bool(exists), exists)
		}
		Operation::Ttl { key } => {
			let Some(entry) = shard.entry(key) else {
				return (StatusCode::KeyNotFound, ResponsePayload::Null, false);
			};

			match entry.expires_at {
				None => (StatusCode::Ok, ResponsePayload::Int64(-1), true),
				Some(deadline) => {
					if now >= deadline {
						(StatusCode::KeyNotFound, ResponsePayload::Null, false)
					} else {
						#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
						let ms = deadline.duration_since(now).as_millis() as i64;
						(StatusCode::Ok, ResponsePayload::Int64(ms), true)
					}
				}
			}
		}
		mutating_op => unreachable!("Invariant violation: mutating op {mutating_op:?} in read path"),
	}
}

/// Executes a mutating storage operation, recording undo records if running within a transaction.
pub fn execute_mutation(
	shard: &mut Shard,
	lru: &LruPolicy,
	op: &Operation,
	ctx: &mut TxContext<'_>,
	now: Instant,
	clock: u32,
) -> (StatusCode, ResponsePayload, usize) {
	match op {
		Operation::Set { key, value } => {
			apply_write(shard, lru, key, value.clone(), ctx, clock, |old| {
				if old.is_some_and(|e| !e.is_expired(now)) {
					StatusCode::Ok
				} else {
					StatusCode::Created
				}
			})
		}

		Operation::Add { key, value } => {
			if shard.is_key_active(key, now) {
				return (StatusCode::KeyExists, ResponsePayload::Null, 0);
			}
			apply_write(shard, lru, key, value.clone(), ctx, clock, |_| {
				StatusCode::Created
			})
		}

		Operation::Replace { key, value } => {
			if !shard.is_key_active(key, now) {
				return (StatusCode::KeyNotFound, ResponsePayload::Null, 0);
			}
			apply_write(shard, lru, key, value.clone(), ctx, clock, |_| {
				StatusCode::Ok
			})
		}

		Operation::Delete { key } => {
			if let Some(removed) = shard.remove(key) {
				let was_expired = removed.is_expired(now);
				ctx.record_undo(UndoRecord::Restore {
					key: key.clone().into_boxed_str(),
					entry: removed,
				});
				if was_expired {
					(StatusCode::KeyNotFound, ResponsePayload::Null, 0)
				} else {
					(StatusCode::NoContent, ResponsePayload::Null, 0)
				}
			} else {
				(StatusCode::KeyNotFound, ResponsePayload::Null, 0)
			}
		}

		Operation::Expire { key, ttl_ms } => {
			if !shard.is_key_active(key, now) {
				return (StatusCode::KeyNotFound, ResponsePayload::Null, 0);
			}

			let deadline = now
				.checked_add(Duration::from_millis(*ttl_ms))
				.unwrap_or(now);
			if let Some(prev) = shard.update_expiry(key, Some(deadline), now) {
				ctx.record_undo(UndoRecord::RevertExpiry {
					key: key.clone().into_boxed_str(),
					previous_deadline: prev,
				});
				(StatusCode::NoContent, ResponsePayload::Null, 0)
			} else {
				(StatusCode::KeyNotFound, ResponsePayload::Null, 0)
			}
		}

		Operation::Persist { key } => match shard.entry(key) {
			Some(e) if !e.is_expired(now) => {
				if e.expires_at.is_none() {
					(StatusCode::PreconditionFailed, ResponsePayload::Null, 0)
				} else if let Some(prev) = shard.update_expiry(key, None, now) {
					ctx.record_undo(UndoRecord::RevertExpiry {
						key: key.clone().into_boxed_str(),
						previous_deadline: prev,
					});
					(StatusCode::NoContent, ResponsePayload::Null, 0)
				} else {
					(StatusCode::KeyNotFound, ResponsePayload::Null, 0)
				}
			}
			_ => (StatusCode::KeyNotFound, ResponsePayload::Null, 0),
		},

		read_op => {
			let (status, payload, _) = execute_read(shard, read_op, now, clock);
			(status, payload, 0)
		}
	}
}

/// Consolidated mutation helper eliminating duplication across `SET`, `ADD`, and `REPLACE`.
#[inline]
fn apply_write(
	shard: &mut Shard,
	lru: &LruPolicy,
	key: &str,
	value: Bytes,
	ctx: &mut TxContext<'_>,
	clock: u32,
	status_fn: impl FnOnce(Option<&super::entry::Entry>) -> StatusCode,
) -> (StatusCode, ResponsePayload, usize) {
	let mut evicted = 0;

	// Shield existing key from self-eviction
	shard.touch(key, clock);

	let delta = shard.delta_required(key, value.len());
	if delta > 0 && shard.used_memory_bytes() + delta > shard.max_memory_bytes() {
		if !ctx.allow_inline_eviction {
			return (StatusCode::StorageFull, ResponsePayload::Null, 0);
		}
		match lru.evict_headroom(shard, delta, clock) {
			Ok(ev) => evicted += ev,
			Err(CapacityExceeded) => return (StatusCode::StorageFull, ResponsePayload::Null, 0),
		}
	}

	let status = status_fn(shard.entry(key));

	let old_entry = match shard.insert(key, value, None, clock) {
		Ok(old) => old,
		Err(CapacityExceeded) => return (StatusCode::StorageFull, ResponsePayload::Null, 0),
	};

	if let Some(old) = old_entry {
		ctx.record_undo(UndoRecord::Restore {
			key: key.into(),
			entry: old,
		});
	} else {
		ctx.record_undo(UndoRecord::Delete { key: key.into() });
	}

	(status, ResponsePayload::Null, evicted)
}

#[cfg(test)]
mod tests {
	use super::*;
	use smallvec::SmallVec;
	use std::time::Duration;

	fn make_test_fixture(capacity: usize) -> (Shard, LruPolicy) {
		(Shard::new(capacity, 64), LruPolicy::new(128))
	}

	// =========================================================================
	// Read Execution Tests
	// =========================================================================

	#[test]
	fn test_execute_read_get() {
		let (mut shard, _) = make_test_fixture(1024);
		let now = Instant::now();

		// 1. Missing key
		let (status, payload, is_hit) =
			execute_read(&shard, &Operation::Get { key: "k:1".into() }, now, 10);
		assert_eq!(status, StatusCode::KeyNotFound);
		assert_eq!(payload, ResponsePayload::Null);
		assert!(!is_hit);

		// 2. Active hit
		shard
			.insert("k:1", Bytes::from_static(b"val1"), None, 10)
			.unwrap();
		let (status, payload, is_hit) =
			execute_read(&shard, &Operation::Get { key: "k:1".into() }, now, 15);
		assert_eq!(status, StatusCode::Ok);
		assert_eq!(payload, ResponsePayload::Raw(Bytes::from_static(b"val1")));
		assert!(is_hit);

		// 3. Expired hit -> returns KeyExpired and is_hit = false
		let past = now
			.checked_sub(Duration::from_millis(10))
			.expect("valid time");
		shard
			.insert("k:exp", Bytes::from_static(b"val2"), Some(past), 10)
			.unwrap();
		let (status, payload, is_hit) = execute_read(
			&shard,
			&Operation::Get {
				key: "k:exp".into(),
			},
			now,
			15,
		);
		assert_eq!(status, StatusCode::KeyExpired);
		assert_eq!(payload, ResponsePayload::Null);
		assert!(!is_hit);
	}

	#[test]
	fn test_execute_read_exists() {
		let (mut shard, _) = make_test_fixture(1024);
		let now = Instant::now();

		// Absent
		let (status, payload, exists) = execute_read(
			&shard,
			&Operation::Exists {
				key: "k:none".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::Ok);
		assert_eq!(payload, ResponsePayload::Bool(false));
		assert!(!exists);

		// Active
		shard
			.insert("k:act", Bytes::from_static(b"val"), None, 0)
			.unwrap();
		let (status, payload, exists) = execute_read(
			&shard,
			&Operation::Exists {
				key: "k:act".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::Ok);
		assert_eq!(payload, ResponsePayload::Bool(true));
		assert!(exists);

		// Expired
		let past = now
			.checked_sub(Duration::from_millis(10))
			.expect("valid time");
		shard
			.insert("k:exp", Bytes::from_static(b"val"), Some(past), 0)
			.unwrap();
		let (status, payload, exists) = execute_read(
			&shard,
			&Operation::Exists {
				key: "k:exp".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::Ok);
		assert_eq!(payload, ResponsePayload::Bool(false));
		assert!(!exists);
	}

	#[test]
	fn test_execute_read_ttl() {
		let (mut shard, _) = make_test_fixture(1024);
		let now = Instant::now();

		// 1. Missing
		let (status, _, is_hit) = execute_read(
			&shard,
			&Operation::Ttl {
				key: "k:none".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::KeyNotFound);
		assert!(!is_hit);

		// 2. Persistent entry -> -1
		shard
			.insert("k:perm", Bytes::from_static(b"val"), None, 0)
			.unwrap();
		let (status, payload, is_hit) = execute_read(
			&shard,
			&Operation::Ttl {
				key: "k:perm".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::Ok);
		assert_eq!(payload, ResponsePayload::Int64(-1));
		assert!(is_hit);

		// 3. Active TTL -> remaining ms
		let future = now + Duration::from_millis(5000);
		shard
			.insert("k:ttl", Bytes::from_static(b"val"), Some(future), 0)
			.unwrap();
		let (status, payload, is_hit) = execute_read(
			&shard,
			&Operation::Ttl {
				key: "k:ttl".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::Ok);
		if let ResponsePayload::Int64(ms) = payload {
			assert!(ms > 0 && ms <= 5000);
		} else {
			panic!("Expected Int64 payload for TTL");
		}
		assert!(is_hit);

		// 4. Expired entry -> KeyNotFound
		let past = now
			.checked_sub(Duration::from_millis(10))
			.expect("valid time");
		shard
			.insert("k:exp", Bytes::from_static(b"val"), Some(past), 0)
			.unwrap();
		let (status, _, is_hit) = execute_read(
			&shard,
			&Operation::Ttl {
				key: "k:exp".into(),
			},
			now,
			0,
		);
		assert_eq!(status, StatusCode::KeyNotFound);
		assert!(!is_hit);
	}

	// =========================================================================
	// Mutation Execution Tests
	// =========================================================================

	#[test]
	fn test_execute_mutation_set_lifecycle() {
		let (mut shard, lru) = make_test_fixture(1024);
		let now = Instant::now();
		let clock = 10;
		let mut undo_log = SmallVec::new();

		// 1. Initial Set on empty key -> Created (records UndoRecord::Delete)
		let op1 = Operation::Set {
			key: "user:1".into(),
			value: Bytes::from_static(b"Alice"),
		};
		let (s1, _, ev1) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &op1, &mut ctx, now, clock)
		};
		assert_eq!(s1, StatusCode::Created);
		assert_eq!(ev1, 0);
		assert_eq!(undo_log.len(), 1);
		assert!(matches!(undo_log[0], UndoRecord::Delete { .. }));

		// 2. Overwrite active key -> Ok (records UndoRecord::Restore)
		let op2 = Operation::Set {
			key: "user:1".into(),
			value: Bytes::from_static(b"AliceUpdated"),
		};
		let (s2, _, ev2) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &op2, &mut ctx, now, clock)
		};
		assert_eq!(s2, StatusCode::Ok);
		assert_eq!(ev2, 0);
		assert_eq!(undo_log.len(), 2);
		assert!(matches!(undo_log[1], UndoRecord::Restore { .. }));

		// 3. Overwrite expired key -> Created
		let past = now
			.checked_sub(Duration::from_millis(10))
			.expect("valid time");
		shard
			.insert("user:ghost", Bytes::from_static(b"old"), Some(past), clock)
			.unwrap();
		let op3 = Operation::Set {
			key: "user:ghost".into(),
			value: Bytes::from_static(b"new"),
		};
		let (s3, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &op3, &mut ctx, now, clock)
		};
		assert_eq!(s3, StatusCode::Created);
	}

	#[test]
	fn test_execute_mutation_add_and_replace() {
		let (mut shard, lru) = make_test_fixture(1024);
		let now = Instant::now();
		let clock = 10;
		let mut ctx = TxContext::direct();

		// 1. ADD on absent key -> Created
		let add_op = Operation::Add {
			key: "item:1".into(),
			value: Bytes::from_static(b"v1"),
		};
		let (s1, _, _) = execute_mutation(&mut shard, &lru, &add_op, &mut ctx, now, clock);
		assert_eq!(s1, StatusCode::Created);

		// 2. ADD on existing active key -> KeyExists
		let (s2, _, _) = execute_mutation(&mut shard, &lru, &add_op, &mut ctx, now, clock);
		assert_eq!(s2, StatusCode::KeyExists);

		// 3. REPLACE on existing key -> Ok
		let replace_op = Operation::Replace {
			key: "item:1".into(),
			value: Bytes::from_static(b"v2"),
		};
		let (s3, _, _) = execute_mutation(&mut shard, &lru, &replace_op, &mut ctx, now, clock);
		assert_eq!(s3, StatusCode::Ok);

		// 4. REPLACE on absent key -> KeyNotFound
		let replace_missing = Operation::Replace {
			key: "item:missing".into(),
			value: Bytes::from_static(b"v3"),
		};
		let (s4, _, _) = execute_mutation(&mut shard, &lru, &replace_missing, &mut ctx, now, clock);
		assert_eq!(s4, StatusCode::KeyNotFound);
	}

	#[test]
	fn test_execute_mutation_delete() {
		let (mut shard, lru) = make_test_fixture(1024);
		let now = Instant::now();
		let clock = 10;
		let mut undo_log = SmallVec::new();

		shard
			.insert("k:del", Bytes::from_static(b"val"), None, clock)
			.unwrap();

		// 1. Delete active key -> NoContent
		let del_op = Operation::Delete {
			key: "k:del".into(),
		};
		let (status, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &del_op, &mut ctx, now, clock)
		};
		assert_eq!(status, StatusCode::NoContent);
		assert_eq!(undo_log.len(), 1);
		assert!(matches!(undo_log[0], UndoRecord::Restore { .. }));

		// 2. Delete absent key -> KeyNotFound
		let (status, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &del_op, &mut ctx, now, clock)
		};
		assert_eq!(status, StatusCode::KeyNotFound);
	}

	#[test]
	fn test_execute_mutation_expire_and_persist() {
		let (mut shard, lru) = make_test_fixture(1024);
		let now = Instant::now();
		let clock = 10;
		let mut undo_log = SmallVec::new();

		shard
			.insert("k:sess", Bytes::from_static(b"val"), None, clock)
			.unwrap();

		// 1. Persist on entry that is already persistent -> PreconditionFailed
		let persist_op = Operation::Persist {
			key: "k:sess".into(),
		};
		let (s1, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &persist_op, &mut ctx, now, clock)
		};
		assert_eq!(s1, StatusCode::PreconditionFailed);

		// 2. Expire attached to active key -> NoContent
		let expire_op = Operation::Expire {
			key: "k:sess".into(),
			ttl_ms: 10_000,
		};
		let (s2, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &expire_op, &mut ctx, now, clock)
		};
		assert_eq!(s2, StatusCode::NoContent);
		assert!(matches!(
			undo_log.last(),
			Some(UndoRecord::RevertExpiry { .. })
		));

		// 3. Persist on TTL key -> NoContent
		let (s3, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &persist_op, &mut ctx, now, clock)
		};
		assert_eq!(s3, StatusCode::NoContent);

		// 4. Expire on absent key -> KeyNotFound
		let exp_missing = Operation::Expire {
			key: "k:none".into(),
			ttl_ms: 5000,
		};
		let (s4, _, _) = {
			let mut ctx = TxContext::transactional(&mut undo_log);
			execute_mutation(&mut shard, &lru, &exp_missing, &mut ctx, now, clock)
		};
		assert_eq!(s4, StatusCode::KeyNotFound);
	}

	#[test]
	fn test_execute_mutation_inline_eviction_modes() {
		// Shard capacity = 50 bytes
		let (mut shard, lru) = make_test_fixture(50);
		let now = Instant::now();
		let clock = 10;

		// Baseline entry: 3 + 30 = 33 bytes
		shard
			.insert("k:1", Bytes::from(vec![0xAA; 30]), None, clock)
			.unwrap();

		// Second entry demands 3 + 25 = 28 bytes (33 + 28 = 61 > 50)
		let op = Operation::Set {
			key: "k:2".into(),
			value: Bytes::from(vec![0xBB; 25]),
		};

		// 1. Transactional mode prohibits inline eviction -> StorageFull
		let mut undo_log = SmallVec::new();
		let mut tx_ctx = TxContext::transactional(&mut undo_log);
		let (s_tx, _, ev_tx) = execute_mutation(&mut shard, &lru, &op, &mut tx_ctx, now, clock);
		assert_eq!(s_tx, StatusCode::StorageFull);
		assert_eq!(ev_tx, 0);
		assert_eq!(shard.len(), 1); // "k:1" was protected

		// 2. Direct mode permits inline eviction -> evicts "k:1" and succeeds with Created
		let mut direct_ctx = TxContext::direct();
		let (s_dir, _, ev_dir) = execute_mutation(&mut shard, &lru, &op, &mut direct_ctx, now, clock);
		assert_eq!(s_dir, StatusCode::Created);
		assert_eq!(ev_dir, 1);
		assert_eq!(shard.len(), 1);
		assert!(shard.entry("k:2").is_some());
		assert!(shard.entry("k:1").is_none());
	}

	#[test]
	fn test_execute_mutation_self_eviction_shielding() {
		// Shard capacity = 50 bytes
		let (mut shard, lru) = make_test_fixture(50);
		let now = Instant::now();
		let clock = 10;

		// Populate "k:self" taking 6 + 30 = 36 bytes
		shard
			.insert("k:self", Bytes::from(vec![0xAA; 30]), None, 1)
			.unwrap();

		// Overwrite "k:self" expanding to 40 bytes (requires 10 bytes expansion, total 46 <= 50)
		let expand_op = Operation::Set {
			key: "k:self".into(),
			value: Bytes::from(vec![0xBB; 40]),
		};
		let mut direct_ctx = TxContext::direct();
		let (status, _, evicted) =
			execute_mutation(&mut shard, &lru, &expand_op, &mut direct_ctx, now, clock);
		assert_eq!(status, StatusCode::Ok);
		assert_eq!(evicted, 0);
		assert_eq!(shard.len(), 1);
		assert_eq!(shard.get_value("k:self", now, clock).unwrap().len(), 40);
	}
}
