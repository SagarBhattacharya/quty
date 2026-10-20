//! Transaction contexts, inverse action logs, and RAII rollback guards.

use super::entry::Entry;
use super::shard::Shard;
use smallvec::SmallVec;
use std::time::Instant;

/// Inverse action required to undo a forward mutation during rollback.
pub enum UndoRecord {
	/// Undoes a newly inserted key by removing it.
	Delete { key: Box<str> },
	/// Undoes an overwrite or deletion by restoring the original entry.
	Restore { key: Box<str>, entry: Entry },
	/// Undoes an expiration change by restoring the previous deadline.
	RevertExpiry {
		key: Box<str>,
		previous_deadline: Option<Instant>,
	},
}

impl UndoRecord {
	#[inline]
	fn apply(self, shard: &mut Shard) {
		match self {
			Self::Delete { key } => {
				shard.remove(&key);
			}
			Self::Restore { key, entry } => {
				shard.restore_entry(key, entry);
			}
			Self::RevertExpiry {
				key,
				previous_deadline,
			} => {
				shard.revert_expiry(&key, previous_deadline);
			}
		}
	}
}

/// Transaction execution context parameterizing single-op vs batch execution.
pub struct TxContext<'a> {
	/// Whether the mutation is permitted to run inline LRU eviction.
	pub allow_inline_eviction: bool,
	undo_log: Option<&'a mut SmallVec<UndoRecord, 16>>,
}

impl<'a> TxContext<'a> {
	/// Context for single operations (allows inline eviction, zero undo overhead).
	#[inline]
	#[must_use]
	pub fn direct() -> Self {
		Self {
			allow_inline_eviction: true,
			undo_log: None,
		}
	}

	/// Context for batch operations (disallows eviction, records undo log).
	#[inline]
	pub fn transactional(log: &'a mut SmallVec<UndoRecord, 16>) -> Self {
		Self {
			allow_inline_eviction: false,
			undo_log: Some(log),
		}
	}

	/// Pushes an inverse action if recording undo history.
	#[inline]
	pub fn record_undo(&mut self, record: UndoRecord) {
		if let Some(log) = &mut self.undo_log {
			log.push(record);
		}
	}
}

/// RAII transaction guard for single-shard atomic batch execution.
pub struct RollbackGuard<'a> {
	shard: &'a mut Shard,
	undo_stack: SmallVec<UndoRecord, 16>,
	committed: bool,
}

impl<'a> RollbackGuard<'a> {
	/// Constructs a new [`RollbackGuard`].
	#[must_use]
	pub fn new(shard: &'a mut Shard, expected_ops: usize) -> Self {
		Self {
			shard,
			undo_stack: SmallVec::with_capacity(expected_ops),
			committed: false,
		}
	}

	/// Returns a mutable reference to the underlying shard and the undo log.
	#[inline]
	pub fn split_mut(&mut self) -> (&mut Shard, &mut SmallVec<UndoRecord, 16>) {
		(self.shard, &mut self.undo_stack)
	}

	/// Disarms the guard on batch success.
	#[inline]
	pub fn commit(mut self) {
		self.committed = true;
	}
}

impl Drop for RollbackGuard<'_> {
	fn drop(&mut self) {
		if !self.committed {
			while let Some(action) = self.undo_stack.pop() {
				action.apply(self.shard);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{RollbackGuard, TxContext};
	use crate::database::exec::execute_mutation;
	use crate::database::lru::LruPolicy;
	use crate::database::shard::Shard;
	use bytes::Bytes;
	use quty_proto::{Operation, StatusCode};
	use std::time::Instant;

	#[test]
	fn test_rollback_on_failed_suboperation() {
		let mut shard = Shard::new(1024, 8192);
		let lru = LruPolicy::new(128);
		let now = Instant::now();
		let clock = 0;

		shard
			.insert("account:1", Bytes::from_static(b"initial_val"), None, clock)
			.unwrap();
		let initial_bytes = shard.used_memory_bytes();

		{
			let mut guard = RollbackGuard::new(&mut shard, 3);
			let (active_shard, undo_log) = guard.split_mut();
			let mut ctx = TxContext::transactional(undo_log);

			let op1 = Operation::Set {
				key: "account:1".into(),
				value: Bytes::from_static(b"modified_val"),
			};
			let (s1, _, _) = execute_mutation(active_shard, &lru, &op1, &mut ctx, now, clock);
			assert_eq!(s1, StatusCode::Ok);

			let op2 = Operation::Set {
				key: "account:2".into(),
				value: Bytes::from_static(b"brand_new"),
			};
			let (s2, _, _) = execute_mutation(active_shard, &lru, &op2, &mut ctx, now, clock);
			assert_eq!(s2, StatusCode::Created);

			let op3 = Operation::Add {
				key: "account:1".into(),
				value: Bytes::from_static(b"conflict"),
			};
			let (s3, _, _) = execute_mutation(active_shard, &lru, &op3, &mut ctx, now, clock);
			assert_eq!(s3, StatusCode::KeyExists);
		}

		assert_eq!(shard.len(), 1);
		assert_eq!(shard.used_memory_bytes(), initial_bytes);

		let restored_val = shard.get_value("account:1", now, clock).unwrap();
		assert_eq!(restored_val.as_ref(), b"initial_val");
		assert!(shard.get_value("account:2", now, clock).is_none());
	}

	#[test]
	fn test_rollback_on_thread_panic_unwinding() {
		let mut shard = Shard::new(1024, 8192);
		let lru = LruPolicy::new(128);
		let now = Instant::now();
		let clock = 0;

		shard
			.insert("ledger:10", Bytes::from_static(b"100"), None, clock)
			.unwrap();

		let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			let mut guard = RollbackGuard::new(&mut shard, 2);
			let (active_shard, undo_log) = guard.split_mut();
			let mut ctx = TxContext::transactional(undo_log);

			execute_mutation(
				active_shard,
				&lru,
				&Operation::Set {
					key: "ledger:10".into(),
					value: Bytes::from_static(b"200"),
				},
				&mut ctx,
				now,
				clock,
			);

			panic!("Simulated worker thread crash!");
		}));

		assert!(result.is_err(), "Worker thread must unwind");
		assert_eq!(shard.len(), 1);
		let val = shard.get_value("ledger:10", now, clock).unwrap();
		assert_eq!(val.as_ref(), b"100");
	}
}
