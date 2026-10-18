use crate::db::entry::Entry;
use crate::db::shard::{CapacityExceeded, Shard};
use quty_proto::{Operation, ResponsePayload, StatusCode};
use smallvec::SmallVec;
use std::time::{Duration, Instant};

/// Inverse action required to undo a forward mutation.
pub enum UndoAction {
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

impl UndoAction {
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

/// RAII transaction guard for single-shard atomic batch execution.
pub struct RollbackGuard<'a> {
	shard: &'a mut Shard,
	undo_stack: SmallVec<UndoAction, 16>,
	committed: bool,
}

impl<'a> RollbackGuard<'a> {
	pub fn new(shard: &'a mut Shard, expected_ops: usize) -> Self {
		Self {
			shard,
			undo_stack: SmallVec::with_capacity(expected_ops),
			committed: false,
		}
	}

	/// Disarms the guard on full batch success.
	#[inline]
	pub fn commit(mut self) {
		self.committed = true;
	}

	/// Executes a single batch operation, pushing an inverse action upon mutation.
	pub fn execute_op(
		&mut self,
		op: &Operation,
		now: Instant,
		clock: u32,
	) -> (StatusCode, ResponsePayload) {
		match op {
			Operation::Get { key } => {
				match self.shard.get_value(key, now, clock) {
					Some(value) => (StatusCode::Ok, ResponsePayload::Raw(value)),
					None => {
						let status = match self.shard.entry(key) {
							Some(entry) if entry.is_expired(now) => StatusCode::KeyExpired,
							_ => StatusCode::KeyNotFound,
						};
						(status, ResponsePayload::Null)
					}
				}
			}
			Operation::Exists { key } => {
				let exists = self
					.shard
					.entry(key)
					.map(|e| !e.is_expired(now))
					.unwrap_or(false);
				(StatusCode::Ok, ResponsePayload::Bool(exists))
			}
			Operation::Ttl { key } => {
				let entry = match self.shard.entry(key) {
					Some(e) => e,
					None => return (StatusCode::KeyNotFound, ResponsePayload::Null),
				};

				match entry.expires_at {
					None => (StatusCode::Ok, ResponsePayload::Int64(-1)),
					Some(deadline) => {
						if now >= deadline {
							(StatusCode::KeyNotFound, ResponsePayload::Null)
						} else {
							let ms = deadline.duration_since(now).as_millis() as i64;
							(StatusCode::Ok, ResponsePayload::Int64(ms))
						}
					}
				}
			}
			Operation::Set { key, value } => {
				let boxed_key: Box<str> = key.clone().into_boxed_str();
				match self.shard.insert_no_evict(
					boxed_key.clone(),
					value.clone(),
					None,
					clock,
				) {
					Ok(Some(old_entry)) => {
						let was_expired = old_entry.is_expired(now);
						self.undo_stack.push(UndoAction::Restore {
							key: boxed_key,
							entry: old_entry,
						});
						if was_expired {
							(StatusCode::Created, ResponsePayload::Null)
						} else {
							(StatusCode::Ok, ResponsePayload::Null)
						}
					}
					Ok(None) => {
						self.undo_stack.push(UndoAction::Delete { key: boxed_key });
						(StatusCode::Created, ResponsePayload::Null)
					}
					Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
				}
			}
			Operation::Add { key, value } => {
				let is_active = self.shard.entry(key).map(|e| !e.is_expired(now));
				match is_active {
					Some(true) => (StatusCode::KeyExists, ResponsePayload::Null),
					_ => {
						let boxed_key: Box<str> = key.clone().into_boxed_str();
						match self.shard.insert_no_evict(
							boxed_key.clone(),
							value.clone(),
							None,
							clock,
						) {
							Ok(Some(old_entry)) => {
								self.undo_stack.push(UndoAction::Restore {
									key: boxed_key,
									entry: old_entry,
								});
								(StatusCode::Created, ResponsePayload::Null)
							}
							Ok(None) => {
								self.undo_stack.push(UndoAction::Delete { key: boxed_key });
								(StatusCode::Created, ResponsePayload::Null)
							}
							Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
						}
					}
				}
			}
			Operation::Replace { key, value } => {
				let is_active = self.shard.entry(key).map(|e| !e.is_expired(now));
				match is_active {
					Some(true) => {
						let boxed_key: Box<str> = key.clone().into_boxed_str();
						match self.shard.insert_no_evict(
							boxed_key.clone(),
							value.clone(),
							None,
							clock,
						) {
							Ok(Some(old_entry)) => {
								self.undo_stack.push(UndoAction::Restore {
									key: boxed_key,
									entry: old_entry,
								});
								(StatusCode::Ok, ResponsePayload::Null)
							}
							Ok(None) => (StatusCode::Ok, ResponsePayload::Null),
							Err(CapacityExceeded) => (StatusCode::StorageFull, ResponsePayload::Null),
						}
					}
					_ => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
			Operation::Delete { key } => {
				match self.shard.remove(key) {
					Some(entry) => {
						let was_expired = entry.is_expired(now);
						self.undo_stack.push(UndoAction::Restore {
							key: key.clone().into_boxed_str(),
							entry,
						});
						if was_expired {
							(StatusCode::KeyNotFound, ResponsePayload::Null)
						} else {
							(StatusCode::NoContent, ResponsePayload::Null)
						}
					}
					None => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
			Operation::Expire { key, ttl_ms } => {
				let is_active = self.shard.entry(key).map(|e| !e.is_expired(now));
				if is_active != Some(true) {
					return (StatusCode::KeyNotFound, ResponsePayload::Null);
				}

				let deadline = now.checked_add(Duration::from_millis(*ttl_ms)).unwrap_or(now);
				let prev = self.shard.update_expiry(key, Some(deadline), now).flatten();
				self.undo_stack.push(UndoAction::RevertExpiry {
					key: key.clone().into_boxed_str(),
					previous_deadline: prev,
				});
				(StatusCode::NoContent, ResponsePayload::Null)
			}
			Operation::Persist { key } => {
				let entry_status = self
					.shard
					.entry(key)
					.map(|e| (e.is_expired(now), e.expires_at.is_some()));

				match entry_status {
					Some((false, true)) => {
						let prev = self.shard.update_expiry(key, None, now).flatten();
						self.undo_stack.push(UndoAction::RevertExpiry {
							key: key.clone().into_boxed_str(),
							previous_deadline: prev,
						});
						(StatusCode::NoContent, ResponsePayload::Null)
					}
					Some((false, false)) => (StatusCode::PreconditionFailed, ResponsePayload::Null),
					_ => (StatusCode::KeyNotFound, ResponsePayload::Null),
				}
			}
		}
	}
}

impl<'a> Drop for RollbackGuard<'a> {
	fn drop(&mut self) {
		if !self.committed {
			// Revert all mutations in reverse order
			while let Some(action) = self.undo_stack.pop() {
				action.apply(self.shard);
			}
		}
	}
}