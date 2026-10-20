//! Dumb partition storage primitive with dense key indexing.

use super::entry::Entry;
use ahash::AHashMap;
use bytes::Bytes;
use std::time::Instant;

/// Error returned when shard storage capacity is exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityExceeded;

/// Storage partition holding hash buckets, dense key index, and byte counters.
pub struct Shard {
	pub(super) entries: AHashMap<Box<str>, Entry>,
	pub(super) keys: Vec<Box<str>>,
	pub(super) used_payload_bytes: usize,
	pub(super) max_payload_bytes: usize,
}

impl Shard {
	/// Creates a new empty partition shard.
	#[must_use]
	pub fn new(max_payload_bytes: usize, initial_capacity: usize) -> Self {
		Self {
			entries: AHashMap::with_capacity(initial_capacity),
			keys: Vec::with_capacity(initial_capacity),
			used_payload_bytes: 0,
			max_payload_bytes,
		}
	}

	#[inline]
	fn entry_size(key_len: usize, val_len: usize) -> usize {
		key_len + val_len
	}

	/// Computes the net memory delta required to store or update a key-value pair.
	#[inline]
	#[must_use]
	pub fn delta_required(&self, key: &str, new_val_len: usize) -> usize {
		let new_size = Self::entry_size(key.len(), new_val_len);
		let old_size = self
			.entries
			.get(key)
			.map_or(0, |e| Self::entry_size(key.len(), e.value.len()));
		new_size.saturating_sub(old_size)
	}

	/// Touches the access clock on a key if present, shielding it from eviction.
	#[inline]
	pub fn touch(&self, key: &str, current_clock: u32) {
		if let Some(entry) = self.entries.get(key) {
			entry.touch(current_clock);
		}
	}

	/// Retrieves an immutable reference to an entry by key.
	#[inline]
	#[must_use]
	pub fn entry(&self, key: &str) -> Option<&Entry> {
		self.entries.get(key)
	}

	/// Returns `true` if a key is present and has not expired.
	#[inline]
	#[must_use]
	pub fn is_key_active(&self, key: &str, now: Instant) -> bool {
		self.entries.get(key).is_some_and(|e| !e.is_expired(now))
	}

	/// Total count of keys in the dense index.
	#[inline]
	#[must_use]
	pub fn key_count(&self) -> usize {
		self.keys.len()
	}

	/// Fetches a key clone at index position for sampling.
	#[inline]
	#[must_use]
	pub fn key_at(&self, idx: usize) -> Option<Box<str>> {
		self.keys.get(idx).cloned()
	}

	/// Reads entry bytes and updates the LRU access clock if unexpired.
	pub fn get_value(&self, key: &str, now: Instant, current_clock: u32) -> Option<Bytes> {
		let entry = self.entries.get(key)?;
		if entry.is_expired(now) {
			return None;
		}
		entry.touch(current_clock);
		Some(entry.value.clone())
	}

	/// Inserts or updates an entry without redundant heap allocation on existing keys.
	///
	/// # Errors
	///
	/// Returns [`CapacityExceeded`] if available memory cannot accommodate the delta.
	pub fn insert(
		&mut self,
		key: &str,
		value: Bytes,
		expires_at: Option<Instant>,
		current_clock: u32,
	) -> Result<Option<Entry>, CapacityExceeded> {
		let new_size = Self::entry_size(key.len(), value.len());

		if let Some(existing) = self.entries.get_mut(key) {
			let old_size = Self::entry_size(key.len(), existing.value.len());

			if new_size > old_size {
				let delta = new_size - old_size;
				if self.used_payload_bytes + delta > self.max_payload_bytes {
					return Err(CapacityExceeded);
				}
				self.used_payload_bytes += delta;
			} else {
				let delta = old_size - new_size;
				self.used_payload_bytes = self.used_payload_bytes.saturating_sub(delta);
			}

			let key_idx = existing.key_index;
			let old = std::mem::replace(
				existing,
				Entry::new(value, expires_at, current_clock, key_idx),
			);
			return Ok(Some(old));
		}

		if self.used_payload_bytes + new_size > self.max_payload_bytes {
			return Err(CapacityExceeded);
		}

		self.used_payload_bytes += new_size;
		let boxed: Box<str> = key.into();
		let key_idx = self.keys.len();
		self.keys.push(boxed.clone());
		self
			.entries
			.insert(boxed, Entry::new(value, expires_at, current_clock, key_idx));

		Ok(None)
	}

	/// Removes an entry and updates the indexed sampling array via O(1) swap-remove.
	pub fn remove(&mut self, key: &str) -> Option<Entry> {
		let removed = self.entries.remove(key)?;
		let freed_size = Self::entry_size(key.len(), removed.value.len());
		self.used_payload_bytes = self.used_payload_bytes.saturating_sub(freed_size);

		let idx = removed.key_index;
		if idx < self.keys.len() {
			self.keys.swap_remove(idx);
			if idx < self.keys.len() {
				let swapped_key = &self.keys[idx];
				if let Some(swapped_entry) = self.entries.get_mut(swapped_key) {
					swapped_entry.key_index = idx;
				}
			}
		}

		Some(removed)
	}

	/// Updates expiration deadline without taking panicking branches.
	///
	/// Returns:
	/// * `Some(Some(prev))` if the key was active and had an existing deadline.
	/// * `Some(None)` if the key was active and had no deadline (was persistent).
	/// * `None` if the key was not found or was expired.
	#[allow(clippy::option_option)]
	pub fn update_expiry(
		&mut self,
		key: &str,
		expires_at: Option<Instant>,
		now: Instant,
	) -> Option<Option<Instant>> {
		match self.entries.get(key).map(|e| e.is_expired(now)) {
			Some(true) => {
				self.remove(key);
				None
			}
			Some(false) => {
				let entry = self.entries.get_mut(key)?;
				let prev = entry.expires_at;
				entry.expires_at = expires_at;
				Some(prev)
			}
			None => None,
		}
	}

	/// Directly restores a displaced entry during transaction rollback.
	pub fn restore_entry(&mut self, key: Box<str>, mut entry: Entry) {
		let restored_size = Self::entry_size(key.len(), entry.value.len());

		if let Some(existing) = self.entries.get_mut(&key) {
			let current_size = Self::entry_size(key.len(), existing.value.len());
			entry.key_index = existing.key_index;
			*existing = entry;
			self.used_payload_bytes = self
				.used_payload_bytes
				.saturating_sub(current_size)
				.saturating_add(restored_size);
		} else {
			entry.key_index = self.keys.len();
			self.keys.push(key.clone());
			self.used_payload_bytes = self.used_payload_bytes.saturating_add(restored_size);
			self.entries.insert(key, entry);
		}
	}

	/// Reverts an entry expiration deadline during transaction rollback.
	pub fn revert_expiry(&mut self, key: &str, previous_deadline: Option<Instant>) {
		if let Some(entry) = self.entries.get_mut(key) {
			entry.expires_at = previous_deadline;
		}
	}

	/// Wipes all entries and resets memory metrics.
	pub fn clear(&mut self) {
		self.entries.clear();
		self.keys.clear();
		self.used_payload_bytes = 0;
	}

	#[inline]
	#[must_use]
	pub fn len(&self) -> usize {
		self.entries.len()
	}

	#[inline]
	#[must_use]
	#[cfg(test)]
	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	#[inline]
	#[must_use]
	pub fn used_memory_bytes(&self) -> usize {
		self.used_payload_bytes
	}

	#[inline]
	#[must_use]
	pub fn max_memory_bytes(&self) -> usize {
		self.max_payload_bytes
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::Bytes;
	use std::time::Duration;

	#[test]
	fn test_shard_initial_state() {
		let shard = Shard::new(1024, 64);
		assert_eq!(shard.len(), 0);
		assert!(shard.is_empty());
		assert_eq!(shard.key_count(), 0);
		assert_eq!(shard.used_memory_bytes(), 0);
		assert_eq!(shard.max_memory_bytes(), 1024);
		assert!(shard.key_at(0).is_none());
	}

	#[test]
	fn test_shard_basic_insert_get_remove_lifecycle() {
		let mut shard = Shard::new(1024, 64);
		let now = Instant::now();
		let key = "user:profile";
		let val = Bytes::from_static(b"profile_payload"); // 15 bytes, key = 12 bytes -> total 27 bytes

		// 1. Initial insert
		let insert_res = shard.insert(key, val.clone(), None, 10);
		assert!(matches!(insert_res, Ok(None)));
		assert_eq!(shard.len(), 1);
		assert!(!shard.is_empty());
		assert_eq!(shard.key_count(), 1);
		assert_eq!(shard.used_memory_bytes(), 27);
		assert!(shard.is_key_active(key, now));

		// 2. Read back
		let read_val = shard.get_value(key, now, 20);
		assert_eq!(read_val, Some(val));

		// 3. Remove entry
		let removed = shard.remove(key);
		assert!(removed.is_some());
		assert_eq!(shard.len(), 0);
		assert!(shard.is_empty());
		assert_eq!(shard.key_count(), 0);
		assert_eq!(shard.used_memory_bytes(), 0);
		assert!(shard.get_value(key, now, 20).is_none());
		assert!(!shard.is_key_active(key, now));
	}

	#[test]
	fn test_delta_required_computation() {
		let mut shard = Shard::new(1024, 64);
		let key = "account:1"; // 9 bytes

		// Absent key: demands full entry size (9 + 10 = 19 bytes)
		assert_eq!(shard.delta_required(key, 10), 19);

		// Insert baseline of 10-byte payload
		shard
			.insert(key, Bytes::from_static(b"0123456789"), None, 0)
			.unwrap();

		// Expanding to 15-byte payload demands 5 bytes delta
		assert_eq!(shard.delta_required(key, 15), 5);

		// Shrinking to 4-byte payload demands 0 bytes delta
		assert_eq!(shard.delta_required(key, 4), 0);

		// Unchanged payload demands 0 bytes delta
		assert_eq!(shard.delta_required(key, 10), 0);
	}

	#[test]
	fn test_shard_in_place_overwrite_memory_accounting() {
		let mut shard = Shard::new(1024, 64);
		let key = "cache:key"; // 9 bytes

		// Initial 9 + 11 = 20 bytes
		shard
			.insert(key, Bytes::from_static(b"payload_val"), None, 0)
			.unwrap();
		assert_eq!(shard.used_memory_bytes(), 20);

		// Overwrite with larger payload (16 bytes) -> 9 + 16 = 25 bytes
		let overwrite_larger = shard.insert(key, Bytes::from_static(b"longer_payload__"), None, 0);
		assert!(matches!(overwrite_larger, Ok(Some(_))));
		assert_eq!(shard.used_memory_bytes(), 25);
		assert_eq!(shard.len(), 1);

		// Overwrite with smaller payload (4 bytes) -> 9 + 4 = 13 bytes
		let overwrite_smaller = shard.insert(key, Bytes::from_static(b"tiny"), None, 0);
		assert!(matches!(overwrite_smaller, Ok(Some(_))));
		assert_eq!(shard.used_memory_bytes(), 13);
		assert_eq!(shard.len(), 1);
	}

	#[test]
	fn test_shard_insert_capacity_exceeded() {
		let mut shard = Shard::new(50, 64);

		// 1. Initial insert exceeding maximum capacity fails
		let oversized = shard.insert("big:key", Bytes::from(vec![0u8; 50]), None, 0);
		assert_eq!(oversized.unwrap_err(), CapacityExceeded);
		assert_eq!(shard.len(), 0);
		assert_eq!(shard.used_memory_bytes(), 0);

		// 2. Insert valid entry: 5 + 35 = 40 bytes (<= 50)
		assert!(
			shard
				.insert("k:fit", Bytes::from(vec![0u8; 35]), None, 0)
				.is_ok()
		);
		assert_eq!(shard.used_memory_bytes(), 40);

		// 3. Second insert pushing memory to 40 + 15 = 55 (> 50) fails
		let second = shard.insert("k:two", Bytes::from(vec![0u8; 10]), None, 0);
		assert_eq!(second.unwrap_err(), CapacityExceeded);
		assert_eq!(shard.len(), 1);
		assert_eq!(shard.used_memory_bytes(), 40);

		// 4. In-place expansion pushing memory over limit fails and keeps original
		let expansion = shard.insert("k:fit", Bytes::from(vec![0u8; 48]), None, 0);
		assert_eq!(expansion.unwrap_err(), CapacityExceeded);
		assert_eq!(shard.used_memory_bytes(), 40);
		assert_eq!(
			shard.get_value("k:fit", Instant::now(), 0).unwrap().len(),
			35
		);
	}

	#[test]
	fn test_lazy_expiry_in_get_value_and_is_key_active() {
		let mut shard = Shard::new(1024, 64);
		let now = Instant::now();
		let key = "sess:temp";

		let past = now
			.checked_sub(Duration::from_millis(50))
			.expect("valid time");
		shard
			.insert(key, Bytes::from_static(b"secret"), Some(past), 0)
			.unwrap();

		// Expired entry is neither active nor readable
		assert!(!shard.is_key_active(key, now));
		assert!(shard.get_value(key, now, 10).is_none());

		// Entry remains physically retained until evicted or removed
		assert!(shard.entry(key).is_some());
		assert_eq!(shard.len(), 1);
		assert_eq!(shard.used_memory_bytes(), 9 + 6);
	}

	#[test]
	fn test_touch_updates_entry_clock() {
		let mut shard = Shard::new(1024, 64);
		shard
			.insert("k:1", Bytes::from_static(b"val"), None, 10)
			.unwrap();

		assert_eq!(shard.entry("k:1").unwrap().idle_duration(20), 10);

		shard.touch("k:1", 20);
		assert_eq!(shard.entry("k:1").unwrap().idle_duration(20), 0);
	}

	#[test]
	fn test_swap_remove_key_indexing() {
		let mut shard = Shard::new(1024, 64);

		shard
			.insert("k:0", Bytes::from_static(b"v0"), None, 0)
			.unwrap();
		shard
			.insert("k:1", Bytes::from_static(b"v1"), None, 0)
			.unwrap();
		shard
			.insert("k:2", Bytes::from_static(b"v2"), None, 0)
			.unwrap();
		assert_eq!(shard.key_count(), 3);

		// Remove the middle key ("k:1")
		let removed = shard.remove("k:1");
		assert!(removed.is_some());
		assert_eq!(shard.key_count(), 2);

		// "k:2" is swapped from tail into index 1
		assert_eq!(shard.key_at(0).as_deref(), Some("k:0"));
		assert_eq!(shard.key_at(1).as_deref(), Some("k:2"));
		assert_eq!(shard.entry("k:2").unwrap().key_index, 1);

		// Remove tail key ("k:2")
		shard.remove("k:2");
		assert_eq!(shard.key_count(), 1);
		assert_eq!(shard.key_at(0).as_deref(), Some("k:0"));

		// Remove remaining head key ("k:0")
		shard.remove("k:0");
		assert_eq!(shard.key_count(), 0);
		assert!(shard.is_empty());
	}

	#[test]
	fn test_update_expiry_distinctions() {
		let mut shard = Shard::new(1024, 64);
		let now = Instant::now();

		shard
			.insert("k:ttl", Bytes::from_static(b"val"), None, 0)
			.unwrap();

		// 1. Attaching TTL to persistent key -> returns Some(None)
		let t1 = now + Duration::from_secs(60);
		let prev1 = shard.update_expiry("k:ttl", Some(t1), now);
		assert_eq!(prev1, Some(None));

		// 2. Updating TTL on active key -> returns Some(Some(prev))
		let t2 = now + Duration::from_secs(120);
		let prev2 = shard.update_expiry("k:ttl", Some(t2), now);
		assert_eq!(prev2, Some(Some(t1)));

		// 3. Stripping TTL -> returns Some(Some(prev))
		let prev3 = shard.update_expiry("k:ttl", None, now);
		assert_eq!(prev3, Some(Some(t2)));

		// 4. Expired entry update removes key and returns None
		let past = now
			.checked_sub(Duration::from_millis(10))
			.expect("valid time");
		shard
			.insert("k:exp", Bytes::from_static(b"val"), Some(past), 0)
			.unwrap();
		assert_eq!(shard.update_expiry("k:exp", Some(t1), now), None);
		assert!(shard.entry("k:exp").is_none());

		// 5. Missing key returns None
		assert_eq!(shard.update_expiry("k:missing", Some(t1), now), None);
	}

	#[test]
	fn test_restore_entry_for_new_and_existing() {
		let mut shard = Shard::new(1024, 64);
		let key: Box<str> = "restored:key".into(); // 12 bytes

		// 1. Restore brand-new entry
		let entry1 = Entry::new(Bytes::from_static(b"first_val"), None, 10, 0); // 9 bytes -> 21 bytes
		shard.restore_entry(key.clone(), entry1);
		assert_eq!(shard.len(), 1);
		assert_eq!(shard.key_count(), 1);
		assert_eq!(shard.used_memory_bytes(), 21);
		assert_eq!(shard.key_at(0).as_deref(), Some("restored:key"));

		// 2. Restore over existing entry with smaller payload
		let entry2 = Entry::new(Bytes::from_static(b"tiny"), None, 20, 0); // 4 bytes -> 16 bytes
		shard.restore_entry(key, entry2);
		assert_eq!(shard.len(), 1);
		assert_eq!(shard.key_count(), 1);
		assert_eq!(shard.used_memory_bytes(), 16);
	}

	#[test]
	fn test_revert_expiry() {
		let mut shard = Shard::new(1024, 64);
		let now = Instant::now();
		let deadline = now + Duration::from_secs(300);

		shard
			.insert("k:rev", Bytes::from_static(b"val"), None, 0)
			.unwrap();
		assert!(shard.entry("k:rev").unwrap().expires_at.is_none());

		shard.revert_expiry("k:rev", Some(deadline));
		assert_eq!(shard.entry("k:rev").unwrap().expires_at, Some(deadline));
	}

	#[test]
	fn test_clear_resets_everything() {
		let mut shard = Shard::new(1024, 64);
		shard
			.insert("k:1", Bytes::from_static(b"v1"), None, 0)
			.unwrap();
		shard
			.insert("k:2", Bytes::from_static(b"v2"), None, 0)
			.unwrap();

		assert_eq!(shard.len(), 2);
		assert!(shard.used_memory_bytes() > 0);

		shard.clear();
		assert_eq!(shard.len(), 0);
		assert!(shard.is_empty());
		assert_eq!(shard.key_count(), 0);
		assert_eq!(shard.used_memory_bytes(), 0);
	}
}
