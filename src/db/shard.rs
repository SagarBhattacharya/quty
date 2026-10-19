use crate::db::entry::Entry;
use bytes::Bytes;
use smallvec::SmallVec;
use std::time::Instant;
use ahash::AHashMap;

pub const DEFAULT_SAMPLE_SIZE: usize = 5;
pub const MAX_EVICTION_ATTEMPTS: usize = 16;
pub const INLINE_HEADROOM_BYTES: usize = 64 * 1024; // 64 KB buffer

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityExceeded;

pub struct Shard {
	entries: AHashMap<Box<str>, Entry>,
	used_payload_bytes: usize,
	max_payload_bytes: usize,
	sample_cursor: usize, // Monotonic stride cursor for uniform sampling
	eviction_burst_limit: usize,
}

impl Shard {
	pub fn new(
		max_payload_bytes: usize,
		eviction_burst_limit: usize,
		initial_capacity: usize,
	) -> Self {
		Self {
			// Pre-allocate bucket space for ~8k keys per shard
			entries: AHashMap::with_capacity(initial_capacity),
			used_payload_bytes: 0,
			max_payload_bytes,
			sample_cursor: 0,
			eviction_burst_limit,
		}
	}

	#[inline(always)]
	fn entry_size(key: impl AsRef<[u8]>, val: impl AsRef<[u8]>) -> usize {
		key.as_ref().len() + val.as_ref().len()
	}

	#[inline]
	fn try_adjust_memory(
		used_bytes: &mut usize,
		max_bytes: usize,
		old_size: usize,
		new_size: usize,
	) -> bool {
		if new_size > old_size {
			let delta = new_size - old_size;
			if *used_bytes + delta > max_bytes {
				return false;
			}
			*used_bytes += delta;
		} else {
			let delta = old_size - new_size;
			*used_bytes = used_bytes.saturating_sub(delta);
		}
		true
	}

	// -------------------------------------------------------------------------
	// Core Storage Primitives
	// -------------------------------------------------------------------------

	#[inline]
	pub fn entry(&self, key: &str) -> Option<&Entry> {
		self.entries.get(key)
	}

	#[inline]
	pub fn get_value(
		&self,
		key: &str,
		now: Instant,
		current_clock: u32
	) -> Option<Bytes> {
		let entry = self.entries.get(key)?;
		if entry.is_expired(now) {
			return None;
		}
		entry.touch(current_clock);
		Some(entry.value.clone())
	}

	/// Inserts an entry with automatic inline eviction if capacity is reached.
	pub fn insert(
		&mut self,
		key: Box<str>,
		value: Bytes,
		expires_at: Option<Instant>,
		current_clock: u32,
	) -> Result<(Option<Entry>, usize), CapacityExceeded> {
		if let Some(existing) = self.entries.get(&key) {
			existing.touch(current_clock); // Protect key from its own headroom eviction
		}
		
		let new_size = Self::entry_size(&*key, &value);
		let old_size = self
			.entries
			.get(&key)
			.map(|e| Self::entry_size(&*key, &e.value))
			.unwrap_or(0);

		let mut evicted_count = 0;

		// Verify if mutation demands memory expansion beyond current budget
		if new_size > old_size {
			let required = new_size - old_size;
			if self.used_payload_bytes + required > self.max_payload_bytes {
				// Cap headroom so it never exceeds 1/8 of shard capacity
				let headroom = INLINE_HEADROOM_BYTES.min(self.max_payload_bytes / 8);
				evicted_count = self.evict_headroom(required, headroom, current_clock)?;
			}
		}

		// Apply mutation
		if let Some(existing) = self.entries.get_mut(&key) {
			Self::try_adjust_memory(
				&mut self.used_payload_bytes,
				self.max_payload_bytes,
				old_size,
				new_size,
			);
			let old_entry = std::mem::replace(
				existing,
				Entry::new(value, expires_at, current_clock),
			);
			Ok((Some(old_entry), evicted_count))
		} else {
			Self::try_adjust_memory(
				&mut self.used_payload_bytes,
				self.max_payload_bytes,
				0,
				new_size,
			);
			self.entries.insert(key, Entry::new(value, expires_at, current_clock));
			Ok((None, evicted_count))
		}
	}

	pub fn remove(&mut self, key: &str) -> Option<Entry> {
		let removed = self.entries.remove(key)?;
		let freed_size = Self::entry_size(key, &removed.value);
		Self::try_adjust_memory(
			&mut self.used_payload_bytes,
			self.max_payload_bytes,
			freed_size,
			0,
		);
		Some(removed)
	}

	pub fn update_expiry(
		&mut self,
		key: &str,
		expires_at: Option<Instant>,
		now: Instant,
	) -> Option<Option<Instant>> {
		let is_valid = self.entries.get(key).map(|e| !e.is_expired(now));
		match is_valid {
			Some(true) => {
				let entry = self.entries.get_mut(key).unwrap();
				let prev = entry.expires_at;
				entry.expires_at = expires_at;
				Some(prev)
			}
			Some(false) => {
				self.remove(key);
				None
			}
			None => None,
		}
	}

	/// Inserts an entry without running inline eviction.
	///
	/// Used by atomic batches to guarantee that a batch operation never evicts
	/// unrelated keys. If capacity is exceeded, it fails immediately.
	pub fn insert_no_evict(
		&mut self,
		key: Box<str>,
		value: Bytes,
		expires_at: Option<Instant>,
		current_clock: u32,
	) -> Result<Option<Entry>, CapacityExceeded> {
		let new_size = Self::entry_size(&*key, &value);
		let old_size = self
			.entries
			.get(&key)
			.map(|e| Self::entry_size(&*key, &e.value))
			.unwrap_or(0);

		if new_size > old_size {
			let required = new_size - old_size;
			if self.used_payload_bytes + required > self.max_payload_bytes {
				return Err(CapacityExceeded);
			}
		}

		if let Some(existing) = self.entries.get_mut(&key) {
			Self::try_adjust_memory(
				&mut self.used_payload_bytes,
				self.max_payload_bytes,
				old_size,
				new_size,
			);
			let old_entry = std::mem::replace(
				existing,
				Entry::new(value, expires_at, current_clock),
			);
			Ok(Some(old_entry))
		} else {
			Self::try_adjust_memory(
				&mut self.used_payload_bytes,
				self.max_payload_bytes,
				0,
				new_size,
			);
			self.entries.insert(key, Entry::new(value, expires_at, current_clock));
			Ok(None)
		}
	}

	/// Directly restores a displaced entry during rollback.
	///
	/// Accounts for memory adjustments without touching the LRU clock or triggering eviction.
	pub fn restore_entry(&mut self, key: Box<str>, entry: Entry) {
		let restored_size = Self::entry_size(&*key, &entry.value);
		let key_size = key.len();
		if let Some(current) = self.entries.insert(key, entry) {
			let current_size = key_size + current.value.len();
			self.used_payload_bytes = self
				.used_payload_bytes
				.saturating_sub(current_size)
				.saturating_add(restored_size);
		} else {
			self.used_payload_bytes = self.used_payload_bytes.saturating_add(restored_size);
		}
	}

	/// Reverts an entry's expiration deadline to its pre-batch state.
	pub fn revert_expiry(&mut self, key: &str, previous_deadline: Option<Instant>) {
		if let Some(entry) = self.entries.get_mut(key) {
			entry.expires_at = previous_deadline;
		}
	}

	pub fn clear(&mut self) {
		self.entries.clear();
		self.used_payload_bytes = 0;
		self.sample_cursor = 0;
	}

	// -------------------------------------------------------------------------
	// Eviction Machinery
	// -------------------------------------------------------------------------

	/// Uniformly samples `sample_size` keys across the map using a rotating cursor stride.
	pub fn find_lru_candidate(
		&mut self,
		sample_size: usize,
		current_clock: u32
	) -> Option<Box<str>> {
		let len = self.entries.len();
		if len == 0 {
			return None;
		}

		// Advance cursor with a large prime stride to cycle uniformly across buckets
		self.sample_cursor = (self.sample_cursor + 37) % len;
		let skip_amount = self.sample_cursor;

		let mut oldest_key: Option<Box<str>> = None;
		let mut max_idle: u32 = 0;

		// Chain the iterator to wrap around the map seamlessly
		let sample_count = sample_size.min(len);
		let sampled = self.entries.iter().skip(skip_amount)
			.chain(self.entries.iter()).take(sample_count);

		for (k, entry) in sampled {
			let idle = entry.idle_duration(current_clock);
			if oldest_key.is_none() || idle >= max_idle {
				max_idle = idle;
				oldest_key = Some(k.clone());
			}
		}

		oldest_key
	}

	/// Evicts up to `max_keys` LRU candidates until target bytes are reached.
	/// Returns the number of keys evicted.
	pub fn evict_to_target(
		&mut self,
		target_bytes: usize,
		current_clock: u32,
	) -> usize {
		let mut evicted = 0;

		while self.used_payload_bytes > target_bytes 
			&& evicted < self.eviction_burst_limit {
			if let Some(candidate_key) = self.find_lru_candidate(
				DEFAULT_SAMPLE_SIZE, current_clock
			) {
				self.remove(&candidate_key);
				evicted += 1;
			} else {
				break;
			}
		}

		evicted
	}

	/// Emergency inline eviction: frees `required_bytes + headroom_bytes` up to a bounded attempt ceiling.
	pub fn evict_headroom(
		&mut self,
		required_bytes: usize,
		headroom_bytes: usize,
		current_clock: u32,
	) -> Result<usize, CapacityExceeded> {
		if required_bytes > self.max_payload_bytes {
			return Err(CapacityExceeded);
		}

		// Clamp effective headroom so target_bytes is never crushed to 0 when capacity exists
		let effective_headroom = headroom_bytes
			.min(self.max_payload_bytes / 8);
		let target_bytes = self
			.max_payload_bytes
			.saturating_sub(required_bytes + effective_headroom);
		let mut evicted = 0;

		for _ in 0..MAX_EVICTION_ATTEMPTS {
			if self.used_payload_bytes <= target_bytes {
				break;
			}

			if let Some(candidate) = self.find_lru_candidate(
				DEFAULT_SAMPLE_SIZE, 
				current_clock
			) {
				self.remove(&candidate);
				evicted += 1;
			} else {
				break;
			}
		}

		if self.used_payload_bytes + required_bytes 
			<= self.max_payload_bytes {
			Ok(evicted)
		} else {
			Err(CapacityExceeded)
		}
	}

	// -------------------------------------------------------------------------
	// Housekeeping
	// -------------------------------------------------------------------------

	#[inline]
	pub fn len(&self) -> usize {
		self.entries.len()
	}

	#[inline]
	pub fn used_memory_bytes(&self) -> usize {
		self.used_payload_bytes
	}

	#[inline]
	pub fn max_memory_bytes(&self) -> usize {
		self.max_payload_bytes
	}

	pub fn sweep_expired(&mut self, sample_size: usize, now: Instant) -> usize {
		let len = self.entries.len();
		if len == 0 {
			return 0;
		}

		// Cap sample count to available elements to prevent duplicate items from chain wrap
		let sample_count = sample_size.min(len);
		self.sample_cursor = (self.sample_cursor + 37) % len;
		let skip_amount = self.sample_cursor;

		let mut expired_keys: SmallVec<Box<str>, 16> = SmallVec::new();

		let sampled = self
			.entries
			.iter()
			.skip(skip_amount)
			.chain(self.entries.iter())
			.take(sample_count);

		for (k, entry) in sampled {
			if entry.is_expired(now) {
				expired_keys.push(k.clone());
			}
		}

		let purged = expired_keys.len();
		for key in &expired_keys {
			self.remove(key);
		}
		purged
	}
}