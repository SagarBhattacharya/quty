//! Stateless Sampled LRU replacement policy engine.

use super::shard::{CapacityExceeded, Shard};
use smallvec::SmallVec;
use std::time::Instant;

/// Default candidate sample size for Sampled LRU.
pub const DEFAULT_SAMPLE_SIZE: usize = 5;
/// Maximum candidate searches allowed before declaring capacity failure.
pub const MAX_EVICTION_ATTEMPTS: usize = 16;
/// Default headroom buffer target for inline evictions (64 KB).
pub const INLINE_HEADROOM_BYTES: usize = 64 * 1024;

/// Stateless Sampled LRU eviction policy.
#[derive(Debug, Clone, Copy)]
pub struct LruPolicy {
	sample_size: usize,
	eviction_burst_limit: usize,
}

impl LruPolicy {
	/// Creates a new [`LruPolicy`].
	#[must_use]
	pub const fn new(eviction_burst_limit: usize) -> Self {
		Self {
			sample_size: DEFAULT_SAMPLE_SIZE,
			eviction_burst_limit,
		}
	}

	/// Selects the least recently used candidate key from a sample.
	///
	/// If the shard has fewer keys than `sample_size`, inspects all keys directly.
	#[must_use]
	pub fn find_candidate(&self, shard: &Shard, current_clock: u32) -> Option<Box<str>> {
		let num_keys = shard.key_count();
		if num_keys == 0 {
			return None;
		}

		let mut oldest_key: Option<Box<str>> = None;
		let mut max_idle: u32 = 0;

		if num_keys <= self.sample_size {
			for idx in 0..num_keys {
				if let Some(candidate_key) = shard.key_at(idx)
					&& let Some(entry) = shard.entry(&candidate_key)
				{
					let idle = entry.idle_duration(current_clock);
					if oldest_key.is_none() || idle >= max_idle {
						max_idle = idle;
						oldest_key = Some(candidate_key);
					}
				}
			}
		} else {
			for _ in 0..self.sample_size {
				let rand_idx = fastrand::usize(..num_keys);
				if let Some(candidate_key) = shard.key_at(rand_idx)
					&& let Some(entry) = shard.entry(&candidate_key)
				{
					let idle = entry.idle_duration(current_clock);
					if oldest_key.is_none() || idle >= max_idle {
						max_idle = idle;
						oldest_key = Some(candidate_key);
					}
				}
			}
		}

		oldest_key
	}

	/// Evicts entries until the shard's memory usage drops below `target_bytes`.
	pub fn evict_to_target(
		&self,
		shard: &mut Shard,
		target_bytes: usize,
		current_clock: u32,
	) -> usize {
		let mut evicted = 0;

		while shard.used_memory_bytes() > target_bytes && evicted < self.eviction_burst_limit {
			if let Some(candidate_key) = self.find_candidate(shard, current_clock) {
				shard.remove(&candidate_key);
				evicted += 1;
			} else {
				break;
			}
		}

		evicted
	}

	/// Emergency inline eviction: reclaims memory to fit `required_bytes` plus headroom.
	///
	/// # Errors
	///
	/// Returns [`CapacityExceeded`] if the requested bytes cannot be reclaimed.
	pub fn evict_headroom(
		&self,
		shard: &mut Shard,
		required_bytes: usize,
		current_clock: u32,
	) -> Result<usize, CapacityExceeded> {
		let max_payload = shard.max_memory_bytes();
		if required_bytes > max_payload {
			return Err(CapacityExceeded);
		}

		let effective_headroom = INLINE_HEADROOM_BYTES.min(max_payload / 8);
		let target_bytes = max_payload.saturating_sub(required_bytes + effective_headroom);

		let mut candidates: SmallVec<Box<str>, 16> = SmallVec::new();
		let mut projected_used_bytes = shard.used_memory_bytes();

		for _ in 0..MAX_EVICTION_ATTEMPTS {
			if projected_used_bytes <= target_bytes {
				break;
			}

			if let Some(candidate) = self.find_candidate(shard, current_clock) {
				if candidates.contains(&candidate) {
					break;
				}

				if let Some(entry) = shard.entry(&candidate) {
					let entry_bytes = candidate.len() + entry.value.len();
					projected_used_bytes = projected_used_bytes.saturating_sub(entry_bytes);
					candidates.push(candidate);
				} else {
					break;
				}
			} else {
				break;
			}
		}

		if projected_used_bytes + required_bytes > max_payload {
			// Write cannot succeed. Abort immediately WITHOUT deleting anything!
			return Err(CapacityExceeded);
		}

		let mut evicted = 0;
		for key in &candidates {
			if shard.remove(key).is_some() {
				evicted += 1;
			}
		}

		Ok(evicted)
	}

	/// Active expiration sweep: samples keys and removes expired items.
	///
	/// If `sample_size >= num_keys`, sweeps every key in the shard directly.
	#[allow(clippy::unused_self)]
	pub fn sweep_expired(&self, shard: &mut Shard, sample_size: usize, now: Instant) -> usize {
		let num_keys = shard.key_count();
		if num_keys == 0 {
			return 0;
		}

		let mut expired_keys: SmallVec<Box<str>, 16> = SmallVec::new();

		if num_keys <= sample_size {
			for idx in 0..num_keys {
				if let Some(key) = shard.key_at(idx)
					&& let Some(entry) = shard.entry(&key)
					&& entry.is_expired(now)
				{
					expired_keys.push(key);
				}
			}
		} else {
			for _ in 0..sample_size {
				let rand_idx = fastrand::usize(..num_keys);
				if let Some(key) = shard.key_at(rand_idx)
					&& let Some(entry) = shard.entry(&key)
					&& entry.is_expired(now)
					&& !expired_keys.contains(&key)
				{
					expired_keys.push(key);
				}
			}
		}

		let mut purged = 0;
		for key in &expired_keys {
			if shard.remove(key).is_some() {
				purged += 1;
			}
		}
		purged
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::Bytes;
	use std::time::Duration;

	fn make_test_shard(capacity_bytes: usize) -> Shard {
		Shard::new(capacity_bytes, 1024)
	}

	#[test]
	fn test_find_candidate_on_empty_shard() {
		let shard = make_test_shard(1024);
		let lru = LruPolicy::new(128);

		assert!(lru.find_candidate(&shard, 100).is_none());
	}

	#[test]
	fn test_find_candidate_picks_oldest_idle_entry() {
		let mut shard = make_test_shard(1024);
		let lru = LruPolicy::new(128);

		shard
			.insert("k:fresh", Bytes::from_static(b"1"), None, 95)
			.unwrap();
		shard
			.insert("k:old", Bytes::from_static(b"2"), None, 10)
			.unwrap();

		// Querying with current_clock = 100: "k:old" idle is 90, "k:fresh" idle is 5
		let candidate = lru.find_candidate(&shard, 100);
		assert_eq!(candidate.as_deref(), Some("k:old"));
	}

	#[test]
	fn test_evict_to_target_reduces_memory() {
		let mut shard = make_test_shard(1000);
		let lru = LruPolicy::new(128);

		// Iterate directly with u32 to avoid i as u32 cast
		for i in 0..10u32 {
			let key = format!("k:{i:02}");
			shard
				.insert(&key, Bytes::from(vec![0xAA; 16]), None, i)
				.unwrap();
		}
		assert_eq!(shard.used_memory_bytes(), 200);

		let evicted = lru.evict_to_target(&mut shard, 100, 100);
		assert!(evicted >= 5);
		assert!(shard.used_memory_bytes() <= 100);
	}

	#[test]
	fn test_evict_headroom_capacity_wall() {
		let mut shard = make_test_shard(100);
		let lru = LruPolicy::new(128);

		// Fill shard with 80 bytes
		shard
			.insert("k:1", Bytes::from(vec![0xAA; 77]), None, 10)
			.unwrap();
		assert_eq!(shard.used_memory_bytes(), 80);

		// Demanding 90 bytes when capacity is 100 requires evicting "k:1"
		let res = lru.evict_headroom(&mut shard, 90, 50);
		assert!(res.is_ok());
		assert_eq!(shard.len(), 0);

		// Demanding 120 bytes exceeds shard max_payload_bytes (100) -> returns CapacityExceeded
		let err = lru.evict_headroom(&mut shard, 120, 50);
		assert_eq!(err, Err(CapacityExceeded));
	}

	#[test]
	fn test_sweep_expired_purges_without_overcounting() {
		let mut shard = make_test_shard(1024);
		let lru = LruPolicy::new(128);
		let now = Instant::now();

		let past = now
			.checked_sub(Duration::from_millis(50))
			.expect("valid time");
		let future = now + Duration::from_secs(60);

		shard
			.insert("k:exp1", Bytes::from_static(b"v"), Some(past), 0)
			.unwrap();
		shard
			.insert("k:exp2", Bytes::from_static(b"v"), Some(past), 0)
			.unwrap();
		shard
			.insert("k:act", Bytes::from_static(b"v"), Some(future), 0)
			.unwrap();

		assert_eq!(shard.len(), 3);

		// Sweep sample size of 10 over 3 keys
		let purged = lru.sweep_expired(&mut shard, 10, now);
		assert_eq!(purged, 2);
		assert_eq!(shard.len(), 1);
		assert!(shard.entry("k:act").is_some());
		assert!(shard.entry("k:exp1").is_none());
		assert!(shard.entry("k:exp2").is_none());
	}

	#[test]
	fn test_failed_headroom_eviction_does_not_delete_keys() {
		let mut shard = Shard::new(100, 64);
		let lru = LruPolicy::new(128);

		for i in 0..5 {
			let k = format!("k:{i}");
			shard.insert(&k, Bytes::from_static(b"1234567"), None, 0).unwrap(); // 3 + 7 = 10B
		}
		assert_eq!(shard.len(), 5);
		assert_eq!(shard.used_memory_bytes(), 50);

		let res = lru.evict_headroom(&mut shard, 110, 0);
		assert_eq!(res, Err(CapacityExceeded));
		assert_eq!(shard.len(), 5, "No keys should be evicted on impossible write");
	}
}
