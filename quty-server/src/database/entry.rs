//! Cache entry data structure with atomic recency clocks.

use bytes::Bytes;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

/// Cached entry holding the binary payload, optional expiration, and access clock.
#[derive(Debug)]
pub struct Entry {
	/// Cached payload slice. O(1) shallow clones via reference-counted slice.
	pub value: Bytes,
	/// Absolute expiration deadline. `None` represents a persistent key.
	pub expires_at: Option<Instant>,
	/// Monotonic timestamp in 100ms units.
	pub last_accessed: AtomicU32,
	/// Index position inside the shard's key-sampling vector.
	pub key_index: usize,
}

impl Entry {
	/// Constructs a new [`Entry`].
	#[inline]
	#[must_use]
	pub fn new(
		value: Bytes,
		expires_at: Option<Instant>,
		current_clock: u32,
		key_index: usize,
	) -> Self {
		Self {
			value,
			expires_at,
			last_accessed: AtomicU32::new(current_clock),
			key_index,
		}
	}

	/// Returns `true` if the entry has passed its expiration deadline.
	#[inline]
	#[must_use]
	pub fn is_expired(&self, now: Instant) -> bool {
		match self.expires_at {
			Some(deadline) => now >= deadline,
			None => false,
		}
	}

	/// Updates the access time if the clock has progressed forward.
	#[inline]
	pub fn touch(&self, current_clock: u32) {
		let prev = self.last_accessed.load(Ordering::Relaxed);
		if prev != current_clock {
			self.last_accessed.store(current_clock, Ordering::Relaxed);
		}
	}

	/// Computes idle age in 100ms ticks for Sampled LRU candidate selection.
	#[inline]
	#[must_use]
	pub fn idle_duration(&self, current_clock: u32) -> u32 {
		current_clock.saturating_sub(self.last_accessed.load(Ordering::Relaxed))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	#[test]
	fn test_entry_expiration_evaluation() {
		let now = Instant::now();
		let current_clock = 100;

		// 1. Persistent entry (expires_at: None) never expires
		let persistent = Entry::new(Bytes::from_static(b"data"), None, current_clock, 0);
		assert!(!persistent.is_expired(now));
		assert!(!persistent.is_expired(now + Duration::from_secs(3600)));

		// 2. Future deadline does not expire at present
		let future_deadline = now + Duration::from_millis(500);
		let future_entry = Entry::new(
			Bytes::from_static(b"data"),
			Some(future_deadline),
			current_clock,
			1,
		);
		assert!(!future_entry.is_expired(now));

		// 3. Past deadline evaluates as expired
		let past = now
			.checked_sub(Duration::from_millis(10))
			.expect("valid time");
		let expired_entry = Entry::new(Bytes::from_static(b"data"), Some(past), current_clock, 2);
		assert!(expired_entry.is_expired(now));
	}

	#[test]
	fn test_touch_and_idle_duration() {
		let entry = Entry::new(Bytes::from_static(b"data"), None, 100, 0);

		// Idle calculation with monotonic clock advance
		assert_eq!(entry.idle_duration(100), 0);
		assert_eq!(entry.idle_duration(150), 50);

		// Saturated subtraction prevents underflow if clock is stale
		assert_eq!(entry.idle_duration(80), 0);

		// Touch updates clock
		entry.touch(200);
		assert_eq!(entry.idle_duration(200), 0);
		assert_eq!(entry.idle_duration(250), 50);
	}
}
