use bytes::Bytes;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

#[derive(Debug)]
pub struct Entry {
	/// Cached payload. O(1) shallow clones via reference-counted slice.
	pub value: Bytes,

	/// Absolute deadline when this entry becomes invalid.
	/// None represents a persistent key.
	pub expires_at: Option<Instant>,

	/// Monotonic timestamp (seconds or ticks since engine launch).
	/// Atomic allows updates under shared `&Entry` (read locks).
	pub last_accessed: AtomicU32,
}

impl Entry {
	pub fn new(
		value: Bytes, 
		expires_at: Option<Instant>, 
		current_clock: u32
	) -> Self {
		Self {
			value,
			expires_at,
			last_accessed: AtomicU32::new(current_clock),
		}
	}

	#[inline]
	pub fn is_expired(&self, now: Instant) -> bool {
		match self.expires_at {
			Some(deadline) => now >= deadline,
			None => false,
		}
	}

	/// Updates the access time if the clock has ticked forward.
	/// Uses Relaxed ordering for minimal CPU overhead.
	#[inline]
	pub fn touch(&self, current_clock: u32) {
		let prev = self.last_accessed.load(Ordering::Relaxed);
		if prev != current_clock {
			self.last_accessed.store(current_clock, Ordering::Relaxed);
		}
	}

	/// Computes idle age for sampled LRU candidates.
	#[inline]
	pub fn idle_duration(&self, current_clock: u32) -> u32 {
		current_clock.saturating_sub(self.last_accessed.load(Ordering::Relaxed))
	}

	/// Exact heap payload footprint for deterministic memory accounting.
	#[inline]
	pub fn payload_size(&self) -> usize {
		self.value.len()
	}
}