//! Cache-line padded engine telemetry counters with 100ms resolution LRU clock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// 64-byte cache-line aligned wrapper preventing false sharing across worker cores.
#[repr(align(64))]
pub struct CachePadded<T> {
	pub value: T,
}

impl<T> CachePadded<T> {
	pub const fn new(value: T) -> Self {
		Self { value }
	}
}

pub struct EngineStats {
	created_at: Instant,
	total_connections: CachePadded<AtomicU64>,
	curr_connections: CachePadded<AtomicU64>,
	get_hits: CachePadded<AtomicU64>,
	get_misses: CachePadded<AtomicU64>,
	evictions: CachePadded<AtomicU64>,
}

impl EngineStats {
	#[must_use]
	pub fn new() -> Self {
		Self {
			created_at: Instant::now(),
			total_connections: CachePadded::new(AtomicU64::new(0)),
			curr_connections: CachePadded::new(AtomicU64::new(0)),
			get_hits: CachePadded::new(AtomicU64::new(0)),
			get_misses: CachePadded::new(AtomicU64::new(0)),
			evictions: CachePadded::new(AtomicU64::new(0)),
		}
	}

	/// Resets all operational telemetry counters back to zero.
	///
	/// Preserves persistent server metrics such as server start time and active connections.
	pub fn reset(&self) {
		self.get_hits.value.store(0, Ordering::Relaxed);
		self.get_misses.value.store(0, Ordering::Relaxed);
		self.evictions.value.store(0, Ordering::Relaxed);
	}

	#[inline]
	pub fn on_client_connected(&self) {
		self.total_connections.value.fetch_add(1, Ordering::Relaxed);
		self.curr_connections.value.fetch_add(1, Ordering::Relaxed);
	}

	#[inline]
	pub fn on_client_disconnected(&self) {
		self.curr_connections.value.fetch_sub(1, Ordering::Relaxed);
	}

	#[inline]
	pub fn inc_hits(&self) {
		self.get_hits.value.fetch_add(1, Ordering::Relaxed);
	}

	#[inline]
	pub fn inc_misses(&self) {
		self.get_misses.value.fetch_add(1, Ordering::Relaxed);
	}

	#[inline]
	pub fn inc_evictions(&self, count: usize) {
		self
			.evictions
			.value
			.fetch_add(count as u64, Ordering::Relaxed);
	}

	#[inline]
	pub fn get_hits(&self) -> u64 {
		self.get_hits.value.load(Ordering::Relaxed)
	}

	#[inline]
	pub fn get_misses(&self) -> u64 {
		self.get_misses.value.load(Ordering::Relaxed)
	}

	#[inline]
	pub fn get_evictions(&self) -> u64 {
		self.evictions.value.load(Ordering::Relaxed)
	}

	#[inline]
	pub fn total_connections(&self) -> u64 {
		self.total_connections.value.load(Ordering::Relaxed)
	}

	#[inline]
	pub fn curr_connections(&self) -> u64 {
		self.curr_connections.value.load(Ordering::Relaxed)
	}

	#[inline]
	pub fn uptime_secs(&self) -> u64 {
		self.created_at.elapsed().as_secs()
	}

	/// Monotonic LRU clock ticked at 100ms intervals.
	#[inline]
	pub fn lru_clock_100ms(&self) -> u32 {
		#[allow(clippy::cast_possible_truncation)]
		((self.created_at.elapsed().as_millis() / 100) as u32)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_connection_and_operational_telemetry() {
		let stats = EngineStats::new();

		// 1. Connection tracking
		stats.on_client_connected();
		stats.on_client_connected();
		assert_eq!(stats.total_connections(), 2);
		assert_eq!(stats.curr_connections(), 2);

		stats.on_client_disconnected();
		assert_eq!(stats.total_connections(), 2);
		assert_eq!(stats.curr_connections(), 1);

		// 2. Cache hits, misses, and evictions
		stats.inc_hits();
		stats.inc_hits();
		stats.inc_misses();
		stats.inc_evictions(5);

		assert_eq!(stats.get_hits(), 2);
		assert_eq!(stats.get_misses(), 1);
		assert_eq!(stats.get_evictions(), 5);
	}

	#[test]
	fn test_lru_clock_monotonicity() {
		let stats = EngineStats::new();
		let c1 = stats.lru_clock_100ms();
		std::thread::sleep(std::time::Duration::from_millis(110));
		let c2 = stats.lru_clock_100ms();

		assert!(c2 > c1);
	}
}
