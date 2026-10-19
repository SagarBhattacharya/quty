use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// 64-byte aligned wrapper to ensure
/// atomics sit on independent cache lines.
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
		self.evictions.value.fetch_add(count as u64, Ordering::Relaxed);
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
}