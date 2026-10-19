use crate::db::shard::{CapacityExceeded, Shard};
use bytes::Bytes;
use std::time::{Duration, Instant};

#[test]
fn test_exact_payload_accounting_lifecycle() {
	let mut shard = Shard::new(1024, 128, 8192);
	let key: Box<str> = "user:1".into(); // 6 bytes
	let val = Bytes::from_static(b"payload_val"); // 11 bytes

	// 1. Initial insert -> 6 + 11 = 17 bytes
	let res = shard.insert(key.clone(), val.clone(), None, 0);
	assert!(matches!(res, Ok((None, 0))));
	assert_eq!(shard.used_memory_bytes(), 17);

	// 2. In-place overwrite with smaller payload
	let smaller_val = Bytes::from_static(b"tiny"); // 4 bytes -> 6 + 4 = 10 bytes
	let res = shard.insert(key.clone(), smaller_val, None, 0);
	assert!(matches!(res, Ok((Some(_), 0))));
	assert_eq!(shard.used_memory_bytes(), 10);

	// 3. Removal zeroes memory accounting
	let removed = shard.remove(&key);
	assert!(removed.is_some());
	assert_eq!(shard.used_memory_bytes(), 0);
}

#[test]
fn test_lazy_ghosting_ttl_under_reads() {
	let mut shard = Shard::new(1024, 128, 8192);
	let now = Instant::now();
	let key: Box<str> = "session:temp".into();
	let val = Bytes::from_static(b"secret");

	let past_deadline = now - Duration::from_millis(50);
	shard
		.insert(key.clone(), val, Some(past_deadline), 0)
		.unwrap();

	// Expired entry returns None without updating last_accessed
	assert!(shard.get_value(&key, now, 10).is_none());

	// Entry remains in memory until a mutating write or active sweep cleans it
	assert!(shard.entry(&key).is_some());
	assert_eq!(shard.len(), 1);
	assert_eq!(shard.used_memory_bytes(), 12 + 6);
}

#[test]
fn test_uniform_sampling_stride_wraparound() {
	let mut shard = Shard::new(10_000, 128, 8192);
	for i in 0..100 {
		let key = format!("k:{:03}", i).into_boxed_str();
		shard.insert(key, Bytes::from_static(b"v"), None, 0).unwrap();
	}

	let mut observed_candidates = std::collections::HashSet::new();

	// Successive candidate searches should advance across the table
	for _ in 0..10 {
		if let Some(candidate) = shard.find_lru_candidate(5, 10) {
			observed_candidates.insert(candidate);
		}
	}

	// Rotating cursor must not fixate on a single index
	assert!(
		observed_candidates.len() > 1,
		"Sampling must rotate across table buckets"
	);
}

#[test]
fn test_insert_no_evict_hard_capacity_wall() {
	let max_cap = 64;
	let mut shard = Shard::new(max_cap, 128, 8192);

	let k1: Box<str> = "k:1".into(); // 3 bytes
	let v1 = Bytes::from(vec![0u8; 40]); // 40 bytes -> total 43 bytes
	assert!(shard.insert_no_evict(k1, v1, None, 0).is_ok());

	// Second item requires 3 + 30 = 33 bytes (43 + 33 = 76 > 64)
	let k2: Box<str> = "k:2".into();
	let v2 = Bytes::from(vec![0u8; 30]);
	let err = shard.insert_no_evict(k2, v2, None, 0);
	
	assert_eq!(err.unwrap_err(), CapacityExceeded);
	assert_eq!(shard.len(), 1, "Failed insert must leave existing entries intact");
}