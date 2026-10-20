//! High-throughput asynchronous TCP connection worker with true per-request latency tracking.

use crate::workload::{KeyGenerator, WorkloadType};
use bytes::BytesMut;
use hdrhistogram::Histogram;
use quty_proto::{Request, Response};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Tracks in-flight request timestamps for individual latency calculation.
struct InFlight {
	request_id: u64,
	sent_at: Instant,
}

/// Runs an isolated benchmarking connection worker.
#[allow(clippy::too_many_arguments)]
pub async fn run_worker(
	target_addr: String,
	pipeline_depth: usize,
	workload: WorkloadType,
	key_count: usize,
	use_zipf: bool,
	val_size: usize,
	worker_id: u64,
	warmup_flag: Arc<AtomicBool>,
	record_flag: Arc<AtomicBool>,
	stop_flag: Arc<AtomicBool>,
) -> Result<Histogram<u64>, Box<dyn std::error::Error + Send + Sync>> {
	let mut stream = TcpStream::connect(&target_addr).await?;
	stream.set_nodelay(true)?;

	let mut key_gen = KeyGenerator::new(key_count, use_zipf, val_size, 42 + worker_id);
	let mut read_buf = BytesMut::with_capacity(64 * 1024);
	let mut write_buf = BytesMut::with_capacity(64 * 1024);

	let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
	let mut req_counter = 1u64;
	let mut in_flight = VecDeque::with_capacity(pipeline_depth * 2);

	// Await coordinated release of all connections
	while !warmup_flag.load(Ordering::Relaxed) {
		tokio::task::yield_now().await;
	}

	while !stop_flag.load(Ordering::Relaxed) {
		write_buf.clear();
		let should_record = record_flag.load(Ordering::Relaxed);

		// 1. Dispatch pipeline batch with per-request timestamps
		for _ in 0..pipeline_depth {
			let cmd = key_gen.next_command(workload);
			let req_id = req_counter;
			req_counter = req_counter.wrapping_add(1);

			let req = Request::new(req_id, cmd);
			req.encode(&mut write_buf);

			if should_record {
				in_flight.push_back(InFlight {
					request_id: req_id,
					sent_at: Instant::now(),
				});
			}
		}

		stream.write_all(&write_buf).await?;

		// 2. Decode responses individually and record true per-request latency
		let mut responses_received = 0;
		while responses_received < pipeline_depth {
			while let Some(res) = Response::decode(&mut read_buf)? {
				responses_received += 1;

				if should_record {
					let now = Instant::now();
					if let Some(tracked) = in_flight.pop_front() {
						if tracked.request_id != res.header.request_id {
							return Err(format!(
								"Worker {worker_id}: Request ID mismatch! Expected {}, got {}",
								tracked.request_id, res.header.request_id
							)
								.into());
						}

						#[allow(clippy::cast_possible_truncation)]
						let latency_us = now
							.duration_since(tracked.sent_at)
							.as_micros()
							.max(1) as u64;

						let _ = histogram.record(latency_us);
					}
				}

				if responses_received == pipeline_depth {
					break;
				}
			}

			if responses_received < pipeline_depth {
				let n = stream.read_buf(&mut read_buf).await?;
				if n == 0 {
					return Err("Server closed connection during benchmark".into());
				}
			}
		}
	}

	Ok(histogram)
}