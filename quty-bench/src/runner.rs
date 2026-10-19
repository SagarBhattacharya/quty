use crate::workload::{KeyGenerator, WorkloadType};
use bytes::BytesMut;
use hdrhistogram::Histogram;
use quty_proto::{Request, Response};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub async fn run_worker(
	target_addr: String,
	pipeline_depth: usize,
	workload: WorkloadType,
	key_count: usize,
	use_zipf: bool,
	val_size: usize,
	worker_id: u64,
	start_flag: Arc<AtomicBool>,
	stop_flag: Arc<AtomicBool>,
) -> Result<Histogram<u64>, Box<dyn std::error::Error + Send + Sync>> {
	let mut stream = TcpStream::connect(&target_addr).await?;
	stream.set_nodelay(true)?;

	let mut key_gen = KeyGenerator::new(key_count, use_zipf, val_size, 42 + worker_id);
	let mut read_buf = BytesMut::with_capacity(64 * 1024);
	let mut write_buf = BytesMut::with_capacity(64 * 1024);

	// Latency histogram: 1 microsecond to 60 seconds range, 3 significant digits
	let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
	let mut req_counter = 1u16;

	// Spin/await until all workers are connected and ready to fire simultaneously
	while !start_flag.load(Ordering::Relaxed) {
		tokio::task::yield_now().await;
	}

	// Benchmark loop
	while !stop_flag.load(Ordering::Relaxed) {
		// 1. Encode a pipeline batch of commands into write_buf
		write_buf.clear();
		for _ in 0..pipeline_depth {
			let cmd = key_gen.next_command(workload);
			let req = Request::new(req_counter, 0, cmd);
			req_counter = req_counter.wrapping_add(1);
			req.encode(&mut write_buf);
		}

		let start_time = Instant::now();

		// 2. Dispatch entire batch in a single TCP payload
		stream.write_all(&write_buf).await?;

		// 3. Drain all corresponding pipeline responses from the stream
		let mut responses_received = 0;
		while responses_received < pipeline_depth {
			while let Some(_res) = Response::decode(&mut read_buf)? {
				responses_received += 1;
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

		let elapsed_us = start_time.elapsed().as_micros() as u64;

		// Record per-operation latency (approximated by dividing batch elapsed time by pipeline depth)
		let per_op_us = (elapsed_us / pipeline_depth as u64).max(1);
		for _ in 0..pipeline_depth {
			let _ = histogram.record(per_op_us);
		}
	}

	Ok(histogram)
}