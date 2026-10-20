//! Command-line runner for the Quty load testing and latency harness.

mod metrics;
mod runner;
mod workload;

use clap::Parser;
use colored::Colorize;
use hdrhistogram::Histogram;
use metrics::BenchmarkStats;
use runner::run_worker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use workload::WorkloadType;

#[derive(Parser, Debug)]
#[command(
	name = "quty-bench",
	author = "Sagar Bhattacharya",
	version = "0.1.0",
	about = "High-concurrency load testing & latency profiling harness for Quty cache server"
)]
struct Args {
	/// Server target address
	#[arg(short = 'a', long = "addr", default_value = "127.0.0.1:6379")]
	addr: String,

	/// Number of concurrent client TCP connections
	#[arg(short = 'c', long = "concurrency", default_value_t = 50)]
	concurrency: usize,

	/// Pipelining depth (commands dispatched per batch frame)
	#[arg(short = 'p', long = "pipeline", default_value_t = 16)]
	pipeline: usize,

	/// Test duration in seconds (post-warmup)
	#[arg(short = 'd', long = "duration", default_value_t = 10)]
	duration_secs: u64,

	/// Warmup duration in seconds before recording telemetry
	#[arg(long = "warmup-secs", default_value_t = 2)]
	warmup_secs: u64,

	/// Workload command profile
	#[arg(short = 'w', long = "workload", value_enum, default_value_t = WorkloadType::Mixed)]
	workload: WorkloadType,

	/// Key-space cardinality (total unique keys)
	#[arg(short = 'k', long = "keys", default_value_t = 100_000)]
	key_count: usize,

	/// Payload value size in bytes
	#[arg(long = "val-size", default_value_t = 64)]
	val_size: usize,

	/// Enable 80/20 Zipfian key access skew instead of uniform access
	#[arg(long = "zipf")]
	zipf: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let args = Args::parse();

	println!(
		"{}",
		"═════════════════════════════════════════════════════════".cyan()
	);
	println!(
		"{}",
		"              QUTY LOAD BENCHMARK HARNESS               "
			.bold()
			.cyan()
	);
	println!(
		"{}",
		"═════════════════════════════════════════════════════════".cyan()
	);
	println!("{:<20} {}", "Target Server:".bold(), args.addr);
	println!("{:<20} {}", "Concurrency:".bold(), args.concurrency);
	println!("{:<20} {}", "Pipeline Depth:".bold(), args.pipeline);
	println!("{:<20} {:?}", "Workload:".bold(), args.workload);
	println!("{:<20} {}", "Key-space Size:".bold(), args.key_count);
	println!("{:<20} {}", "Zipfian Skew:".bold(), args.zipf);
	println!(
		"{:<20} {}s (+ {}s warmup)",
		"Duration:".bold(),
		args.duration_secs,
		args.warmup_secs
	);

	let warmup_flag = Arc::new(AtomicBool::new(false));
	let record_flag = Arc::new(AtomicBool::new(false));
	let stop_flag = Arc::new(AtomicBool::new(false));
	let mut handles = Vec::with_capacity(args.concurrency);

	// 1. Establish connections
	println!("\nEstablishing {} worker connections...", args.concurrency);
	for id in 0..args.concurrency {
		#[allow(clippy::cast_possible_truncation)]
		let handle = tokio::spawn(run_worker(
			args.addr.clone(),
			args.pipeline,
			args.workload,
			args.key_count,
			args.zipf,
			args.val_size,
			id as u64,
			Arc::clone(&warmup_flag),
			Arc::clone(&record_flag),
			Arc::clone(&stop_flag),
		));
		handles.push(handle);
	}

	// 2. Pre-warm phase (Populates initial data and warms memory caches)
	println!(
		"Warming up server and caches ({} seconds)...",
		args.warmup_secs
	);
	warmup_flag.store(true, Ordering::Relaxed);
	tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;

	// 3. Measured Load Phase
	println!(
		"Starting measured load phase ({} seconds)...",
		args.duration_secs
	);
	record_flag.store(true, Ordering::Relaxed);
	let run_start = std::time::Instant::now();
	tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;

	// 4. Terminate workers
	stop_flag.store(true, Ordering::Relaxed);
	let actual_duration = run_start.elapsed();

	// 5. Consolidate HDR histograms
	println!("Aggregating latency histograms across workers...");
	let mut merged_histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;

	for handle in handles {
		match handle.await? {
			Ok(worker_hist) => {
				merged_histogram.add(&worker_hist)?;
			}
			Err(e) => {
				eprintln!("Worker error: {e}");
			}
		}
	}

	// 6. Output formatted summary table
	let stats = BenchmarkStats::new(merged_histogram, actual_duration);
	let title = format!(
		"{:?} (P={}, C={}, Zipf={})",
		args.workload, args.pipeline, args.concurrency, args.zipf
	);
	stats.print_summary(&title);

	Ok(())
}
