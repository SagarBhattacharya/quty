mod metrics;
mod runner;
mod workload;

use clap::Parser;
use colored::*;
use hdrhistogram::Histogram;
use metrics::BenchmarkStats;
use runner::run_worker;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use workload::WorkloadType;

#[derive(Parser, Debug)]
#[command(
	name = "quty-bench",
	about = "High-concurrency load testing & latency profiling harness for Quty cache server"
)]
struct Args {
	/// Server address
	#[arg(short = 'a', long = "addr", default_value = "127.0.0.1:6379")]
	addr: String,

	/// Number of concurrent client connections
	#[arg(short = 'c', long = "concurrency", default_value_t = 50)]
	concurrency: usize,

	/// Pipelining depth (commands dispatched per TCP batch)
	#[arg(short = 'p', long = "pipeline", default_value_t = 16)]
	pipeline: usize,

	/// Test duration in seconds
	#[arg(short = 'd', long = "duration", default_value_t = 10)]
	duration_secs: u64,

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

	println!("{}", "═════════════════════════════════════════════════════════".cyan());
	println!("{}", "              QUTY LOAD BENCHMARK HARNESS               ".bold().cyan());
	println!("{}", "═════════════════════════════════════════════════════════".cyan());
	println!("{:<20} {}", "Target Server:".bold(), args.addr);
	println!("{:<20} {}", "Concurrency:".bold(), args.concurrency);
	println!("{:<20} {}", "Pipeline Depth:".bold(), args.pipeline);
	println!("{:<20} {:?}", "Workload:".bold(), args.workload);
	println!("{:<20} {}", "Key-space Size:".bold(), args.key_count);
	println!("{:<20} {}", "Zipfian Skew:".bold(), args.zipf);
	println!("{:<20} {}s", "Duration:".bold(), args.duration_secs);

	let start_flag = Arc::new(AtomicBool::new(false));
	let stop_flag = Arc::new(AtomicBool::new(false));
	let mut handles = Vec::with_capacity(args.concurrency);

	// 1. Pre-warm connections: Connect all workers
	println!("\nEstablishing {} connections...", args.concurrency);
	for id in 0..args.concurrency {
		let handle = tokio::spawn(run_worker(
			args.addr.clone(),
			args.pipeline,
			args.workload,
			args.key_count,
			args.zipf,
			args.val_size,
			id as u64,
			Arc::clone(&start_flag),
			Arc::clone(&stop_flag),
		));
		handles.push(handle);
	}

	// 2. Warm-up Phase (Populate keys and warm memory caches for 2 seconds)
	println!("Warming up server and caches (2 seconds)...");
	start_flag.store(true, Ordering::Relaxed);
	tokio::time::sleep(Duration::from_secs(2)).await;

	// Reset stop_flag for the actual measured phase
	println!("Starting measured load phase ({} seconds)...", args.duration_secs);
	let run_start = std::time::Instant::now();
	tokio::time::sleep(Duration::from_secs(args.duration_secs)).await;

	// Signal all workers to stop
	stop_flag.store(true, Ordering::Relaxed);
	let actual_duration = run_start.elapsed();

	// 3. Collect and merge worker histograms
	println!("Collecting metrics across workers...");
	let mut merged_histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;

	for handle in handles {
		match handle.await? {
			Ok(worker_hist) => {
				merged_histogram.add(&worker_hist)?;
			}
			Err(e) => {
				eprintln!("Worker failed: {}", e);
			}
		}
	}

	// 4. Output results
	let stats = BenchmarkStats::new(merged_histogram, actual_duration);
	let title = format!(
		"{:?} (P={}, C={}, Zipf={})",
		args.workload, args.pipeline, args.concurrency, args.zipf
	);
	stats.print_summary(&title);

	Ok(())
}