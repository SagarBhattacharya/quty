//! Terminal formatting engine for Quty responses, telemetry, and execution timing.
//!
//! This module formats wire responses returned by the Quty server into human-readable,
//! colorized terminal output adhering to Redis/Memcached CLI conventions.

use colored::Colorize;
use quty_proto::{Response, ResponsePayload, StatsPayload, StatusCode, SubResult};
use std::time::Duration;

/// Formats an elapsed execution duration into a human-readable dimmed string.
///
/// Sub-millisecond intervals are displayed in microseconds (`µs`), while longer
/// intervals are rendered in milliseconds (`ms`) with two decimal places.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use quty_client::format::format_duration;
///
/// let micro = format_duration(Duration::from_micros(450));
/// assert!(micro.contains("450 µs"));
///
/// let milli = format_duration(Duration::from_millis(12));
/// assert!(milli.contains("12.00 ms"));
/// ```
#[must_use]
pub fn format_duration(d: Duration) -> String {
	if d.as_micros() < 1000 {
		format!("({} µs)", d.as_micros()).dimmed().to_string()
	} else {
		#[allow(clippy::cast_precision_loss)]
		format!("({:.2} ms)", d.as_secs_f64() * 1000.0)
			.dimmed()
			.to_string()
	}
}

/// Renders a protocol response with colorized output matching systems CLI conventions.
///
/// Success payloads are printed to standard output (`stdout`), while hard errors,
/// precondition failures, and transaction aborts are printed to standard error (`stderr`).
///
/// # Parameters
///
/// * `res`: The decoded [`Response`] received from the Quty cache server.
/// * `elapsed`: Optional round-trip network execution duration.
pub fn render_response(res: &Response, elapsed: Option<Duration>) {
	let timing = elapsed.map(format_duration).unwrap_or_default();

	match res.header.status {
		StatusCode::Ok => render_success_payload(&res.payload, &timing),
		StatusCode::Created => println!("{} {timing}", "CREATED".bold().green()),
		StatusCode::NoContent => println!("{} {timing}", "OK".bold().green()),
		StatusCode::KeyNotFound => println!("{} {timing}", "(nil)".dimmed()),
		StatusCode::KeyExpired => println!("{} {timing}", "(nil) [expired]".dimmed()),
		StatusCode::KeyExists => {
			eprintln!("{} {timing}", "(error) ERR Key already exists".bold().red());
		}
		StatusCode::PreconditionFailed => eprintln!(
			"{} {timing}",
			"(error) ERR Precondition failed on active key".bold().red()
		),
		StatusCode::StorageFull => eprintln!(
			"{} {timing}",
			"(error) OOM Shard storage capacity exceeded".bold().red()
		),
		StatusCode::KeyTooLarge => eprintln!(
			"{} {timing}",
			"(error) ERR Key exceeds protocol limit (64 KB)"
				.bold()
				.red()
		),
		StatusCode::ValueTooLarge => eprintln!(
			"{} {timing}",
			"(error) ERR Value exceeds protocol limit (32 MB)"
				.bold()
				.red()
		),
		StatusCode::CrossShardViolation => eprintln!(
			"{} {timing}",
			"(error) CROSS_SLOT Batch keys must map to the same shard"
				.bold()
				.red()
		),
		StatusCode::EmptyBatch => {
			eprintln!("{} {timing}", "(error) ERR Empty batch".bold().red());
		}
		StatusCode::BatchShortCircuited => {
			if let ResponsePayload::BatchError(err) = &res.payload {
				eprintln!(
					"{} Batch short-circuited at index {}: {:?} {timing}",
					"(error) TRANSACTION ABORTED".bold().red(),
					err.failed_index,
					err.failure_code
				);
			} else {
				eprintln!("{} {timing}", "(error) TRANSACTION ABORTED".bold().red());
			}
		}
		StatusCode::BadRequest => {
			eprintln!(
				"{} {timing}",
				"(error) ERR Malformed command syntax".bold().red()
			);
		}
		StatusCode::InternalError => {
			eprintln!(
				"{} {timing}",
				"(error) ERR Internal server error".bold().red()
			);
		}
		StatusCode::UnknownOpcode => {
			eprintln!(
				"{} {timing}",
				"(error) ERR Unknown or unassigned opcode".bold().red()
			);
		}
		StatusCode::NestedBatchNotAllowed => {
			eprintln!(
				"{} {timing}",
				"(error) ERR Nested batches are prohibited".bold().red()
			);
		}
		StatusCode::ServerBusy => {
			eprintln!(
				"{} {timing}",
				"(error) BUSY Server is overloaded; retry later"
					.bold()
					.red()
			);
		}
	}
}

/// Renders successful payload variants to standard output.
fn render_success_payload(payload: &ResponsePayload, timing: &str) {
	match payload {
		ResponsePayload::Null => println!("{} {timing}", "OK".bold().green()),
		ResponsePayload::Bool(b) => println!("{} {timing}", format!("(boolean) {b}").yellow()),
		ResponsePayload::Int64(i) => println!("{} {timing}", format!("(integer) {i}").cyan()),
		ResponsePayload::String(s) => println!("\"{s}\" {timing}"),
		ResponsePayload::Raw(b) => match std::str::from_utf8(b) {
			Ok(s) => println!("\"{s}\" {timing}"),
			Err(_) => println!("{:?} {timing}", b.as_ref()),
		},
		ResponsePayload::Stats(stats) => render_telemetry_table(stats, timing),
		ResponsePayload::Array(items) => {
			if items.is_empty() {
				println!("{} {timing}", "(empty list)".dimmed());
				return;
			}
			println!("{} items {timing}", items.len());
			for (idx, item) in items.iter().enumerate() {
				println!("  {}) {}", idx + 1, format_sub_result(item));
			}
		}
		ResponsePayload::BatchError(err) => {
			eprintln!(
				"{} at sub-command #{}: {:?} {timing}",
				"(error) Batch error".bold().red(),
				err.failed_index,
				err.failure_code
			);
		}
	}
}

/// Formats a single item from an atomic batch result into an inline text representation.
///
/// # Examples
///
/// ```
/// use quty_proto::{StatusCode, SubResult, ResponsePayload};
/// use quty_client::format::format_sub_result;
///
/// let item = SubResult {
///     status: StatusCode::Ok,
///     payload: ResponsePayload::String("session_token".into()),
/// };
/// assert!(format_sub_result(&item).contains("session_token"));
/// ```
#[must_use]
pub fn format_sub_result(item: &SubResult) -> String {
	match item.status {
		StatusCode::Ok => match &item.payload {
			ResponsePayload::Null => "OK".bold().green().to_string(),
			ResponsePayload::Bool(b) => format!("(boolean) {b}").yellow().to_string(),
			ResponsePayload::Int64(i) => format!("(integer) {i}").cyan().to_string(),
			ResponsePayload::String(s) => format!("\"{s}\""),
			ResponsePayload::Raw(b) => match std::str::from_utf8(b) {
				Ok(s) => format!("\"{s}\""),
				Err(_) => format!("{:?}", b.as_ref()),
			},
			ResponsePayload::Stats(stats) => format!("<stats: {} keys>", stats.total_keys),
			ResponsePayload::Array(arr) => format!("<array: {} items>", arr.len()),
			ResponsePayload::BatchError(err) => format!(
				"{} at index {}: {:?}",
				"(error) Batch error".bold().red(),
				err.failed_index,
				err.failure_code
			),
		},
		StatusCode::Created => "CREATED".bold().green().to_string(),
		StatusCode::NoContent => "OK".bold().green().to_string(),
		StatusCode::KeyNotFound => "(nil)".dimmed().to_string(),
		StatusCode::KeyExpired => "(nil) [expired]".dimmed().to_string(),
		StatusCode::KeyExists => "(error) ERR Key already exists".bold().red().to_string(),
		StatusCode::PreconditionFailed => "(error) ERR Precondition failed".bold().red().to_string(),
		StatusCode::StorageFull => "(error) OOM Shard storage capacity exceeded"
			.bold()
			.red()
			.to_string(),
		StatusCode::KeyTooLarge => "(error) ERR Key exceeds protocol limit (64 KB)"
			.bold()
			.red()
			.to_string(),
		StatusCode::ValueTooLarge => "(error) ERR Value exceeds protocol limit (32 MB)"
			.bold()
			.red()
			.to_string(),
		StatusCode::CrossShardViolation => "(error) CROSS_SLOT Batch keys must map to the same shard"
			.bold()
			.red()
			.to_string(),
		StatusCode::EmptyBatch => "(error) ERR Empty batch".bold().red().to_string(),
		StatusCode::BatchShortCircuited => "(error) TRANSACTION ABORTED".bold().red().to_string(),
		StatusCode::BadRequest => "(error) ERR Malformed command syntax"
			.bold()
			.red()
			.to_string(),
		StatusCode::InternalError => "(error) ERR Internal server error".bold().red().to_string(),
		StatusCode::UnknownOpcode => "(error) ERR Unknown opcode".bold().red().to_string(),
		StatusCode::NestedBatchNotAllowed => "(error) ERR Nested batch not allowed"
			.bold()
			.red()
			.to_string(),
		StatusCode::ServerBusy => "(error) BUSY Server overloaded".bold().red().to_string(),
	}
}

/// Renders the 8-metric telemetry table returned by the `STATS` command.
fn render_telemetry_table(stats: &StatsPayload, timing: &str) {
	#[allow(clippy::cast_precision_loss)]
	let memory_mb = stats.used_memory_bytes as f64 / (1024.0 * 1024.0);

	println!("{}", "── Server Telemetry ───────────────────────".dimmed());
	println!("{:<22} {}", "uptime_seconds:".blue(), stats.uptime_seconds);
	println!(
		"{:<22} {}",
		"total_connections:".blue(),
		stats.total_connections
	);
	println!(
		"{:<22} {}",
		"active_connections:".blue(),
		stats.active_connections
	);
	println!("{:<22} {}", "total_keys:".blue(), stats.total_keys);
	println!(
		"{:<22} {} ({:.2} MB)",
		"used_memory_bytes:".blue(),
		stats.used_memory_bytes,
		memory_mb
	);
	println!("{:<22} {}", "get_hits:".green(), stats.get_hits);
	println!("{:<22} {}", "get_misses:".yellow(), stats.get_misses);
	println!("{:<22} {}", "evictions:".red(), stats.evictions);
	println!(
		"{}",
		"────────────────────────────────────────────".dimmed()
	);
	if !timing.is_empty() {
		println!("{timing}");
	}
}
