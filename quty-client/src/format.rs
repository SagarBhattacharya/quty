use colored::*;
use quty_proto::{Response, ResponsePayload, StatsPayload, StatusCode, SubResult};
use std::time::Duration;

pub fn format_duration(d: Duration) -> String {
	if d.as_micros() < 1000 {
		format!("({} µs)", d.as_micros()).dimmed().to_string()
	} else {
		format!("({:.2} ms)", d.as_secs_f64() * 1000.0)
			.dimmed()
			.to_string()
	}
}

pub fn render_response(res: &Response, elapsed: Option<Duration>) {
	let timing = elapsed.map(format_duration).unwrap_or_default();

	match res.status {
		StatusCode::Ok => render_success_payload(&res.payload, timing),
		StatusCode::Created => println!("{} {}", "CREATED".bold().green(), timing),
		StatusCode::NoContent => println!("{} {}", "OK".bold().green(), timing),
		StatusCode::KeyNotFound => println!("{} {}", "(nil)".dimmed(), timing),
		StatusCode::KeyExpired => println!("{} {}", "(nil) [expired]".dimmed(), timing),
		StatusCode::KeyExists => {
			eprintln!("{} {}", "(error) ERR Key already exists".bold().red(), timing)
		}
		StatusCode::PreconditionFailed => eprintln!(
			"{} {}",
			"(error) ERR Precondition failed on active key".bold().red(),
			timing
		),
		StatusCode::StorageFull => eprintln!(
			"{} {}",
			"(error) OOM Shard storage capacity exceeded".bold().red(),
			timing
		),
		StatusCode::KeyTooLarge => eprintln!(
			"{} {}",
			"(error) ERR Key exceeds protocol limit (64 KB)".bold().red(),
			timing
		),
		StatusCode::ValueTooLarge => eprintln!(
			"{} {}",
			"(error) ERR Value exceeds protocol limit (32 MB)".bold().red(),
			timing
		),
		StatusCode::CrossShardViolation => eprintln!(
			"{} {}",
			"(error) CROSS_SLOT Batch keys must map to the same shard".bold().red(),
			timing
		),
		StatusCode::EmptyBatch => {
			eprintln!("{} {}", "(error) ERR Empty batch".bold().red(), timing)
		}
		StatusCode::BatchShortCircuited => {
			if let ResponsePayload::BatchError(err) = &res.payload {
				eprintln!(
					"{} Batch short-circuited at index {}: {:?} {}",
					"(error) TRANSACTION ABORTED".bold().red(),
					err.failed_index,
					err.failure_code,
					timing
				);
			} else {
				eprintln!("{} {}", "(error) TRANSACTION ABORTED".bold().red(), timing);
			}
		}
		StatusCode::BadRequest => {
			eprintln!("{} {}", "(error) ERR Malformed command syntax".bold().red(), timing)
		}
		StatusCode::InternalError => {
			eprintln!("{} {}", "(error) ERR Internal server error".bold().red(), timing)
		}
		StatusCode::UnknownOpcode => {
			eprintln!("{} {}", "(error) ERR Unknown or unassigned opcode".bold().red(), timing)
		}
		StatusCode::NestedBatchNotAllowed => {
			eprintln!("{} {}", "(error) ERR Nested batches are prohibited".bold().red(), timing)
		}
		StatusCode::ServerBusy => {
			eprintln!("{} {}", "(error) BUSY Server is overloaded; retry later".bold().red(), timing)
		}
	}
}

fn render_success_payload(payload: &ResponsePayload, timing: String) {
	match payload {
		ResponsePayload::Null => println!("{} {}", "OK".bold().green(), timing),
		ResponsePayload::Bool(b) => println!("{} {}", format!("(boolean) {}", b).yellow(), timing),
		ResponsePayload::Int64(i) => println!("{} {}", format!("(integer) {}", i).cyan(), timing),
		ResponsePayload::String(s) => println!("\"{}\" {}", s.green(), timing),
		ResponsePayload::Raw(b) => match std::str::from_utf8(b) {
			Ok(s) => println!("\"{}\" {}", s.green(), timing),
			Err(_) => println!("{:?} {}", b.as_ref(), timing),
		},
		ResponsePayload::Stats(stats) => render_telemetry_table(stats, timing),
		ResponsePayload::Array(items) => {
			if items.is_empty() {
				println!("{} {}", "(empty list)".dimmed(), timing);
				return;
			}
			println!("{} items {}", items.len(), timing);
			for (idx, item) in items.iter().enumerate() {
				println!("  {}) {}", idx + 1, format_sub_result(item));
			}
		}
		ResponsePayload::BatchError(err) => {
			eprintln!(
				"{} at sub-command #{}: {:?} {}",
				"(error) Batch error".bold().red(),
				err.failed_index,
				err.failure_code,
				timing
			);
		}
	}
}

pub fn format_sub_result(item: &SubResult) -> String {
	match item.status {
		StatusCode::Ok => match &item.payload {
			ResponsePayload::Null => "OK".bold().green().to_string(),
			ResponsePayload::Bool(b) => format!("(boolean) {}", b).yellow().to_string(),
			ResponsePayload::Int64(i) => format!("(integer) {}", i).cyan().to_string(),
			ResponsePayload::String(s) => format!("\"{}\"", s.green()),
			ResponsePayload::Raw(b) => match std::str::from_utf8(b) {
				Ok(s) => format!("\"{}\"", s.green()),
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
		StatusCode::PreconditionFailed => {
			"(error) ERR Precondition failed".bold().red().to_string()
		}
		StatusCode::StorageFull => {
			"(error) OOM Shard storage capacity exceeded".bold().red().to_string()
		}
		StatusCode::KeyTooLarge => {
			"(error) ERR Key exceeds protocol limit (64 KB)".bold().red().to_string()
		}
		StatusCode::ValueTooLarge => {
			"(error) ERR Value exceeds protocol limit (32 MB)".bold().red().to_string()
		}
		StatusCode::CrossShardViolation => {
			"(error) CROSS_SLOT Batch keys must map to the same shard"
				.bold()
				.red()
				.to_string()
		}
		StatusCode::EmptyBatch => "(error) ERR Empty batch".bold().red().to_string(),
		StatusCode::BatchShortCircuited => "(error) TRANSACTION ABORTED".bold().red().to_string(),
		StatusCode::BadRequest => "(error) ERR Malformed command syntax".bold().red().to_string(),
		StatusCode::InternalError => {
			"(error) ERR Internal server error".bold().red().to_string()
		}
		StatusCode::UnknownOpcode => "(error) ERR Unknown opcode".bold().red().to_string(),
		StatusCode::NestedBatchNotAllowed => "(error) ERR Nested batch not allowed".bold().red().to_string(),
		StatusCode::ServerBusy => "(error) BUSY Server overloaded".bold().red().to_string(),
	}
}

fn render_telemetry_table(stats: &StatsPayload, timing: String) {
	println!("{}", "── Server Telemetry ───────────────────────".dimmed());
	println!("{:<22} {}", "uptime_seconds:".blue(), stats.uptime_secs);
	println!("{:<22} {}", "total_connections:".blue(), stats.total_connections);
	println!("{:<22} {}", "active_connections:".blue(), stats.curr_connections);
	println!("{:<22} {}", "total_keys:".blue(), stats.total_keys);
	println!(
		"{:<22} {} ({:.2} MB)",
		"used_memory_bytes:".blue(),
		stats.used_memory_bytes,
		stats.used_memory_bytes as f64 / (1024.0 * 1024.0)
	);
	println!("{:<22} {}", "get_hits:".green(), stats.get_hits);
	println!("{:<22} {}", "get_misses:".yellow(), stats.get_misses);
	println!("{:<22} {}", "evictions:".red(), stats.evictions);
	println!("{}", "────────────────────────────────────────────".dimmed());
	if !timing.is_empty() {
		println!("{}", timing);
	}
}