use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, Color, Row, Table};
use hdrhistogram::Histogram;
use std::time::Duration;

pub struct BenchmarkStats {
	pub total_ops: u64,
	pub duration: Duration,
	pub ops_per_sec: f64,
	pub histogram: Histogram<u64>,
}

impl BenchmarkStats {
	pub fn new(histogram: Histogram<u64>, duration: Duration) -> Self {
		let total_ops = histogram.len();
		let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
		Self {
			total_ops,
			duration,
			ops_per_sec,
			histogram,
		}
	}

	pub fn print_summary(&self, title: &str) {
		let mut table = Table::new();
		table.load_style(UTF8_FULL.with_rounded_corners());

		table.set_header(vec![
			Cell::new("Metric").fg(Color::Cyan),
			Cell::new("Value").fg(Color::Yellow),
		]);

		table.add_row(Row::from(vec![
			Cell::new("Benchmark Target"),
			Cell::new(title),
		]));
		table.add_row(Row::from(vec![
			Cell::new("Total Operations"),
			Cell::new(format!("{}", self.total_ops)),
		]));
		table.add_row(Row::from(vec![
			Cell::new("Duration"),
			Cell::new(format!("{:.2} s", self.duration.as_secs_f64())),
		]));
		table.add_row(Row::from(vec![
			Cell::new("Throughput"),
			Cell::new(format!("{:.2} ops/sec", self.ops_per_sec)).fg(Color::Green),
		]));
		table.add_row(Row::from(vec![
			Cell::new("p50 (Median) Latency"),
			Cell::new(format!("{} µs", self.histogram.value_at_quantile(0.50))),
		]));
		table.add_row(Row::from(vec![
			Cell::new("p90 Latency"),
			Cell::new(format!("{} µs", self.histogram.value_at_quantile(0.90))),
		]));
		table.add_row(Row::from(vec![
			Cell::new("p99 Latency"),
			Cell::new(format!("{} µs", self.histogram.value_at_quantile(0.99)))
				.fg(Color::Magenta),
		]));
		table.add_row(Row::from(vec![
			Cell::new("p99.9 Latency"),
			Cell::new(format!("{} µs", self.histogram.value_at_quantile(0.999))),
		]));
		table.add_row(Row::from(vec![
			Cell::new("Max Latency"),
			Cell::new(format!("{} µs", self.histogram.max())).fg(Color::Red),
		]));

		println!("\n{}", table);
	}
}