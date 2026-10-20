//! Command-line entry point and REPL launcher for `quty-client`.

use clap::Parser;
use colored::Colorize;
use quty_client::Client;
use quty_client::format::render_response;
use quty_client::repl::{parse_repl_input, run_repl};
use std::process::exit;

#[derive(Parser, Debug)]
#[command(
	name = "quty-cli",
	author = "Sagar Bhattacharya",
	version = "0.1.0",
	about = "High-performance interactive terminal client for Quty cache server"
)]
struct Args {
	/// Server hostname or IP address
	#[arg(short = 'H', long = "host", default_value = "127.0.0.1")]
	host: String,

	/// Server port
	#[arg(short = 'p', long = "port", default_value_t = 6379)]
	port: u16,

	/// Raw command arguments. If omitted, launches interactive REPL.
	#[arg(trailing_var_arg = true, allow_hyphen_values = true)]
	raw_command: Vec<String>,
}

#[tokio::main]
async fn main() {
	let args = Args::parse();
	let addr = format!("{}:{}", args.host, args.port);

	let client = match Client::connect(&addr).await {
		Ok(c) => c,
		Err(e) => {
			eprintln!(
				"{} Could not connect to Quty server at {}: {e}",
				"(error) CONNECTION REFUSED".bold().red(),
				addr.bold()
			);
			exit(1);
		}
	};

	// 1. One-off Command Mode (e.g., `quty-cli -p 6379 SET user:1 value`)
	if !args.raw_command.is_empty() {
		let mut client = client;
		let raw_str = args.raw_command.join(" ");
		let command = match parse_repl_input(&raw_str) {
			Ok(cmd) => cmd,
			Err(err) => {
				eprintln!("{} {err}", "(error) SYNTAX".bold().red());
				exit(1);
			}
		};

		match client.execute(command).await {
			Ok(result) => {
				render_response(&result.response, Some(result.elapsed));
				// Only fail process if response is a hard error (cache misses exit 0)
				if result.response.header.status.is_hard_error() {
					exit(1);
				}
			}
			Err(err) => {
				eprintln!("{} Network I/O failure: {err}", "(error)".bold().red());
				exit(1);
			}
		}
		return;
	}

	// 2. Interactive Readline REPL Mode
	run_repl(client).await;
}
