use clap::Parser;
use colored::*;
use quty_client::Client;
use quty_client::format::render_response;
use quty_client::repl::{parse_repl_input, run_repl};
use std::process::exit;

#[derive(Parser, Debug)]
#[command(
	name = "quty-cli",
	author = "Quty Contributors",
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
				"{} Could not connect to Quty server at {}: {}",
				"(error) CONNECTION REFUSED".bold().red(),
				addr.bold(),
				e
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
				eprintln!("{} {}", "(error) SYNTAX".bold().red(), err);
				exit(1);
			}
		};

		match client.execute(command).await {
			Ok(result) => {
				render_response(&result.response, Some(result.elapsed));
				if !matches!(
					result.response.status,
					quty_proto::StatusCode::Ok
						| quty_proto::StatusCode::Created
						| quty_proto::StatusCode::NoContent
				) {
					exit(1);
				}
			}
			Err(err) => {
				eprintln!("{} Network I/O failure: {}", "(error)".bold().red(), err);
				exit(1);
			}
		}
		return;
	}

	// 2. Interactive Readline REPL Mode
	run_repl(client).await;
}