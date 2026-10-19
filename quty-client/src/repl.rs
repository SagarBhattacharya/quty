use crate::format::render_response;
use crate::Client;
use bytes::Bytes;
use colored::*;
use quty_proto::{Command, Operation};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::path::PathBuf;

pub async fn run_repl(mut client: Client) {
	let peer = client
		.peer_addr()
		.map(|a| a.to_string())
		.unwrap_or_else(|_| "quty".to_string());

	let mut rl = match DefaultEditor::new() {
		Ok(editor) => editor,
		Err(e) => {
			eprintln!("Failed to initialize readline interface: {}", e);
			return;
		}
	};

	let history_file = get_history_path();
	if let Some(ref path) = history_file {
		let _ = rl.load_history(path);
	}

	println!("{}", format!("Connected to quty server at {}", peer).green());
	println!("Type {} or {} to disconnect.", "exit".yellow(), "help".yellow());

	loop {
		let prompt = format!("{}> ", peer.bold());
		let readline = rl.readline(&prompt);

		match readline {
			Ok(line) => {
				let trimmed = line.trim();
				if trimmed.is_empty() {
					continue;
				}

				let _ = rl.add_history_entry(trimmed);

				if trimmed.eq_ignore_ascii_case("exit") 
					|| trimmed.eq_ignore_ascii_case("quit") {
					break;
				}

				if trimmed.eq_ignore_ascii_case("help") {
					print_help();
					continue;
				}

				let command = match parse_repl_input(trimmed) {
					Ok(cmd) => cmd,
					Err(err) => {
						eprintln!("{} {}", "(error) ERR".bold().red(), err);
						continue;
					}
				};

				match client.execute(command).await {
					Ok(result) => render_response(&result.response, Some(result.elapsed)),
					Err(err) => {
						eprintln!("{} Connection lost: {}", "(error) FATAL".bold().red(), err);
						break;
					}
				}
			}
			Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
				println!("\nDisconnecting...");
				break;
			}
			Err(err) => {
				eprintln!("REPL error: {}", err);
				break;
			}
		}
	}

	if let Some(ref path) = history_file {
		let _ = rl.save_history(path);
	}
}

pub fn parse_repl_input(input: &str) -> Result<Command, String> {
	let trimmed = input.trim();
	if trimmed.is_empty() {
		return Err("Empty command".into());
	}

	// Check for BATCH (...) syntax
	if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("BATCH") {
		let rest = trimmed[5..].trim();
		if !rest.starts_with('(') || !rest.ends_with(')') {
			return Err("BATCH syntax error: expected BATCH (cmd1, cmd2, ...)".into());
		}

		let inner = &rest[1..rest.len() - 1].trim();
		if inner.is_empty() {
			return Err("BATCH cannot be empty".into());
		}

		let sub_command_strings = split_batch_subcommands(inner);
		let mut ops = Vec::with_capacity(sub_command_strings.len());

		for sub_str in sub_command_strings {
			let tokens = parse_line_into_tokens(&sub_str);
			let op = parse_operation(&tokens)?;
			ops.push(op);
		}

		return Ok(Command::Batch(ops));
	}

	// Single command execution
	let tokens = parse_line_into_tokens(trimmed);
	parse_command(&tokens)
}

/// Splits comma-separated commands inside parentheses while ignoring commas inside quotes.
fn split_batch_subcommands(input: &str) -> Vec<String> {
	let mut parts = Vec::new();
	let mut current = String::new();
	let mut in_quotes = false;

	for ch in input.chars() {
		match ch {
			'"' => {
				in_quotes = !in_quotes;
				current.push(ch);
			}
			',' if !in_quotes => {
				let trimmed = current.trim();
				if !trimmed.is_empty() {
					parts.push(trimmed.to_string());
				}
				current.clear();
			}
			_ => current.push(ch),
		}
	}

	let trimmed = current.trim();
	if !trimmed.is_empty() {
		parts.push(trimmed.to_string());
	}

	parts
}

fn parse_line_into_tokens(input: &str) -> Vec<String> {
	let mut tokens = Vec::new();
	let mut current = String::new();
	let mut in_quotes = false;

	for ch in input.chars() {
		match ch {
			'"' => in_quotes = !in_quotes,
			' ' | '\t' if !in_quotes => {
				if !current.is_empty() {
					tokens.push(current.clone());
					current.clear();
				}
			}
			_ => current.push(ch),
		}
	}

	if !current.is_empty() {
		tokens.push(current);
	}
	tokens
}

pub fn parse_command(tokens: &[String]) -> Result<Command, String> {
	if tokens.is_empty() {
		return Err("Empty command".into());
	}

	let verb = tokens[0].to_ascii_uppercase();

	match verb.as_str() {
		"PING" => Ok(Command::Ping),
		"STATS" => Ok(Command::Stats),
		"RESET" => Ok(Command::Reset),
		_ => parse_operation(tokens).map(Command::Single),
	}
}

fn parse_operation(tokens: &[String]) -> Result<Operation, String> {
	if tokens.is_empty() {
		return Err("Empty operation in batch".into());
	}

	let verb = tokens[0].to_ascii_uppercase();

	match verb.as_str() {
		"GET" => {
			if tokens.len() != 2 {
				return Err("syntax: GET <key>".into());
			}
			Ok(Operation::Get { key: tokens[1].clone() })
		}
		"SET" => {
			if tokens.len() != 3 {
				return Err("syntax: SET <key> <value>".into());
			}
			Ok(Operation::Set {
				key: tokens[1].clone(),
				value: Bytes::copy_from_slice(tokens[2].as_bytes()),
			})
		}
		"ADD" => {
			if tokens.len() != 3 {
				return Err("syntax: ADD <key> <value>".into());
			}
			Ok(Operation::Add {
				key: tokens[1].clone(),
				value: Bytes::copy_from_slice(tokens[2].as_bytes()),
			})
		}
		"REPLACE" => {
			if tokens.len() != 3 {
				return Err("syntax: REPLACE <key> <value>".into());
			}
			Ok(Operation::Replace {
				key: tokens[1].clone(),
				value: Bytes::copy_from_slice(tokens[2].as_bytes()),
			})
		}
		"DEL" | "DELETE" => {
			if tokens.len() != 2 {
				return Err("syntax: DEL <key>".into());
			}
			Ok(Operation::Delete { key: tokens[1].clone() })
		}
		"EXISTS" => {
			if tokens.len() != 2 {
				return Err("syntax: EXISTS <key>".into());
			}
			Ok(Operation::Exists { key: tokens[1].clone() })
		}
		"TTL" => {
			if tokens.len() != 2 {
				return Err("syntax: TTL <key>".into());
			}
			Ok(Operation::Ttl { key: tokens[1].clone() })
		}
		"EXPIRE" => {
			if tokens.len() != 3 {
				return Err("syntax: EXPIRE <key> <ttl_ms>".into());
			}
			let ttl_ms = tokens[2]
				.parse::<u64>()
				.map_err(|_| "ttl_ms must be an unsigned integer".to_string())?;
			Ok(Operation::Expire {
				key: tokens[1].clone(),
				ttl_ms,
			})
		}
		"PERSIST" => {
			if tokens.len() != 2 {
				return Err("syntax: PERSIST <key>".into());
			}
			Ok(Operation::Persist { key: tokens[1].clone() })
		}
		other => Err(format!(
			"invalid operation '{}'. Batches only accept storage operations \
			(SET, GET, ADD, REPLACE, DEL, EXISTS, TTL, EXPIRE, PERSIST)",
			other
		)),
	}
}

fn get_history_path() -> Option<PathBuf> {
	dirs::home_dir().map(|mut p| {
		p.push(".quty_history");
		p
	})
}

fn print_help() {
	println!("{}", "Available commands:".bold());
	println!("  SET <key> <val>                Unconditional upsert");
	println!("  GET <key>                      Retrieve value (updates LRU access clock)");
	println!("  ADD <key> <val>                Insert only if key absent");
	println!("  REPLACE <key> <val>            Update only if key present");
	println!("  DEL <key>                      Remove key and reclaim memory");
	println!("  EXISTS <key>                   Check key presence without touching LRU");
	println!("  TTL <key>                      Query remaining TTL in ms (-1 if persistent)");
	println!("  EXPIRE <key> <ms>              Attach millisecond TTL deadline");
	println!("  PERSIST <key>                  Strip TTL deadline");
	println!("  BATCH (cmd1, cmd2, ...)        Atomic multi-key batch execution");
	println!("  STATS                          View engine telemetry counters");
	println!("  RESET                          Wipe all shards and zero memory");
	println!("  EXIT | QUIT                    Disconnect");
}