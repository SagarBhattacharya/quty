//! Interactive Read-Eval-Print-Loop (REPL) and line parser for Quty.
//!
//! Provides lexical tokenization with quoted-string support, command routing,
//! interactive history persistence via `rustyline`, and validation of protocol bounds.

use crate::Client;
use crate::format::render_response;
use crate::validate::{validate_key, validate_value};
use bytes::Bytes;
use colored::Colorize;
use quty_proto::{Command, Operation};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use std::path::PathBuf;

/// Starts the interactive terminal Read-Eval-Print-Loop (REPL).
///
/// Reads user commands line-by-line via `rustyline`, persists command history
/// to `~/.quty_history`, and prints colorized responses until `EXIT`, `QUIT`,
/// or EOF (`Ctrl+D`) is encountered.
pub async fn run_repl(mut client: Client) {
	let peer = client
		.peer_addr()
		.map_or_else(|_| "quty".to_string(), |a| a.to_string());

	let mut rl = match DefaultEditor::new() {
		Ok(editor) => editor,
		Err(e) => {
			eprintln!("Failed to initialize readline interface: {e}");
			return;
		}
	};

	let history_file = get_history_path();
	if let Some(ref path) = history_file {
		let _ = rl.load_history(path);
	}

	println!("{}", format!("Connected to quty server at {peer}").green());
	println!(
		"Type {} or {} to disconnect.",
		"exit".yellow(),
		"help".yellow()
	);

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

				if trimmed.eq_ignore_ascii_case("exit") || trimmed.eq_ignore_ascii_case("quit") {
					break;
				}

				if trimmed.eq_ignore_ascii_case("help") {
					print_help();
					continue;
				}

				let command = match parse_repl_input(trimmed) {
					Ok(cmd) => cmd,
					Err(err) => {
						eprintln!("{} {err}", "(error) ERR".bold().red());
						continue;
					}
				};

				match client.execute(command).await {
					Ok(result) => render_response(&result.response, Some(result.elapsed)),
					Err(err) => {
						eprintln!("{} Connection lost: {err}", "(error) FATAL".bold().red());
						break;
					}
				}
			}
			Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
				println!("\nDisconnecting...");
				break;
			}
			Err(err) => {
				eprintln!("REPL error: {err}");
				break;
			}
		}
	}

	if let Some(ref path) = history_file {
		let _ = rl.save_history(path);
	}
}

/// Parses raw input lines into an executable [`Command`].
///
/// Supports single key-value operations, control directives (`PING`, `STATS`, `RESET`),
/// and atomic batch syntax (`BATCH (cmd1, cmd2, ...)`).
///
/// # Examples
///
/// ```
/// use quty_client::repl::parse_repl_input;
/// use quty_proto::{Command, Operation};
///
/// let cmd = parse_repl_input("GET session:100").unwrap();
/// assert_eq!(cmd, Command::Single(Operation::Get { key: "session:100".into() }));
/// ```
///
/// # Errors
///
/// Returns an error string if tokenization fails, unknown verbs are supplied,
/// quotes are unclosed, or arguments violate protocol bounds.
pub fn parse_repl_input(input: &str) -> Result<Command, String> {
	let trimmed = input.trim();
	if trimmed.is_empty() {
		return Err("Empty command".into());
	}

	if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("BATCH") {
		let rest = trimmed[5..].trim();
		if !rest.starts_with('(') || !rest.ends_with(')') {
			return Err("BATCH syntax error: expected BATCH (cmd1, cmd2, ...)".into());
		}

		let inner = rest[1..rest.len() - 1].trim();
		if inner.is_empty() {
			return Err("BATCH cannot be empty".into());
		}

		let sub_command_strings = split_batch_subcommands(inner);
		let mut ops = Vec::with_capacity(sub_command_strings.len());

		for sub_str in sub_command_strings {
			let tokens = parse_line_into_tokens(&sub_str)?;
			let op = parse_operation(&tokens)?;
			ops.push(op);
		}

		let batch_cmd = Command::Batch(ops);
		if !batch_cmd.is_single_shard() {
			return Err("Cross-shard violation: all keys in a batch must share the same prefix".into());
		}

		return Ok(batch_cmd);
	}

	let tokens = parse_line_into_tokens(trimmed)?;
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

/// Lexically splits a line into whitespace-delimited tokens respecting quoted strings.
fn parse_line_into_tokens(input: &str) -> Result<Vec<String>, String> {
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

	if in_quotes {
		return Err("Syntax error: unclosed quote in input".into());
	}

	if !current.is_empty() {
		tokens.push(current);
	}
	Ok(tokens)
}

/// Parses an array of tokens into an executable [`Command`].
///
/// # Examples
///
/// ```
/// use quty_client::repl::parse_command;
/// use quty_proto::Command;
///
/// let tokens = vec!["PING".to_string()];
/// assert_eq!(parse_command(&tokens).unwrap(), Command::Ping);
/// ```
///
/// # Errors
///
/// Returns an error string if tokens are empty or represent an invalid command.
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

/// Routes operation tokens by verb to the appropriate arity parser.
fn parse_operation(tokens: &[String]) -> Result<Operation, String> {
	if tokens.is_empty() {
		return Err("Empty operation".into());
	}

	let verb = tokens[0].to_ascii_uppercase();

	match verb.as_str() {
		"GET" | "DEL" | "DELETE" | "EXISTS" | "TTL" | "PERSIST" => parse_unary_key_op(&verb, tokens),
		"SET" | "ADD" | "REPLACE" => parse_kv_mutation_op(&verb, tokens),
		"EXPIRE" => parse_expire_op(tokens),
		other => Err(format!(
			"invalid operation '{other}'. Operations supported: \
      SET, GET, ADD, REPLACE, DEL, EXISTS, TTL, EXPIRE, PERSIST"
		)),
	}
}

/// Parses operations that take only a single key parameter.
fn parse_unary_key_op(verb: &str, tokens: &[String]) -> Result<Operation, String> {
	if tokens.len() != 2 {
		return Err(format!("syntax: {verb} <key>"));
	}
	validate_key(&tokens[1]).map_err(|e| e.to_string())?;
	let key = tokens[1].clone();

	match verb {
		"GET" => Ok(Operation::Get { key }),
		"DEL" | "DELETE" => Ok(Operation::Delete { key }),
		"EXISTS" => Ok(Operation::Exists { key }),
		"TTL" => Ok(Operation::Ttl { key }),
		"PERSIST" => Ok(Operation::Persist { key }),
		_ => unreachable!(),
	}
}

/// Parses operations that require both a key and a value parameter.
fn parse_kv_mutation_op(verb: &str, tokens: &[String]) -> Result<Operation, String> {
	if tokens.len() != 3 {
		return Err(format!("syntax: {verb} <key> <value>"));
	}
	validate_key(&tokens[1]).map_err(|e| e.to_string())?;
	validate_value(tokens[2].as_bytes()).map_err(|e| e.to_string())?;
	let key = tokens[1].clone();
	let value = Bytes::copy_from_slice(tokens[2].as_bytes());

	match verb {
		"SET" => Ok(Operation::Set { key, value }),
		"ADD" => Ok(Operation::Add { key, value }),
		"REPLACE" => Ok(Operation::Replace { key, value }),
		_ => unreachable!(),
	}
}

/// Parses the `EXPIRE <key> <ttl_ms>` operation.
fn parse_expire_op(tokens: &[String]) -> Result<Operation, String> {
	if tokens.len() != 3 {
		return Err("syntax: EXPIRE <key> <ttl_ms>".into());
	}
	validate_key(&tokens[1]).map_err(|e| e.to_string())?;
	let ttl_ms = tokens[2]
		.parse::<u64>()
		.map_err(|_| "ttl_ms must be an unsigned integer".to_string())?;

	Ok(Operation::Expire {
		key: tokens[1].clone(),
		ttl_ms,
	})
}
/// Returns the default path for persistent CLI history (`~/.quty_history`).
fn get_history_path() -> Option<PathBuf> {
	dirs::home_dir().map(|mut p| {
		p.push(".quty_history");
		p
	})
}

/// Prints available commands and usage hints.
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
