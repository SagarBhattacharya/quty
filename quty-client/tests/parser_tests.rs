use bytes::Bytes;
use quty_client::format::{format_duration, format_sub_result};
use quty_client::repl::parse_repl_input;
use quty_proto::constants::{MAX_KEY_SIZE, MAX_VALUE_SIZE};
use quty_proto::{BatchErrorPayload, Command, Operation, ResponsePayload, StatusCode, SubResult, MAX_BATCH_OPERATIONS};
use std::time::Duration;
use quty_client::validate::{validate_batch, validate_key};

#[test]
fn test_parse_single_operations() {
	let cases = vec![
		(
			"GET user:100",
			Command::Single(Operation::Get {
				key: "user:100".into(),
			}),
		),
		(
			"SET user:100 \"hello world\"",
			Command::Single(Operation::Set {
				key: "user:100".into(),
				value: Bytes::from_static(b"hello world"),
			}),
		),
		(
			"ADD user:100 data",
			Command::Single(Operation::Add {
				key: "user:100".into(),
				value: Bytes::from_static(b"data"),
			}),
		),
		(
			"REPLACE user:100 data2",
			Command::Single(Operation::Replace {
				key: "user:100".into(),
				value: Bytes::from_static(b"data2"),
			}),
		),
		(
			"DEL user:100",
			Command::Single(Operation::Delete {
				key: "user:100".into(),
			}),
		),
		(
			"DELETE user:100",
			Command::Single(Operation::Delete {
				key: "user:100".into(),
			}),
		),
		(
			"EXISTS user:100",
			Command::Single(Operation::Exists {
				key: "user:100".into(),
			}),
		),
		(
			"TTL user:100",
			Command::Single(Operation::Ttl {
				key: "user:100".into(),
			}),
		),
		(
			"EXPIRE user:100 5000",
			Command::Single(Operation::Expire {
				key: "user:100".into(),
				ttl_ms: 5000,
			}),
		),
		(
			"PERSIST user:100",
			Command::Single(Operation::Persist {
				key: "user:100".into(),
			}),
		),
		("PING", Command::Ping),
		("STATS", Command::Stats),
		("RESET", Command::Reset),
	];

	for (input, expected) in cases {
		let parsed = parse_repl_input(input).unwrap_or_else(|e| panic!("Failed on '{input}': {e}"));
		assert_eq!(parsed, expected);
	}
}

#[test]
fn test_parse_batch_syntax_with_commas_and_quotes() {
	let input = "BATCH (SET user:1 \"val, with, comma\", GET user:1, DEL user:1)";
	let parsed = parse_repl_input(input).expect("Valid batch must parse");

	let expected = Command::Batch(vec![
		Operation::Set {
			key: "user:1".into(),
			value: Bytes::from_static(b"val, with, comma"),
		},
		Operation::Get {
			key: "user:1".into(),
		},
		Operation::Delete {
			key: "user:1".into(),
		},
	]);

	assert_eq!(parsed, expected);
}

#[test]
fn test_parse_batch_cross_shard_rejected_client_side() {
	let input = "BATCH (SET user:1 val1, SET order:1 val2)";
	let err = parse_repl_input(input).expect_err("Cross shard batch must be rejected");
	assert!(err.contains("Cross-shard violation"));
}

#[test]
fn test_parse_unclosed_quotes_rejected() {
	let input = "SET key:1 \"unclosed value";
	let err = parse_repl_input(input).expect_err("Unclosed quote must fail");
	assert!(err.contains("unclosed quote"));
}

#[test]
fn test_parse_key_and_value_size_limits() {
	let oversized_key = "k".repeat(MAX_KEY_SIZE + 1);
	let err = parse_repl_input(&format!("GET {oversized_key}")).expect_err("Oversized key must fail");
	assert!(err.contains("exceeds maximum protocol limit"));

	let oversized_val = "v".repeat(MAX_VALUE_SIZE + 1);
	let err =
		parse_repl_input(&format!("SET k:1 {oversized_val}")).expect_err("Oversized value must fail");
	assert!(err.contains("exceeds maximum protocol limit"));
}

#[test]
fn test_format_duration_resolution() {
	let micro = format_duration(Duration::from_micros(500));
	assert!(micro.contains("500 µs"));

	let milli = format_duration(Duration::from_millis(2));
	assert!(milli.contains("2.00 ms"));
}

#[test]
fn test_format_sub_result_rendering() {
	let ok_sub = SubResult {
		status: StatusCode::Ok,
		payload: ResponsePayload::String("Alice".into()),
	};
	assert!(format_sub_result(&ok_sub).contains("Alice"));

	let miss_sub = SubResult {
		status: StatusCode::KeyNotFound,
		payload: ResponsePayload::Null,
	};
	assert!(format_sub_result(&miss_sub).contains("(nil)"));

	let abort_sub = SubResult {
		status: StatusCode::BatchShortCircuited,
		payload: ResponsePayload::BatchError(BatchErrorPayload {
			failed_index: 2,
			failure_code: StatusCode::StorageFull,
		}),
	};
	assert!(format_sub_result(&abort_sub).contains("TRANSACTION ABORTED"));
}

#[test]
fn test_validate_empty_key_fails() {
	let err = validate_key("").expect_err("Empty key must be rejected");
	assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn test_validate_key_boundary() {
	let max_key = "k".repeat(MAX_KEY_SIZE);
	assert!(validate_key(&max_key).is_ok());

	let over_key = "k".repeat(MAX_KEY_SIZE + 1);
	assert!(validate_key(&over_key).is_err());
}

#[test]
fn test_validate_batch_limits() {
	// Empty batch fails
	assert!(validate_batch(&[]).is_err());

	// Max batch size passes
	let valid_ops = vec![
		Operation::Get { key: "k:1".into() };
		MAX_BATCH_OPERATIONS
	];
	assert!(validate_batch(&valid_ops).is_ok());

	// Over limit fails
	let oversized_ops = vec![
		Operation::Get { key: "k:1".into() };
		MAX_BATCH_OPERATIONS + 1
	];
	assert!(validate_batch(&oversized_ops).is_err());
}