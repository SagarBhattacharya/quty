//! Client-side input validation for protocol invariants and payload size bounds.

use quty_proto::Operation;
use quty_proto::constants::{MAX_BATCH_OPERATIONS, MAX_KEY_SIZE, MAX_VALUE_SIZE};
use std::io;

/// Validates that a key meets length and non-empty constraints.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if key is empty
/// or is greater than [`MAX_KEY_SIZE`]
pub fn validate_key(key: &str) -> io::Result<()> {
	if key.is_empty() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"Key cannot be empty",
		));
	}
	if key.len() > MAX_KEY_SIZE {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!(
				"Key length ({} bytes) exceeds maximum protocol limit of {MAX_KEY_SIZE} bytes",
				key.len()
			),
		));
	}
	Ok(())
}

/// Validates that a payload value conforms to maximum size constraints.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if value is greater than [`MAX_VALUE_SIZE`]
pub fn validate_value(value: &[u8]) -> io::Result<()> {
	if value.len() > MAX_VALUE_SIZE {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!(
				"Value length ({} bytes) exceeds maximum protocol limit of {MAX_VALUE_SIZE} bytes",
				value.len()
			),
		));
	}
	Ok(())
}

/// Validates that an operation and its operands conform to protocol bounds.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if either key or value
/// fails to conform to validation rules applied by [`validate_value`]
/// and [`validate_key`]
pub fn validate_operation(op: &Operation) -> io::Result<()> {
	match op {
		Operation::Get { key }
		| Operation::Delete { key }
		| Operation::Exists { key }
		| Operation::Ttl { key }
		| Operation::Persist { key }
		| Operation::Expire { key, .. } => validate_key(key),

		Operation::Set { key, value }
		| Operation::Add { key, value }
		| Operation::Replace { key, value } => {
			validate_key(key)?;
			validate_value(value)
		}
	}
}

/// Validates batch operation boundaries (count, cross-shard prefix, and individual operations).
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if any operation
/// fails to conform to its validation rules set by [`validate_operation`]
pub fn validate_batch(ops: &[Operation]) -> io::Result<()> {
	if ops.is_empty() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			"Batch cannot be empty",
		));
	}
	if ops.len() > MAX_BATCH_OPERATIONS {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!(
				"Batch size ({}) exceeds maximum limit of {MAX_BATCH_OPERATIONS} operations",
				ops.len()
			),
		));
	}

	for op in ops {
		validate_operation(op)?;
	}

	Ok(())
}
