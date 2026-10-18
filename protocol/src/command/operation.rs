use bytes::Bytes;
use super::opcode::Opcode;

/// Core key-value operations executed against shard storage engines.
///
/// Every operation targets a key whose prefix (up to the first `:`) determines
/// shard placement. Operations are categorized into read-only accessors and
/// mutating writers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
	// -------------------------------------------------------------------------
	// Read Operations (Shared Shard Read Lock)
	// -------------------------------------------------------------------------

	/// Retrieve the binary payload associated with a key.
	///
	/// * **Wire Opcode:** [`Opcode::Get`] (`0x10`)
	/// * **Expected Status:** [`StatusCode::Ok`] with payload, or [`StatusCode::KeyNotFound`].
	Get {
		/// Unique target key (UTF-8, max 64 KB).
		key: String,
	},

	/// Check existence of a key without transferring its value payload.
	///
	/// * **Wire Opcode:** [`Opcode::Exists`] (`0x11`)
	/// * **Expected Status:** [`StatusCode::Ok`] with `PayloadType::Bool`.
	Exists {
		/// Unique target key (UTF-8, max 64 KB).
		key: String,
	},

	/// Query the remaining time-to-live for a key in milliseconds.
	///
	/// * **Wire Opcode:** [`Opcode::Ttl`] (`0x12`)
	/// * **Expected Status:** [`StatusCode::Ok`] with `PayloadType::Int64` (`-1` = persistent, `>= 0` = remaining ms).
	Ttl {
		/// Unique target key (UTF-8, max 64 KB).
		key: String,
	},

	// -------------------------------------------------------------------------
	// Mutating Operations (Exclusive Shard Write Lock)
	// -------------------------------------------------------------------------

	/// Unconditional upsert of a key-value entry.
	///
	/// Resets any existing expiration on the key, making it persistent.
	///
	/// * **Wire Opcode:** [`Opcode::Set`] (`0x20`)
	/// * **Expected Status:** [`StatusCode::Created`] (new key) or [`StatusCode::Ok`] (overwrite).
	Set {
		/// Target key (UTF-8, max 64 KB).
		key: String,
		/// Arbitrary binary value (max 32 MB).
		value: Bytes,
	},

	/// Conditional insert (insert-if-absent).
	///
	/// Fails with a conflict error if the key already exists and has not expired.
	///
	/// * **Wire Opcode:** [`Opcode::Add`] (`0x21`)
	/// * **Expected Status:** [`StatusCode::Created`] on success, or [`StatusCode::KeyExists`].
	Add {
		/// Target key (UTF-8, max 64 KB).
		key: String,
		/// Arbitrary binary value (max 32 MB).
		value: Bytes,
	},

	/// Conditional update (update-if-present).
	///
	/// Fails if the key does not exist or has expired.
	///
	/// * **Wire Opcode:** [`Opcode::Replace`] (`0x22`)
	/// * **Expected Status:** [`StatusCode::Ok`] on success, or [`StatusCode::KeyNotFound`].
	Replace {
		/// Target key (UTF-8, max 64 KB).
		key: String,
		/// Arbitrary binary value (max 32 MB).
		value: Bytes,
	},

	/// Remove a key and its value, immediately reclaiming accounted memory.
	///
	/// * **Wire Opcode:** [`Opcode::Delete`] (`0x23`)
	/// * **Expected Status:** [`StatusCode::NoContent`] on success, or [`StatusCode::KeyNotFound`].
	Delete {
		/// Target key to expunge (UTF-8, max 64 KB).
		key: String,
	},

	// -------------------------------------------------------------------------
	// Expiration Management (Exclusive Shard Write Lock)
	// -------------------------------------------------------------------------

	/// Set or update a relative time-to-live on an existing key.
	///
	/// * **Wire Opcode:** [`Opcode::Expire`] (`0x30`)
	/// * **Expected Status:** [`StatusCode::NoContent`] on success, or [`StatusCode::KeyNotFound`].
	Expire {
		/// Target key (UTF-8, max 64 KB).
		key: String,
		/// Relative duration in milliseconds before key becomes inaccessible.
		ttl_ms: u64,
	},

	/// Strip expiration metadata from an entry, converting it into a persistent record.
	///
	/// * **Wire Opcode:** [`Opcode::Persist`] (`0x31`)
	/// * **Expected Status:** [`StatusCode::NoContent`] on success, or [`StatusCode::KeyNotFound`] / [`StatusCode::PreconditionFailed`].
	Persist {
		/// Target key to make persistent (UTF-8, max 64 KB).
		key: String,
	},
}

impl Operation {
	/// Returns a borrowed reference to the primary target key.
	#[inline]
	pub fn key(&self) -> &str {
		match self {
			Self::Get { key }
			| Self::Exists { key }
			| Self::Ttl { key }
			| Self::Set { key, .. }
			| Self::Add { key, .. }
			| Self::Replace { key, .. }
			| Self::Delete { key }
			| Self::Expire { key, .. }
			| Self::Persist { key } => key,
		}
	}

	/// Extracts the routing prefix used to map this operation to a shard index.
	///
	/// Returns the slice preceding the first `:` character. If no delimiter
	/// exists, the entire key serves as the routing token.
	///
	/// # Examples
	///
	/// ```
	/// # use bytes::Bytes;
	/// # use crate::protocol::command::Operation;
	/// let op = Operation::Get { key: "account:1024:profile".into() };
	/// assert_eq!(op.routing_key(), "account");
	///
	/// let flat_op = Operation::Delete { key: "session_token_xyz".into() };
	/// assert_eq!(flat_op.routing_key(), "session_token_xyz");
	/// ```
	#[inline]
	pub fn routing_key(&self) -> &str {
		let k = self.key();
		k.split_once(':')
			.map(|(prefix, _)| prefix)
			.unwrap_or(k)
	}

	/// Maps the operation variant to its frozen 1-byte wire opcode.
	#[inline]
	pub fn opcode(&self) -> Opcode {
		match self {
			Self::Get { .. } => Opcode::Get,
			Self::Exists { .. } => Opcode::Exists,
			Self::Ttl { .. } => Opcode::Ttl,
			Self::Set { .. } => Opcode::Set,
			Self::Add { .. } => Opcode::Add,
			Self::Replace { .. } => Opcode::Replace,
			Self::Delete { .. } => Opcode::Delete,
			Self::Expire { .. } => Opcode::Expire,
			Self::Persist { .. } => Opcode::Persist,
		}
	}

	/// Indicates whether this operation is purely read-only.
	///
	/// Read-only operations require only a shared read lock on the shard,
	/// permitting concurrent non-interfering lookups.
	#[inline]
	pub fn is_readonly(&self) -> bool {
		matches!(self, 
			Self::Get { .. } | 
			Self::Exists { .. } | 
			Self::Ttl { .. })
	}
}