//! Binary protocol opcodes for quty (high performance in-memory cache net) (v1).
//!
//! All opcodes represent fixed 1-byte discriminants (`u8`) in the frame header.
//! Numerical values are strictly frozen to preserve backward and forward wire compatibility.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Opcode {
	// =========================================================================
	// Transport & Diagnostics (0x01 - 0x0F)
	// =========================================================================

	/// Liveness check and connection heartbeat.
	///
	/// - **Payload Input:** None (`payload_len == 0`).
	/// - **Behavior:** Validates connection responsiveness without acquiring shard locks
	///   or mutating LRU/telemetry metadata.
	/// - **Return Status:** `StatusCode::Ok` (0x00) with empty payload (Pong).
	/// - **Complexity:** O(1), non-blocking.
	Ping = 0x01,

	/// Retrieves engine runtime telemetry.
	///
	/// - **Payload Input:** None (`payload_len == 0`).
	/// - **Behavior:** Aggregates atomic counters across all shards (hits, misses,
	///   total allocated bytes, uptime, total commands processed) without acquiring
	///   coarse shard write locks.
	/// - **Return Status:** `StatusCode::Ok` (0x00) with serialized binary telemetry payload.
	/// - **Complexity:** O(S) where S is shard count (typically 32–64 atomic reads).
	Stats = 0x02,

	/// Clears all key-value entries and reclaims allocated memory (`FLUSHDB`).
	///
	/// - **Payload Input:** None (`payload_len == 0`).
	/// - **Behavior:** Iterates through every shard, acquires write locks sequentially,
	///   clears primary hashtables, resets sampled LRU metadata, and zeroes payload accounting.
	/// - **Return Status:** `StatusCode::NoContent` (0x02).
	/// - **Complexity:** O(N) where N is total keys across all shards.
	Reset = 0x03,

	// =========================================================================
	// Read Operations (0x10 - 0x1F)
	// =========================================================================

	/// Retrieves the binary payload associated with a key.
	///
	/// - **Payload Input:** `key: String` (UTF-8, max 64 KB).
	/// - **Behavior:** Computes routing prefix up to first `:`, acquires shard read lock,
	///   checks passive expiration (lazily expires entry if TTL passed), updates
	///   sampled LRU access timestamp, and returns the value slice.
	/// - **Return Status:**
	///   - `StatusCode::Ok` (0x00) with raw value bytes (`Bytes`).
	///   - `StatusCode::NotFound` (0x11) if key is missing or expired.
	/// - **Complexity:** O(1) expected.
	Get = 0x10,

	/// Verifies whether a key exists in storage without transferring its payload.
	///
	/// - **Payload Input:** `key: String` (UTF-8, max 64 KB).
	/// - **Behavior:** Computes routing prefix, acquires shard read lock, performs
	///   passive expiration check, and returns a boolean status flag. Avoids reading
	///   or cloning large value buffers.
	/// - **Return Status:**
	///   - `StatusCode::Ok` (0x00) if key exists and is valid.
	///   - `StatusCode::NotFound` (0x11) if key is absent or expired.
	/// - **Complexity:** O(1) expected.
	Exists = 0x11,

	/// Checks the remaining time-to-live of a key.
	///
	/// - **Payload Input:** `key: String` (UTF-8, max 64 KB).
	/// - **Behavior:** Computes routing prefix, acquires shard read lock, evaluates
	///   expiration metadata against current monotonic clock.
	/// - **Return Status:** `StatusCode::Ok` (0x00) with 8-byte big-endian signed integer (`i64`):
	///   - `>= 0`: Remaining TTL in milliseconds.
	///   - `-1`: Key exists and is persistent (no TTL set).
	///   - `-2` (or `StatusCode::NotFound`): Key does not exist or has expired.
	/// - **Complexity:** O(1) expected.
	Ttl = 0x12,

	// =========================================================================
	// Write Operations (0x20 - 0x2F)
	// =========================================================================

	/// Blind upsert of a key-value entry (inserts or overwrites existing).
	///
	/// - **Payload Input:** `key: String` (max 64 KB), `value: Bytes` (max 32 MB).
	/// - **Behavior:** Computes routing prefix, acquires shard write lock, accounts for
	///   delta memory change, updates/inserts hashtable entry, resets entry TTL to
	///   persistent, and updates sampled LRU metadata.
	/// - **Return Status:**
	///   - `StatusCode::Created` (0x01) on new key insertion.
	///   - `StatusCode::Ok` (0x00) on overwrite.
	/// - **Complexity:** O(1) amortized.
	Set = 0x20,

	/// Conditional insertion (insert-if-absent).
	///
	/// - **Payload Input:** `key: String` (max 64 KB), `value: Bytes` (max 32 MB).
	/// - **Behavior:** Computes routing prefix, acquires shard write lock, verifies key
	///   absence (lazily expunging existing expired entry if present). Inserts only if absent.
	/// - **Return Status:**
	///   - `StatusCode::Created` (0x01) if key was inserted.
	///   - `StatusCode::Conflict` (0x12) if key already exists and has not expired.
	/// - **Complexity:** O(1) amortized.
	Add = 0x21,

	/// Conditional update (update-if-present).
	///
	/// - **Payload Input:** `key: String` (max 64 KB), `value: Bytes` (max 32 MB).
	/// - **Behavior:** Computes routing prefix, acquires shard write lock, verifies key
	///   presence and active validity. Replaces payload and recalculates memory footprint.
	/// - **Return Status:**
	///   - `StatusCode::Ok` (0x00) on successful overwrite.
	///   - `StatusCode::NotFound` (0x11) if key is missing or expired.
	/// - **Complexity:** O(1) expected.
	Replace = 0x22,

	/// Deletes a key-value entry and reclaims memory.
	///
	/// - **Payload Input:** `key: String` (max 64 KB).
	/// - **Behavior:** Computes routing prefix, acquires shard write lock, removes entry
	///   from storage, deallocates payload memory, decrements shard memory accounting,
	///   and purges expiration index entry.
	/// - **Return Status:**
	///   - `StatusCode::NoContent` (0x02) on successful removal.
	///   - `StatusCode::NotFound` (0x11) if key did not exist.
	/// - **Complexity:** O(1) expected.
	Delete = 0x23,

	// =========================================================================
	// Expiration Management (0x30 - 0x3F)
	// =========================================================================

	/// Sets or updates a relative time-to-live on an existing key.
	///
	/// - **Payload Input:** `key: String` (max 64 KB), `ttl_ms: u64` (8 bytes big-endian).
	/// - **Behavior:** Computes routing prefix, acquires shard write lock, verifies key
	///   presence. Computes expiration epoch (`now_ms + ttl_ms`) and updates metadata.
	/// - **Return Status:**
	///   - `StatusCode::NoContent` (0x02) on successful TTL assignment.
	///   - `StatusCode::NotFound` (0x11) if key does not exist or has expired.
	/// - **Complexity:** O(1) expected.
	Expire = 0x30,

	/// Strips expiration metadata from an entry, converting it into a persistent record.
	///
	/// - **Payload Input:** `key: String` (max 64 KB).
	/// - **Behavior:** Computes routing prefix, acquires shard write lock, sets entry
	///   expiration to none/infinity.
	/// - **Return Status:**
	///   - `StatusCode::NoContent` (0x02) if expiry was removed.
	///   - `StatusCode::NotFound` (0x11) if key does not exist.
	/// - **Complexity:** O(1) expected.
	Persist = 0x31,

	// =========================================================================
	// Compound Operations (0x40 - 0x4F)
	// =========================================================================

	/// Atomically executes an ordered sequence of sub-commands sharing the same routing prefix.
	///
	/// - **Payload Input:** Series of serialized `SubCommand` records.
	/// - **Behavior:**
	///   1. Extracts the routing prefix (bytes before first `:`) from each sub-request_old's key.
	///   2. Verifies that all sub-commands hash to the exact same shard. If any key diverges,
	///      the batch immediately aborts with `StatusCode::CrossShardKeyViolation` (0x16)
	///      prior to acquiring locks or mutating state.
	///   3. Acquires shard write lock once.
	///   4. Executes all operations sequentially under the lock and short-circuits on failure.
	///   5. Releases lock and returns aggregate status array.
	/// - **Return Status:**
	///   - `StatusCode::Ok` (0x00) with array of sub-request_old status results.
	///   - `StatusCode::CrossShardKeyViolation` (0x16) if keys span multiple shards.
	///   - `StatusCode::InvalidCommand` (0x13) if a nested `Batch` is present.
	/// - **Complexity:** O(M) where M is the count of sub-commands in the batch.
	Batch = 0x40,
}

impl TryFrom<u8> for Opcode {
	type Error = u8;

	#[inline]
	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0x01 => Ok(Self::Ping),
			0x02 => Ok(Self::Stats),
			0x03 => Ok(Self::Reset),
			0x10 => Ok(Self::Get),
			0x11 => Ok(Self::Exists),
			0x12 => Ok(Self::Ttl),
			0x20 => Ok(Self::Set),
			0x21 => Ok(Self::Add),
			0x22 => Ok(Self::Replace),
			0x23 => Ok(Self::Delete),
			0x30 => Ok(Self::Expire),
			0x31 => Ok(Self::Persist),
			0x40 => Ok(Self::Batch),
			unknown => Err(unknown),
		}
	}
}