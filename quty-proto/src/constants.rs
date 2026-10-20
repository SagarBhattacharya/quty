//! Protocol-wide constants, magic markers, and memory bounds.

/// Protocol magic identification byte (ASCII 'QUTY').
pub const PROTOCOL_MAGIC: u32 = 0x5155_5459;

/// Current supported wire protocol version.
pub const PROTOCOL_VERSION_V1: u16 = 0x0001;

/// Fixed length in bytes of the request header.
pub const REQUEST_HEADER_SIZE: usize = 20;

/// Fixed length in bytes of the response header.
pub const RESPONSE_HEADER_SIZE: usize = 20;

/// Maximum allowable key size enforced at runtime (64 KB).
pub const MAX_KEY_SIZE: usize = 64 * 1024;

/// Maximum allowable value size enforced at runtime (32 MB).
pub const MAX_VALUE_SIZE: usize = 32 * 1024 * 1024;

/// Maximum number of sub-operations allowable in a single batch (64).
pub const MAX_BATCH_OPERATIONS: usize = 64;

/// Maximum total wire payload size allowed in a single frame (64 MB).
/// Accommodates full 32 MB single values plus generous batch overhead.
pub const MAX_PAYLOAD_SIZE: usize = 64 * 1024 * 1024;

/// Absolute maximum frame size (Header + Payload).
pub const MAX_FRAME_SIZE: usize = REQUEST_HEADER_SIZE + MAX_PAYLOAD_SIZE;
