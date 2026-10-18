pub const HEADER_SIZE: usize = 8;
pub const MAX_KEY_SIZE: usize = 32 * 1024;          // 32 KB
pub const MAX_VALUE_SIZE: usize = 32 * 1024 * 1024; // 32 MB

pub const MAX_FRAME_SIZE: usize = 
	HEADER_SIZE +
	MAX_KEY_SIZE + 
	MAX_VALUE_SIZE + 
	16;

pub const STATS_PAYLOAD_SIZE: usize = 64; // 8 fields * 8 bytes