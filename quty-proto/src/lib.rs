#![warn(missing_docs)]
//! # quty-proto
//!
//! Pure binary wire framing, command AST, and serialization codec library
//! for the Quty in-memory cache protocol (v1).

pub mod command;
pub mod constants;
pub mod request;
pub mod response;

pub use command::{Command, CommandType, Opcode, Operation, OperationDecodeError, routing_key};
pub use constants::*;
pub use request::{Request, RequestDecodeError, RequestHeader};
pub use response::{
	BatchErrorPayload, PayloadType, Response, ResponseDecodeError, ResponseHeader, ResponsePayload,
	StatsPayload, StatusCode, SubResult,
};

// Helpers

#[inline]
fn to_u32_len(len: usize) -> u32 {
	u32::try_from(len).expect("protocol wire size overflow: length exceeds u32::MAX")
}
