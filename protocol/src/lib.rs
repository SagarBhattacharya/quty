use thiserror::Error;

pub mod response;
pub mod constants;
pub mod command;
pub mod request;

pub use command::{Opcode, Command, Operation};
pub use constants::*;

pub use request::{
	Request,
	RequestHeader,
};

pub use response::{
	ResponseHeader,
	PayloadType,
	Response,
	ResponsePayload,
	StatsPayload,
	StatusCode,
	SubResult
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RequestDecodeError {
	#[error("Incomplete frame buffer")]
	Incomplete,

	#[error("Unknown opcode: {0:#04x}")]
	UnknownOpcode(u8),

	#[error("Invalid UTF-8 sequence in key")]
	InvalidUtf8,

	#[error("Key size {0} exceeds limit of 64KB")]
	KeyTooLarge(usize),

	#[error("Value size {0} exceeds limit of 32MB")]
	ValueTooLarge(usize),

	#[error("Frame payload length {0} exceeds absolute ceiling")]
	FrameTooLarge(u32),

	#[error("Payload decode failure: {0}")]
	InvalidPayload(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResponseDecodeError {
	#[error("Incomplete response buffer")]
	Incomplete,

	#[error("Unknown status code: {0:#04x}")]
	UnknownStatusCode(u8),

	#[error("Unknown payload type: {0:#04x}")]
	UnknownPayloadType(u8),

	#[error("Invalid UTF-8 sequence in response text")]
	InvalidUtf8,

	#[error("Payload length mismatch: expected {expected}, found {found}")]
	PayloadLengthMismatch { expected: usize, found: usize },

	#[error("Invalid response payload: {0}")]
	InvalidPayload(String),
}