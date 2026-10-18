use crate::command::Command;

mod header;
mod codec;
mod op_codec;

pub use header::RequestHeader;

/// Fully decoded request_old containing
/// transport metadata and the target datatype.
#[derive(Debug, PartialEq, Eq)]
pub struct Request {
	pub request_id: u16,
	pub flags: u8,
	pub command: Command,
}

impl Request {
	pub fn new(
		request_id: u16,
		flags: u8,
		command: Command
	) -> Self {
		Self { request_id, flags, command }
	}
}

#[cfg(test)]
mod tests {
	use bytes::{BufMut, Bytes, BytesMut};
	use crate::*;

	#[test]
	fn test_request_header_non_destructive_peek() {
		let mut buf = BytesMut::new();
		buf.put_u16(0x0A0B);
		buf.put_u8(Opcode::Get as u8);
		buf.put_u8(0x00);
		buf.put_u32(12);

		let initial_len = buf.len();
		let header = RequestHeader::decode(&mut buf)
			.unwrap().expect("Header must decode");

		assert_eq!(header.request_id, 0x0A0B);
		assert_eq!(header.opcode, Opcode::Get);
		assert_eq!(header.payload_len, 12);
		assert_eq!(buf.len(), initial_len, "Peek must not consume buffer bytes");
	}

	#[test]
	fn test_zero_arg_command_rejects_spurious_payload() {
		let mut buf = BytesMut::new();
		buf.put_u16(1);
		buf.put_u8(Opcode::Ping as u8);
		buf.put_u8(0);
		buf.put_u32(4); // Non-zero payload length for zero-arg command
		buf.extend_from_slice(b"ping");

		let err = Request::decode(&mut buf).unwrap_err();
		assert!(matches!(err, RequestDecodeError::InvalidPayload(_)));
	}

	#[test]
	fn test_key_length_boundary_limit_enforced() {
		let mut buf = BytesMut::new();
		let oversized_key_len = (MAX_KEY_SIZE + 1) as u16;

		buf.put_u16(2);
		buf.put_u8(Opcode::Delete as u8);
		buf.put_u8(0);
		buf.put_u32(2 + oversized_key_len as u32);

		buf.put_u16(oversized_key_len);
		buf.resize(buf.len() + oversized_key_len as usize, 0x61);

		let err = Request::decode(&mut buf).unwrap_err();
		assert!(matches!(err, RequestDecodeError::KeyTooLarge(len) if len == oversized_key_len as usize));
	}

	#[test]
	fn test_value_zero_copy_freeze_semantics() {
		let raw_payload = vec![0xAB; 8192];
		let req = Request {
			request_id: 3,
			flags: 0,
			command: Command::Single(Operation::Set {
				key: "blob:buffer".into(),
				value: Bytes::copy_from_slice(&raw_payload),
			}),
		};

		let mut buf = BytesMut::new();
		req.encode(&mut buf);

		let decoded = Request::decode(&mut buf).unwrap().unwrap();
		if let Command::Single(Operation::Set { value, .. }) = decoded.command {
			assert_eq!(value.as_ref(), raw_payload.as_slice());
		} else {
			panic!("Decoded command variant mismatch");
		}
	}

	#[test]
	fn test_batch_rejects_nested_batch_and_admin_opcodes() {
		let forbidden_opcodes = [Opcode::Batch, Opcode::Ping, Opcode::Stats, Opcode::Reset];

		for forbidden in forbidden_opcodes {
			let mut buf = BytesMut::new();
			buf.put_u16(4);
			buf.put_u8(Opcode::Batch as u8);
			buf.put_u8(0);
			buf.put_u32(2 + 1); // 2B count + 1B opcode

			buf.put_u16(1); // sub_count = 1
			buf.put_u8(forbidden as u8);

			let err = Request::decode(&mut buf).unwrap_err();
			assert!(
				matches!(err, RequestDecodeError::InvalidPayload(_)),
				"Opcode {forbidden:?} must be rejected inside Batch"
			);
		}
	}
}