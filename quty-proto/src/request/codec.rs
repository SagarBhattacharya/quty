use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::command::{Command, Opcode, Operation};
use crate::constants::*;
use crate::request::header::RequestHeader;
use crate::request::Request;
use crate::RequestDecodeError;

impl Request {
	/// Decodes a complete Request frame from an incoming TCP buffer.
	pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, RequestDecodeError> {
		let header = match RequestHeader::decode(src)? {
			Some(h) => h,
			None => return Ok(None),
		};

		let total_frame_len = HEADER_SIZE + header.payload_len as usize;
		if src.len() < total_frame_len {
			return Ok(None);
		}

		// Split off the verified frame slice and discard the 8-byte header
		let mut frame = src.split_to(total_frame_len);
		frame.advance(HEADER_SIZE);

		let command = match header.opcode {
			Opcode::Ping => {
				Self::ensure_empty(&frame)?;
				Command::Ping
			}
			Opcode::Stats => {
				Self::ensure_empty(&frame)?;
				Command::Stats
			}
			Opcode::Reset => {
				Self::ensure_empty(&frame)?;
				Command::Reset
			}
			Opcode::Batch => {
				let operations = Self::decode_batch_payload(&mut frame)?;
				Command::Batch(operations)
			}
			// Direct reuse: all standalone storage operations funnel through Operation::decode
			storage_opcode => {
				let op = Operation::decode(storage_opcode, &mut frame)?;
				Self::ensure_empty(&frame)?;
				Command::Single(op)
			}
		};

		Ok(Some(Request {
			request_id: header.request_id,
			flags: header.flags,
			command,
		}))
	}

	/// Serializes a complete Request frame into `dst`.
	pub fn encode(&self, dst: &mut BytesMut) {
		let opcode = self.command.opcode();

		// Reserve 8-byte header slot
		let header_offset = dst.len();
		dst.put_u16(self.request_id);
		dst.put_u8(opcode as u8);
		dst.put_u8(self.flags);
		dst.put_u32(0); // Temporary length placeholder

		let payload_start = dst.len();

		match &self.command {
			Command::Ping | Command::Stats | Command::Reset => {}
			Command::Single(op) => {
				op.encode(dst);
			}
			Command::Batch(operations) => {
				dst.put_u16(operations.len() as u16);
				for op in operations {
					dst.put_u8(op.opcode() as u8);
					op.encode(dst);
				}
			}
		}

		let payload_len = (dst.len() - payload_start) as u32;
		// Backfill payload length at header offset 4..8
		dst[header_offset + 4..header_offset + 8]
			.copy_from_slice(&payload_len.to_be_bytes());
	}

	#[inline]
	fn ensure_empty(buf: &BytesMut) -> Result<(), RequestDecodeError> {
		if buf.has_remaining() {
			return Err(RequestDecodeError::InvalidPayload(
				"Unexpected trailing bytes in payload".into(),
			));
		}
		Ok(())
	}

	fn decode_batch_payload(buf: &mut BytesMut) -> Result<Vec<Operation>, RequestDecodeError> {
		if buf.remaining() < 2 {
			return Err(RequestDecodeError::Incomplete);
		}
		let count = buf.get_u16() as usize;
		let mut operations = Vec::with_capacity(count);

		for _ in 0..count {
			if buf.remaining() < 1 {
				return Err(RequestDecodeError::Incomplete);
			}
			let sub_opcode_byte = buf.get_u8();
			let sub_opcode = Opcode::try_from(sub_opcode_byte)
				.map_err(RequestDecodeError::UnknownOpcode)?;

			// Invariant: batch cannot contain nested batch or admin commands
			match sub_opcode {
				Opcode::Batch => {
					return Err(RequestDecodeError::InvalidPayload(
						"Nested Batch is strictly prohibited".into(),
					));
				}
				Opcode::Ping | Opcode::Stats | Opcode::Reset => {
					return Err(RequestDecodeError::InvalidPayload(
						"Transport/Admin commands cannot be batched".into(),
					));
				}
				storage_op => {
					operations.push(Operation::decode(storage_op, buf)?);
				}
			}
		}

		Self::ensure_empty(buf)?;
		Ok(operations)
	}
}