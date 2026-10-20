# quty-proto

A zero-allocation, stream-oriented binary wire protocol codec and 
command abstract syntax tree for the Quty in-memory cache engine.

`quty-proto` contains pure framing logic and serialization state machines. 
It operates entirely on `bytes::BytesMut` and has zero dependencies on 
network sockets, runtime executors, or engine storage internals.

## Protocol Overview

- **Transport:** TCP stream with `TCP_NODELAY = 1`.
- **Endianness:** All integer fields are big-endian (network byte order).
- **Magic Identifier:** `0x51555459` (ASCII `"QUTY"`).
- **Protocol Version:** `0x0001` (v1).
- **Framing:** Fixed 20-byte header preceding an optional variable-length payload.
- **Maximum Key Size:** 64 KiB (65,536 bytes).
- **Maximum Value Size:** 32 MiB (33,554,432 bytes).
- **Maximum Batch Operations:** 64 sub-operations per batch transaction.
- **Maximum Frame Payload:** 64 MiB per request or response frame.

The decoder supports incremental TCP stream processing. Once the complete
20-byte header is available, it validates the header and rejects payload
lengths above 64 MiB before waiting for the body. Request flags must be zero.
Malformed fixed-width payloads, invalid batch counts, and unexpected trailing
bytes are rejected.

## Wire Frame Layouts

### Request Frame (Client → Server)

The request frame consists of a fixed 20-byte header followed by the 
optional payload.

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                     Magic Identifier (0x51555459)             |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |        Version (0x0001)       |  Cmd Type (u8)|   Flags (u8)  |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                   Request ID (Bytes 0..=3)                    |
 |                   Request ID (Bytes 4..=7)                    |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                  Payload Length in Bytes (u32)                |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                        Payload Bytes...                       |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Response Frame (Server → Client)

The response frame uses the same header structure as the request frame, 
with the command type replaced by a status code and payload type.

```text
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                     Magic Identifier (0x51555459)             |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |        Version (0x0001)       |  Status (u8)  | PayloadT (u8) |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                   Request ID (Bytes 0..=3)                    |
 |                   Request ID (Bytes 4..=7)                    |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                  Payload Length in Bytes (u32)                |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                        Payload Bytes...                       |
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

## Quickstart

Add `quty-proto` to your `Cargo.toml`:

```toml
[dependencies]
quty-proto = { path = "../quty-proto" }
bytes = "1.9"
```

### Encoding a Request

```rust
use bytes::BytesMut;
use quty_proto::{Command, Operation, Request};

let req = Request::new(
	101,
	Command::Single(Operation::Get {
		key: "user:session:100".to_string(),
	}),
);

let mut buf = BytesMut::new();
req.encode(&mut buf);
```

### Streaming Resumable Decoding

The decoder supports incremental processing of TCP streams. 
If a frame is incomplete, it returns `Ok(None)` so the caller can wait for more data.

```rust
use bytes::BytesMut;
use quty_proto::Response;

fn process_incoming_stream(buf: &mut BytesMut) {
	loop {
		match Response::decode(buf) {
			Ok(Some(response)) => {
				println!(
					"Received response for req_id={} with status {:?}",
					response.header.request_id,
					response.header.status
				);
			}
			Ok(None) => {
				// Incomplete frame; wait for the next socket read.
				break;
			}
			Err(e) => {
				eprintln!("Protocol framing error: {}", e);
				break;
			}
		}
	}
}
```

## Status Codes

| Code   | Name                    | Classification | Description                                             |
|--------|-------------------------|----------------|---------------------------------------------------------|
| `0x00` | `Ok`                    | Success        | Normal operational completion                           |
| `0x01` | `Created`               | Success        | Record newly inserted                                   |
| `0x02` | `NoContent`             | Success        | Mutation applied with no return body                    |
| `0x10` | `KeyNotFound`           | Soft Error     | Target key is absent or expired; does not abort a batch |
| `0x11` | `KeyExpired`            | Soft Error     | Key expired and lazily reclaimed                        |
| `0x12` | `KeyExists`             | Hard Error     | Conditional insert (`ADD`) failed                       |
| `0x13` | `PreconditionFailed`    | Hard Error     | Conditional update (`REPLACE`) failed                   |
| `0x20` | `StorageFull`           | Hard Error     | Shard memory capacity reached                           |
| `0x30` | `KeyTooLarge`           | Hard Error     | Key exceeds the 64 KB limit                             |
| `0x31` | `ValueTooLarge`         | Hard Error     | Value exceeds the 32 MB limit                           |
| `0x32` | `CrossShardViolation`   | Hard Error     | Batch accesses multiple distinct shards                 |
| `0x33` | `EmptyBatch`            | Hard Error     | Batch contains zero operations                          |
| `0x34` | `BatchShortCircuited`   | Hard Error     | Prior operation failed; batch aborted                   |
| `0x40` | `BadRequest`            | Protocol Error | Corrupted framing or invalid payload                    |
| `0x41` | `InternalError`         | Protocol Error | Unhandled engine fault                                  |
| `0x42` | `UnknownOpcode`         | Protocol Error | Unrecognized opcode identifier                          |
| `0x43` | `NestedBatchNotAllowed` | Protocol Error | Recursive batch detected                                |
| `0x44` | `ServerBusy`            | Backpressure   | Server draining or queue saturated                      |
