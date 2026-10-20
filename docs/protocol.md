# Binary Wire Protocol Specification (v1)

This document defines the wire specification for the Quty protocol (v1). Any compliant client or driver implementation in any language must adhere to the framing, field widths, endianness, and error invariants defined here.

## 1. Network Transport Invariants

- **Transport Layer:** TCP stream. Sockets MUST configure `TCP_NODELAY = 1` to disable Nagle's algorithm.
- **Byte Ordering:** All multi-byte numerical fields (16-bit, 32-bit, and 64-bit) MUST be encoded in Big-Endian (Network Byte Order).
- **Framing Invariant:** Every network message begins with a fixed 20-byte header. The header explicitly declares the payload byte length (`payload_len`), allowing decoders to determine frame boundaries before consuming variable-length payload bytes.

## 2. Frame and Payload Limits

- Every request and response begins with a 20-byte header.
- A frame payload must not exceed 64 MiB.
- A batch request or response array must contain no more than 64 operations
	or results.
- Decoders validate complete headers before waiting for payload bodies.
- Request flags must be zero.
- Fixed-width response payloads must have their exact defined lengths.
- Invalid lengths, malformed payloads, and unexpected trailing bytes are
	rejected.

## 3. Request Framing

### Header Layout (20 Bytes)

| Offset   | Field          | Type  | Description                                                                                                       |
|----------|----------------|-------|-------------------------------------------------------------------------------------------------------------------|
| `0..4`   | `magic`        | `u32` | Must be `0x51555459` (ASCII `"QUTY"`). Frames with any other value MUST trigger immediate connection termination. |
| `4..6`   | `version`      | `u16` | Wire version. Current version is `0x0001`.                                                                        |
| `6`      | `command_type` | `u8`  | Discriminator defining the command structure.                                                                     |
| `7`      | `flags`        | `u8`  | Reserved configuration flags. MUST be set to `0x00`.                                                              |
| `8..16`  | `request_id`   | `u64` | Client-assigned request identifier. Mirrored back unchanged in the server's response header.                      |
| `16..20` | `payload_len`  | `u32` | Length in bytes of the payload following this header.                                                             |

### Command Types (`command_type`)

| Value  | Name     | Description                                                              |
|--------|----------|--------------------------------------------------------------------------|
| `0x01` | `Single` | Followed by exactly one encoded storage operation.                       |
| `0x02` | `Batch`  | Followed by a 4-byte operation count and consecutive encoded operations. |
| `0x03` | `Ping`   | Heartbeat request. `payload_len` MUST be `0`.                            |
| `0x04` | `Stats`  | Telemetry inspection. `payload_len` MUST be `0`.                         |
| `0x05` | `Reset`  | Engine flush and metric reset. `payload_len` MUST be `0`.                |

## 4. Storage Operation Encoding

When `command_type` is `Single` (`0x01`) or operations are included inside a `Batch` (`0x02`), each storage operation MUST use the following payload encoding.

### Operation Opcodes

| Opcode | Name      | Layout                                                                                 |
|--------|-----------|----------------------------------------------------------------------------------------|
| `0x01` | `GET`     | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8]`                                                 |
| `0x02` | `SET`     | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8][ValLen: 4B][Val Bytes]`                          |
| `0x03` | `ADD`     | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8][ValLen: 4B][Val Bytes]` (store only if absent)   |
| `0x04` | `REPLACE` | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8][ValLen: 4B][Val Bytes]` (update only if present) |
| `0x05` | `DELETE`  | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8]`                                                 |
| `0x06` | `EXISTS`  | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8]`                                                 |
| `0x07` | `TTL`     | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8]`                                                 |
| `0x08` | `EXPIRE`  | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8][TTL ms: 8B]`                                     |
| `0x09` | `PERSIST` | `[Opcode: 1B][KeyLen: 4B][Key: UTF-8]`                                                 |

**Encoding requirements:**

- `KeyLen` and `ValLen` are 4-byte unsigned integers (`u32`) in big-endian byte order.
- `Key` contains exactly `KeyLen` bytes of UTF-8 encoded data.
- `Val Bytes` contains exactly `ValLen` bytes of arbitrary binary data.
- `TTL ms` is an 8-byte unsigned integer (`u64`) in big-endian byte order.

### Batch Payload Structure (`0x02`)

A batch payload consists of a 4-byte operation count followed by the encoded operations in sequence.

```text
[Item Count: 4B (u32)]
[Encoded Operation 0]
[Encoded Operation 1]
...
[Encoded Operation N-1]
```

The item count specifies the number of encoded operations in the batch.

## 5. Response Framing

### Header Layout (20 Bytes)

| Offset   | Field          | Type  | Description                                             |
|----------|----------------|-------|---------------------------------------------------------|
| `0..4`   | `magic`        | `u32` | Must be `0x51555459` (ASCII `"QUTY"`).                  |
| `4..6`   | `version`      | `u16` | Wire version (`0x0001`).                                |
| `6`      | `status`       | `u8`  | Execution result status code.                           |
| `7`      | `payload_type` | `u8`  | Discriminator defining the format of the response body. |
| `8..16`  | `request_id`   | `u64` | Correlated client request ID echoed verbatim.           |
| `16..20` | `payload_len`  | `u32` | Length in bytes of the trailing payload body.           |

### Payload Types (`payload_type`)

| Value  | Name       | Payload Format                                                                             |
|--------|------------|--------------------------------------------------------------------------------------------|
| `0x00` | `Null`     | Exactly 0 bytes.                                                                           |
| `0x01` | `Bool`     | Exactly 1 byte: `0x01` = true, `0x00` = false.                                             |
| `0x02` | `Int64`    | Exactly 8 bytes containing a signed big-endian integer (`i64`).                            |
| `0x03` | `String`   | Raw UTF-8 bytes matching `payload_len`.                                                    |
| `0x04` | `Raw`      | Raw binary payload bytes matching `payload_len`.                                           |
| `0x05` | `Stats`    | Exactly 64 bytes containing 8 contiguous 64-bit big-endian unsigned counters.              |
| `0x06` | `Array`    | A 4-byte item count followed by `N` item records.                                          |
| `0x07` | `BatchErr` | Exactly 5 bytes: a 4-byte failed operation index followed by a 1-byte failure status code. |

#### Array Payload Structure (`0x06`)

An array payload begins with a 4-byte item count followed by the encoded item records.

```text
[Item Count: 4B (u32)]
[Item Record 0]
[Item Record 1]
...
[Item Record N-1]
```

Each item record uses the following structure:

```text
[Status: 1B]
[PayloadType: 1B]
[ItemLen: 4B (u32)]
[Payload Bytes: ItemLen bytes]
```

`ItemLen` specifies the length of the payload bytes for the corresponding item, excluding the 6-byte item header.

#### Batch Error Payload Structure (`0x07`)

The `BatchErr` payload contains information about the operation that caused a batch to fail.

```text
[Failed Index: 4B (u32)]
[Failure Status Code: 1B]
```

The failed index is zero-based and identifies the operation that triggered the failure.

## 6. Shard Partitioning & Transaction Invariants

### 6.1 Prefix Routing

All keys are routed to a shard using the string prefix preceding the first colon (`:`).

- `session:user_123` and `session:user_456` map to the same partition.
- If no colon exists (e.g., `counter`), the entire key string is hashed.

### 6.2 Batch Single-Shard Constraint

Atomic batches (`0x02`) MUST NOT cross shard boundaries.

If a batch contains keys that resolve to different shards, the server MUST abort before executing any mutations and return status code `0x32` (`CrossShardViolation`).

### 6.3 Rollback Semantics

If any operation in a batch produces a hard error, such as `StorageFull` or `KeyExists`, all previous mutations in that batch MUST be rolled back immediately.

The server MUST return status code `0x34` (`BatchShortCircuited`) with a `BatchErr` payload identifying the failed operation and its failure status code.

