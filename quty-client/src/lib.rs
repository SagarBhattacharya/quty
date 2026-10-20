//! High-performance asynchronous client driver for the Quty in-memory cache server.
//!
//! Provides connection pooling primitives, monotonic request tracking, zero-allocation
//! wire encoding, half-open socket timeout detection, and pipeline state poisoning guards.

pub mod format;
pub mod repl;
pub mod validate;

use bytes::{Bytes, BytesMut};
use quty_proto::{Command, Operation, Request, Response};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::timeout;
use crate::validate::{validate_batch, validate_operation};

/// Default timeout applied to read operations to detect hung or half-open TCP connections.
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Asynchronous client connection manager for the Quty cache server.
///
/// Manages streaming socket I/O over TCP with `TCP_NODELAY` enabled. If a framing,
/// network, or request-correlation error occurs during command execution, the client is
/// automatically marked as poisoned to prevent desynchronized socket reads.
pub struct Client {
	stream: TcpStream,
	read_buf: BytesMut,
	write_buf: BytesMut,
	request_id_counter: u64,
	read_timeout: Option<Duration>,
	is_poisoned: bool,
}

/// The response and round-trip execution timing for a completed command.
#[derive(Debug)]
pub struct ExecutionResult {
	/// Framed protocol response returned by the server.
	pub response: Response,
	/// Round-trip duration including serialization, transport, and deserialization.
	pub elapsed: Duration,
}

impl Client {
	/// Connects to a running Quty server and enables `TCP_NODELAY`.
	///
	/// Configures the default read timeout of 5 seconds.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if the TCP handshake fails.
	pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
		let stream = TcpStream::connect(addr).await?;
		stream.set_nodelay(true)?;

		Ok(Self {
			stream,
			read_buf: BytesMut::with_capacity(64 * 1024),
			write_buf: BytesMut::with_capacity(64 * 1024),
			request_id_counter: 1,
			read_timeout: Some(DEFAULT_READ_TIMEOUT),
			is_poisoned: false,
		})
	}

	/// Sets or clears the per-operation read timeout.
	///
	/// Passing `None` disables socket read timeouts entirely.
	pub fn set_read_timeout(&mut self, read_timeout: Option<Duration>) {
		self.read_timeout = read_timeout;
	}

	/// Returns `true` if this connection experienced an I/O or framing fault and was poisoned.
	///
	/// Once poisoned, all subsequent calls to [`Client::execute`] fail immediately
	/// with [`io::ErrorKind::BrokenPipe`].
	#[must_use]
	pub const fn is_poisoned(&self) -> bool {
		self.is_poisoned
	}

	/// Returns the remote peer address of the connected server.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if socket metadata cannot be retrieved.
	pub fn peer_addr(&self) -> io::Result<SocketAddr> {
		self.stream.peer_addr()
	}

	/// Dispatches a raw protocol [`Command`] and awaits a framed [`Response`].
	///
	/// Encodes the command directly into the write buffer, transmits it over TCP,
	/// and accumulates incoming socket bytes until a complete response frame is received.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if the client is poisoned, network transport fails,
	/// the socket read times out, framing is invalid, or request correlation mismatches.
	pub async fn execute(&mut self, command: Command) -> io::Result<ExecutionResult> {
		if self.is_poisoned {
			return Err(io::Error::new(
				io::ErrorKind::BrokenPipe,
				"Client connection is poisoned from a prior \
				transport or framing error; reconnect required",
			));
		}

		match &command {
			Command::Single(op) => validate_operation(op)?,
			Command::Batch(ops) => validate_batch(ops)?,
			Command::Ping | Command::Stats | Command::Reset => {}
		}

		match self.execute_internal(command).await {
			Ok(res) => Ok(res),
			Err(e) => {
				self.is_poisoned = true;
				Err(e)
			}
		}
	}

	/// Internal execution loop wrapped by the poisoning guard.
	async fn execute_internal(&mut self, command: Command) -> io::Result<ExecutionResult> {
		let req_id = self.request_id_counter;
		self.request_id_counter = self.request_id_counter.wrapping_add(1);

		let req = Request::new(req_id, command);
		req.encode(&mut self.write_buf);

		let start = Instant::now();
		self.stream.write_all(&self.write_buf).await?;
		self.write_buf.clear();

		loop {
			if let Some(res) = Response::decode(&mut self.read_buf)
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?
			{
				if res.header.request_id != req_id {
					return Err(io::Error::new(
						io::ErrorKind::InvalidData,
						format!(
							"Request correlation error: expected ID {req_id}, received {}",
							res.header.request_id
						),
					));
				}

				// --- Memory Capacity Reclamation ---
				// If all bytes in the buffer have been consumed and previous large payload reads
				// (up to MAX_VALUE_SIZE 32 MB) caused the buffer to expand past 1 MB, reallocate
				// back down to the baseline 64 KB capacity to prevent persistent memory creep.
				if self.read_buf.is_empty() && self.read_buf.capacity() > 1024 * 1024 {
					self.read_buf = BytesMut::with_capacity(64 * 1024);
				}

				let elapsed = start.elapsed();
				return Ok(ExecutionResult {
					response: res,
					elapsed,
				});
			}

			let read_future = self.stream.read_buf(&mut self.read_buf);
			let bytes_read = match self.read_timeout {
				Some(duration) => match timeout(duration, read_future).await {
					Ok(read_res) => read_res?,
					Err(_) => {
						return Err(io::Error::new(
							io::ErrorKind::TimedOut,
							"Timed out waiting for server response",
						));
					}
				},
				None => read_future.await?,
			};

			if bytes_read == 0 {
				return Err(io::Error::new(
					io::ErrorKind::UnexpectedEof,
					"Connection closed by server",
				));
			}
		}
	}

	/// Sends a server liveliness heartbeat (`PING`).
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn ping(&mut self) -> io::Result<ExecutionResult> {
		self.execute(Command::Ping).await
	}

	/// Requests internal engine and partition statistics (`STATS`).
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn stats(&mut self) -> io::Result<ExecutionResult> {
		self.execute(Command::Stats).await
	}

	/// Flushes all entries and clears partition telemetry (`RESET`).
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn reset(&mut self) -> io::Result<ExecutionResult> {
		self.execute(Command::Reset).await
	}

	/// Reads the binary value associated with `key`.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn get(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Get { key: key.into() }))
			.await
	}

	/// Unconditionally writes a key-value pair.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn set(
		&mut self,
		key: impl Into<String>,
		value: impl Into<Bytes>,
	) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Set {
				key: key.into(),
				value: value.into(),
			}))
			.await
	}

	/// Conditionally stores a key-value pair only if `key` does not already exist.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn add(
		&mut self,
		key: impl Into<String>,
		value: impl Into<Bytes>,
	) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Add {
				key: key.into(),
				value: value.into(),
			}))
			.await
	}

	/// Conditionally updates a key-value pair only if `key` is present.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn replace(
		&mut self,
		key: impl Into<String>,
		value: impl Into<Bytes>,
	) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Replace {
				key: key.into(),
				value: value.into(),
			}))
			.await
	}

	/// Deletes a key and reclaims payload memory.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn delete(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Delete { key: key.into() }))
			.await
	}

	/// Checks if a key exists without mutating LRU recency clocks.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn exists(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Exists { key: key.into() }))
			.await
	}

	/// Queries the remaining lifespan of `key` in milliseconds.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn ttl(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Ttl { key: key.into() }))
			.await
	}

	/// Attaches an expiration timer to `key`.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn expire(
		&mut self,
		key: impl Into<String>,
		ttl_ms: u64,
	) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Expire {
				key: key.into(),
				ttl_ms,
			}))
			.await
	}

	/// Strips any expiration deadline from `key`.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn persist(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self
			.execute(Command::Single(Operation::Persist { key: key.into() }))
			.await
	}

	/// Executes an atomic multi-operation batch.
	///
	/// # Errors
	///
	/// Returns [`io::Error`] if network transmission drops or the connection is poisoned.
	pub async fn batch(&mut self, ops: Vec<Operation>) -> io::Result<ExecutionResult> {
		self.execute(Command::Batch(ops)).await
	}
}
