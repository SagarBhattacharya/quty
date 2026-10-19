pub mod repl;
pub mod format;

mod proto {
	#[allow(unused)]
	pub use quty_proto::*;
}

use bytes::{Bytes, BytesMut};
use quty_proto::*;
use std::io;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

pub struct Client {
	stream: TcpStream,
	read_buf: BytesMut,
	write_buf: BytesMut,
	request_id_counter: u16,
}

#[derive(Debug)]
pub struct ExecutionResult {
	pub response: Response,
	pub elapsed: std::time::Duration,
}

impl Client {
	pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
		let stream = TcpStream::connect(addr).await?;
		stream.set_nodelay(true)?;

		Ok(Self {
			stream,
			read_buf: BytesMut::with_capacity(64 * 1024),
			write_buf: BytesMut::with_capacity(64 * 1024),
			request_id_counter: 1,
		})
	}

	pub fn peer_addr(&self) -> io::Result<SocketAddr> {
		self.stream.peer_addr()
	}

	/// Dispatches a raw protocol Command and awaits a framed Response.
	pub async fn execute(&mut self, command: Command) -> io::Result<ExecutionResult> {
		let req_id = self.request_id_counter;
		self.request_id_counter = self.request_id_counter.wrapping_add(1);

		let req = Request::new(req_id, 0, command);
		req.encode(&mut self.write_buf);

		let start = Instant::now();
		self.stream.write_all(&self.write_buf).await?;
		self.write_buf.clear();

		loop {
			if let Some(res) = Response::decode(&mut self.read_buf)
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{:?}", e)))?
			{
				let elapsed = start.elapsed();
				return Ok(ExecutionResult {
					response: res,
					elapsed,
				});
			}

			let bytes_read = self.stream.read_buf(&mut self.read_buf).await?;
			if bytes_read == 0 {
				return Err(io::Error::new(
					io::ErrorKind::UnexpectedEof,
					"Connection closed by server",
				));
			}
		}
	}

	// -------------------------------------------------------------------------
	// Ergonomic Command Methods
	// -------------------------------------------------------------------------

	pub async fn ping(&mut self) -> io::Result<ExecutionResult> {
		self.execute(Command::Ping).await
	}

	pub async fn stats(&mut self) -> io::Result<ExecutionResult> {
		self.execute(Command::Stats).await
	}

	pub async fn reset(&mut self) -> io::Result<ExecutionResult> {
		self.execute(Command::Reset).await
	}

	pub async fn get(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Get { key: key.into() }))
			.await
	}

	pub async fn set(
		&mut self,
		key: impl Into<String>,
		value: impl Into<Bytes>,
	) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Set {
			key: key.into(),
			value: value.into(),
		}))
			.await
	}

	pub async fn add(
		&mut self,
		key: impl Into<String>,
		value: impl Into<Bytes>,
	) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Add {
			key: key.into(),
			value: value.into(),
		}))
			.await
	}

	pub async fn replace(
		&mut self,
		key: impl Into<String>,
		value: impl Into<Bytes>,
	) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Replace {
			key: key.into(),
			value: value.into(),
		}))
			.await
	}

	pub async fn delete(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Delete { key: key.into() }))
			.await
	}

	pub async fn exists(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Exists { key: key.into() }))
			.await
	}

	pub async fn ttl(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Ttl { key: key.into() }))
			.await
	}

	pub async fn expire(
		&mut self,
		key: impl Into<String>,
		ttl_ms: u64,
	) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Expire {
			key: key.into(),
			ttl_ms,
		}))
			.await
	}

	pub async fn persist(&mut self, key: impl Into<String>) -> io::Result<ExecutionResult> {
		self.execute(Command::Single(Operation::Persist { key: key.into() }))
			.await
	}

	pub async fn batch(&mut self, ops: Vec<Operation>) -> io::Result<ExecutionResult> {
		self.execute(Command::Batch(ops)).await
	}
}