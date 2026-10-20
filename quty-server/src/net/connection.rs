//! Client connection worker managing socket I/O, pipelining, and framing.

use crate::database::Engine;
use crate::net::handler::handle_request;
use bytes::BytesMut;
use quty_proto::{
	OperationDecodeError, Request, RequestDecodeError, Response, ResponsePayload, StatusCode,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

/// Default socket buffer capacity (64 KB).
pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;
/// Threshold past which drained buffers are shrunk back to baseline (1 MB).
pub const BUFFER_RECLAIM_THRESHOLD: usize = 1024 * 1024;

/// Connection actor managing streaming read and write cycles for a connected client.
pub struct Connection {
	socket: TcpStream,
	peer_addr: SocketAddr,
	read_buf: BytesMut,
	write_buf: BytesMut,
	engine: Arc<Engine>,
	cancel_token: CancellationToken,
	flush_threshold: usize,
}

impl Connection {
	/// Creates a new client connection actor and configures `TCP_NODELAY`.
	pub fn new(
		socket: TcpStream,
		peer_addr: SocketAddr,
		engine: Arc<Engine>,
		cancel_token: CancellationToken,
		flush_threshold: usize,
	) -> Self {
		let _ = socket.set_nodelay(true);
		Self {
			socket,
			peer_addr,
			read_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
			write_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
			engine,
			cancel_token,
			flush_threshold,
		}
	}

	/// Primary connection loop processing socket reads and write buffer flushes.
	pub async fn run(mut self) {
		self.engine.on_client_connected();
		trace!(peer = %self.peer_addr, "Client connected");

		let _guard = ConnectionGuard(Arc::clone(&self.engine), self.peer_addr);

		loop {
			tokio::select! {
				biased;

				() = self.cancel_token.cancelled() => {
					debug!(peer = %self.peer_addr, "Shutdown signal received; draining in-flight requests");
					let _ = self.process_incoming_frames().await;
					break;
				}

				read_res = self.socket.read_buf(&mut self.read_buf) => {
					match read_res {
						Ok(0) => {
							trace!(peer = %self.peer_addr, "Client closed connection (EOF)");
							return;
						}
						Ok(n) => {
							trace!(peer = %self.peer_addr, bytes_read = n, "Received network frame bytes");
							if self.process_incoming_frames().await.is_err() {
								return;
							}
						}
						Err(e) => {
							warn!(peer = %self.peer_addr, error = %e, "Socket read error");
							return;
						}
					}
				}
			}
		}

		let _ = self.socket.shutdown().await;
	}

	async fn process_incoming_frames(&mut self) -> Result<(), ()> {
		loop {
			match Request::decode(&mut self.read_buf) {
				Ok(Some(request)) => {
					let req_id = request.request_id;
					let response = handle_request(request, &self.engine);

					trace!(
						peer = %self.peer_addr,
						request_id = req_id,
						status = ?response.header.status,
						"Processed command"
					);

					response.encode(&mut self.write_buf);

					if self.write_buf.len() >= self.flush_threshold {
						self.flush_write_buffer().await?;
					}
				}
				Ok(None) => break,
				Err(err) => {
					error!(peer = %self.peer_addr, error = ?err, "Protocol framing violation");
					let status = match err {
						RequestDecodeError::OpError(op_err) => match op_err {
							OperationDecodeError::UnexpectedEof | OperationDecodeError::InvalidUtf8 => {
								StatusCode::BadRequest
							}
							OperationDecodeError::UnknownOpcode(_) => StatusCode::UnknownOpcode,
							OperationDecodeError::KeyTooLarge(_) => StatusCode::KeyTooLarge,
							OperationDecodeError::ValueTooLarge(_) => StatusCode::ValueTooLarge,
						},
						_ => StatusCode::BadRequest,
					};

					let fatal_response = Response::new(0, status, ResponsePayload::Null);
					fatal_response.encode(&mut self.write_buf);
					let _ = self.flush_write_buffer().await;
					return Err(());
				}
			}
		}

		// Memory compaction check
		if self.read_buf.is_empty() && self.read_buf.capacity() > BUFFER_RECLAIM_THRESHOLD {
			self.read_buf = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);
		}

		self.flush_write_buffer().await
	}

	#[inline]
	async fn flush_write_buffer(&mut self) -> Result<(), ()> {
		if !self.write_buf.is_empty() {
			if let Err(e) = self.socket.write_all(&self.write_buf).await {
				warn!(peer = %self.peer_addr, error = %e, "Socket write failure");
				return Err(());
			}
			self.write_buf.clear();

			if self.write_buf.capacity() > BUFFER_RECLAIM_THRESHOLD {
				self.write_buf = BytesMut::with_capacity(DEFAULT_BUFFER_SIZE);
			}
		}
		Ok(())
	}
}

struct ConnectionGuard(Arc<Engine>, SocketAddr);

impl Drop for ConnectionGuard {
	fn drop(&mut self) {
		self.0.on_client_disconnected();
		trace!(peer = %self.1, "Client disconnected");
	}
}
