mod handler;

use crate::db::Engine;
use crate::net::handler::handle_request;
use bytes::BytesMut;
use quty_proto::{Request, RequestDecodeError, Response, ResponsePayload, StatusCode};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024; // 64 KB
const FLUSH_THRESHOLD_BYTES: usize = 128 * 1024;

pub struct Connection {
	socket: TcpStream,
	peer_addr: SocketAddr,
	read_buf: BytesMut,
	write_buf: BytesMut,
	engine: Arc<Engine>,
	cancel_token: CancellationToken,
}

impl Connection {
	pub fn new(
		socket: TcpStream,
		peer_addr: SocketAddr,
		engine: Arc<Engine>,
		cancel_token: CancellationToken,
	) -> Self {
		Self {
			socket,
			peer_addr,
			read_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
			write_buf: BytesMut::with_capacity(DEFAULT_BUFFER_SIZE),
			engine,
			cancel_token,
		}
	}

	pub async fn run(mut self) {
		self.engine.on_client_connected();
		trace!(peer = %self.peer_addr, "Client connected");

		let _guard = ConnectionGuard(Arc::clone(&self.engine), self.peer_addr);

		loop {
			tokio::select! {
				biased;

				_ = self.cancel_token.cancelled() => {
					debug!(peer = %self.peer_addr, "Shutdown signal received; draining in-flight requests");
					// Process any remaining bytes already buffered in read_buf
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

		// Final graceful flush before exiting task
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
						status = ?response.status,
						"Processed command"
					);

					response.encode(&mut self.write_buf);

					if self.write_buf.len() >= FLUSH_THRESHOLD_BYTES {
						self.flush_write_buffer().await?;
					}
				}
				Ok(None) => break, // Incomplete frame; wait for more data
				Err(err) => {
					error!(peer = %self.peer_addr, error = ?err, "Protocol framing violation");
					let status = match err {
						RequestDecodeError::KeyTooLarge(_) => StatusCode::KeyTooLarge,
						RequestDecodeError::ValueTooLarge(_) => StatusCode::ValueTooLarge,
						RequestDecodeError::InvalidUtf8 => StatusCode::BadRequest,
						RequestDecodeError::UnknownOpcode(_) => StatusCode::UnknownOpcode,
						_ => StatusCode::BadRequest,
					};

					let fatal_response = Response::new(0, status, ResponsePayload::Null);
					fatal_response.encode(&mut self.write_buf);
					let _ = self.flush_write_buffer().await;
					return Err(());
				}
			}
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