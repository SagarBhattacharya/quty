mod handler;
mod connection;

use crate::config::ServerConfig;
use crate::db::Engine;
use connection::Connection;
use std::error::Error;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

pub struct Server {
	config: ServerConfig,
	engine: Arc<Engine>,
	cancel_token: CancellationToken,
	tracker: TaskTracker,
}

impl Server {
	pub fn new(config: ServerConfig) -> Self {
		let engine = Engine::new(config.engine.clone());
		Self {
			config,
			engine,
			cancel_token: CancellationToken::new(),
			tracker: TaskTracker::new(),
		}
	}

	pub async fn run(self) -> Result<(), Box<dyn Error>> {
		info!(
			bind = %self.config.bind_addr,
			shards = self.config.engine.num_shards,
			max_memory_mb = self.config.engine.max_memory_bytes / (1024 * 1024),
			"Booting Quty cache server"
		);

		// 1. Background maintenance sweep task
		let sweep_engine = Arc::clone(&self.engine);
		let sweep_cancel = self.cancel_token.clone();
		let sweep_ms = self.config.sweep_interval_ms;
		let high_wm = self.config.high_watermark;
		let low_wm = self.config.low_watermark;

		self.tracker.spawn(async move {
			let mut ticker = interval(Duration::from_millis(sweep_ms));
			loop {
				tokio::select! {
					_ = sweep_cancel.cancelled() => {
						info!("Maintenance sweep worker shut down");
						break;
					}
					_ = ticker.tick() => {
						let (expired, evicted) = sweep_engine.run_maintenance_sweep(high_wm, low_wm);
						if expired > 0 || evicted > 0 {
							info!(expired = expired, evicted = evicted, "Maintenance sweep executed");
						}
					}
				}
			}
		});

		// 2. Bind TCP listener
		let listener = TcpListener::bind(self.config.bind_addr).await?;
		info!(addr = %self.config.bind_addr, "TCP listener active; accepting connections");

		// 3. Supervised accept loop
		let accept_engine = Arc::clone(&self.engine);
		let accept_cancel = self.cancel_token.clone();
		let accept_tracker = self.tracker.clone();
		let flush_threshold = self.config.flush_threshold_bytes;

		tokio::select! {
			res = Self::accept_loop(
				listener, accept_engine, 
				accept_tracker, accept_cancel.clone(), 
				flush_threshold
			) => {
				if let Err(e) = res {
					error!(error = %e, "Fatal accept loop crash");
				}
			}
			_ = signal::ctrl_c() => {
				info!("SIGINT received. Initiating graceful shutdown...");
			}
		}

		// 4. Drain connections cleanly
		self.shutdown().await;
		Ok(())
	}

	async fn accept_loop(
		listener: TcpListener,
		engine: Arc<Engine>,
		tracker: TaskTracker,
		cancel_token: CancellationToken,
		flush_threshold: usize,
	) -> Result<(), Box<dyn Error>> {
		loop {
			tokio::select! {
				_ = cancel_token.cancelled() => break,
				res = listener.accept() => {
					let (socket, peer_addr) = res?;
					let conn_engine = Arc::clone(&engine);
					let conn_cancel = cancel_token.clone();

					tracker.spawn(async move {
						let conn = Connection::new(
							socket, peer_addr, 
							conn_engine, conn_cancel, 
							flush_threshold
						);
						conn.run().await;
					});
				}
			}
		}
		Ok(())
	}

	async fn shutdown(self) {
		self.cancel_token.cancel();
		self.tracker.close();

		let timeout_duration = Duration::from_secs(self.config.shutdown_timeout_secs);
		info!(timeout_secs = self.config.shutdown_timeout_secs, "Draining active connections...");

		tokio::select! {
			_ = self.tracker.wait() => {
				info!("All in-flight tasks drained cleanly. Shutdown complete.");
			}
			_ = tokio::time::sleep(timeout_duration) => {
				warn!("Graceful shutdown timed out; forcing process termination.");
			}
		}
	}
}