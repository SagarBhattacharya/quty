use quty::db::{Engine, EngineConfig};
use quty::net::Connection;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::time::{interval, Duration};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const BIND_ADDR: &str = "0.0.0.0:6379";
const NUM_SHARDS: usize = 64;
const MAX_MEMORY_BYTES: usize = 512 * 1024 * 1024; // 512 MB

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	// Initialize structured tracing with RUST_LOG env-filter (default: info)
	tracing_subscriber::registry()
		.with(
			EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| EnvFilter::new("info,server=debug")),
		)
		.with(tracing_subscriber::fmt::layer())
		.init();

	info!(
		shards = NUM_SHARDS,
		max_memory_mb = MAX_MEMORY_BYTES / (1024 * 1024),
		"Initializing Quty cache engine"
	);

	let config = EngineConfig {
		num_shards: NUM_SHARDS,
		max_memory_bytes: MAX_MEMORY_BYTES,
	};
	let engine = Engine::new(config);

	let cancel_token = CancellationToken::new();
	let tracker = TaskTracker::new();

	// 1. Background maintenance task
	let sweep_engine = Arc::clone(&engine);
	let sweep_cancel = cancel_token.clone();
	tracker.spawn(async move {
		let mut ticker = interval(Duration::from_millis(100));
		loop {
			tokio::select! {
				_ = sweep_cancel.cancelled() => {
					info!("Maintenance sweep task stopped");
					break;
				}
				_ = ticker.tick() => {
					let (expired, evicted) = sweep_engine.run_maintenance_sweep(0.80, 0.70);
					if expired > 0 || evicted > 0 {
						info!(expired = expired, evicted = evicted, "Maintenance sweep executed");
					}
				}
			}
		}
	});

	// 2. TCP Listener
	let listener = TcpListener::bind(BIND_ADDR).await?;
	info!(addr = BIND_ADDR, "Server listening for client connections");

	// 3. Supervised Accept Loop
	let accept_engine = Arc::clone(&engine);
	let accept_cancel = cancel_token.clone();
	let accept_tracker = tracker.clone();

	tokio::select! {
		res = run_accept_loop(listener, accept_engine, accept_tracker, accept_cancel.clone()) => {
			if let Err(e) = res {
				error!(error = %e, "Accept loop encountered fatal error");
			}
		}
		_ = signal::ctrl_c() => {
			info!("Received SIGINT (Ctrl+C). Initiating graceful shutdown...");
		}
	}

	// 4. Trigger cancellation across all components
	cancel_token.cancel();

	// 5. Close tracker and await in-flight task draining
	tracker.close();
	info!("Waiting for active client connections to drain...");

	tokio::select! {
		_ = tracker.wait() => {
			info!("All connections drained cleanly. Shutdown complete.");
		}
		_ = tokio::time::sleep(Duration::from_secs(3)) => {
			warn!("Shutdown timed out after 3 seconds. Forcing termination.");
		}
	}

	Ok(())
}

async fn run_accept_loop(
	listener: TcpListener,
	engine: Arc<Engine>,
	tracker: TaskTracker,
	cancel_token: CancellationToken,
) -> Result<(), Box<dyn Error>> {
	loop {
		tokio::select! {
			_ = cancel_token.cancelled() => break,
			accept_res = listener.accept() => {
				let (socket, peer_addr): (tokio::net::TcpStream, SocketAddr) = accept_res?;
				let conn_engine = Arc::clone(&engine);
				let conn_cancel = cancel_token.clone();

				tracker.spawn(async move {
					let connection = Connection::new(
						socket, peer_addr,
						conn_engine, conn_cancel
					);
					connection.run().await;
				});
			}
		}
	}
	Ok(())
}