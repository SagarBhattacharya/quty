//! Entry point for the `quty-server` daemon.

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::Parser;
use quty_server::{CliArgs, Server, ServerConfig};
use std::error::Error;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let args = CliArgs::parse();

	tracing_subscriber::registry()
		.with(
			EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| EnvFilter::new("info,quty_server=debug")),
		)
		.with(tracing_subscriber::fmt::layer())
		.init();

	let server_config =
		ServerConfig::from_args(&args).map_err(|err| format!("Configuration error: {err}"))?;

	let server = Server::new(server_config);
	server.run().await
}
