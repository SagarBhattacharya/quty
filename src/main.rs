#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod config;
mod db;
mod net;

use clap::Parser;
use quty::{ServerConfig, Server, CliArgs};
use std::error::Error;
use tracing_subscriber::{
	layer::SubscriberExt, 
	util::SubscriberInitExt, 
	EnvFilter
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let args = CliArgs::parse();

	tracing_subscriber::registry()
		.with(
			EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| EnvFilter::new("info,quty=debug")),
		)
		.with(tracing_subscriber::fmt::layer())
		.init();

	let server_config = ServerConfig::from_args(args)
		.map_err(|err| format!("Configuration error: {}", err))?;

	let server = Server::new(server_config);
	server.run().await
}