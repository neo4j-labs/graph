#![feature(vec_into_raw_parts)]

mod actions;
mod catalog;
mod server;

use std::net::IpAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::Parser;
use log::info;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let CliOpts { host, port } = CliOpts::new();

    let addr = std::net::SocketAddr::new(host, port);
    let service = crate::server::FlightServiceImpl::new();
    let service = FlightServiceServer::new(service);
    info!("Starting server at {addr}");
    Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}

#[derive(Debug, Parser)]
#[clap(
    version,
    about = "Graph Arrow Server",
    arg_required_else_help = true,
    disable_help_subcommand = true,
    infer_long_args = true
)]
struct CliOpts {
    /// Host address
    #[clap(default_value = "::1", display_order = 0)]
    host: IpAddr,

    /// Port
    #[clap(default_value_t = 50051, display_order = 1)]
    port: u16,
}

impl CliOpts {
    fn new() -> Self {
        Self::parse()
    }
}
