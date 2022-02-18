#![feature(vec_into_raw_parts)]

//! An [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) server
//! implementation that allows clients to create and manage graphs in memory,
//! run algorithms on them and stream results back to the client.
//!
//! Clients communicate with the server via an Arrow Flight client, such as
//! [pyarrow](https://pypi.org/project/pyarrow/). Server commands, also called
//! Flight actions, are encoded via JSON. Currently supported commands include
//! creating graphs, relabeling graphs and computing algorithms, such as PageRank,
//! Triangle Count and SSSP. Algorithm results are streamed to the client via
//! the do_get command and nicely wrapped in Arrow record batches.
//!
//! Check the `examples` folder for scripts that demonstrate client-server interaction.

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
