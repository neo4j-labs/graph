#![feature(vec_into_raw_parts)]

mod actions;
mod catalog;
mod server;

use std::net::IpAddr;

use arrow_flight::flight_service_server::FlightServiceServer;
use clap::AppSettings::ArgRequiredElseHelp;
use clap::{
    AppSettings::{DeriveDisplayOrder, DisableHelpSubcommand, InferLongArgs},
    Parser,
};
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
    global_setting = ArgRequiredElseHelp,
    global_setting = DeriveDisplayOrder,
    global_setting = DisableHelpSubcommand,
    global_setting = InferLongArgs,
)]
struct CliOpts {
    /// Host address
    #[clap(default_value = "::1")]
    host: IpAddr,

    /// Port
    #[clap(default_value_t = 50051)]
    port: u16,
}

impl CliOpts {
    fn new() -> Self {
        Self::parse()
    }
}
