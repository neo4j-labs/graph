mod actions;
mod server;

use arrow_flight::flight_service_server::FlightServiceServer;
use log::info;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let addr = "[::1]:50051".parse()?;
    let service = crate::server::FlightServiceImpl::new();
    let service = FlightServiceServer::new(service);
    info!("Starting server at {addr}");
    Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}
