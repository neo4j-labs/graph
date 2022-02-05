mod actions;
mod server;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = crate::server::FlightServiceImpl::new();
    let service = FlightServiceServer::new(service);
    Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}
