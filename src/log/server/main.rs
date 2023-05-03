use ddb::log::service::{LogServer, LogService};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let service = LogService::new()?;

    Server::builder()
        .add_service(LogServer::new(service))
        .serve(addr)
        .await?;

    return Ok(());
}
