use ddb::{
    error::Result,
    log::{rpc::log_server::LogServer, service::LogService},
};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let dir = std::path::Path::new("store/logs");
    let addr = "[::1]:50001".parse()?;
    let service = LogService::new(&dir)?;

    Server::builder()
        .add_service(LogServer::new(service))
        .serve(addr)
        .await?;

    return Ok(());
}
