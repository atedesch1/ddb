use super::logger::Logger;
use crate::error::Result;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::rpc::log_server::Log;
use super::rpc::{LogEntries, LogEntry};

pub use super::rpc::log_server::LogServer;

#[derive(Debug)]
pub struct LogService {
    logger: Logger,
}

impl LogService {
    pub fn new() -> Result<Self> {
        let dir = std::path::Path::new("store/logs");
        let logger = Logger::new(&dir)?;
        return Ok(LogService { logger });
    }
}

#[tonic::async_trait]
impl Log for LogService {
    async fn log(&self, request: Request<LogEntry>) -> std::result::Result<Response<()>, Status> {
        println!("Got a request: {:?}", request);

        let req = request.into_inner();

        self.logger.append(req.entry)?;
        self.logger.commit(1)?;

        return Ok(Response::new(()));
    }

    async fn retrieve_logs(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<LogEntries>, Status> {
        let entries = self.logger.read()?;

        return Ok(Response::new(LogEntries { entries }));
    }

    type StreamLogsStream = ReceiverStream<std::result::Result<LogEntries, Status>>;

    async fn stream_logs(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<Self::StreamLogsStream>, Status> {
        todo!()
    }
}
