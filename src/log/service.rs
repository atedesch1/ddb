use std::sync::Arc;

use super::logger::Logger;
use crate::error::Result;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::rpc::log_server::Log;
use super::rpc::{LogEntries, LogEntry};

pub use super::rpc::log_server::LogServer;

#[derive(Debug)]
pub struct LogService {
    logger: Arc<Logger>,
}

async fn commit_task(logger: Arc<Logger>) -> () {
    loop {
        let uncommitted = logger.uncommitted().unwrap();
        if uncommitted > 0 {
            let to_commit = match uncommitted {
                1..=5 => uncommitted,
                _ => 5,
            };

            logger.commit(to_commit).unwrap();
        }
    }
}

impl LogService {
    pub fn new() -> Result<Self> {
        let dir = std::path::Path::new("store/logs");
        let logger = Arc::new(Logger::new(&dir)?);

        let log_committer = logger.clone();
        tokio::spawn(commit_task(log_committer));

        return Ok(LogService {
            logger: logger.clone(),
        });
    }
}

#[tonic::async_trait]
impl Log for LogService {
    async fn log(&self, request: Request<LogEntry>) -> std::result::Result<Response<()>, Status> {
        println!("Got a request: {:?}", request);

        let req = request.into_inner();

        self.logger.append(req.entry)?;

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
