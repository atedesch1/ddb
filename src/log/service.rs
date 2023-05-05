use std::sync::Arc;

use super::logger::Logger;
use crate::error::Result;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::rpc::log_server::Log;
use super::rpc::{LogEntries, LogEntry};

pub use super::rpc::log_server::LogServer;

#[derive(Debug)]
pub struct LogService {
    logger: Arc<Logger>,
}

impl LogService {
    pub fn new() -> Result<Self> {
        let dir = std::path::Path::new("store/logs");
        let logger = Arc::new(Logger::new(&dir)?);

        let log_committer = logger.clone();
        tokio::spawn(Self::commit_task(log_committer));

        return Ok(LogService { logger });
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
        let entries = self.logger.read_all()?;

        return Ok(Response::new(LogEntries { entries }));
    }

    type StreamLogsStream = ReceiverStream<std::result::Result<LogEntry, Status>>;

    async fn stream_logs(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<Self::StreamLogsStream>, Status> {
        let (tx, rx) = mpsc::channel(5);

        let logger = self.logger.clone();
        tokio::spawn(async move {
            let mut idx = 0;
            while let Ok(entry) = logger.get(idx) {
                tx.send(Ok(LogEntry { entry })).await.unwrap();
                idx += 1;
            }
        });

        return Ok(Response::new(ReceiverStream::new(rx)));
    }
}
