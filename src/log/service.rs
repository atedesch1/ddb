use super::logger::Logger;
use crate::error::Result;

use tonic::{Request, Response, Status};

use super::rpc::log_server::{Log};
use super::rpc::{LogEntries, LogEntry};
use std::sync::Mutex;

#[derive(Debug)]
pub struct LogService {
    logger: Mutex<Logger>,
}

impl LogService {
    pub fn new() -> Result<Self> {
        let dir = std::path::Path::new("store/logs");
        let logger = Mutex::new(Logger::new(&dir)?);
        return Ok(LogService { logger });
    }
}

#[tonic::async_trait]
impl Log for LogService {
    async fn log(&self, request: Request<LogEntry>) -> std::result::Result<Response<()>, Status> {
        println!("Got a request: {:?}", request);

        let req = request.into_inner();

        let mut logger = self.logger.lock().unwrap();

        logger.append(req.entry);
        logger.commit(1)?;

        return Ok(Response::new(()));
    }

    async fn retrieve_logs(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<LogEntries>, Status> {
        let mut logger = self.logger.lock().unwrap();
        let entries = logger.read()?;

        return Ok(Response::new(LogEntries { entries }));
    }
}
