use tonic::transport::Channel;

use crate::{
    error::Result,
    log::rpc::{log_client::LogClient, LogEntry},
};
use std::collections::HashMap;

use super::encoding::KVOperation;

pub struct KVStore {
    store: HashMap<Vec<u8>, Vec<u8>>,
    log_client: LogClient<Channel>,
}

impl KVStore {
    pub async fn new() -> Result<Self> {
        let mut log_client = LogClient::connect("http://[::1]:50001").await?;
        let store = Self::init_from_db(&mut log_client).await?;
        return Ok(KVStore { store, log_client });
    }

    async fn init_from_db(client: &mut LogClient<Channel>) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let mut store: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut stream = client.stream_logs(()).await?.into_inner();
        while let Some(log_entry) = stream.message().await? {
            match KVOperation::decode(log_entry.entry)? {
                KVOperation::Delete(key) => store.remove(&key),
                KVOperation::Set(key, value) => store.insert(key.into(), value.into()),
                KVOperation::Get => None,
            };
        }
        Ok(store)
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        return self.store.get(key.into());
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        self.log_client
            .log(LogEntry {
                entry: KVOperation::set(key, value).encode(),
            })
            .await?;
        return Ok(self.store.insert(key.into(), value.into()));
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let None = self.get(key) {
            return Ok(None);
        }
        self.log_client
            .log(LogEntry {
                entry: KVOperation::delete(key).encode(),
            })
            .await?;
        return Ok(self.store.remove(key.into()));
    }

    pub fn list(&self) -> Vec<(&Vec<u8>, &Vec<u8>)> {
        return self.store.iter().collect();
    }
}
