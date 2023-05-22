use tonic::{transport::Channel, Request, Response, Status};

use crate::{
    error::{Error, Result},
    log::rpc::{log_client::LogClient, LogEntry},
};
use std::collections::HashMap;
use tokio::sync::{Mutex, RwLock};

use super::{
    encoding::KVOperation,
    rpc::{cache_server::Cache, CacheComparison, CacheState, Operation, OperationType},
};

pub struct Store {
    store: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    log_client: LogClient<Channel>,
}

impl Store {
    pub async fn new() -> Result<Self> {
        let mut log_client = Self::try_connect_db().await?;
        let store = RwLock::new(Self::init_from_db(&mut log_client).await?);
        return Ok(Store { store, log_client });
    }

    async fn try_connect_db() -> Result<LogClient<Channel>> {
        for attempt in 1..=5 {
            match LogClient::connect("http://[::1]:50001").await {
                Ok(client) => {
                    return Ok(client);
                }
                Err(_) => {
                    println!("Log client: connection to log storage attempt {} failed. Retrying in 5 seconds...", attempt);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
        return Err(Error::Internal(
            "Log client: connection to log store failed".into(),
        ));
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
        return Ok(store);
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let store = self.store.read().await;
        let value = (*store).get(key.into()).cloned();
        return Ok(value);
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>> {
        self.log_client
            .log(LogEntry {
                entry: KVOperation::set(key, value).encode(),
            })
            .await?;
        let mut store = self.store.write().await;
        return Ok((*store).insert(key.into(), value.into()));
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let None = self.get(key).await? {
            return Ok(None);
        }
        self.log_client
            .log(LogEntry {
                entry: KVOperation::delete(key).encode(),
            })
            .await?;
        let mut store = self.store.write().await;
        return Ok((*store).remove(key.into()));
    }

    pub async fn list(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let store = self.store.read().await;
        return (*store)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
    }
}

//Master:
//Create server, wait for incoming StreamOperatons RPC

//Slave:
//Create client, try connecting to master server, call streamoperations
enum NodeType {
    Master,
    Slave,
}

pub struct Node {
    //ty: Mutex<NodeType>,
    store: Mutex<Store>,
}

impl Node {
    fn new(ty: NodeType) -> Result<()> {
        if let NodeType::Master = ty {}
        return Ok(());
    }
}

#[tonic::async_trait]
impl Cache for Node {
   //async fn execute_operation(
   //    &self,
   //    request: Request<Operation>,
   //) -> std::result::Result<Response<()>, Status> {
   //    let req = request.into_inner();

   //    let mut store = self.store.lock().await;

   //    match req.ty() {
   //        OperationType::Get => {}
   //        OperationType::Set => {
   //            store.set(&req.key[..], &req.value.unwrap()[..]).await?;
   //        }
   //        OperationType::Delete => {
   //            store.delete(&(req.key)[..]).await?;
   //        }
   //    }

   //    return Ok(Response::new(()));
   //}
    async fn compare_state(
        &self,
        request: Request<CacheState>,
    ) -> std::result::Result<Response<CacheComparison>, Status> {
        todo!()
    }
}
