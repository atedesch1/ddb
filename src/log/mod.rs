mod logger;
mod rpc {
    tonic::include_proto!("log");
}
pub mod service;
