mod logger;
pub mod rpc {
    tonic::include_proto!("log");
}
pub mod service;
