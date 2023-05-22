mod encoding;
pub mod rpc {
    tonic::include_proto!("cache");
}
pub mod kv;
