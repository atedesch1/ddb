fn main() -> Result<(), Box<dyn std::error::Error>> {
  tonic_build::compile_protos("./src/log/log.proto")?;
  tonic_build::compile_protos("./src/cache/cache.proto")?;
  Ok(())
}
