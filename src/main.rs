use mr_world_core::proto_type::world::oracle::oracle_server::OracleServer;

use crate::oracle::SimmonsOracle;

pub mod kafka_helper;
pub mod oracle;
pub mod analysis;

#[tokio::main]
async fn main() {
    env_logger::init();
    let addr = "0.0.0.0:10007".parse().unwrap();
    let mut oracle = SimmonsOracle::default();
    let svc = OracleServer::new(oracle);
    log::info!("Simmons server created and now building");
    match tonic::transport::Server::builder()
        .add_service(svc)
        .serve(addr)
        .await
    {
        Ok(_) => log::info!("gRPC shutdown gracefully!"),
        Err(e) => log::info!("gRPC failed ungracefully {:#?}", e),
    }
}
