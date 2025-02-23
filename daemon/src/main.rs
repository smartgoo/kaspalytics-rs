mod cache;
mod listener;

use env_logger::{Builder, Env};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use log::{info, LevelFilter};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Init Logger
    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, LevelFilter::Info)
        .init();

    info!("Loading config...");
    let config = kaspalytics_utils::config::Config::from_env();

    info!("Initializing wRPC client...");
    let rpc_client = Arc::new(
        KaspaRpcClient::new(
            WrpcEncoding::Borsh,
            Some(&config.rpc_url),
            None,
            Some(config.network_id),
            None,
        )
        .unwrap(),
    );

    info!("Connecting wRPC client...");
    rpc_client.connect(None).await.unwrap();

    let cache = Arc::new(cache::Cache::default());

    let handle = tokio::spawn(async move {
        listener::DagListener::new(cache.clone(), rpc_client.clone())
            .run()
            .await;
    });

    handle.await.unwrap();
}
