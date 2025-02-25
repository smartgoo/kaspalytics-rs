mod cache;
mod listener;
mod saver;

use env_logger::{Builder, Env};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use log::info;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let config = kaspalytics_utils::config::Config::from_env();

    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, config.log_level)
        .init();

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

    kaspalytics_utils::check_rpc_node_status(&config, rpc_client.clone()).await;

    let cache = Arc::new(cache::Cache::default());

    let listener_cache = cache.clone();
    let listener_handle = tokio::spawn(async move {
        listener::DagListener::new(listener_cache, rpc_client.clone())
            .run()
            .await;
    });

    let saver_cache = cache.clone();
    let saver_handle = tokio::spawn(async move {
        saver::Saver::new(saver_cache).run().await;
    });

    let _ = tokio::join!(listener_handle, saver_handle);
}
