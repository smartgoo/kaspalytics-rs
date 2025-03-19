mod analyzer;
mod cache;
mod listener;

use cache::Cache;
use env_logger::{Builder, Env};
use kaspa_rpc_core::{api::rpc::RpcApi, RpcError};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::config::{Config, Env as KaspalyticsEnv};
use kaspalytics_utils::email::send_email;
use kaspalytics_utils::{check_rpc_node_status, database};
use log::{debug, error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let config = Config::from_env();

    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, config.log_level)
        .init();

    info!("kaspalyticsd starting...");

    let db = database::Database::new(config.db_uri.clone());
    let pg_pool = db
        .open_connection_pool(config.db_max_pool_size)
        .await
        .unwrap();

    // Insert static records to PG DB
    database::initialize::insert_enums(&pg_pool).await.unwrap();

    // Ensure DB NetworkId matches NetworkId from .env file
    database::initialize::validate_db_network(&config, &pg_pool).await;

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

    debug!("Connecting wRPC client...");
    rpc_client.connect(None).await.unwrap();

    check_rpc_node_status(&config, rpc_client.clone()).await;

    let cache = match Cache::load_cache_state(config.clone()).await {
        Ok(cache) => {
            let last_known_chain_block = cache.last_known_chain_block().unwrap();
            match rpc_client.get_block(last_known_chain_block, false).await {
                Ok(_) => {
                    info!(
                        "Cache last_known_chain_block {} still held by Kaspa node",
                        last_known_chain_block,
                    );
                    Arc::new(cache)
                }
                Err(RpcError::RpcSubsystem(_)) => {
                    info!(
                        "Cache last_known_chain_block {} no longer held by Kaspa node. Resetting cache",
                        last_known_chain_block
                    );
                    Arc::new(Cache::default())
                }
                Err(err) => {
                    panic!("Unhandled RPC error during cache initialization: {}", err);
                }
            }
        }
        Err(_) => Arc::new(Cache::default()),
    };

    let shutdown_indicator = Arc::new(AtomicBool::new(false));

    // Listener task
    let listener_cache = cache.clone();
    let mut listener = listener::DagListener::new(
        config.clone(),
        listener_cache,
        rpc_client.clone(),
        shutdown_indicator.clone(),
    );
    let listener_handle = tokio::spawn(async move {
        listener.run().await;
    });

    // Analyzer task
    let analyzer_cache = cache.clone();
    // let listener_shutdown_rx = shutdown_tx.subscribe();
    let analyzer = analyzer::Analyzer::new(analyzer_cache, pg_pool, shutdown_indicator.clone());
    let analyzer_handle = tokio::spawn(async move {
        analyzer.run().await;
    });

    // Handle interrupt
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Received interrupt, shutting down...");
        shutdown_indicator.store(true, Ordering::Relaxed);
    });

    match tokio::try_join!(listener_handle, analyzer_handle) {
        Ok(_) => {
            info!("Shutdown complete");
        }
        Err(e) => {
            error!("{}", e);

            if config.env == KaspalyticsEnv::Prod {
                let _ = send_email(
                    &config,
                    "kaspalyticsd failed!".to_string(),
                    format!("{}", e),
                );
            }
        }
    }
}
