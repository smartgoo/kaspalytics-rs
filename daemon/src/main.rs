mod analyzer;
mod cache;
mod dag;

use env_logger::{Builder, Env};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::database;
use log::{debug, error, info};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let config = kaspalytics_utils::config::Config::from_env();

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

    kaspalytics_utils::check_rpc_node_status(&config, rpc_client.clone()).await;

    let cache = Arc::new(cache::Cache::default());

    let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

    // Listener task
    let listener_cache = cache.clone();
    let listener_shutdown_rx = shutdown_tx.subscribe();
    let mut listener = dag::DagListener::new(listener_cache, rpc_client.clone(), pg_pool.clone());
    let listener_handle = tokio::spawn(async move {
        listener.run(listener_shutdown_rx).await;
    });

    // Analyzer task
    let analyzer_cache = cache.clone();
    let listener_shutdown_rx = shutdown_tx.subscribe();
    let analyzer = analyzer::Analyzer::new(analyzer_cache, pg_pool);
    let analyzer_handle = tokio::spawn(async move {
        analyzer.run(listener_shutdown_rx).await;
    });

    // Handle interrupt
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Received Ctrl + C, shutting down...");
        shutdown_tx.send(()).unwrap();
    });

    match tokio::try_join!(listener_handle, analyzer_handle) {
        Ok(_) => {
            info!("Shutdown complete");
        }
        Err(e) => {
            error!("{}", e);

            if config.env == kaspalytics_utils::config::Env::Prod {
                kaspalytics_utils::email::send_email(
                    &config,
                    "kaspalyticsd failed!".to_string(),
                    format!("{}", e),
                );
            }
        }
    }
}
