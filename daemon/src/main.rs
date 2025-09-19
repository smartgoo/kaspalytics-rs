mod analysis;
mod collector;
mod ingest;
mod storage;
mod web;
mod writer;

use ingest::cache::{DagCache, Manager, Reader};
use kaspa_rpc_core::{api::rpc::RpcApi, RpcError};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::config::{Config, Env as KaspalyticsEnv};
use kaspalytics_utils::email::send_email;
use kaspalytics_utils::log::{init_logger, LogTarget};
use kaspalytics_utils::{check_rpc_node_status, database};
use log::{debug, error, info};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use storage::cache::Cache;
use storage::Storage;

struct AppContext {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    pg_pool: PgPool,
    rpc_client: Arc<KaspaRpcClient>,
    dag_cache: Arc<DagCache>,
    storage: Arc<Storage>,
}

#[tokio::main]
async fn main() {
    let config = Config::from_env();

    init_logger(&config, "daemon").unwrap();

    info!(target: LogTarget::Daemon.as_str(), "kaspalyticsd starting...");

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

    debug!(target: LogTarget::Daemon.as_str(), "Connecting wRPC client...");
    rpc_client.connect(None).await.unwrap();

    check_rpc_node_status(&config, rpc_client.clone()).await;

    let dag_cache = match DagCache::load_cache_state(config.clone()).await {
        Ok(dag_cache) => {
            let last_known_chain_block = dag_cache.last_known_chain_block().unwrap();
            match rpc_client.get_block(last_known_chain_block, false).await {
                Ok(_) => {
                    info!(
                        target: LogTarget::Daemon.as_str(),
                        "DagCache last_known_chain_block {} still held by Kaspa node",
                        last_known_chain_block,
                    );
                    Arc::new(dag_cache)
                }
                Err(RpcError::RpcSubsystem(_)) => {
                    info!(
                        target: LogTarget::Daemon.as_str(),
                        "DagCache last_known_chain_block {} no longer held by Kaspa node. Resetting cache",
                        last_known_chain_block
                    );
                    Arc::new(DagCache::default())
                }
                Err(err) => {
                    panic!("Unhandled RPC error during cache initialization: {}", err);
                }
            }
        }
        Err(e) => {
            error!(target: LogTarget::Daemon.as_str(), "Error loading DagCache from disk: {}", e);
            Arc::new(DagCache::default())
        }
    };

    let context = Arc::new(AppContext {
        config: config.clone(),
        shutdown_flag: Arc::new(AtomicBool::new(false)),
        pg_pool: pg_pool.clone(),
        rpc_client,
        dag_cache,
        storage: Arc::new(Storage::new(Arc::new(Cache::default()), pg_pool)),
    });

    let (writer_tx, writer_rx) = tokio::sync::mpsc::channel(100);

    // Ingest task
    let ingest = ingest::DagIngest::new(
        config.clone(),
        context.shutdown_flag.clone(),
        writer_tx,
        context.rpc_client.clone(),
        context.dag_cache.clone(),
    );

    // Writer task
    let mut writer = writer::Writer::new(context.pg_pool.clone(), writer_rx);

    // Collector task
    let collector = collector::Collector::new(context.clone());

    // Web Server task
    let web_server = web::WebServer::new(context.clone());

    // Handle interrupt
    let interrupt_shutdown_flag = context.shutdown_flag.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!(target: LogTarget::Daemon.as_str(), "Received interrupt, shutting down...");
        interrupt_shutdown_flag.store(true, Ordering::Relaxed);
    });

    let run_result = tokio::try_join!(
        tokio::spawn(async move {
            ingest.run().await;
        }),
        tokio::spawn(async move {
            writer.run().await;
        }),
        tokio::spawn(async move {
            collector.run().await;
        }),
        tokio::spawn(async move {
            web_server.run().await;
        }),
    );

    match run_result {
        Ok(_) => {
            info!(target: LogTarget::Daemon.as_str(), "Shutdown complete");
        }
        Err(e) => {
            context.shutdown_flag.store(true, Ordering::Relaxed);

            error!(target: LogTarget::Daemon.as_str(), "{}", e);

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
