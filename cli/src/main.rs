mod cli;
mod cmds;

use clap::Parser;
use cli::{Cli, Commands};
use cmds::{blocks::pipeline::BlockAnalysis, utxo::pipeline::UtxoBasedPipeline};
use env_logger::{Builder, Env};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::database;
use log::{debug, info};
use std::sync::Arc;
use std::time::Instant;

const REQUIRED_CLI_SOFT_FD_LIMIT: u64 = 10 * 1024;

#[tokio::main]
async fn main() {
    let config = kaspalytics_utils::config::Config::from_env();

    let cli = Cli::parse();

    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, cli.global_args.log_level)
        .init();

    // TODO probably should move this to only the CLI commands that require it (block and utxo pipelines)
    kaspa_utils::fd_budget::try_set_fd_limit(REQUIRED_CLI_SOFT_FD_LIMIT).unwrap_or_else(|_| {
        panic!(
            "kaspalytics-cli requires {} fd limit",
            REQUIRED_CLI_SOFT_FD_LIMIT
        )
    });

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

    // Ensure node is synced, is same network/suffix as supplied CLI args, is utxoindexed
    // WARNING:
    //  - Some commands reads direct from RocksDB
    //  - So this is an assumption that RPC node is same node we are reading DB of
    //  - TODO find better way to validate these via db as opposed to RPC
    kaspalytics_utils::check_rpc_node_status(&config, rpc_client.clone()).await;

    // Open PG connection pool
    let db = database::Database::new(config.db_uri.clone());
    let pg_pool = db
        .open_connection_pool(config.db_max_pool_size)
        .await
        .unwrap();

    // Insert static records to PG DB
    database::initialize::insert_enums(&pg_pool).await.unwrap();

    // Ensure DB NetworkId matches NetworkId from .env file
    let db_network_id = database::initialize::get_meta_network_id(&pg_pool)
        .await
        .unwrap();
    if db_network_id.is_none() {
        // First time running with this PG database, save network
        database::initialize::insert_network_meta(&pg_pool, config.network_id)
            .await
            .unwrap();
    } else {
        // PG database has been used in the past
        // Validate network/suffix saved in db matches NetworkId supplied via CLI
        if config.network_id != db_network_id.unwrap() {
            panic!("PG database network does not match network supplied via CLI")
        }
    }

    // Run submitted CLI command
    let start = Instant::now();
    info!("{:?} command starting...", cli.command);
    match cli.command {
        Commands::BlockPipeline {
            start_time: _,
            end_time: _,
        } => BlockAnalysis::run(config, pg_pool).await,
        Commands::CoinMarketHistory => {
            cmds::price::get_coin_market_history(config, pg_pool).await;
        }
        Commands::HomePageRefresh => {
            cmds::home_page::home_page_data_refresh(rpc_client, pg_pool).await;
        }
        Commands::SnapshotDaa => cmds::daa::snapshot_daa_timestamp(rpc_client, pg_pool).await,
        Commands::SnapshotHashRate => {
            cmds::hash_rate::snapshot_hash_rate(rpc_client, pg_pool).await;
        }
        Commands::UtxoPipeline => {
            UtxoBasedPipeline::new(config.clone(), rpc_client, pg_pool)
                .run()
                .await
        }
    }

    info!(
        "{:?} command finished in {:.2}s",
        cli.command,
        start.elapsed().as_secs_f32()
    );
}
