mod cli;
mod cmds;

use clap::Parser;
use cli::{Cli, Commands};
use cmds::{blocks::pipeline::BlockAnalysis, utxo::pipeline::UtxoBasedPipeline};
use env_logger::{Builder, Env};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::{database, TARGET_FD_LIMIT};
use log::{debug, info};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let config = kaspalytics_utils::config::Config::from_env();

    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, cli.global_args.log_level)
        .init();

    let (soft, hard) = rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap();
    debug!("fd limit before: soft = {}, hard = {}", soft, hard);

    if rlimit::increase_nofile_limit(TARGET_FD_LIMIT as u64).unwrap() < TARGET_FD_LIMIT as u64 {
        panic!(
            "{:?}",
            rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap().0
        );
    };

    debug!(
        "fd limit after: {}",
        rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap().0
    );

    // Open PG connection pool
    let db = database::Database::new(config.db_uri.clone());
    let pg_pool = db
        .open_connection_pool(config.db_max_pool_size)
        .await
        .unwrap();

    // Insert static records to PG DB
    database::initialize::insert_enums(&pg_pool).await.unwrap();

    // Ensure DB NetworkId matches NetworkId from .env file
    database::initialize::validate_db_network(&config, &pg_pool).await;

    // TODO probably should move to only cmds that use rpc client
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
