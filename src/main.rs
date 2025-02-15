mod cli;
mod cmds;
mod database;
mod kaspad;
mod utils;

use clap::Parser;
use cli::{Cli, Commands};
use cmds::{blocks::pipeline::BlockAnalysis, utxo::pipeline::UtxoBasedPipeline};
use env_logger::{Builder, Env};
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use log::info;
use std::io;
use std::sync::Arc;
use utils::config::Config;

fn prompt_confirmation(prompt: &str) -> bool {
    println!("{}", prompt);
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

async fn check_rpc_node_status(config: &Config, rpc_client: Arc<KaspaRpcClient>) {
    rpc_client.connect(None).await.unwrap();

    let server_info = rpc_client.get_server_info().await.unwrap();

    if !server_info.is_synced {
        panic!("RPC node is not synced")
    }

    if !server_info.has_utxo_index {
        panic!("RPC node does is not utxo-indexed")
    }

    if server_info.network_id.network_type != *config.network_id {
        panic!("RPC host network does not match network supplied via CLI")
    }
}

#[tokio::main]
async fn main() {
    // Load config from .env file
    let config = crate::utils::config::Config::from_env();

    // Parse CLI command and args
    let cli = Cli::parse();

    // Init Logger
    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, cli.global_args.log_level)
        .init();

    info!("Initializing application...");

    // Ensure node is synced, is same network/suffix as supplied CLI args, is utxoindexed
    // This check is done via RPC
    // WARNING:
    //  - This app reads direct from RocksDB.
    //  - So this is an assumption that the RPC node is same node we are reading DB of
    //  - TODO find better way to validate these via db as opposed to RPC
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
    check_rpc_node_status(&config, rpc_client.clone()).await;
    info!("wRPC connected");

    // Get PG connection pool
    let db = database::Database::new(config.db_uri.clone());
    let pg_pool = db.open_connection_pool(5u32).await.unwrap();

    // Apply PG migrations and insert static records
    database::initialize::apply_migrations(&pg_pool)
        .await
        .unwrap();
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
    info!("Running command...");
    match cli.command {
        Commands::BlockPipeline {
            start_time: _,
            end_time: _,
        } => BlockAnalysis::run(config, &pg_pool).await,
        Commands::ResetDb => {
            if config.env == utils::config::Env::Prod {
                panic!("Cannot use --reset-db in production.")
            }

            let prompt = format!(
                "DANGER!!! Are you sure you want to drop and recreate the PG database {}? (y/N)?",
                db.database_name
            );

            if prompt_confirmation(prompt.as_str()) {
                db.drop_and_create_database().await.unwrap();
            }
        }
        Commands::SnapshotDaa => {
            crate::cmds::daa::snapshot_daa_timestamp(rpc_client.clone(), &pg_pool).await
        }
        Commands::UtxoPipeline => {
            UtxoBasedPipeline::new(config.clone(), rpc_client.clone(), pg_pool.clone())
                .run()
                .await
        }
    }
}
