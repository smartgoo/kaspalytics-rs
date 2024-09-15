mod args;
mod database;
mod kaspad;
mod service;
mod utils;

use args::Args;
use chrono::{Duration, Utc};
use clap::Parser;
use env_logger::{Builder, Env};
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use log::{info, LevelFilter};
use std::io;

fn prompt_confirmation(prompt: &str) -> bool {
    println!("{}", prompt);
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

#[tokio::main]
async fn main() {
    let config = crate::utils::config::Config::from_env();

    // Parse CLI args
    let args = Args::parse();

    // Init Logger
    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, LevelFilter::Info)
        .init();

    info!("Initializing application");

    // Init and connect RPC Client
    let rpc_client = KaspaRpcClient::new(
        WrpcEncoding::Borsh,
        config.rpc_url.as_deref(),
        None,
        Some(config.network_id),
        None,
    )
    .unwrap();
    rpc_client.connect(None).await.unwrap();

    // Ensure RPC node is synced, is same network/suffix as supplied CLI args, is utxoindexed
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

    // Optionally drop & recreate PG database based on CLI args
    if args.reset_db {
        if config.env == utils::config::Env::Prod {
            panic!("Cannot use --reset-db in production.")
        }

        let db_name = config
            .db_uri
            .split('/')
            .last()
            .expect("Invalid connection string");

        let prompt = format!(
            "DANGER!!! Are you sure you want to drop and recreate the PG database {}? (y/N)?",
            db_name
        );
        let reset_db = prompt_confirmation(prompt.as_str());
        if reset_db {
            // Connect without specifying PG database in order to drop and recreate
            let base_url = database::conn::parse_base_url(&config.db_uri);
            let mut conn = database::conn::open_connection(&base_url).await.unwrap();

            info!("Dropping PG database {}", db_name);
            database::conn::drop_db(&mut conn, db_name).await.unwrap();

            info!("Creating PG database {}", db_name);
            database::conn::create_db(&mut conn, db_name).await.unwrap();

            database::conn::close_connection(conn).await.unwrap();
        }
    }

    // Init PG database connection pool
    let db_pool = database::conn::open_connection_pool(&config.db_uri)
        .await
        .unwrap();

    // Apply PG database migrations and insert static records
    database::initialize::apply_migrations(&db_pool)
        .await
        .unwrap();
    database::initialize::insert_enums(&db_pool).await.unwrap();

    // Ensure DB NetworkId matches CLI
    let db_network_id = database::initialize::get_meta_network_id(&db_pool)
        .await
        .unwrap();
    if db_network_id.is_none() {
        // First time running with this PG database, save network
        database::initialize::insert_network_meta(&db_pool, config.network_id)
            .await
            .unwrap();
    } else {
        // PG database has been used in the past
        // Validate network/suffix saved in db matches network supplied via CLI
        if config.network_id != db_network_id.unwrap() {
            panic!("PG database network does not match network supplied via CLI")
        }
    }

    // Init Rusty Kaspa dir and rocksdb Consensus Storage object
    let kaspad_dirs = crate::kaspad::dirs::Dirs::new(config.app_dir.clone(), config.network_id);
    let storage = crate::kaspad::db::init_consensus_storage(
        config.network_id,
        kaspad_dirs.active_consensus_db_dir,
    );

    // Create Analysis process
    // Default analysis window to yesterday if no args are passed
    let (start_time, end_time) = match (args.start_time, args.end_time) {
        (None, None) => {
            let start_of_today = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
            let start_of_yesterday = start_of_today - Duration::days(1);
            let end_of_yesterday = start_of_today - Duration::milliseconds(1);
            (
                start_of_yesterday.and_utc().timestamp_millis() as u64,
                end_of_yesterday.and_utc().timestamp_millis() as u64,
            )
        }
        (Some(start), None) => (start, 0),
        (None, Some(end)) => (0, end),
        (Some(start), Some(end)) => (start, end),
    };

    let mut daily_analysis_process =
        service::analysis::Analysis::new_from_time_window(config, storage, start_time, end_time);

    daily_analysis_process.run(&db_pool).await;
}
