mod args;
mod database;
mod kaspad;
mod service;
mod utils;

use args::Args;
use chrono::Utc;
use clap::Parser;
use env_logger::{Builder, Env};
use kaspa_database::prelude::StoreError;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use log::{error, info, LevelFilter};
use std::io;
use tokio::time::{sleep, Duration};

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

    info!("Initializing application...");

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
    // WARNING: This app reads direct from RocksDB, so we are making the assumption
    // that the RPC node is same node as node we are reading DB from
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

    let db = database::Database::new(config.db_uri.clone());

    // Optionally drop & recreate PG database based on CLI args
    if args.reset_db {
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

    // Init PG database connection pool
    let db_pool = db.open_connection_pool(5u32).await.unwrap();

    // Apply PG database migrations and insert static records
    database::initialize::apply_migrations(&db_pool)
        .await
        .unwrap();
    database::initialize::insert_enums(&db_pool).await.unwrap();

    // Ensure DB NetworkId matches NetworkId from CLI args
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
        // Validate network/suffix saved in db matches NetworkId supplied via CLI
        if config.network_id != db_network_id.unwrap() {
            panic!("PG database network does not match network supplied via CLI")
        }
    }

    // Init Rusty Kaspa dir and rocksdb Consensus Storage object
    let kaspad_dirs = crate::kaspad::dirs::Dirs::new(config.app_dir.clone(), config.network_id);

    // Create Analysis process
    // Default analysis window to yesterday if no args are passed
    let (start_time, end_time) = match (args.start_time, args.end_time) {
        (None, None) => {
            let start_of_today = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
            let start_of_yesterday = start_of_today - chrono::Duration::days(1);
            let end_of_yesterday = start_of_today - chrono::Duration::milliseconds(1);
            (
                start_of_yesterday.and_utc().timestamp_millis() as u64,
                end_of_yesterday.and_utc().timestamp_millis() as u64,
            )
        }
        (Some(start), None) => (start, 0),
        (None, Some(end)) => (0, end),
        (Some(start), Some(end)) => (start, end),
    };

    // Sporadically (once a week-ish) a RocksDB error will be raised:
    // "Error rocksdb error IO error: No such file or directory: While open a file for random read: rusty-kaspa/kaspa-mainnet/datadir/consensus/consensus-002/1504776.sst: No such file or directory while getting block cb0c56da0c4c7948c5bf29c0f8eddbde11fc02df7641a2f27053c702bb96aef5 from database"
    // I have a hunch that is because this program is running while node pruning is in progress
    // And that during/after pruning, RocksDB is performing compaction
    // The read_only connection is supposed to create a snapshot (I think?) and prevent this (again, or so I think)...
    // The below loop is an attempt to catch this error and retry every X minutes for up to X retry attempts
    // Let's see how this goes...
    let mut retries = 0;
    let max_retries = 24; // Retry up to this many times
    let retry_delay = Duration::from_secs(5 * 60); // Retry every X time period
    loop {
        let storage = crate::kaspad::db::init_consensus_storage(
            config.network_id,
            kaspad_dirs.active_consensus_db_dir.clone(),
        );

        let mut daily_analysis_process = service::analysis::Analysis::new_from_time_window(
            config.clone(),
            storage.clone(),
            start_time,
            end_time,
        );

        match daily_analysis_process.run(&db_pool).await {
            Ok(_) => break,
            Err(StoreError::DbError(_)) if retries < max_retries => {
                // Close database connection before sleeping
                // Inside retries window. Sleep and try again
                drop(daily_analysis_process);
                drop(storage);

                retries += 1;
                error!(
                    "Database error on tx_analysis attempt {}/{}. Retrying in 5 minutes...",
                    retries, max_retries
                );
                sleep(retry_delay).await;
            }
            Err(StoreError::DbError(_)) => {
                // After max retries, send alert email and exit
                error!(
                    "Analysis::tx_analysis failed after {} attempts. Exiting...",
                    retries
                );
                crate::utils::email::send_email(
                    &config,
                    format!("{} | kaspalytics-rs alert", config.env),
                    "Analysis::tx_analysis reached max retries due to database error.".to_string(),
                );
                break;
            }
            Err(e) => {
                // Handle other errors and exit
                error!("Analysis::tx_analysis failed with error: {:?}", e);
                crate::utils::email::send_email(
                    &config,
                    format!("{} | kaspalytics-rs alert", config.env),
                    format!("Analysis::tx_analysis failed with error: {:?}", e),
                );
                break;
            }
        }
    }
}
