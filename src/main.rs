mod args;
mod database;
mod kaspad;
mod service;
mod utils;

use args::Args;
use clap::Parser;
use env_logger::{Builder, Env};
use kaspa_consensus_core::network::NetworkId;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_wrpc_client::{KaspaRpcClient, Resolver, WrpcEncoding};
use log::{info, LevelFilter};
use std::{io, path::PathBuf, sync::Arc};

fn prompt_confirmation(prompt: &str) -> bool {
    println!("{}", prompt);
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

#[tokio::main]
async fn main() {
    // Load .env
    dotenvy::dotenv().unwrap();

    // Parse CLI args
    let args = Args::parse();

    // Init Logger
    Builder::from_env(Env::default().default_filter_or("info"))
        .filter(None, LevelFilter::Info)
        .init();

    info!("Initializing application");

    // Get NetworkId based on CLI args
    let network_id = NetworkId::try_new(args.network)
        .unwrap_or_else(|_| NetworkId::with_suffix(args.network, args.netsuffix.unwrap()));

    // Init and connect RPC Client
    let resolver = match &args.rpc_url {
        Some(url) => Resolver::new(vec![Arc::new(url.clone())]),
        None => Resolver::default(),
    };
    let encoding = args.rpc_encoding.unwrap_or(WrpcEncoding::Borsh);
    let rpc_client = Arc::new(
        KaspaRpcClient::new(
            encoding,
            args.rpc_url.as_deref(),
            Some(resolver),
            Some(network_id),
            None,
        )
        .unwrap(),
    );
    rpc_client.connect(None).await.unwrap();

    // Optionally drop & recreate PG database based on CLI args
    if args.reset_db {
        let db_name = args
            .db_url
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
            let base_url = database::conn::parse_base_url(&args.db_url);
            let mut conn = database::conn::open_connection(&base_url).await.unwrap();

            info!("Dropping PG database {}", db_name);
            database::conn::drop_db(&mut conn, db_name).await.unwrap();

            info!("Creating PG database {}", db_name);
            database::conn::create_db(&mut conn, db_name).await.unwrap();

            database::conn::close_connection(conn).await.unwrap();
        }
    }

    // Init PG database connection pool
    let db_pool = database::conn::open_connection_pool(&args.db_url)
        .await
        .unwrap();

    // Apply PG database migrations and insert static records
    database::initialize::apply_migrations(&db_pool)
        .await
        .unwrap();
    database::initialize::insert_enums(&db_pool).await.unwrap();

    // Ensure RPC node is synced, is same network/suffix as supplied CLI args, is utxoindexed
    let server_info = rpc_client.get_server_info().await.unwrap();
    assert!(server_info.is_synced, "Kaspad node is not synced");
    if !server_info.is_synced {
        panic!("RPC node is not synced")
    }
    if !server_info.has_utxo_index {
        panic!("RPC node does is not utxo-indexed")
    }
    if server_info.network_id.network_type != *network_id {
        panic!("RPC host network does not match network supplied via CLI")
    }

    // Get NetworkId from PG database
    let db_network_id = database::initialize::get_meta_network_id(&db_pool)
        .await
        .unwrap();
    if db_network_id.is_none() {
        // First time running with this PG database, save network
        database::initialize::insert_network_meta(&db_pool, network_id)
            .await
            .unwrap();
    } else {
        // PG database has been used in the past
        // Validate network/suffix saved in db matches network supplied via CLI
        if network_id != db_network_id.unwrap() {
            panic!("PG database network does not match network supplied via CLI")
        }
    }

    // Init Rusty Kaspa dir and rocksdb Consensus Storage object
    let kaspad_dirs =
        crate::kaspad::dirs::Dirs::new(args.kaspad_app_dir.map(PathBuf::from), network_id);
    let storage =
        crate::kaspad::db::init_consensus_storage(network_id, kaspad_dirs.active_consensus_db_dir);

    // Run Analysis process
    let mut daily_analysis_process =
        service::analysis::Analysis::new_from_time_window(storage, network_id);

    daily_analysis_process.run(&db_pool).await;
}
