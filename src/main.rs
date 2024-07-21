mod args;
mod database;
mod kaspad;
mod service;

use args::Args;
use clap::Parser;
use env_logger::{Builder, Env};
use kaspa_consensus_core::network::NetworkId;
use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::{KaspaRpcClient, Resolver, WrpcEncoding};
use log::{info, LevelFilter};
use std::{io, sync::Arc};
use tokio::{
    sync::{mpsc, Mutex},
    task,
};

const META_DB: &str = "meta";
const CONSENSUS_DB: &str = "consensus";

fn prompt_confirmation(prompt: &str) -> bool {
    println!("{}", prompt);
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

#[tokio::main]
async fn main() {
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

    // Init RPC Client
    let resolver = Resolver::default();
    let encoding = args.rpc_encoding.unwrap_or(WrpcEncoding::Borsh);
    let rpc_client = KaspaRpcClient::new(
        encoding,
        args.rpc_url.as_deref(),
        Some(resolver),
        Some(network_id),
        None,
    )
    .unwrap();
    rpc_client.connect(None).await.unwrap();

    // Init Rusty Kaspa dirs
    let app_dir = kaspad::get_app_dir_from_args(&args);
    let db_dir = kaspad::get_db_dir(app_dir, network_id);
    let meta_db_dir = db_dir.join(META_DB);
    let current_meta_dir = kaspad::db::meta_db_dir(meta_db_dir);
    let consensus_db_dir = db_dir.join(CONSENSUS_DB).join(current_meta_dir);

    // Optionally drop database based on CLI args
    if args.reset_db {
        let db_name = args
            .db_url
            .split('/')
            .last()
            .expect("Invalid connection string");

        let prompt = format!(
            "DANGER!!! Are you sure you want to drop and recreate the database {}? (y/N)?",
            db_name
        );
        let reset_db = prompt_confirmation(prompt.as_str());
        if reset_db {
            // Connect without specifying database in order to drop and recreate
            let base_url = database::conn::parse_base_url(&args.db_url);
            let mut conn = database::conn::open_connection(&base_url).await.unwrap();

            info!("Dropping database {}", db_name);
            database::conn::drop_db(&mut conn, db_name).await.unwrap();

            info!("Creating database {}", db_name);
            database::conn::create_db(&mut conn, db_name).await.unwrap();

            database::conn::close_connection(conn).await.unwrap();
        }
    }

    // Init PG DB connection pool
    let db_pool = database::conn::open_connection_pool(&args.db_url)
        .await
        .unwrap();

    // Apply PG DB migrations and insert static records
    database::initialize::apply_migrations(&db_pool)
        .await
        .unwrap();
    database::initialize::insert_enums(&db_pool).await.unwrap();

    // Ensure RPC node is synced and is same network/suffix as supplied CLI args
    let server_info = rpc_client.get_server_info().await.unwrap();
    assert!(server_info.is_synced, "Kaspad node is not synced");
    if !server_info.is_synced {
        panic!("Kaspad node is not synced")
    }
    if server_info.network_id.network_type != *network_id {
        panic!("Kaspad RPC host network does not match network supplied via CLI")
    }

    // Get NetworkId of PG DB instance
    let db_network_id = database::initialize::get_meta_network_id(&db_pool)
        .await
        .unwrap();
    if db_network_id.is_none() {
        // First time running with this database
        // TODO run on tokio loop and start at same time as other services?
        // Store network
        database::initialize::insert_network_meta(&db_pool, server_info.network_id)
            .await
            .unwrap();

        // Insert pruning point utxo set to Postgres
        // So we can resolve all outpoints for transactions from PP up and do analysis on this data
        kaspad::db::pp_utxo_set_to_pg(&db_pool, network_id, consensus_db_dir).await;
    } else {
        // Database has been used in the past
        // Validate database meta network/suffix matches network supplied via CLI
        if network_id != db_network_id.unwrap() {
            panic!("Database network does not match network supplied via CLI")
        }
    }

    // TODO get last block hash checkpoint from DB. Temporarily hardcoded to pruning point hash
    let GetBlockDagInfoResponse {
        pruning_point_hash, ..
    } = rpc_client.get_block_dag_info().await.unwrap();

    let cache = Arc::new(Mutex::new(service::cache::DAGCache::new()));
    let rpc_client = Arc::new(rpc_client);

    let (tx, rx) = mpsc::channel::<service::Event>(32);

    let blocks_processor =
        service::blocks::BlocksProcess::new(rpc_client.clone(), cache.clone(), tx);
    let blocks_handle = task::spawn(async move {
        blocks_processor.run(pruning_point_hash).await;
    });

    let mut vspc_processor =
        service::vspc::VirtualChainProcess::new(rpc_client.clone(), cache.clone(), rx);
    let vspc_handle = task::spawn(async move {
        vspc_processor.run(pruning_point_hash).await;
    });

    let _ = tokio::join!(blocks_handle, vspc_handle);

    // TODO need to store UTXOStateOf <block hash> in Meta? And check if node has block hash?
    // If node has block hash, utxo set should be in sync with that.
    // If node has not have block hash, I think it'll have issues since UTXO set is for older data
    // I can probably just use checkpoint as last indexed thing?
}
