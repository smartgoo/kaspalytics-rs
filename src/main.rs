mod args;
mod cache;
mod database;
mod kaspad;
mod service;

use args::Args;
use clap::Parser;
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use kaspa_wrpc_client::{KaspaRpcClient, Resolver, WrpcEncoding};
use kaspa_rpc_core::api::rpc::RpcApi;
use env_logger::{Builder, Env};
use log::{LevelFilter, info};
// use moka::future::Cache as MokaCache;
use std::io;


const META_DB: &str = "meta";
const CONSENSUS_DB: &str = "consensus";


fn prompt_confirmation(prompt: &str) -> bool {
    println!("{}", prompt);
    // println!("DANGER!!! Are you sure you want to drop and recreate the database? (y/N): ");
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    matches!(input.trim().to_lowercase().as_str(), "y" | "yes")
}

#[tokio::main]
async fn main() {
    info!("Initializing application");

    // Parse CLI args
    let args = Args::parse();
    let network = kaspad::network(args.kaspad_network.clone(), args.kaspad_network_suffix);

    // Logger
    let mut builder = Builder::from_env(Env::default().default_filter_or("info"));
    builder.filter(None, LevelFilter::Debug);
    builder.init();

    // Optionally drop database based on CLI args
    if args.reset_db {
        let db_name = args.db_url.split('/').last().expect("Invalid connection string");

        // Get user confirmation first
        let prompt = format!("DANGER!!! Are you sure you want to drop and recreate the database {}? (y/N)?", db_name);
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
    let db_pool = database::conn::open_connection_pool(&args.db_url).await.unwrap();

    // Apply PG DB migrations and insert static records
    database::initialize::apply_migrations(&db_pool).await.unwrap();
    database::initialize::insert_enums(&db_pool).await.unwrap();

    // Init RPC client
    // TODO use resolver and pool of kaspad's
    let resolver = Resolver::default();
    let network_id = NetworkId::new(NetworkType::Mainnet); // TODO expose as CLI arg
    // TODO build RPC client per CLI args
    // let rpc_client = KaspaRpcClient::new(WrpcEncoding::Borsh, Some(args.kaspad_rpc_url.as_str()), Some(resolver), Some(network_id), None).unwrap();
    let rpc_client = KaspaRpcClient::new(WrpcEncoding::Borsh, None, Some(resolver), Some(network_id), None).unwrap();
    rpc_client.connect(None).await.unwrap();

    // Ensure RPC node is synced and is same network/network suffix as supplied CLI args
    let server_info = rpc_client.get_server_info().await.unwrap();
    assert!(server_info.is_synced, "Kaspad node is not synced");
    assert_eq!(server_info.network_id.network_type, *network, "Kaspad RPC host is for different network/suffix than supplied");

    // TODO Validate PG DB meta network/suffix matches supplied CLI
    let db_network = database::initialize::get_meta_network(&db_pool).await.unwrap();
    let db_network_suffix = database::initialize::get_meta_network(&db_pool).await.unwrap();

    // Rusty Kaspa dirs
    let app_dir = kaspad::get_app_dir_from_args(&args);
    let db_dir = kaspad::get_db_dir(app_dir, network);
    let meta_db_dir = db_dir.join(META_DB);
    let current_meta_dir = kaspad::db::meta_db_dir(meta_db_dir);
    let consensus_db_dir = db_dir.join(CONSENSUS_DB).join(current_meta_dir);

    // If first time running, store pruning point UTXO set in PG DB
    // TODO run on tokio loop and start at same time as other services?
    if db_network.is_none() {
        // Store network
        database::initialize::store_network_meta(&db_pool, server_info.network_id).await.unwrap();
        // Insert pruning point utxo set to Postgres
        // So we can resolve all outpoints for transactions from PP up and do analysis on this data
        // kaspad::db::pp_utxo_set_to_pg(&db_pool, network, consensus_db_dir).await;
    }

    service::initial_sync::initial_sync(rpc_client.clone()).await;

    // TODO do I need to store UTXOStateOf <block hash> in Meta? And check if node has block hash?
        // If node has block hash, utxo set should be in sync with that.
        // If node has not have block hash, I think I'll have issues since UTXO set is for older data
        // I can probably just use checkpoint as last indexed thing?

}
