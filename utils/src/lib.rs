pub mod coingecko;
pub mod config;
pub mod database;
pub mod dirs;
pub mod email;
pub mod granularity;
pub mod formatters;
pub mod kaspad;
pub mod log;
pub mod math;

use kaspa_rpc_core::api::rpc::RpcApi;
use kaspad::db::KASPAD_RDB_FD_ALLOCATION;
use std::sync::Arc;

pub const TARGET_FD_LIMIT: i32 = KASPAD_RDB_FD_ALLOCATION * 2;

pub async fn check_rpc_node_status(config: &config::Config, rpc_client: Arc<dyn RpcApi>) {
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
