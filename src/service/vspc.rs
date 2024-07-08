use kaspa_rpc_core::message::GetBlockDagInfoResponse;
use kaspa_rpc_core::{GetBlocksRequest, RpcTransactionInputVerboseData, RpcTransactionVerboseData};
use kaspa_rpc_core::{api::rpc::RpcApi, RpcBlock, RpcHash, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use moka::future::Cache;
use std::time::Duration;
use std::thread;

pub async fn vspc_collector(rpc_client: KaspaRpcClient, block_cache: Cache<RpcHash, RpcBlock>, tx_cache: Cache<RpcTransactionId, Vec<RpcHash>>) -> () {
    
}