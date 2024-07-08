use crate::cache::cache::Cache;
use kaspa_consensus_core::Hash;
use kaspa_rpc_core::api::rpc;
use kaspa_rpc_core::message::GetBlockDagInfoResponse;
use kaspa_rpc_core::{GetBlocksRequest, GetVirtualChainFromBlockRequest, RpcTransactionInputVerboseData, RpcTransactionVerboseData};
use kaspa_rpc_core::{api::rpc::RpcApi, RpcBlock, RpcHash, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::time::Duration;
use std::thread;

pub async fn initial_sync(rpc_client: KaspaRpcClient) -> () {
    let block_cache = Cache::<RpcHash, RpcBlock>::new();
    let tx_cache = Cache::<RpcTransactionId, Vec<RpcHash>>::new();

    let GetBlockDagInfoResponse { pruning_point_hash, .. } = rpc_client.get_block_dag_info().await.unwrap();
    println!("{}", pruning_point_hash);

    let mut low_hash = pruning_point_hash.clone();
    info!("Filling blocks cache from {}", low_hash);
    loop {
        // TODO loop and analyze in chunks... shouldn't loda all blocks and vspc into mem
        let GetBlockDagInfoResponse { tip_hashes, .. } = rpc_client.get_block_dag_info().await.unwrap();

        let blocks_response = rpc_client.get_blocks_call(GetBlocksRequest {
            low_hash: Some(low_hash.clone()),
            include_blocks: true,
            include_transactions: true
        }).await.unwrap();

        for i in 0..blocks_response.block_hashes.len() {
            let block_hash = &blocks_response.block_hashes[i];
            let block = &blocks_response.blocks[i];

            block_cache.insert(block_hash.clone(), block.clone());

            let _ = block.transactions.clone().into_iter().map(|tx| {
                let transaction_id = &tx.verbose_data.unwrap().transaction_id;
                let in_cache = tx_cache.get(&transaction_id);
                match in_cache {
                    Some(mut blocks) => {
                        blocks.push(block_hash.clone());
                        tx_cache.update(&transaction_id, blocks);
                    },
                    None => {
                        tx_cache.insert(*transaction_id, vec![*block_hash]);
                    }
                }
            });
        };

        if tip_hashes.contains(&low_hash) {
            // TODO trigger real-time service to start
            info!("Synced to tip hash. Starting analysis and real-time service.");
            // thread::sleep(Duration::from_millis(5000));
            break;
        }

        low_hash = blocks_response.block_hashes.last().unwrap().clone();
        info!("Blocks cache size {}", block_cache.entry_count());
        info!("Transactions cache size {}", tx_cache.entry_count());
    }

    let vspc = rpc_client.get_virtual_chain_from_block_call(GetVirtualChainFromBlockRequest{
        start_hash: pruning_point_hash,
        include_accepted_transaction_ids: true
    }).await.unwrap();

}