use super::cache::DAGCache;
use kaspa_consensus_core::tx::TransactionOutpoint;
use kaspa_rpc_core::{api::rpc::RpcApi, message::*, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::{thread, time::Duration};

pub async fn initial_sync(rpc_client: KaspaRpcClient) {
    // TODO return type
    let cache = DAGCache::new();

    let GetBlockDagInfoResponse {
        pruning_point_hash, ..
    } = rpc_client.get_block_dag_info().await.unwrap();
    println!("{}", pruning_point_hash);

    let mut low_hash = pruning_point_hash;
    info!("Filling blocks cache from {}", low_hash);

    loop {
        // TODO loop and analyze in chunks... shouldn't loda all blocks and vspc into mem?
        let GetBlockDagInfoResponse { tip_hashes, .. } =
            rpc_client.get_block_dag_info().await.unwrap();

        let blocks_response = rpc_client
            .get_blocks_call(GetBlocksRequest {
                low_hash: Some(low_hash),
                include_blocks: true,
                include_transactions: true,
            })
            .await
            .unwrap();

        for i in 0..blocks_response.block_hashes.len() {
            let block_hash = blocks_response.block_hashes[i];
            let block = blocks_response.blocks[i].clone();

            // Insert block into cache
            cache.blocks.insert(block_hash, block.clone().into());

            let mut transactions = Vec::<RpcTransactionId>::new();
            for transaction in block.transactions {
                let transaction_id = transaction.verbose_data.clone().unwrap().transaction_id;
                transactions.push(transaction_id);

                let in_cache = cache.transactions.get(&transaction_id);
                match in_cache {
                    Some(_) => {
                        // Transaction exists already in cache
                        // Update transactions_blocks by adding block hash
                        let mut blocks = cache.transactions_blocks.get(&transaction_id).unwrap();
                        blocks.push(block_hash);
                        cache.transactions_blocks.update(&transaction_id, blocks);
                    }
                    None => {
                        // Transaction is not in cache
                        cache
                            .transactions
                            .insert(transaction_id, transaction.clone());
                        cache
                            .transactions_blocks
                            .insert(transaction_id, vec![block_hash]);

                        // Insert outputs
                        for (index, output) in transaction.outputs.into_iter().enumerate() {
                            let outpoint = TransactionOutpoint::new(transaction_id, index as u32);
                            cache.outputs.insert(outpoint.into(), output);
                        }
                    }
                }
            }

            cache.blocks_transactions.insert(block_hash, transactions);
        }

        if tip_hashes.contains(&low_hash) {
            // TODO trigger real-time service to start
            info!("Synced to tip hash. Starting analysis and real-time service.");
            thread::sleep(Duration::from_millis(5000));
        }

        low_hash = *blocks_response.block_hashes.last().unwrap();
        info!("blocks cache size {}", cache.blocks.entry_count());
        info!("transactions cache size {}", cache.transactions.entry_count());
        info!("outputs cache size {}", cache.outputs.entry_count());
        info!("blocks_transactions cache size {}", cache.blocks_transactions.entry_count());
        info!("transactions_blocks cache size {}", cache.transactions_blocks.entry_count());
    }

    let vspc = rpc_client
        .get_virtual_chain_from_block_call(GetVirtualChainFromBlockRequest {
            start_hash: pruning_point_hash,
            include_accepted_transaction_ids: true,
        })
        .await
        .unwrap();
}
