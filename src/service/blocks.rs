use super::{cache::DAGCache, Event};
use kaspa_consensus_core::{tx::TransactionOutpoint, Hash};
use kaspa_rpc_core::{api::rpc::RpcApi, message::*, RpcBlock, RpcHash, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::{collections::HashMap, os::unix, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex};

pub struct BlocksProcess {
    rpc_client: Arc<KaspaRpcClient>,
    cache: Arc<Mutex<DAGCache>>,
    tx: mpsc::Sender<Event>,
}

impl BlocksProcess {
    pub fn new(
        rpc_client: Arc<KaspaRpcClient>,
        cache: Arc<Mutex<DAGCache>>,
        tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            tx,
            rpc_client,
            cache,
        }
    }
}

impl BlocksProcess {
    // async fn process_transaction(&self) {}

    async fn process_block(&self, hash: Hash, block: RpcBlock) {
        // TODO return result

        let mut cache = self.cache.lock().await;

        // Insert block into DAA cache
        cache
            .blocks
            .insert(hash.clone(), block.clone().into());

        // Insert block into daa cache
        if let Some(blocks) = cache.daas_blocks.get_mut(&block.header.daa_score) {
            if !blocks.contains(&hash) {
                blocks.push(hash.clone());
            }
        } else {
            cache
                .daas_blocks
                .insert(block.header.daa_score, vec![hash.clone()]);
        }

        // TODO is this needed if using get_virtual_chain_from_block ? I think so...
        if block.verbose_data.unwrap().is_chain_block {
            cache.chain_blocks.insert(hash, true);
        }

        let mut transactions = Vec::<RpcTransactionId>::new();
        for transaction in block.transactions {
            let transaction_id = transaction.verbose_data.clone().unwrap().transaction_id;
            transactions.push(transaction_id);

            match cache.transactions.get(&transaction_id) {
                Some(_) => {
                    // Transaction exists already in cache
                    // Update transactions_blocks by adding block hash
                    let mut blocks = cache
                        .transactions_blocks
                        .get(&transaction_id)
                        .unwrap()
                        .clone();
                    blocks.push(hash);
                    cache.transactions_blocks.insert(transaction_id, blocks);
                }
                None => {
                    // Transaction is not in cache
                    cache
                        .transactions
                        .insert(transaction_id, transaction.clone());
                    cache
                        .transactions_blocks
                        .insert(transaction_id, vec![hash]);

                    // Insert outputs
                    for (index, output) in transaction.outputs.into_iter().enumerate() {
                        let outpoint =
                            TransactionOutpoint::new(transaction_id, index as u32);
                        cache.outputs.insert(outpoint.into(), output);
                    }
                }
            }
        }

        cache.blocks_transactions.insert(hash, transactions);
    }
    
    pub async fn run(&self, mut low_hash: RpcHash) -> ! {
        info!("Starting from low_hash {}", low_hash);

        loop {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            let blocks_response = self
                .rpc_client
                .get_blocks_call(GetBlocksRequest {
                    low_hash: Some(low_hash),
                    include_blocks: true,
                    include_transactions: true,
                })
                .await
                .unwrap();

            for i in 0..blocks_response.block_hashes.len() {
                let hash = blocks_response.block_hashes[i];
                let block = blocks_response.blocks[i].clone();

                self.process_block(hash, block).await;
            }

            low_hash = *blocks_response.block_hashes.last().unwrap();

            let mut cache = self.cache.lock().await;
            cache.prune();
            cache.print_cache_sizes();
            drop(cache); // TODO get rid of

            if tip_hashes.contains(&low_hash) {
                // TODO trigger real-time service to start
                info!("Synced to tip hash. Starting analysis and real-time service.");

                if let Err(e) = self.tx.send(Event::InitialSyncReachedTip).await {
                    println!("Failed to send event: {:?}", e);
                }

                tokio::time::sleep(Duration::from_millis(5_000)).await;
            }

            if let Err(e) = self.tx.send(Event::GetBlocksBatch).await {
                println!("Failed to send event: {:?}", e);
            }
        }
    }
}
