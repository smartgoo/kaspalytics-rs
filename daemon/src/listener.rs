use crate::cache::{Cache, CacheTransaction, CacheBlock};

use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio::time::sleep;

pub struct DagListener {
    cache: Arc<RwLock<Cache>>,
    rpc_client: Arc<KaspaRpcClient>,
}

impl DagListener {
    pub fn new(cache: Arc<RwLock<Cache>>, rpc_client: Arc<KaspaRpcClient>) -> Self {
        DagListener { cache, rpc_client }
    }
}

impl DagListener {
    pub async fn run(&self) {
        let GetBlockDagInfoResponse {
            pruning_point_hash, ..
        } = self.rpc_client.get_block_dag_info().await.unwrap();

        let mut low_hash = pruning_point_hash;
        info!("Starting from low_hash {:?}", low_hash);

        loop {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            let blocks = self.rpc_client
                .get_blocks(Some(low_hash), true, true)
                .await
                .unwrap();

            let vspc = self.rpc_client
                .get_virtual_chain_from_block(low_hash, true)
                .await
                .unwrap();

            {
                // Acquire RwLock for entirety of cache updates
                let mut cache = self.cache.write().await;

                for block in blocks.blocks.iter() {
                    for tx in block.transactions.iter() {
                        let tx_id = tx.verbose_data.as_ref().unwrap().transaction_id;

                        cache
                            .transactions
                            .entry(tx_id)
                            .or_insert(CacheTransaction::from(tx.clone()))
                            .blocks
                            .push(block.header.hash);
                    }

                    cache
                        .blocks
                        .insert(block.header.hash, CacheBlock::from(block.clone()));

                    if block.header.timestamp > cache.tip_timestamp {
                        cache.tip_timestamp = block.header.timestamp;
                    }
                }

                for removed_chain_block in vspc.removed_chain_block_hashes.iter() {
                    cache
                        .blocks
                        .entry(*removed_chain_block)
                        .and_modify(|block| block.is_chain_block = false);

                    let removed_transactions = cache
                        .accepting_block_transactions
                        .remove(removed_chain_block)
                        .unwrap();

                    for tx_id in removed_transactions.iter() {
                        cache
                            .transactions
                            .entry(*tx_id)
                            .and_modify(|tx| tx.accepting_block_hash = None);
                    }
                }

                for acceptance in vspc.accepted_transaction_ids.iter() {
                    if !cache.blocks.contains_key(&acceptance.accepting_block_hash) {
                        break;
                    }

                    low_hash = acceptance.accepting_block_hash;

                    cache
                        .blocks
                        .entry(acceptance.accepting_block_hash)
                        .and_modify(|block| block.is_chain_block = true);

                    for tx_id in acceptance.accepted_transaction_ids.iter() {
                        cache
                            .transactions
                            .entry(*tx_id).and_modify(|tx| {
                            tx.accepting_block_hash = Some(acceptance.accepting_block_hash)
                        });
                    }

                    cache
                        .accepting_block_transactions.insert(
                        acceptance.accepting_block_hash,
                        acceptance.accepted_transaction_ids.clone(),
                    );
                }

                cache.prune();
                cache.log_size();
            }

            if tip_hashes.contains(&low_hash) {
                info!("at tip, sleeping");
                sleep(Duration::from_millis(5000)).await;
            }
        }
    }
}