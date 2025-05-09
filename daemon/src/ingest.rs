use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::GetBlockDagInfoResponse;
use kaspalytics_utils::config::Config;
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

use crate::cache::{model::PrunedBlock, Cache, CacheReader, CacheWriter};

pub struct DagIngest {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    writer_tx: Sender<Vec<PrunedBlock>>,
    rpc_client: Arc<dyn RpcApi>,
    cache: Arc<Cache>,
}

impl DagIngest {
    pub fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        writer_tx: Sender<Vec<PrunedBlock>>,
        rpc_client: Arc<dyn RpcApi>,
        cache: Arc<Cache>,
    ) -> Self {
        DagIngest {
            config,
            shutdown_flag,
            writer_tx,
            rpc_client,
            cache,
        }
    }
}

impl DagIngest {
    pub async fn shutdown(&self) {
        self.cache
            .store_cache_state(self.config.clone())
            .await
            .unwrap();
    }

    pub async fn run(&self) {
        // Determine starting hash
        if self.cache.last_known_chain_block().is_none() {
            let GetBlockDagInfoResponse {
                pruning_point_hash, ..
            } = self.rpc_client.get_block_dag_info().await.unwrap();

            self.cache.set_last_known_chain_block(pruning_point_hash);

            info!(
                "DagIngest starting from pruning point {:?}",
                self.cache.last_known_chain_block(),
            );
        } else {
            info!(
                "DagIngest starting from cache last known chain block {:?}",
                self.cache.last_known_chain_block(),
            );
        }

        while !self.shutdown_flag.load(Ordering::Relaxed) {
            let start = Instant::now();

            // Assume not synced at start of every loop
            self.cache.set_synced(false);

            let block_dag_response = self.rpc_client.get_block_dag_info().await.unwrap();
            let (blocks, vspc) = tokio::try_join!(
                self.rpc_client.get_blocks(
                    Some(self.cache.last_known_chain_block().unwrap()),
                    true,
                    true
                ),
                self.rpc_client.get_virtual_chain_from_block(
                    self.cache.last_known_chain_block().unwrap(),
                    true
                ),
            )
            .unwrap();
            let rpc_end = start.elapsed().as_millis();

            self.cache
                .set_tip_timestamp(blocks.blocks.last().unwrap().header.timestamp);

            // Add blocks to cache
            for block in blocks.blocks.iter() {
                self.cache.add_block(block);
            }

            // Process removed chain blocks
            for removed_chain_block in vspc.removed_chain_block_hashes {
                self.cache.remove_chain_block(&removed_chain_block);
            }

            // Process added chain blocks
            for acceptance in vspc.accepted_transaction_ids {
                if !self
                    .cache
                    .contains_block_hash(&acceptance.accepting_block_hash)
                {
                    break;
                }

                self.cache
                    .set_last_known_chain_block(acceptance.accepting_block_hash);

                self.cache.add_chain_block_acceptance_data(acceptance);
            }

            // Check if synced
            if self
                .cache
                .contains_block_hash(&block_dag_response.tip_hashes[0])
            {
                self.cache.set_synced(true);
            }

            // Prune data and send pruned to broker
            let pruned_blocks = self.cache.prune();
            self.writer_tx.send(pruned_blocks).await.unwrap();

            if self.cache.synced() {
                info!("DagIngest at tip. Sleeping");
                sleep(Duration::from_secs(10)).await;
            } else {
                let tip_timestamp = self.cache.tip_timestamp() / 1000;
                let secs_behind = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    - tip_timestamp;
                info!(
                    "DagIngest {}s behind | Iter time {}ms (RPC calls {}ms) | channel capacity: {}",
                    secs_behind,
                    rpc_end,
                    start.elapsed().as_millis(),
                    self.writer_tx.capacity(),
                );
            }
        }

        self.shutdown().await;
    }
}
