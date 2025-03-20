use crate::cache::Cache;
use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::config::Config;
use log::info;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::time::sleep;

pub struct DagListener {
    config: Config,
    cache: Arc<Cache>,
    rpc_client: Arc<KaspaRpcClient>,
    shutdown_indicator: Arc<AtomicBool>,
}

impl DagListener {
    pub fn new(
        config: Config,
        cache: Arc<Cache>,
        rpc_client: Arc<KaspaRpcClient>,
        shutdown_indicator: Arc<AtomicBool>,
    ) -> Self {
        DagListener {
            config,
            cache,
            rpc_client,
            shutdown_indicator,
        }
    }
}

impl DagListener {
    async fn process_blocks(&self) {
        let blocks = self
            .rpc_client
            .get_blocks(
                Some(self.cache.last_known_chain_block().await.unwrap()),
                true,
                true,
            )
            .await
            .unwrap(); // TODO error handling

        for block in blocks.blocks {
            self.cache.add_block(block);
        }
    }

    async fn process_vspc(&mut self) {
        let vspc = self
            .rpc_client
            .get_virtual_chain_from_block(self.cache.last_known_chain_block().await.unwrap(), true)
            .await
            .unwrap(); // TODO error handling

        // Handle removed chain blocks
        for removed_chain_block in vspc.removed_chain_block_hashes {
            self.cache.remove_chain_block(removed_chain_block);
        }

        // Handle added chain blocks
        for acceptance_obj in vspc.accepted_transaction_ids {
            // If GetBlocks has not yet reached this block, break
            if !self
                .cache
                .blocks
                .contains_key(&acceptance_obj.accepting_block_hash)
            {
                break;
            }

            self.cache
                .set_last_known_chain_block(acceptance_obj.accepting_block_hash.clone())
                .await;

            self.cache
                .add_chain_block_acceptance_data(acceptance_obj);
        }

        let block = self
            .rpc_client
            .get_block(self.cache.last_known_chain_block().await.unwrap(), false)
            .await
            .unwrap();
        self.cache.set_tip_timestamp(block.header.timestamp);
    }

    pub async fn shutdown(&mut self) {
        info!("DagListner shutting down...");

        self.cache
            .store_cache_state(self.config.clone())
            .await
            .unwrap();
    }

    pub async fn run(&mut self) {
        if self.cache.last_known_chain_block().await.is_none() {
            let GetBlockDagInfoResponse {
                pruning_point_hash, ..
            } = self.rpc_client.get_block_dag_info().await.unwrap();

            info!("Starting from pruning point {:?}", pruning_point_hash);

            self.cache
                .set_last_known_chain_block(pruning_point_hash)
                .await;
        } else {
            info!(
                "Starting from cache last_known_chain_block {:?}",
                self.cache.last_known_chain_block().await
            );
        }

        while !self.shutdown_indicator.load(Ordering::Relaxed) {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            self.process_blocks().await;
            self.process_vspc().await;
            self.cache.prune();
            self.cache.log_size();

            if tip_hashes.contains(&self.cache.last_known_chain_block().await.unwrap()) {
                // TODO set synced to false if ever synced then falls out of sync
                self.cache.set_synced(true);

                info!("Listener at tip, sleeping");
                sleep(Duration::from_secs(10)).await;
            }
        }

        self.shutdown().await;
    }
}
