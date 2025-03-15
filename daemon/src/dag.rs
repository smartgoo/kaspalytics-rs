use crate::cache::Cache;
use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::config::Config;
use log::info;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;

pub struct DagListener {
    config: Config,
    cache: Arc<Cache>,
    rpc_client: Arc<KaspaRpcClient>,
    shutdown_flag: Arc<AtomicBool>,
}

impl DagListener {
    pub fn new(config: Config, cache: Arc<Cache>, rpc_client: Arc<KaspaRpcClient>) -> Self {
        DagListener {
            config,
            cache,
            rpc_client,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl DagListener {
    async fn process_blocks(&self) {
        let blocks = self
            .rpc_client
            .get_blocks(Some(self.cache.low_hash().await.unwrap()), true, true)
            .await
            .unwrap();

        let tip_timestamp = blocks.blocks.last().unwrap().header.timestamp;

        for block in blocks.blocks {
            self.cache.insert_block(block);
        }

        self.cache.set_tip_timestamp(tip_timestamp);
    }

    async fn process_vspc(&mut self) {
        let vspc = self
            .rpc_client
            .get_virtual_chain_from_block(self.cache.low_hash().await.unwrap(), true)
            .await
            .unwrap();

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

            // Set cache low hash, will pick up next iteration
            self.cache
                .set_low_hash(acceptance_obj.accepting_block_hash)
                .await;

            self.cache.add_chain_block_acceptance_data(acceptance_obj);
        }
    }

    async fn main_loop(&mut self) {
        if self.cache.low_hash().await.is_none() {
            let GetBlockDagInfoResponse {
                pruning_point_hash, ..
            } = self.rpc_client.get_block_dag_info().await.unwrap();

            info!("Starting from pruning point {:?}", pruning_point_hash);

            self.cache.set_low_hash(pruning_point_hash).await;
        } else {
            info!(
                "Starting from cache low_hash {:?}",
                self.cache.low_hash().await
            );
        }

        while !self.shutdown_flag.load(Ordering::SeqCst) {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            self.process_blocks().await;
            self.process_vspc().await;

            self.cache.prune();

            self.cache.log_size();

            if tip_hashes.contains(&self.cache.low_hash().await.unwrap()) {
                // TODO set synced to false if ever synced then falls out of sync
                self.cache.set_synced(true);

                // TODO log how long it takes to reach tip
                info!("Listener at tip, sleeping");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    pub async fn shutdown(&mut self) {
        info!("DagListner shutting down...");

        self.cache
            .store_cache_state(self.config.clone())
            .await
            .unwrap();
    }

    pub async fn run(&mut self, mut shutdown_rx: Receiver<()>) {
        let shutdown_flag = self.shutdown_flag.clone();

        let _ = tokio::join!(
            tokio::spawn(async move {
                shutdown_rx.recv().await.unwrap();
                shutdown_flag.store(true, Ordering::SeqCst);
            }),
            async {
                self.main_loop().await;
            }
        );

        self.shutdown().await;
    }
}
