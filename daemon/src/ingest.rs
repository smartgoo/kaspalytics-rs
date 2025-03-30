use crate::cache::Cache;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    api::rpc::RpcApi, GetBlockDagInfoResponse, RpcAcceptedTransactionIds, RpcBlock,
};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::config::Config;
use log::{debug, info};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

pub enum IngestMessage {
    Blocks(Vec<RpcBlock>),
    AddedAcceptances(Vec<RpcAcceptedTransactionIds>),
    RemovedChainBlocks(Vec<Hash>),
}

pub struct DagIngest {
    config: Config,
    tx: Sender<IngestMessage>,
    cache: Arc<Cache>,
    rpc_client: Arc<KaspaRpcClient>,
    shutdown_flag: Arc<AtomicBool>,
}

impl DagIngest {
    pub fn new(
        config: Config,
        tx: Sender<IngestMessage>,
        cache: Arc<Cache>,
        rpc_client: Arc<KaspaRpcClient>,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Self {
        DagIngest {
            config,
            tx,
            cache,
            rpc_client,
            shutdown_flag,
        }
    }
}

impl DagIngest {
    async fn process_blocks(&self) {
        let s = std::time::Instant::now();
        let blocks = self
            .rpc_client
            .get_blocks(
                Some(self.cache.last_known_chain_block().await.unwrap()),
                true,
                true,
            )
            .await
            .unwrap(); // TODO error handling
        debug!("get_blocks took {}ms", s.elapsed().as_millis());

        // Add to cache
        // And identify new blocks that are not in DB yet based on existence in cache
        // This is to get around unique constraints in DB
        let mut new_blocks: Vec<RpcBlock> = vec![];
        for block in blocks.blocks.iter() {
            if self.cache.add_block(block) {
                new_blocks.push(block.clone())
            }
        }

        self.tx
            .send(IngestMessage::Blocks(new_blocks))
            .await
            .unwrap();
    }

    async fn process_vspc(&mut self) {
        let s = std::time::Instant::now();
        let vspc = self
            .rpc_client
            .get_virtual_chain_from_block(self.cache.last_known_chain_block().await.unwrap(), true)
            .await
            .unwrap(); // TODO error handling
        debug!("get_virtual_chain_from_block took {}ms", s.elapsed().as_millis());

        // Handle removed chain blocks
        let mut db_removals = vec![];
        for removed_chain_block in vspc.removed_chain_block_hashes {
            if self.cache.remove_chain_block(removed_chain_block) {
                db_removals.push(removed_chain_block);
            }
        }

        // Send removals to db writer
        if !db_removals.is_empty() {
            self.tx
                .send(IngestMessage::RemovedChainBlocks(db_removals))
                .await
                .unwrap();
        }

        // Handle added chain blocks
        let mut db_acceptances = vec![];
        for acceptance in vspc.accepted_transaction_ids.iter() {
            // If GetBlocks has not yet reached this block, break
            if !self
                .cache
                .blocks
                .contains_key(&acceptance.accepting_block_hash)
            {
                break;
            }

            self.cache
                .set_last_known_chain_block(acceptance.accepting_block_hash)
                .await;

            self.cache
                .add_chain_block_acceptance_data(acceptance.clone());

            db_acceptances.push(acceptance.clone());
        }

        // Send new acceptances to db writer
        self.tx
            .send(IngestMessage::AddedAcceptances(db_acceptances))
            .await
            .unwrap();

        // Get last known chain block data
        let block = self
            .rpc_client
            .get_block(self.cache.last_known_chain_block().await.unwrap(), false)
            .await
            .unwrap(); // TODO error handling

        self.cache.set_tip_timestamp(block.header.timestamp);
    }

    pub async fn shutdown(&mut self) {
        info!("Exiting loop, shutting down...");

        self.cache
            .store_cache_state(self.config.clone())
            .await
            .unwrap();
    }

    pub async fn run(&mut self) {
        // Determine starting hash to iterate up from
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

        while !self.shutdown_flag.load(Ordering::Relaxed) {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            self.process_blocks().await;
            self.process_vspc().await;
            self.cache.prune();

            if tip_hashes.contains(&self.cache.last_known_chain_block().await.unwrap()) {
                // TODO set synced to false if ever synced then falls out of sync
                self.cache.set_synced(true);

                info!("At tip. Sleeping");
                sleep(Duration::from_secs(10)).await;
            } else {
                let tip_timestamp = self.cache.tip_timestamp() / 1000;
                let secs_behind = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    - tip_timestamp;
                info!(
                    "{}s behind (cache tip_timestamp {}) | channel capacity: {}",
                    secs_behind,
                    tip_timestamp,
                    self.tx.capacity(),
                );
            }
        }

        self.shutdown().await;
    }
}
