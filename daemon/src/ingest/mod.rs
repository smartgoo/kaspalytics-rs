mod block_ingest;
mod block_writer;
mod vspc_ingest;
mod vspc_writer;

use crate::cache::Cache;
use block_ingest::BlockIngest;
use block_writer::BlockWriter;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcAcceptedTransactionIds, RpcBlock};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::config::Config;
use sqlx::PgPool;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use vspc_ingest::VspcIngest;
use vspc_writer::VspcWriter;

pub enum IngestMessage {
    Blocks(Vec<RpcBlock>),
    Acceptances(Vec<RpcAcceptedTransactionIds>),
    RemovedChainBlocks(Vec<Hash>),
}

pub struct DagIngest {
    config: Config,
    cache: Arc<Cache>,
    rpc_client: Arc<KaspaRpcClient>,
    pg_pool: PgPool,
    shutdown_flag: Arc<AtomicBool>,
}

impl DagIngest {
    pub fn new(
        config: Config,
        cache: Arc<Cache>,
        rpc_client: Arc<KaspaRpcClient>,
        pg_pool: PgPool,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Self {
        DagIngest {
            config,
            cache,
            rpc_client,
            pg_pool,
            shutdown_flag,
        }
    }
}

impl DagIngest {
    pub async fn run(&self) {
        // Block handling
        let (block_tx, block_rx) = tokio::sync::mpsc::channel(100);

        let block_ingest = BlockIngest::new(
            self.config.clone(),
            self.shutdown_flag.clone(),
            self.cache.clone(),
            block_tx,
        )
        .await;

        let mut block_writer = BlockWriter::new(
            self.config.clone(),
            self.shutdown_flag.clone(),
            self.cache.clone(),
            self.pg_pool.clone(),
            block_rx,
        );

        // VSPC Handling
        let (vspc_tx, vspc_rx) = tokio::sync::mpsc::channel(100);

        let vspc_ingest = VspcIngest::new(
            self.config.clone(),
            self.shutdown_flag.clone(),
            self.cache.clone(),
            vspc_tx,
        )
        .await;

        let mut vspc_writer = VspcWriter::new(
            self.config.clone(),
            self.shutdown_flag.clone(),
            self.cache.clone(),
            vspc_rx,
        );

        tokio::try_join!(
            tokio::task::spawn(async move {
                block_ingest.run().await;
            }),
            tokio::task::spawn(async move {
                block_writer.run().await;
            }),
            tokio::task::spawn(async move {
                vspc_ingest.run().await;
            }),
            tokio::task::spawn(async move { vspc_writer.run().await }),
        )
        .unwrap();
    }
}

// OLD
// impl DagIngest {
//     async fn process_blocks(&self) {
//         let blocks = self
//             .rpc_client
//             .get_blocks(
//                 Some(self.cache.last_known_chain_block().unwrap()),
//                 true,
//                 true,
//             )
//             .await
//             .unwrap(); // TODO error handling

//         for block in blocks.blocks {
//             self.cache.add_block(&block);
//         }
//     }

//     async fn process_vspc(&mut self) {
//         let vspc = self
//             .rpc_client
//             .get_virtual_chain_from_block(self.cache.last_known_chain_block().unwrap(), true)
//             .await
//             .unwrap(); // TODO error handling

//         // Handle removed chain blocks
//         for removed_chain_block in vspc.removed_chain_block_hashes {
//             self.cache.remove_chain_block(&removed_chain_block);
//         }

//         // Handle added chain blocks
//         for acceptance_obj in vspc.accepted_transaction_ids {
//             // If GetBlocks has not yet reached this block, break
//             if !self
//                 .cache
//                 .blocks
//                 .contains_key(&acceptance_obj.accepting_block_hash)
//             {
//                 break;
//             }

//             self.cache
//                 .set_last_known_chain_block(acceptance_obj.accepting_block_hash);

//             self.cache.add_chain_block_acceptance_data(acceptance_obj);
//         }

//         let block = self
//             .rpc_client
//             .get_block(self.cache.last_known_chain_block().unwrap(), false)
//             .await
//             .unwrap(); // TODO error handling

//         self.cache.set_tip_timestamp(block.header.timestamp);
//     }

//     pub async fn shutdown(&mut self) {
//         info!("Exiting loop, shutting down...");

//         // TODO move to writer
//         self.cache
//             .store_cache_state(self.config.clone())
//             .await
//             .unwrap();
//     }

//     pub async fn run(&mut self) {
//         // Determine starting hash to iterate up from
//         if self.cache.last_known_chain_block().is_none() {
//             let GetBlockDagInfoResponse {
//                 pruning_point_hash, ..
//             } = self.rpc_client.get_block_dag_info().await.unwrap();

//             info!("Starting from pruning point {:?}", pruning_point_hash);

//             self.cache
//                 .set_last_known_chain_block(pruning_point_hash);
//         } else {
//             info!(
//                 "Starting from cache last_known_chain_block {:?}",
//                 self.cache.last_known_chain_block()
//             );
//         }

//         while !self.shutdown_flag.load(Ordering::Relaxed) {
//             let GetBlockDagInfoResponse { tip_hashes, .. } =
//                 self.rpc_client.get_block_dag_info().await.unwrap();

//             self.process_blocks().await;
//             self.process_vspc().await;

//             let pruning_timestamp = self.cache.tip_timestamp() - 60 * 1000;
//             // let denormalized_blocks = self.cache.flush_to_writer(pruning_timestamp);
//             self.cache.prune(pruning_timestamp);

//             // self.tx.send(denormalized_blocks).await.unwrap();

//             if tip_hashes.contains(&self.cache.last_known_chain_block().unwrap()) {
//                 // TODO set synced to false if ever synced then falls out of sync
//                 self.cache.set_synced(true);

//                 info!("At tip. Sleeping");
//                 sleep(Duration::from_secs(10)).await;
//             } else {
//                 let tip_timestamp = self.cache.tip_timestamp() / 1000;
//                 let secs_behind = std::time::SystemTime::now()
//                     .duration_since(std::time::UNIX_EPOCH)
//                     .unwrap()
//                     .as_secs()
//                     - tip_timestamp;
//                 info!(
//                     "{}s behind (cache tip_timestamp {}) | channel capacity: {}",
//                     secs_behind,
//                     tip_timestamp,
//                     self.tx.capacity(),
//                 );
//             }
//         }

//         self.shutdown().await;
//     }
// }
