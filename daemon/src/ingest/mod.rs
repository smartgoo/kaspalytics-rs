pub mod cache;
mod listener;
pub mod model;
mod pipeline;
mod second;

use cache::{CacheStateError, DagCache, Reader, Writer};
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::GetBlockDagInfoResponse;
use kaspalytics_utils::{config::Config, log::LogTarget};
use log::{error, info};
use model::PrunedBlock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    CacheState(#[from] cache::CacheStateError),

    #[error("{0}")]
    Rpc(#[from] kaspa_rpc_core::RpcError),

    #[error("Failed to send pruned blocks to writer: {0}")]
    ChannelSend(#[from] tokio::sync::mpsc::error::SendError<Vec<PrunedBlock>>),

    #[error("Listener error: {0}")]
    Listener(#[from] kaspa_wrpc_client::error::Error),

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub struct DagIngest {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    writer_tx: Sender<Vec<PrunedBlock>>,
    rpc_client: Arc<dyn RpcApi>,
    dag_cache: Arc<DagCache>,
}

impl DagIngest {
    pub fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        writer_tx: Sender<Vec<PrunedBlock>>,
        rpc_client: Arc<dyn RpcApi>,
        dag_cache: Arc<DagCache>,
    ) -> Self {
        DagIngest {
            config,
            shutdown_flag,
            writer_tx,
            rpc_client,
            dag_cache,
        }
    }
}

impl DagIngest {
    pub async fn initial_sync_to_tip(&self) -> Result<(), Error> {
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            let start = Instant::now();

            // Assume not synced at start of every loop
            // self.cache.set_synced(false);

            let block_dag_response = self.rpc_client.get_block_dag_info().await?;
            let (blocks, vspc) = tokio::try_join!(
                self.rpc_client.get_blocks(
                    Some(self.dag_cache.last_known_chain_block().unwrap()),
                    true,
                    true
                ),
                self.rpc_client.get_virtual_chain_from_block(
                    self.dag_cache.last_known_chain_block().unwrap(),
                    true
                ),
            )?;
            let rpc_end = start.elapsed().as_millis();

            self.dag_cache
                .set_tip_timestamp(blocks.blocks.last().unwrap().header.timestamp);

            // Add blocks to cache
            for block in blocks.blocks.iter() {
                // self.dag_cache.add_block(block);
                pipeline::block_add_pipeline(&self.dag_cache, block);
            }

            // Process removed chain blocks
            for removed_chain_block in vspc.removed_chain_block_hashes {
                pipeline::remove_chain_block_pipeline(&self.dag_cache, &removed_chain_block);
            }

            // Process added chain blocks
            for acceptance in vspc.accepted_transaction_ids {
                if !self
                    .dag_cache
                    .contains_block_hash(&acceptance.accepting_block_hash)
                {
                    break;
                }

                self.dag_cache
                    .set_last_known_chain_block(acceptance.accepting_block_hash);

                // self.dag_cache.add_chain_block_acceptance_data(acceptance);
                pipeline::add_chain_block_acceptance_pipeline(&self.dag_cache, acceptance);
            }

            // Prune data and send pruned to broker
            let pruned_blocks = self.dag_cache.prune();
            self.writer_tx.send(pruned_blocks).await?;

            // Check if synced
            if self
                .dag_cache
                .contains_block_hash(&block_dag_response.tip_hashes[0])
            {
                self.dag_cache.set_synced(true);
            }

            if self.dag_cache.synced() {
                break;
            } else {
                let tip_timestamp = self.dag_cache.tip_timestamp() / 1000;
                info!(
                    target: LogTarget::Daemon.as_str(),
                    "DagIngest initial sync {}s behind | loop time {}ms (RPC calls {}ms) | channel capacity: {}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        - tip_timestamp,
                    rpc_end,
                    start.elapsed().as_millis(),
                    self.writer_tx.capacity(),
                );
            }
        }

        info!(target: LogTarget::Daemon.as_str(), "DagIngest initial sync reached tip. Transitioning to notify");

        Ok(())
    }

    fn spawn_subscriber_task(&self) -> JoinHandle<Result<(), Error>> {
        let config = self.config.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let dag_cache = self.dag_cache.clone();

        tokio::task::spawn(async move {
            let listener =
                listener::Listener::try_new(config.network_id, Some(config.rpc_url), dag_cache)?;

            listener.start().await?;

            while !shutdown_flag.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }

            info!(target: LogTarget::Daemon.as_str(), "DagIngest Listener shutting down...");
            listener.stop().await?;
            info!(target: LogTarget::Daemon.as_str(), "DagIngest Listener shut down complete");

            Ok::<(), Error>(())
        })
    }

    fn spawn_pruning_task(&self) -> JoinHandle<Result<(), Error>> {
        let shutdown_flag = self.shutdown_flag.clone();
        let dag_cache = self.dag_cache.clone();
        let writer_tx = self.writer_tx.clone();

        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            while !shutdown_flag.load(Ordering::Relaxed) {
                let pruned_blocks = dag_cache.prune();
                writer_tx.send(pruned_blocks).await?;
                dag_cache.log_cache_size();
                interval.tick().await;
            }

            Ok::<(), Error>(())
        })
    }

    fn spawn_monitor_task(&self) -> JoinHandle<Result<(), Error>> {
        let shutdown_flag = self.shutdown_flag.clone();
        let dag_cache = self.dag_cache.clone();

        tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            interval.tick().await;

            while !shutdown_flag.load(Ordering::Relaxed) {
                let tip_timestamp = dag_cache.tip_timestamp();
                info!(
                    target: LogTarget::Daemon.as_str(),
                    "DagIngest Monitor last known chain block {:?}, tip timestamp {}ms behind",
                    dag_cache.last_known_chain_block().unwrap(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
                        - tip_timestamp,
                );
                interval.tick().await;
            }

            Ok::<(), Error>(())
        })
    }
}

impl DagIngest {
    pub async fn shutdown(&self) -> Result<(), CacheStateError> {
        self.dag_cache
            .store_cache_state(self.config.clone())
            .await?;

        Ok(())
    }

    pub async fn run(&self) -> Result<(), Error> {
        // Determine starting hash
        if self.dag_cache.last_known_chain_block().is_none() {
            let GetBlockDagInfoResponse {
                pruning_point_hash, ..
            } = self.rpc_client.get_block_dag_info().await?;

            self.dag_cache
                .set_last_known_chain_block(pruning_point_hash);

            info!(
                target: LogTarget::Daemon.as_str(),
                "DagIngest starting from pruning point {:?}",
                self.dag_cache.last_known_chain_block(),
            );
        } else {
            info!(
                target: LogTarget::Daemon.as_str(),
                "DagIngest starting from cache last known chain block {:?}",
                self.dag_cache.last_known_chain_block().unwrap(),
            );
        }

        // Initial sync to tip using iterative GetBlocks/GetVSPC
        self.initial_sync_to_tip().await?;

        // Spawn tasks
        match tokio::try_join!(
            self.spawn_subscriber_task(),
            self.spawn_pruning_task(),
            self.spawn_monitor_task(),
        ) {
            Ok(_) => self.shutdown().await?,
            Err(e) => {
                error!(target: LogTarget::Daemon.as_str(), "DagIngest shutting down due to error: {}", e);
                Err(e)?
            }
        }

        Ok(())
    }
}
