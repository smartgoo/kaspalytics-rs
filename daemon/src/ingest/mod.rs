pub mod cache;
mod listener;
pub mod model;
mod pipeline;
mod second;

use cache::{CacheStateError, DagCache, Manager, Reader, Writer};
use chrono::Utc;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::GetBlockDagInfoResponse;
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::{config::Config, log::LogTarget};
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use crate::ingest::model::PruningBatch;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    CacheState(#[from] cache::CacheStateError),

    #[error("{0}")]
    Rpc(#[from] kaspa_rpc_core::RpcError),

    #[error("Failed to send pruned blocks to writer: {0}")]
    ChannelSend(#[from] tokio::sync::mpsc::error::SendError<PruningBatch>),

    #[error("Listener error: {0}")]
    Listener(#[from] kaspa_wrpc_client::error::Error),

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

pub struct DagIngest {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    writer_tx: Sender<PruningBatch>,
    rpc_client: Arc<KaspaRpcClient>,
    dag_cache: Arc<DagCache>,
}

impl DagIngest {
    pub fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        writer_tx: Sender<PruningBatch>,
        rpc_client: Arc<KaspaRpcClient>,
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

            debug!(
                target: LogTarget::Daemon.as_str(),
                "low hash {:?}", self.dag_cache.last_known_chain_block().unwrap(),
            );

            let block_dag_response = self.rpc_client.get_block_dag_info().await?;
            let (blocks, vspc) = tokio::try_join!(
                self.rpc_client.get_blocks(
                    Some(self.dag_cache.last_known_chain_block().unwrap()),
                    true,
                    true
                ),
                self.rpc_client.get_virtual_chain_from_block_custom(
                    self.dag_cache.last_known_chain_block().unwrap(),
                ),
            )?;

            let rpc_end = start.elapsed().as_millis();

            self.dag_cache
                .set_tip_timestamp(blocks.blocks.last().unwrap().header.timestamp);

            // Add blocks to cache
            for block in blocks.blocks.iter() {
                pipeline::block_add_pipeline(self.dag_cache.clone(), block);
            }

            // Process removed chain blocks
            for removed_chain_block in vspc.removed_chain_block_hashes {
                pipeline::remove_chain_block_pipeline(self.dag_cache.clone(), &removed_chain_block);
            }

            // Process added chain blocks
            for acceptance in vspc.added_acceptance_data {
                if !self
                    .dag_cache
                    .contains_block_hash(&acceptance.accepting_chain_block_header.hash)
                {
                    break;
                }

                self.dag_cache
                    .set_last_known_chain_block(acceptance.accepting_chain_block_header.hash);

                pipeline::add_chain_block_acceptance_pipeline(self.dag_cache.clone(), acceptance);
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

    fn spawn_block_subscriber_task(&self) -> JoinHandle<Result<(), Error>> {
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

    fn spawn_vspc_task(&self) -> JoinHandle<Result<(), Error>> {
        let shutdown_flag = self.shutdown_flag.clone();
        let dag_cache = self.dag_cache.clone();
        let rpc_client = self.rpc_client.clone();

        tokio::task::spawn(async move {
            // Do a one time get_blocks to cover gap
            // where gap is time between initial_sync completing
            // and listener starting
            let blocks = rpc_client
                .get_blocks(
                    Some(dag_cache.last_known_chain_block().unwrap()),
                    true,
                    true,
                )
                .await
                .unwrap();

            for block in blocks.blocks.iter() {
                pipeline::block_add_pipeline(dag_cache.clone(), block);
            }

            let mut interval = tokio::time::interval(Duration::from_secs(5));
            while !shutdown_flag.load(Ordering::Relaxed) {
                // Always assume we are no longer synced to tip at start
                dag_cache.set_synced(false);

                // Get tip hashes to check synced at end of iter
                // let GetBlockDagInfoResponse { tip_hashes, .. } =
                //     rpc_client.get_block_dag_info().await?;

                // Get VSPC
                let vspc = rpc_client
                    .get_virtual_chain_from_block_custom(
                        dag_cache.last_known_chain_block().unwrap(),
                    )
                    .await?;

                // Process removed chain blocks
                for removed_chain_block in vspc.removed_chain_block_hashes {
                    pipeline::remove_chain_block_pipeline(dag_cache.clone(), &removed_chain_block);
                }

                // Process added chain blocks
                for acceptance in vspc.added_acceptance_data {
                    if !dag_cache.contains_block_hash(&acceptance.accepting_chain_block_header.hash)
                    {
                        warn!(
                            target: LogTarget::Daemon.as_str(),
                            "not in cache: {:?} | {:?}",
                            acceptance.accepting_chain_block_header.hash,
                            acceptance.accepting_chain_block_header.timestamp
                        );

                        continue;
                    }

                    dag_cache
                        .set_last_known_chain_block(acceptance.accepting_chain_block_header.hash);

                    let accepting_block_ts = acceptance.accepting_chain_block_header.timestamp;
                    pipeline::add_chain_block_acceptance_pipeline(dag_cache.clone(), acceptance);

                    // if tip_hashes.contains(&acceptance.accepting_chain_block_header.hash) {
                    //     dag_cache.set_synced(true);
                    // }
                    // Sleep when we get to a VSPC block that is within 5 seconds of current time
                    // This indicates we are near DAG Tip, trying to stay a little behind it
                    let ts = Utc::now().timestamp_millis();
                    if (ts - accepting_block_ts as i64) < 5000 {
                        info!(
                            target: LogTarget::Daemon.as_str(),
                            "{} - {} = {}ms behind",
                            ts,
                            accepting_block_ts,
                            ts - accepting_block_ts as i64,
                        );

                        dag_cache.set_synced(true);

                        break;
                    }
                }

                if dag_cache.synced() {
                    interval.tick().await;
                } else {
                    let tip_timestamp = dag_cache.tip_timestamp();
                    info!(
                        target: LogTarget::Daemon.as_str(),
                        "vspc_task {}ms behind, not sleeping",
                        Utc::now().timestamp_millis() - tip_timestamp as i64,
                    );
                }
            }

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
                // dag_cache.log_cache_size();
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

        // self.dag_cache.set_last_known_chain_block("e4eede8a0954a634595dc9e26a88dc2488a55e45c5796ff93814aa40761ac644".parse().unwrap());

        // Initial sync to tip using iterative GetBlocks/GetVSPC
        self.initial_sync_to_tip().await?;

        // Spawn tasks
        match tokio::try_join!(
            self.spawn_block_subscriber_task(),
            self.spawn_vspc_task(),
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
