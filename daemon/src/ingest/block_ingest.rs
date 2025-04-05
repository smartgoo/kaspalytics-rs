use crate::Cache;
use kaspa_rpc_core::GetBlockDagInfoResponse;
use kaspa_rpc_core::{api::rpc::RpcApi, RpcBlock};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::config::Config;
use log::info;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::mpsc::Sender, time::sleep};

pub struct BlockIngest {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    cache: Arc<Cache>,
    tx: Sender<Vec<RpcBlock>>,
    rpc_client: KaspaRpcClient,
}

impl BlockIngest {
    pub async fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        cache: Arc<Cache>,
        tx: Sender<Vec<RpcBlock>>,
    ) -> Self {
        let rpc_client = KaspaRpcClient::new(
            WrpcEncoding::Borsh,
            Some(&config.rpc_url),
            None,
            Some(config.network_id),
            None,
        )
        .unwrap();
        rpc_client.connect(None).await.unwrap();

        BlockIngest {
            config,
            shutdown_flag,
            cache,
            tx,
            rpc_client,
        }
    }
}

impl BlockIngest {
    pub async fn run(&self) {
        let mut tip_timestamp = 0;

        // TODO where to start from?
        // Temporarily hardcoded to pruning point
        let GetBlockDagInfoResponse {
            pruning_point_hash, ..
        } = self.rpc_client.get_block_dag_info().await.unwrap();
        self.cache.set_low_hash(pruning_point_hash);

        info!(
            "BlockIngest starting from pruning point {:?}",
            pruning_point_hash,
        );

        // Main loop
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            let blocks = self
                .rpc_client
                .get_blocks(self.cache.low_hash(), true, true)
                .await
                .unwrap(); // TODO catch errors

            // TODO clean up
            if !blocks.blocks.is_empty() {
                tip_timestamp = blocks.blocks.last().unwrap().header.timestamp;

                self.cache
                    .set_low_hash(blocks.blocks.last().unwrap().header.hash);

                self.tx.send(blocks.blocks).await.unwrap();
            }

            if tip_hashes.contains(&self.cache.low_hash().unwrap()) {
                info!("BlockIngest at tip. Sleeping");
                sleep(Duration::from_secs(10)).await;
            } else {
                let tip_timestamp = tip_timestamp / 1000;
                let secs_behind = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    - tip_timestamp;
                info!(
                    "BlockIngest {}s behind | channel capacity: {}",
                    secs_behind,
                    self.tx.capacity(),
                );
            }
        }
    }
}
