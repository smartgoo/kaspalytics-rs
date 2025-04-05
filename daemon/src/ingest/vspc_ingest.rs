use crate::cache::Cache;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::{GetBlockDagInfoResponse, GetVirtualChainFromBlockResponse};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use kaspalytics_utils::config::Config;
use log::info;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{sync::mpsc::Sender, time::sleep};

pub struct VspcIngest {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    cache: Arc<Cache>,
    tx: Sender<GetVirtualChainFromBlockResponse>,
    rpc_client: KaspaRpcClient,
}

impl VspcIngest {
    pub async fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        cache: Arc<Cache>,
        tx: Sender<GetVirtualChainFromBlockResponse>,
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

        VspcIngest {
            config,
            shutdown_flag,
            cache,
            tx,
            rpc_client,
        }
    }
}

impl VspcIngest {
    pub async fn run(&self) {
        let mut tip_timestamp = 0;

        // TODO where to start from?
        // Temporarily hardcoded to pruning point
        let GetBlockDagInfoResponse {
            pruning_point_hash, ..
        } = self.rpc_client.get_block_dag_info().await.unwrap();
        self.cache.set_last_known_chain_block(pruning_point_hash);
        info!(
            "VspcIngest starting from pruning point {:?}",
            pruning_point_hash,
        );

        // Main loop
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            let vspc = self
                .rpc_client
                .get_virtual_chain_from_block(self.cache.last_known_chain_block().unwrap(), true)
                .await
                .unwrap(); // TODO error handling

            // TODO logic that prevents this from sending data within X
            // DAAs of DAG Tip (or X seconds, etc.)
            // To prevent having to process reorgs

            // TODO clean up these so they're consistent with eachother
            if !vspc.added_chain_block_hashes.is_empty() {
                tip_timestamp = self
                    .rpc_client
                    .get_block(*vspc.added_chain_block_hashes.last().unwrap(), false)
                    .await
                    .unwrap() // TODO error handling
                    .header
                    .timestamp;

                self.cache.set_last_known_chain_block(
                    *vspc.added_chain_block_hashes.last().unwrap(),
                );

                self.tx.send(vspc).await.unwrap();
            }

            if tip_hashes.contains(&self.cache.last_known_chain_block().unwrap()) {
                info!("VspcIngest at tip. Sleeping");
                sleep(Duration::from_secs(10)).await;
            } else {
                let tip_timestamp = tip_timestamp / 1000;
                let secs_behind = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    - tip_timestamp;
                info!(
                    "VspcIngest {}s behind | channel capacity: {}",
                    secs_behind,
                    self.tx.capacity(),
                );
            }
        }
    }
}
