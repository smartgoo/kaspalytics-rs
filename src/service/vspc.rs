use super::{cache::DAGCache, Event};
use kaspa_rpc_core::{api::rpc::RpcApi, message::*, RpcHash};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub struct VirtualChainProcess {
    rpc_client: Arc<KaspaRpcClient>,
    cache: Arc<Mutex<DAGCache>>,
    rx: mpsc::Receiver<Event>,
    // virtual_chain_response: Option<GetVirtualChainFromBlockResponse>,
    initial_sync_in_progress: bool,
}

impl VirtualChainProcess {
    pub fn new(
        rpc_client: Arc<KaspaRpcClient>,
        cache: Arc<Mutex<DAGCache>>,
        rx: mpsc::Receiver<Event>,
    ) -> Self {
        Self {
            rpc_client,
            cache,
            rx,
            // virtual_chain_response: None,
            initial_sync_in_progress: true,
        }
    }

    pub async fn get_virtual_chain(&mut self, low_hash: RpcHash) {
        // TODO return result
        use std::time::Instant;
        let start = Instant::now();

        let request = GetVirtualChainFromBlockRequest::new(low_hash, true);
        let response = self
            .rpc_client
            .get_virtual_chain_from_block_call(request)
            .await
            .unwrap();

        let duration = start.elapsed();

        let accepted_block_count = response.added_chain_block_hashes.len();
        let accepted_tx_count: usize = response
            .accepted_transaction_ids
            .iter()
            .map(|item| item.accepted_transaction_ids.len())
            .sum();

        let mut cache = self.cache.lock().await;
        cache.added_chain_block_hashes = response.added_chain_block_hashes;
        cache.removed_chain_block_hashes = response.removed_chain_block_hashes;
        cache.accepted_transaction_ids = response.accepted_transaction_ids;

        info!(
            "VSPC: time taken: {:?} | num blocks added {:?} | num txs {:?}",
            duration, accepted_block_count, accepted_tx_count
        );
    }

    pub async fn run(&mut self, low_hash: RpcHash) {
        self.get_virtual_chain(low_hash).await;

        while let Some(event) = self.rx.recv().await {
            match event {
                Event::GetBlocksBatch => {
                    let cache = self.cache.lock().await;

                    let (_, most_recent_blocks) = cache.daas_blocks.iter().rev().next().unwrap();

                    let all_exist = most_recent_blocks
                        .iter()
                        .all(|block| 
                            cache.added_chain_block_hashes.contains(block)
                        );
                    
                    info!("all blocks in vspc cache: {}", all_exist);
                },
                _ => unimplemented!(),
            }
        }
    }
}
