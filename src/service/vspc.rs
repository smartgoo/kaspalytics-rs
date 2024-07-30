use super::{cache::DAGCache, Event};
use kaspa_grpc_client::GrpcClient;
use kaspa_rpc_core::{api::rpc::RpcApi, message::*, RpcHash};
// use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

pub struct VirtualChainProcess {
    rpc_client: Arc<GrpcClient>,
    cache: Arc<Mutex<DAGCache>>,
    rx: mpsc::Receiver<Event>,
    // virtual_chain_response: Option<GetVirtualChainFromBlockResponse>,
    initial_sync_in_progress: bool,
}

impl VirtualChainProcess {
    pub fn new(
        rpc_client: Arc<GrpcClient>,
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

    pub async fn get_vspc(
        &mut self,
        low_hash: RpcHash,
    ) {
        // TODO return result
        let request = GetVirtualChainFromBlockRequest::new(low_hash, true);
        let response = self
            .rpc_client
            .get_virtual_chain_from_block_call(request)
            .await
            .unwrap();

        self.process_response(response).await;
    }

    async fn process_response(&self, response: GetVirtualChainFromBlockResponse) {
        // TODO return result

        let mut cache = self.cache.lock().await;

        // Process removed_chain_block_hashes
        for hash in response.removed_chain_block_hashes {
            let transactions = cache.chain_blocks.remove(&hash).unwrap();

            for transaction in transactions {
                cache.transaction_accepting_block.remove(&transaction);
            }
        }

        for obj in response.accepted_transaction_ids {
            cache.chain_blocks.insert(
                obj.accepting_block_hash,
                obj.accepted_transaction_ids.clone(),
            );

            for transaction in obj.accepted_transaction_ids {
                cache
                    .transaction_accepting_block
                    .insert(obj.accepting_block_hash, transaction);
            }
        }

        // TODO store some hash to call vspc from next? I think this depends on blocks in cache and if initial sync is done
    }

    pub async fn run(&mut self, low_hash: RpcHash) {
        while let Some(event) = self.rx.recv().await {
            match event {
                Event::GetBlocksBatch => {
                    // TODO get vspc

                    let cache = self.cache.lock().await;

                    let (_, most_recent_blocks) = cache.daas_blocks.iter().next_back().unwrap();

                    let all_exist = most_recent_blocks
                        .iter()
                        .all(|block| cache.chain_blocks.contains_key(block));

                    info!("all blocks in vspc cache: {}", all_exist);
                }
                _ => unimplemented!(),
            }
        }
    }
}
