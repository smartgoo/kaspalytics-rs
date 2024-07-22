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
        let response =
            self.rpc_client
                .get_virtual_chain_from_block_call(request)
                .await
                .unwrap();

        let duration = start.elapsed();

        let accepted_block_count = response
            .added_chain_block_hashes
            .len();
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
        // Perform initial request and store result
        // Store virtual_chain_result on cache
        self.get_virtual_chain(low_hash).await;

        while let Some(event) = self.rx.recv().await {
            match event {
                _ => {
                    let cache = self.cache.lock().await;

                    let oldest_block = cache.blocks.first_key_value().unwrap();
                    let newest_block = cache.blocks.last_key_value().unwrap();
                    info!("Oldest Block: {:?} | Newest Block: {:?}", oldest_block.0, newest_block.0);

                    // let oldest_block_index = cache.added_chain_block_hashes.iter().position(|&x| x == *oldest_block.0);
                    // let newest_block_index = cache.added_chain_block_hashes.iter().position(|&x| x == *newest_block.0);
                    // // TODO check removed_chain_block_hashes?
                    // info!("{:?} {:?}", oldest_block_index, newest_block_index);

                    // if oldest_block_index.is_some() && newest_block_index.is_some() {
                    //     let mut added = 0;
                    //     for hash in cache.added_chain_block_hashes.clone() {
                    //         // Set verbose_data.chain_block in cache.blocks to True
                    //         added += 1;
                    //     }
                    //     info!("{} added chain_blocks", added);

                    //     let mut removed = 0;
                    //     for hash in cache.removed_chain_block_hashes.clone() {
                    //         // Set verbose_data.chain_block in cache.blocks to False
                    //         removed += 1;
                    //     }
                    //     info!("{} removed chain_blocks", removed);
                    // }
                }
                // Event::GetBlocksBatch => {
                //     if self.initial_sync_in_progress {
                //         info!("VSPC Processor received GetBlocksBatch message, but initial sync is in progress. Applying VSPC to collected blocks.");
                //         continue;
                //     }
                // }
                // Event::InitialSyncReachedTip => {
                //     info!("VSPC Processor received InitialSyncReachedTip message");

                //     self.initial_sync_in_progress = false;
                //     // Apply self.response from initial sync
                // }
            }

            // TODO how to save response from inital sync since it would be wasteful to continually query with such a far back low_hash

            // For added_block in response...
            // ensure all blocks in cache.blocks are set to chain_block true
            // anything tx related?
            // if block not in blocks cache... exit?

            // For removed_blocks in response...
            // ensure all blocks in cache.blocks are set to chain_block false
            // anything tx related?
            // if block not in blocks cache... exit?

            // For tx in accepted_transactions...
            // set in cache.acceptances
            // how to handle reorgs?
            // if tx not in tx cache... we haven't seen it in a block yet?
        }
    }
}
