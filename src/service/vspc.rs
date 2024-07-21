use super::{cache::DAGCache, Event};
use kaspa_consensus_core::tx::TransactionOutpoint;
use kaspa_database::prelude::Cache;
use kaspa_rpc_core::{api::rpc::RpcApi, message::*, RpcHash, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::{sync::Arc, thread, time::Duration};
use tokio::sync::{mpsc, Mutex};

pub struct VirtualChainProcess {
    rpc_client: Arc<KaspaRpcClient>,
    cache: Arc<Mutex<DAGCache>>,
    rx: mpsc::Receiver<Event>,
    virtual_chain_response: Option<GetVirtualChainFromBlockResponse>,
    initial_sync_in_progress: bool,
}

impl VirtualChainProcess {
    pub fn new(rpc_client: Arc<KaspaRpcClient>, cache: Arc<Mutex<DAGCache>>, rx: mpsc::Receiver<Event>) -> Self {
        Self { rpc_client, cache, rx, virtual_chain_response: None, initial_sync_in_progress: true }
    }

    pub async fn get_virtual_chain(&mut self, low_hash: RpcHash) {
        // TODO return result
        use std::time::Instant;
        let start = Instant::now();

        let request = GetVirtualChainFromBlockRequest::new(low_hash, true);
        self.virtual_chain_response = Some(self.rpc_client.get_virtual_chain_from_block_call(request).await.unwrap());

        let duration = start.elapsed();

        let accepted_block_count = self.virtual_chain_response.as_ref().unwrap().added_chain_block_hashes.len();
        let accepted_tx_count: usize = self.virtual_chain_response.as_ref().unwrap().accepted_transaction_ids
            .iter()
            .map(|item| item.accepted_transaction_ids.len())
            .sum();

        println!("VSPC: time taken: {:?} | num blocks added {:?} | num txs {:?}", duration, accepted_block_count, accepted_tx_count);
    }

    pub async fn run(&mut self, low_hash: RpcHash) {
        // Perform initial request and store result
        self.get_virtual_chain(low_hash).await;

        while let Some(event) = self.rx.recv().await {
            match event {
                Event::GetBlocksBatch => { 
                    if self.initial_sync_in_progress {
                        info!("VSPC Processor received GetBlocksBatch message, but initial sync is in progress. Doing nothing");
                        continue
                    }
                 },
                Event::InitialSyncReachedTip => { 
                    info!("VSPC Processor received InitialSyncReachedTip message");

                    self.initial_sync_in_progress = false;
                    // Apply self.response from initial sync 
                 },
            }

            // TODO how to save response from inital sync since it would be wasteful to continually query with such a far back low_hash

            // For added_block in response:
                // ensure all blocks in cache.blocks are set to chain_block true
                // anything tx related?
                // if block not in blocks cache... exit?
            
            // For removed_blocks in response:
                // ensure all blocks in cache.blocks are set to chain_block false
                // anything tx related?
                // if block not in blocks cache... exit?
            
            // For tx in accepted_transactions
                // set in cache.acceptances
                // how to handle reorgs?
                // if tx not in tx cache... we haven't seen it in a block yet?
        }
    }
}