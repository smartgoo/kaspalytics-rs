use super::{cache::DAGCache, Event};
use kaspa_consensus_core::tx::TransactionOutpoint;
use kaspa_rpc_core::{api::rpc::RpcApi, message::*, RpcHash, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use std::{sync::Arc, thread, time::Duration};
use tokio::sync::{mpsc, Mutex};

pub struct BlocksProcess {
    rpc_client: Arc<KaspaRpcClient>,
    cache: Arc<Mutex<DAGCache>>,
    tx: mpsc::Sender<Event>,
}

impl BlocksProcess {
    pub fn new(rpc_client: Arc<KaspaRpcClient>, cache: Arc<Mutex<DAGCache>>, tx: mpsc::Sender<Event>) -> Self {
        Self { tx, rpc_client, cache }
    }

    pub async fn run(&self, mut low_hash: RpcHash) -> ! {
        info!("Filling blocks cache from low_hash {}", low_hash);

        loop {
            // TODO loop and analyze in chunks... shouldn't loda all blocks and vspc into mem?
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();
    
            let blocks_response = self.rpc_client
                .get_blocks_call(GetBlocksRequest {
                    low_hash: Some(low_hash),
                    include_blocks: true,
                    include_transactions: true,
                })
                .await
                .unwrap();
            
            let mut cache = self.cache.lock().await;
            for i in 0..blocks_response.block_hashes.len() {
                let block_hash = blocks_response.block_hashes[i];
                let block = blocks_response.blocks[i].clone();
    
                // Insert block into cache
                cache.blocks.insert(block_hash, block.clone().into());
    
                let mut transactions = Vec::<RpcTransactionId>::new();
                for transaction in block.transactions {
                    let transaction_id = transaction.verbose_data.clone().unwrap().transaction_id;
                    transactions.push(transaction_id);
    
                    let in_cache = cache.transactions.get(&transaction_id);
                    match in_cache {
                        Some(_) => {
                            // Transaction exists already in cache
                            // Update transactions_blocks by adding block hash
                            let mut blocks = cache.transactions_blocks.get(&transaction_id).unwrap().clone();
                            blocks.push(block_hash);
                            cache.transactions_blocks.insert(transaction_id, blocks); // TODO ensure this is correct, should create if key doesn't exist, update if key does
                        }
                        None => {
                            // Transaction is not in cache
                            cache
                                .transactions
                                .insert(transaction_id, transaction.clone());
                            cache
                                .transactions_blocks
                                .insert(transaction_id, vec![block_hash]);
    
                            // Insert outputs
                            for (index, output) in transaction.outputs.into_iter().enumerate() {
                                let outpoint = TransactionOutpoint::new(transaction_id, index as u32);
                                cache.outputs.insert(outpoint.into(), output);
                            }
                        }
                    }
                }
    
                cache.blocks_transactions.insert(block_hash, transactions);
            }
    
            if tip_hashes.contains(&low_hash) {
                // TODO trigger real-time service to start
                info!("Synced to tip hash. Starting analysis and real-time service.");

                // Emit message on channel to VSPC Processor
                if let Err(e) = self.tx.send(Event::InitialSyncReachedTip).await {
                    println!("Failed to send event: {:?}", e);
                }

                thread::sleep(Duration::from_millis(5000));
            }

            cache.prune();
    
            low_hash = *blocks_response.block_hashes.last().unwrap();
            info!("blocks cache size {}", cache.blocks.len());
            info!("transactions cache size {}", cache.transactions.len());
            info!("outputs cache size {}", cache.outputs.len());
            info!("blocks_transactions cache size {}", cache.blocks_transactions.len());
            info!("transactions_blocks cache size {}", cache.transactions_blocks.len());

            // Emit event
            if let Err(e) = self.tx.send(Event::GetBlocksBatch).await {
                println!("Failed to send event: {:?}", e);
            }
        }
    }
}
