use crate::cache::{Cache, CacheBlock, CacheTransaction};
use kaspa_hashes::Hash;
use kaspa_rpc_core::RpcAcceptedTransactionIds;
use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use sqlx::PgPool;
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;

pub struct DagListener {
    cache: Arc<Cache>,
    rpc_client: Arc<KaspaRpcClient>,
    pg_pool: PgPool,
    shutdown_flag: Arc<AtomicBool>,
}

impl DagListener {
    pub fn new(cache: Arc<Cache>, rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) -> Self {
        DagListener {
            cache,
            rpc_client,
            pg_pool,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl DagListener {
    async fn process_blocks(&self) {
        let blocks = self
            .rpc_client
            .get_blocks(Some(self.cache.low_hash().await.unwrap()), true, true)
            .await
            .unwrap();

        for block in blocks.blocks.iter() {
            self.cache
                .blocks
                .insert(block.header.hash, CacheBlock::from(block.clone()));

            let block_epoch_second = block.header.timestamp / 1000;

            // Increment block count for given second
            // TODO try to move to another task
            self.cache
                .per_second
                .entry(block_epoch_second)
                .and_modify(|ps| ps.block_count += 1)
                .or_default();

            for tx in block.transactions.iter() {
                let tx_id = tx.verbose_data.as_ref().unwrap().transaction_id;

                self.cache
                    .transactions
                    .entry(tx_id)
                    .and_modify(|entry| entry.blocks.push(block.header.hash))
                    .or_insert(CacheTransaction::from(tx.clone()));

                // Increment tx count for given second
                // TODO try to move to another task
                self.cache
                    .per_second
                    .entry(block_epoch_second)
                    .and_modify(|ps| ps.transaction_count += 1);
            }

            self.cache.tip_timestamp.store(
                blocks.blocks.last().unwrap().header.timestamp,
                Ordering::SeqCst,
            );
        }
    }

    async fn process_vspc_removed(&self, removed_chain_blocks: Vec<Hash>) {
        for removed_chain_block in removed_chain_blocks.iter() {
            // TODO handling for when reorg is below cache depth

            self.cache
                .blocks
                .entry(*removed_chain_block)
                .and_modify(|block| block.is_chain_block = false);

            let (_, removed_transactions) = self
                .cache
                .accepting_block_transactions
                .remove(removed_chain_block)
                .unwrap();

            for tx_id in removed_transactions.iter() {
                self.cache
                    .transactions
                    .entry(*tx_id)
                    .and_modify(|tx| tx.accepting_block_hash = None);

                let tx_timestamp = self.cache.transactions.get(tx_id).unwrap().block_time / 1000;

                // Decrement effective tx count for given second
                // TODO try to move to another task
                self.cache
                    .per_second
                    .entry(tx_timestamp)
                    .and_modify(|ps| ps.effective_transaction_count -= 1);
            }
        }
    }

    async fn process_vspc_added(&mut self, acceptance_data: Vec<RpcAcceptedTransactionIds>) {
        for acceptance in acceptance_data.iter() {
            if !self
                .cache
                .blocks
                .contains_key(&acceptance.accepting_block_hash)
            {
                break;
            }

            self.cache
                .set_low_hash(acceptance.accepting_block_hash)
                .await;

            self.cache
                .blocks
                .entry(acceptance.accepting_block_hash)
                .and_modify(|block| block.is_chain_block = true);

            for tx_id in acceptance.accepted_transaction_ids.iter() {
                self.cache.transactions.entry(*tx_id).and_modify(|tx| {
                    tx.accepting_block_hash = Some(acceptance.accepting_block_hash)
                });

                let tx_timestamp = self.cache.transactions.get(tx_id).unwrap().block_time / 1000;

                // Increment effective tx count for given second
                // TODO try to move to another task
                self.cache
                    .per_second
                    .entry(tx_timestamp)
                    .and_modify(|ps| ps.effective_transaction_count += 1);
            }

            self.cache.accepting_block_transactions.insert(
                acceptance.accepting_block_hash,
                acceptance.accepted_transaction_ids.clone(),
            );
        }
    }

    async fn main_loop(&mut self) {
        let GetBlockDagInfoResponse {
            pruning_point_hash, ..
        } = self.rpc_client.get_block_dag_info().await.unwrap();

        self.cache.set_low_hash(pruning_point_hash).await;
        info!("Starting from low_hash {:?}", self.cache.low_hash().await);

        while !self.shutdown_flag.load(Ordering::SeqCst) {
            let GetBlockDagInfoResponse { tip_hashes, .. } =
                self.rpc_client.get_block_dag_info().await.unwrap();

            self.process_blocks().await;

            let vspc = self
                .rpc_client
                .get_virtual_chain_from_block(self.cache.low_hash().await.unwrap(), true)
                .await
                .unwrap();

            self.process_vspc_removed(vspc.removed_chain_block_hashes)
                .await;
            self.process_vspc_added(vspc.accepted_transaction_ids).await;

            self.cache.prune();
            self.cache.log_size();

            if tip_hashes.contains(&self.cache.low_hash().await.unwrap()) {
                // TODO set synced to false if ever synced then falls out of sync
                self.cache.set_synced(true);

                // TODO log how long it takes to reach tip
                info!("Listener at tip, sleeping");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    pub async fn shutdown(&mut self) {
        info!("DagListner shutting down...");

        self.cache.store_cache_state(&self.pg_pool).await.unwrap();
    }

    pub async fn run(&mut self, mut shutdown_rx: Receiver<()>) {
        let shutdown_flag = self.shutdown_flag.clone();

        let _ = tokio::join!(
            tokio::spawn(async move {
                shutdown_rx.recv().await.unwrap();
                shutdown_flag.store(true, Ordering::SeqCst);
            }),
            async {
                self.main_loop().await;
            }
        );

        self.shutdown().await;
    }
}
