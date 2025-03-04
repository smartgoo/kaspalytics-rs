use dashmap::DashMap;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    RpcBlock, RpcSubnetworkId, RpcTransaction, RpcTransactionId, RpcTransactionInput,
    RpcTransactionOutput,
};
use log::info;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
pub struct CacheBlock {
    pub hash: Hash,

    // RpcBlockHeader fields
    pub timestamp: u64,
    pub daa_score: u64,

    pub transactions: Vec<RpcTransactionId>,

    // RpcBlockVerboseData
    pub selected_parent_hash: Hash,
    pub is_chain_block: bool,
}

impl From<RpcBlock> for CacheBlock {
    fn from(value: RpcBlock) -> Self {
        CacheBlock {
            hash: value.header.hash,
            timestamp: value.header.timestamp,
            daa_score: value.header.daa_score,
            transactions: value
                .transactions
                .iter()
                .map(|tx| tx.verbose_data.clone().unwrap().transaction_id)
                .collect(),
            selected_parent_hash: value.verbose_data.clone().unwrap().selected_parent_hash,
            is_chain_block: value.verbose_data.unwrap().is_chain_block,
        }
    }
}

#[allow(dead_code)]
pub struct CacheTransaction {
    pub id: RpcTransactionId,
    pub inputs: Vec<RpcTransactionInput>,
    pub outpouts: Vec<RpcTransactionOutput>,
    // lock_time: u64,
    pub subnetwork_id: RpcSubnetworkId,
    pub gas: u64,
    pub payload: Vec<u8>,
    pub mass: u64,
    pub compute_mass: u64,
    // block_time: u64,
    pub blocks: Vec<Hash>,
    pub block_time: u64,
    pub accepting_block_hash: Option<Hash>,
}

impl From<RpcTransaction> for CacheTransaction {
    fn from(value: RpcTransaction) -> Self {
        CacheTransaction {
            id: value.verbose_data.clone().unwrap().transaction_id,
            inputs: value.inputs,
            outpouts: value.outputs,
            // lock_time: value.lock_time,
            subnetwork_id: value.subnetwork_id,
            gas: value.gas,
            payload: value.payload,
            mass: value.mass,
            compute_mass: value.verbose_data.clone().unwrap().compute_mass,
            blocks: vec![value.verbose_data.clone().unwrap().block_hash],
            block_time: value.verbose_data.clone().unwrap().block_time,
            accepting_block_hash: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct SecondMetrics {
    pub block_count: u64,
    pub transaction_count: u64,
    pub effective_transaction_count: u64,
}

#[derive(Default)]
pub struct Cache {
    // Synced to DAG tip
    pub synced: AtomicBool,

    pub tip_timestamp: AtomicU64,

    pub blocks: DashMap<Hash, CacheBlock>,
    pub transactions: DashMap<RpcTransactionId, CacheTransaction>,
    pub accepting_block_transactions: DashMap<Hash, Vec<RpcTransactionId>>,

    pub per_second: DashMap<u64, SecondMetrics>,
}

impl Cache {
    pub fn log_size(&self) {
        info!(
            "tip_timestamp: {} | blocks: {} | transactions {} | accepting_blocks_transactions {} | per_second {}",
            self.tip_timestamp.load(Ordering::SeqCst) / 1000,
            self.blocks.len(),
            self.transactions.len(),
            self.accepting_block_transactions.len(),
            self.per_second.len(),
        );
    }
}

impl Cache {
    pub fn prune(&self) {
        // TODO refactor this

        // TODO better handling of window size
        // Currently targeting 1 hour of blocks
        let window = 3600 * 1000;
        let pruning_timestamp = self.tip_timestamp.fetch_sub(window, Ordering::SeqCst) - window;

        // Prune blocks
        let mut candidate_blocks: Vec<Hash> = vec![];
        for block in self.blocks.iter() {
            if block.timestamp < pruning_timestamp {
                candidate_blocks.push(*block.key());
            }
        }

        let mut candidate_txs = Vec::new();
        for hash in candidate_blocks {
            // Remove block from blocks cache
            let (_, removed_block) = self.blocks.remove(&hash).unwrap();

            // Remove removed block from CachedTransaction.blocks
            // Capture transactions that have no blocks in cache
            for tx in removed_block.transactions {
                if let Some(mut cached_tx) = self.transactions.get_mut(&tx) {
                    cached_tx.blocks.retain(|b| *b != hash);

                    if cached_tx.blocks.is_empty() {
                        candidate_txs.push(tx);
                    }
                }
            }

            // Remove transactions
            for tx in candidate_txs.iter() {
                self.transactions.remove(tx);
            }

            // Remove block from accepting_block_transaction
            self.accepting_block_transactions.remove(&hash);
        }

        // ------
        // Prune per second stats
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let threshold = now - 86400 * 2;
        self.per_second.retain(|second, _| *second > threshold);
    }
}
