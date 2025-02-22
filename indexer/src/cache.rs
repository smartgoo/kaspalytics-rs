use std::collections::HashMap;

use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    RpcBlock, RpcSubnetworkId, RpcTransaction, RpcTransactionId, RpcTransactionInput,
    RpcTransactionOutput,
};
use log::info;

#[allow(dead_code)]
pub struct CacheBlock {
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
            blocks: vec![value.verbose_data.unwrap().block_hash],
            accepting_block_hash: None,
        }
    }
}

// TODO explore DashMap or Moka for caches?
#[derive(Default)]
pub struct Cache {
    pub tip_timestamp: u64,

    pub blocks: HashMap<Hash, CacheBlock>,
    pub transactions: HashMap<RpcTransactionId, CacheTransaction>,
    pub accepting_block_transactions: HashMap<Hash, Vec<RpcTransactionId>>,
}

impl Cache {
    pub fn log_size(&self) {
        info!(
            "tip_timestamp: {} | blocks: {} | transactions {} | accepting_blocks_transactions {}",
            self.tip_timestamp / 1000,
            self.blocks.len(),
            // self.block_transactions.len(),
            self.transactions.len(),
            self.accepting_block_transactions.len()
        );
    }
}

impl Cache {
    pub fn prune(&mut self) {
        let mut candidate_blocks: Vec<Hash> = vec![];

        let pruning_timestamp = self.tip_timestamp - 3600 * 1000;

        for (hash, block) in self.blocks.iter() {
            if block.timestamp < pruning_timestamp {
                candidate_blocks.push(*hash);
            }
        }

        // if !candidate_blocks.is_empty() {
        //     info!("Pruning {} blocks", candidate_blocks.len());
        // }

        for hash in candidate_blocks {
            // Remove block from blocks cache
            let removed_block = self.blocks.remove(&hash).unwrap();

            // Remove removed block from CachedTransaction.blocks
            // Remove transactions whose blocks are no longer in cache
            for tx in removed_block.transactions {
                if let Some(cached_tx) = self.transactions.get_mut(&tx) {
                    if let Some(idx) = cached_tx
                        .blocks
                        .iter()
                        .position(|&block_hash| block_hash == hash)
                    {
                        cached_tx.blocks.remove(idx);

                        if cached_tx.blocks.is_empty() {
                            self.transactions.remove(&tx);
                        }
                    }
                }
            }

            // Remove block from accepting_block_transaction
            self.accepting_block_transactions.remove(&hash);
        }
    }
}
