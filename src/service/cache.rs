use super::models::*;
use kaspa_consensus_core::Hash;
use kaspa_rpc_core::{RpcBlock, RpcHash, RpcTransaction, RpcTransactionId, RpcTransactionOutput};
use log::info;
use std::collections::BTreeMap;

pub struct DAGCache {
    pub daas_blocks: BTreeMap<u64, Vec<RpcHash>>,
    pub blocks: BTreeMap<RpcHash, RpcBlock>,
    pub blocks_transactions: BTreeMap<RpcHash, Vec<RpcTransactionId>>,
    pub transactions: BTreeMap<RpcTransactionId, RpcTransaction>,
    pub transactions_blocks: BTreeMap<RpcTransactionId, Vec<RpcHash>>,
    pub outputs: BTreeMap<CacheTransactionOutpoint, RpcTransactionOutput>,

    pub chain_blocks: BTreeMap<RpcHash, Vec<RpcTransactionId>>,
    pub transaction_accepting_block: BTreeMap<RpcTransactionId, RpcHash>,
}

impl DAGCache {
    pub fn new() -> Self {
        Self {
            daas_blocks: BTreeMap::<u64, Vec<RpcHash>>::new(),
            blocks: BTreeMap::<RpcHash, RpcBlock>::new(),
            blocks_transactions: BTreeMap::<RpcHash, Vec<RpcTransactionId>>::new(),
            transactions: BTreeMap::<RpcTransactionId, RpcTransaction>::new(),
            transactions_blocks: BTreeMap::<RpcTransactionId, Vec<RpcHash>>::new(),
            outputs: BTreeMap::<CacheTransactionOutpoint, RpcTransactionOutput>::new(),

            chain_blocks: BTreeMap::<RpcHash, Vec<RpcTransactionId>>::new(),
            transaction_accepting_block: BTreeMap::<RpcTransactionId, RpcHash>::new(),
        }
    }

    pub fn print_cache_sizes(&self) {
        info!("daas_blocks size: {}", self.daas_blocks.len());
        info!("blocks size: {}", self.blocks.len());
        info!(
            "blocks_transactions size: {}",
            self.blocks_transactions.len()
        );
        info!("transactions size: {}", self.transactions.len());
        info!(
            "transactions_blocks size: {}",
            self.transactions_blocks.len()
        );
        info!("outputs size {}", self.outputs.len());

        info!("chain_blocks size: {}", self.chain_blocks.len());
        info!(
            "transaction_accepting_block size: {}",
            self.transaction_accepting_block.len()
        );
    }
}

impl DAGCache {
    fn remove_block(&mut self, hash: &Hash) {
        // TODO return result
        self.blocks.remove(hash);
        self.chain_blocks.remove(hash);

        let transaction_ids = self.blocks_transactions.remove(hash).unwrap();
        for transaction_id in transaction_ids {
            let transaction_in_blocks = self.transactions_blocks.get_mut(&transaction_id).unwrap();

            match transaction_in_blocks.len() {
                1 => {
                    // Transaction is only in one cached block, remove
                    let transaction = self.transactions.remove(&transaction_id).unwrap();
                    self.transactions_blocks.remove(&transaction_id);
                    self.transaction_accepting_block.remove(&transaction_id);

                    // Remove only spent outputs
                    // TODO cap how many ouputs are in cache for memory purposes, while keeping as many as possible
                    transaction.inputs.iter().for_each(|input| {
                        self.outputs.remove(&input.previous_outpoint.into());
                    });
                }
                _ => {
                    // Transaction is in other cached blocks, remove block from self.transactions_blocks vec
                    transaction_in_blocks.retain(|&inner_hash| *hash != inner_hash);
                }
            }
        }
    }

    pub fn prune(&mut self) {
        // Keep 600 DAAs of blocks in memory
        // TODO make this adjust based on network block speed?
        while self.daas_blocks.len() > 600 {
            let (_, hashes) = self.daas_blocks.pop_first().unwrap();

            for hash in hashes {
                self.remove_block(&hash);
            }
        }
        let (daa, _) = self.daas_blocks.first_key_value().unwrap();
        info!("oldest daa in cache {}", daa);
    }
}
