use super::models::*;
use indexmap::IndexMap;
use kaspa_consensus::model::stores::daa;
use kaspa_consensus_core::{daa_score_timestamp::DaaScoreTimestamp, tx::TransactionId, Hash};
use kaspa_database::prelude::Cache;
use kaspa_rpc_core::{
    RpcAcceptedTransactionIds, RpcBlock, RpcHash, RpcTransaction, RpcTransactionId, RpcTransactionOutput
};
use log::info;
use std::{collections::{BTreeMap, VecDeque}, fs::File, time::Instant};
use std::io::Write;
use bincode::serialize;
use serde::{Serialize, Deserialize};

pub struct DAGCache {
    pub daas_blocks: BTreeMap<u64, Vec<RpcHash>>,
    pub blocks: BTreeMap<RpcHash, RpcBlock>,
    pub blocks_transactions: BTreeMap<RpcHash, Vec<RpcTransactionId>>,
    pub chain_blocks: BTreeMap<RpcHash, bool>,
    pub transactions: BTreeMap<RpcTransactionId, RpcTransaction>,
    pub transactions_blocks: BTreeMap<RpcTransactionId, Vec<RpcHash>>,
    pub transactions_acceptances: BTreeMap<RpcHash, Vec<TransactionId>>,
    pub outputs: BTreeMap<CacheTransactionOutpoint, RpcTransactionOutput>,

    // VSPC fields
    pub removed_chain_block_hashes: Vec<RpcHash>,
    pub added_chain_block_hashes: Vec<RpcHash>,
    pub accepted_transaction_ids: Vec<RpcAcceptedTransactionIds>,

    pub buffer: BTreeMap<u64, Vec<RpcBlock>>,
}

impl DAGCache {
    pub fn new() -> Self {
        Self {
            daas_blocks: BTreeMap::<u64, Vec<RpcHash>>::new(),
            blocks: BTreeMap::<RpcHash, RpcBlock>::new(),
            blocks_transactions: BTreeMap::<RpcHash, Vec<RpcTransactionId>>::new(),
            chain_blocks: BTreeMap::<RpcHash, bool>::new(),
            transactions: BTreeMap::<RpcTransactionId, RpcTransaction>::new(),
            transactions_blocks: BTreeMap::<RpcTransactionId, Vec<RpcHash>>::new(),
            transactions_acceptances: BTreeMap::<RpcHash, Vec<TransactionId>>::new(),
            outputs: BTreeMap::<CacheTransactionOutpoint, RpcTransactionOutput>::new(),

            removed_chain_block_hashes: Vec::<RpcHash>::new(),
            added_chain_block_hashes: Vec::<RpcHash>::new(),
            accepted_transaction_ids: Vec::<RpcAcceptedTransactionIds>::new(),

            buffer: BTreeMap::<u64, Vec<RpcBlock>>::new(),
        }
    }

    pub fn print_cache_sizes(&self) {
        info!("daas_blocks size: {}", self.daas_blocks.len());
        info!("blocks size: {}", self.blocks.len());
        info!("blocks_transactions size: {}", self.blocks_transactions.len());
        info!("chain_blocks size: {}", self.chain_blocks.len());
        info!("transactions size: {}", self.transactions.len());
        info!("transactions_blocks size: {}", self.transactions_blocks.len());
        info!("transactions_acceptances size: {}", self.transactions_acceptances.len());
        info!("outputs size {}", self.outputs.len());
        info!("buffer size {}", self.buffer.len());
    }
}

impl DAGCache {
    fn move_daas_to_buffer(&mut self, daa_score: u64, hashes: &Vec<Hash>) {
        // TODO return result
        let blocks: Vec<_> = hashes
            .iter()
            .map(|hash| self.blocks.get(&hash).unwrap().clone())
            .collect();
        self.buffer.insert(daa_score, blocks);
    }

    fn remove_block(&mut self, hash: &Hash) {
        // TODO return result
        self.blocks.remove(hash);
        self.chain_blocks.remove(hash);

        let transaction_ids = self.blocks_transactions.remove(&hash).unwrap();
        for transaction_id in transaction_ids {
            let transaction_in_blocks =
                self.transactions_blocks.get_mut(&transaction_id).unwrap();
            
            match transaction_in_blocks.len() {
                1 => {
                    // Transaction is only in this block, remove
                    let transaction = self.transactions.remove(&transaction_id).unwrap();
                    self.transactions_blocks.remove(&transaction_id);

                    // Remove only spent outputs
                    // TODO cap how many ouputs are in cache for memory purposes, while keeping as many as possible
                    transaction.inputs.iter().for_each(|input| {
                        self.outputs.remove(&input.previous_outpoint.into());
                    });
                },
                _ => {
                    // Transaction is in other blocks, remove from self.transactions_blocks vec
                    transaction_in_blocks.retain(|&hash| hash != hash);
                }
            }
        }
    }

    fn flush_to_file(&mut self) {
        let start = Instant::now();

        // Get the first and last keys from the BTreeMap
        let first_key = self.buffer.keys().next();
        let last_key = self.buffer.keys().next_back();

        if let (Some(first), Some(last)) = (first_key, last_key) {
            let filename = format!("{}-{}.bin", first, last);
            let mut file = File::create(&filename).unwrap();

            for (_key, blocks) in &self.buffer {
                for block in blocks {
                    let serialized = serialize(block).unwrap();
                    file.write_all(&serialized).unwrap();
                }
            }

            let duration = start.elapsed();
            info!("write to file time: {:?}", duration);
            info!("file written: {}", filename);
            self.buffer.clear();
        } else {
            info!("The buffer is empty, no file written.");
        }
    }

    pub fn prune(&mut self) {
        // Keep 600 DAAs of blocks in memory
        // TODO make this adjust based on network block speed?
        while self.daas_blocks.len() > 600 {
            let (daa_score, hashes) = self.daas_blocks.pop_first().unwrap();

            // self.move_daas_to_buffer(daa_score, &hashes);

            for hash in hashes {
                self.remove_block(&hash);
            }
        }
        let (daa, _) = self.daas_blocks.first_key_value().unwrap();
        info!("oldest daa in cache {}", daa);
        // info!("buffer size: {}", self.buffer.len());

        // if self.buffer.len() > 86_400 / 10 {
        //     self.flush_to_file();
        // }
    }
}
