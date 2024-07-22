use super::models::*;
use kaspa_consensus_core::tx::TransactionId;
use kaspa_rpc_core::{RpcAcceptedTransactionIds, RpcHash, RpcTransaction, RpcTransactionId, RpcTransactionOutput};
use std::collections::BTreeMap;

pub struct DAGCache {
    pub blocks: BTreeMap<RpcHash, CacheBlock>,
    pub transactions: BTreeMap<RpcTransactionId, RpcTransaction>,
    pub blocks_transactions: BTreeMap<RpcHash, Vec<RpcTransactionId>>,
    pub transactions_blocks: BTreeMap<RpcTransactionId, Vec<RpcHash>>,
    pub transactions_acceptances: BTreeMap<RpcHash, Vec<TransactionId>>,
    pub outputs: BTreeMap<CacheTransactionOutpoint, RpcTransactionOutput>,
    
    // VSPC fields
    pub removed_chain_block_hashes: Vec<RpcHash>,
    pub added_chain_block_hashes: Vec<RpcHash>,
    pub accepted_transaction_ids: Vec<RpcAcceptedTransactionIds>,
}

impl DAGCache {
    pub fn new() -> Self {
        Self {
            blocks: BTreeMap::<RpcHash, CacheBlock>::new(),
            transactions: BTreeMap::<RpcTransactionId, RpcTransaction>::new(),
            blocks_transactions: BTreeMap::<RpcHash, Vec<RpcTransactionId>>::new(),
            transactions_blocks: BTreeMap::<RpcTransactionId, Vec<RpcHash>>::new(),
            transactions_acceptances: BTreeMap::<RpcHash, Vec<TransactionId>>::new(),
            outputs: BTreeMap::<CacheTransactionOutpoint, RpcTransactionOutput>::new(),
            removed_chain_block_hashes: Vec::<RpcHash>::new(),
            added_chain_block_hashes: Vec::<RpcHash>::new(),
            accepted_transaction_ids: Vec::<RpcAcceptedTransactionIds>::new(),
        }
    }

    // pub fn prune(&mut self) {
    //     // TODO long term, want to archive pruned data so we can reanalyze if needed
    //     // TODO hardcoded to 1000 temporarily
    //     while self.blocks.len() > 1000 {
    //         if let Some((block_hash, _)) = self.blocks.pop_last() {
    //             // Remove blocks_transactions entry for given block
    //             let transactions = self.blocks_transactions.remove(&block_hash).unwrap();

    //             for transaction in transactions {
    //                 // If block_hash is only entry in transactions_blocks...
    //                 // transactions.remove(transaction_id)
    //                 // transactions_blocks.remove(transaction_id)
    //                 // outputs.remove()
    //                 // acceptances.remove()
    //             }
    //         }
    //     }
    // }
}
