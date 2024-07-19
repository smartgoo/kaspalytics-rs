use crate::cache::{models::*, Cache};
use kaspa_consensus_core::tx::TransactionId;
use kaspa_rpc_core::{RpcHash, RpcTransaction, RpcTransactionId, RpcTransactionOutput};

pub struct DAGCache {
    pub blocks: Cache<RpcHash, CacheBlock>,

    pub transactions: Cache<RpcTransactionId, RpcTransaction>,

    pub blocks_transactions: Cache<RpcHash, Vec<RpcTransactionId>>,
    pub transactions_blocks: Cache<RpcTransactionId, Vec<RpcHash>>,

    pub acceptances: Cache<RpcHash, Vec<TransactionId>>,

    pub outputs: Cache<CacheTransactionOutpoint, RpcTransactionOutput>,
}

impl DAGCache {
    pub fn new() -> Self {
        Self {
            blocks: Cache::<RpcHash, CacheBlock>::new(),
            transactions: Cache::<RpcTransactionId, RpcTransaction>::new(),
            blocks_transactions: Cache::<RpcHash, Vec<RpcTransactionId>>::new(),
            transactions_blocks: Cache::<RpcTransactionId, Vec<RpcHash>>::new(),
            acceptances: Cache::<RpcHash, Vec<TransactionId>>::new(),
            outputs: Cache::<CacheTransactionOutpoint, RpcTransactionOutput>::new(),
        }
    }
}
