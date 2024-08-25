use kaspa_rpc_core::{
    RpcBlock, RpcBlockVerboseData, RpcHeader, RpcScriptPublicKey, RpcSubnetworkId, RpcTransaction,
    RpcTransactionId, RpcTransactionOutpoint, RpcTransactionVerboseData,
};

#[derive(Clone)]
pub struct CacheBlock {
    pub header: RpcHeader,
    pub verbose_data: Option<RpcBlockVerboseData>,
}

impl From<RpcBlock> for CacheBlock {
    fn from(value: RpcBlock) -> Self {
        Self {
            header: value.header,
            verbose_data: value.verbose_data,
        }
    }
}

#[derive(Clone)]
pub struct CacheTransaction {
    version: u16,
    lock_time: u64,
    subnetwork_id: RpcSubnetworkId,
    gas: u64,
    payload: Vec<u8>,
    mass: u64,
    verbose_data: Option<RpcTransactionVerboseData>,
}

impl From<RpcTransaction> for CacheTransaction {
    fn from(value: RpcTransaction) -> Self {
        Self {
            version: value.version,
            lock_time: value.lock_time,
            subnetwork_id: value.subnetwork_id,
            gas: value.gas,
            payload: value.payload,
            mass: value.mass,
            verbose_data: value.verbose_data,
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct CacheTransactionOutpoint {
    pub transaction_id: RpcTransactionId,
    pub index: u32,
}

impl From<RpcTransactionOutpoint> for CacheTransactionOutpoint {
    fn from(value: RpcTransactionOutpoint) -> Self {
        Self {
            transaction_id: value.transaction_id,
            index: value.index,
        }
    }
}

pub struct DbTransactionOutput {
    pub transaction_id: RpcTransactionId,
    pub index: u32,
    pub value: u64,
    pub script_public_key: RpcScriptPublicKey,
}
