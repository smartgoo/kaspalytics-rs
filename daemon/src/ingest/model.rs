use chrono::{DateTime, Utc};
use kaspa_consensus_core::subnets::SubnetworkId;
use kaspa_consensus_core::tx::{ScriptPublicKey, TransactionId};
use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    RpcBlock, RpcTransaction, RpcTransactionInput, RpcTransactionOutpoint, RpcTransactionOutput,
};
use kaspa_txscript::script_class::ScriptClass;
use serde::{Deserialize, Serialize};

pub type CacheTransactionId = TransactionId;
pub type CacheScriptPublicKey = ScriptPublicKey;
pub type CacheScriptClass = ScriptClass;

#[derive(Serialize, Deserialize, Clone)]
pub struct CacheBlock {
    pub hash: Hash,
    pub timestamp: u64,
    pub daa_score: u64,
    pub parents: Vec<Hash>,
    pub transactions: Vec<CacheTransactionId>,
    pub is_chain_block: bool,
    pub seen_at: DateTime<Utc>,
}

impl CacheBlock {
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<Hash>() + // hash
        std::mem::size_of::<u64>() + // timestamp
        std::mem::size_of::<u64>() + // daa_score
        std::mem::size_of::<Vec<Hash>>() + (self.parents.len() * std::mem::size_of::<Hash>()) + // parents
        std::mem::size_of::<Vec<CacheTransactionId>>() + (self.transactions.len() * std::mem::size_of::<CacheTransactionId>()) + // transactions
        std::mem::size_of::<bool>() + // is_chain_block
        std::mem::size_of::<DateTime<Utc>>() // seen_at
    }
}

impl From<RpcBlock> for CacheBlock {
    fn from(value: RpcBlock) -> Self {
        CacheBlock {
            hash: value.header.hash,
            timestamp: value.header.timestamp,
            daa_score: value.header.daa_score,
            parents: value.header.parents_by_level[0].clone(),
            transactions: value
                .transactions
                .iter()
                .map(|tx| tx.verbose_data.clone().unwrap().transaction_id)
                .collect(),
            is_chain_block: value.verbose_data.unwrap().is_chain_block,
            seen_at: Utc::now(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheTransactionOutput {
    pub value: u64,
    pub script_public_key: CacheScriptPublicKey,
    pub script_public_key_type: CacheScriptClass,
    pub script_public_key_address: String,
}

impl CacheTransactionOutput {
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<u64>() + // value
        std::mem::size_of::<CacheScriptPublicKey>() + // script_public_key
        std::mem::size_of::<CacheScriptClass>() + // script_public_key_type
        std::mem::size_of::<String>() + self.script_public_key_address.len() // script_public_key_address
    }
}

impl From<RpcTransactionOutput> for CacheTransactionOutput {
    fn from(value: RpcTransactionOutput) -> Self {
        Self {
            value: value.value,
            script_public_key: value.script_public_key,
            script_public_key_type: value.verbose_data.clone().unwrap().script_public_key_type,
            script_public_key_address: value
                .verbose_data
                .unwrap()
                .script_public_key_address
                .to_string(),
        }
    }
}

pub type CacheTransactionOutpoint = RpcTransactionOutpoint;

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheTransactionInput {
    pub previous_outpoint: CacheTransactionOutpoint,
    pub signature_script: Vec<u8>,
    pub sequence: u64,
    pub sig_op_count: u8,
}

impl CacheTransactionInput {
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<CacheTransactionOutpoint>() + // previous_outpoint
        std::mem::size_of::<Vec<u8>>() + self.signature_script.len() + // signature_script
        std::mem::size_of::<u64>() + // sequence
        std::mem::size_of::<u8>() // sig_op_count
    }
}

impl From<RpcTransactionInput> for CacheTransactionInput {
    fn from(value: RpcTransactionInput) -> Self {
        Self {
            previous_outpoint: value.previous_outpoint,
            signature_script: value.signature_script,
            sequence: value.sequence,
            sig_op_count: value.sig_op_count,
        }
    }
}

pub type CacheSubnetworkId = SubnetworkId;

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheTransaction {
    pub id: CacheTransactionId,
    pub inputs: Vec<CacheTransactionInput>,
    pub outputs: Vec<CacheTransactionOutput>,
    pub lock_time: u64,
    pub subnetwork_id: CacheSubnetworkId,
    pub gas: u64,
    pub payload: Vec<u8>,
    pub mass: u64,
    pub compute_mass: u64,
    pub blocks: Vec<Hash>,
    pub block_time: u64,
    pub accepting_block_hash: Option<Hash>,
}

impl CacheTransaction {
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<CacheTransactionId>() + // id
        std::mem::size_of::<Vec<CacheTransactionInput>>() + self.inputs.iter().map(|i| i.estimate_size()).sum::<usize>() + // inputs
        std::mem::size_of::<Vec<CacheTransactionOutput>>() + self.outputs.iter().map(|o| o.estimate_size()).sum::<usize>() + // outputs
        std::mem::size_of::<u64>() + // lock_time
        std::mem::size_of::<CacheSubnetworkId>() + // subnetwork_id
        std::mem::size_of::<u64>() + // gas
        std::mem::size_of::<Vec<u8>>() + self.payload.len() + // payload
        std::mem::size_of::<u64>() + // mass
        std::mem::size_of::<u64>() + // compute_mass
        std::mem::size_of::<Vec<Hash>>() + (self.blocks.len() * std::mem::size_of::<Hash>()) + // blocks
        std::mem::size_of::<u64>() + // block_time
        std::mem::size_of::<Option<Hash>>() // accepting_block_hash
    }
}

impl From<RpcTransaction> for CacheTransaction {
    fn from(value: RpcTransaction) -> Self {
        CacheTransaction {
            id: value.verbose_data.clone().unwrap().transaction_id,
            inputs: value
                .inputs
                .iter()
                .map(|o| CacheTransactionInput::from(o.clone()))
                .collect(),
            outputs: value
                .outputs
                .iter()
                .map(|o| CacheTransactionOutput::from(o.clone()))
                .collect(),
            lock_time: value.lock_time,
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

pub struct PrunedBlock {
    pub hash: Hash,
    pub timestamp: u64,
    pub daa_score: u64,
    pub transactions: Vec<CacheTransaction>,
}
