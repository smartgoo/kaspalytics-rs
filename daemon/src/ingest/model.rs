use crate::analysis::transactions::protocol::TransactionProtocol;
use chrono::{DateTime, Utc};
use kaspa_addresses::Address;
use kaspa_consensus_core::subnets::SubnetworkId;
use kaspa_consensus_core::tx::{ScriptPublicKey, TransactionId};
use kaspa_consensus_core::BlueWorkType;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    RpcBlock, RpcScriptClass, RpcTransaction, RpcTransactionInput, RpcTransactionOutpoint,
    RpcTransactionOutput, RpcUtxoEntry,
};
use kaspa_txscript::script_class::ScriptClass;
use serde::{Deserialize, Serialize};

pub type CacheTransactionId = TransactionId;
pub type CacheScriptPublicKey = ScriptPublicKey;
pub type CacheScriptClass = ScriptClass;

#[derive(Serialize, Deserialize, Clone)]
pub struct CacheBlock {
    // Header fields
    pub hash: Hash,
    pub version: u16,
    pub parent_hashes: Vec<Hash>,
    pub hash_merkle_root: Hash,
    pub accepted_id_merkle_root: Hash,
    pub utxo_commitment: Hash,
    pub timestamp: u64,
    pub bits: u32,
    pub nonce: u64,
    pub daa_score: u64,
    pub blue_work: BlueWorkType,
    pub blue_score: u64,
    pub pruning_point: Hash,
    // Verbose data fields
    pub difficulty: f64,
    pub selected_parent_hash: Hash,
    pub is_chain_block: bool,
    // Transactions
    pub transactions: Vec<CacheTransactionId>,
    // Misc
    pub seen_at: DateTime<Utc>,
}

// impl CacheBlock {
//     pub fn estimate_size(&self) -> usize {
//         std::mem::size_of::<Hash>() + // hash
//         std::mem::size_of::<u64>() + // timestamp
//         std::mem::size_of::<u64>() + // daa_score
//         std::mem::size_of::<Vec<Hash>>() + (self.parents.len() * std::mem::size_of::<Hash>()) + // parents
//         std::mem::size_of::<Vec<CacheTransactionId>>() + (self.transactions.len() * std::mem::size_of::<CacheTransactionId>()) + // transactions
//         std::mem::size_of::<bool>() + // is_chain_block
//         std::mem::size_of::<DateTime<Utc>>() // seen_at
//     }
// }

impl From<RpcBlock> for CacheBlock {
    fn from(value: RpcBlock) -> Self {
        let verbose_data = value.verbose_data.unwrap();
        CacheBlock {
            // Header fields
            hash: value.header.hash,
            version: value.header.version,
            parent_hashes: value.header.parents_by_level[0].clone(),
            hash_merkle_root: value.header.hash_merkle_root,
            accepted_id_merkle_root: value.header.accepted_id_merkle_root,
            utxo_commitment: value.header.utxo_commitment,
            timestamp: value.header.timestamp,
            bits: value.header.bits,
            nonce: value.header.nonce,
            daa_score: value.header.daa_score,
            blue_work: value.header.blue_work,
            blue_score: value.header.blue_score,
            pruning_point: value.header.pruning_point,
            // Verbose data fields
            difficulty: verbose_data.difficulty,
            selected_parent_hash: verbose_data.selected_parent_hash,
            is_chain_block: verbose_data.is_chain_block,
            // Transactions
            transactions: value
                .transactions
                .iter()
                .map(|tx| tx.verbose_data.clone().unwrap().transaction_id)
                .collect(),
            // Misc
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
pub struct CacheUtxoEntry {
    pub amount: u64,
    pub script_public_key: ScriptPublicKey,
    pub is_coinbase: bool,
    pub script_public_key_type: Option<RpcScriptClass>,
    pub script_public_key_address: Option<Address>,
}

impl From<RpcUtxoEntry> for CacheUtxoEntry {
    fn from(value: RpcUtxoEntry) -> Self {
        let (spkt, spka) = if let Some(verbose_data) = value.verbose_data {
            (
                verbose_data.script_public_key_type.clone(),
                verbose_data.script_public_key_address,
            )
        } else {
            (None, None)
        };

        Self {
            amount: value.amount,
            script_public_key: value.script_public_key,
            is_coinbase: value.is_coinbase,
            script_public_key_type: spkt,
            script_public_key_address: spka,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheTransactionInput {
    pub previous_outpoint: CacheTransactionOutpoint,
    pub signature_script: Vec<u8>,
    pub sequence: u64,
    pub sig_op_count: u8,
    pub utxo_entry: Option<CacheUtxoEntry>,
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
        let cache_utxo = if let Some(verbose_data) = value.verbose_data {
            verbose_data.utxo_entry.map(CacheUtxoEntry::from)
        } else {
            None
        };

        Self {
            previous_outpoint: value.previous_outpoint,
            signature_script: value.signature_script,
            sequence: value.sequence,
            sig_op_count: value.sig_op_count,
            utxo_entry: cache_utxo,
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
    pub protocol: Option<TransactionProtocol>,
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
            protocol: None,
        }
    }
}

pub struct PruningBatch {
    pub blocks: Vec<CacheBlock>,
    pub transactions: Vec<CacheTransaction>,
}

pub struct PrunedBlock {
    // Header fields
    pub hash: Hash,
    pub version: u16,
    pub parent_hashes: Vec<Hash>,
    pub hash_merkle_root: Hash,
    pub accepted_id_merkle_root: Hash,
    pub utxo_commitment: Hash,
    pub timestamp: u64,
    pub bits: u32,
    pub nonce: u64,
    pub daa_score: u64,
    pub blue_work: BlueWorkType,
    pub blue_score: u64,
    pub pruning_point: Hash,
    // Verbose data fields
    pub difficulty: f64,
    pub selected_parent_hash: Hash,
    pub is_chain_block: bool,
    // Transactions
    pub transactions: Vec<CacheTransaction>,
}

impl PrunedBlock {
    pub fn new(block: CacheBlock, transactions: Vec<CacheTransaction>) -> Self {
        Self {
            // Header fields from CacheBlock
            hash: block.hash,
            version: block.version,
            parent_hashes: block.parent_hashes,
            hash_merkle_root: block.hash_merkle_root,
            accepted_id_merkle_root: block.accepted_id_merkle_root,
            utxo_commitment: block.utxo_commitment,
            timestamp: block.timestamp,
            bits: block.bits,
            nonce: block.nonce,
            daa_score: block.daa_score,
            blue_work: block.blue_work,
            blue_score: block.blue_score,
            pruning_point: block.pruning_point,
            // Verbose data fields from CacheBlock
            difficulty: block.difficulty,
            selected_parent_hash: block.selected_parent_hash,
            is_chain_block: block.is_chain_block,
            // Transactions from parameter
            transactions,
        }
    }
}