use crate::ingest::model::{
    CacheBlock, CacheTransaction, CacheTransactionInput, CacheTransactionOutput,
};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;

pub struct DbBlock {
    pub block_hash: Vec<u8>,
    pub version: i16,
    // pub parent_hashes: Vec<Vec<u8>>,
    pub hash_merkle_root: Vec<u8>,
    pub accepted_id_merkle_root: Vec<u8>,
    pub utxo_commitment: Vec<u8>,
    pub block_time: DateTime<Utc>,
    pub bits: i32,
    pub nonce: i64,
    pub daa_score: i64,
    pub blue_work: Vec<u8>,
    pub blue_score: i64,
    pub pruning_point: Vec<u8>,
    // Verbose data fields
    pub difficulty: f64,
    pub selected_parent_hash: Vec<u8>,
    pub is_chain_block: bool,
}

impl From<CacheBlock> for DbBlock {
    fn from(value: CacheBlock) -> Self {
        DbBlock {
            block_hash: value.hash.as_bytes().to_vec(),
            version: value.version as i16,
            // parent_hashes: value.parent_hashes.iter().map(|h| h.as_bytes().to_vec()).collect(),
            hash_merkle_root: value.hash_merkle_root.as_bytes().to_vec(),
            accepted_id_merkle_root: value.accepted_id_merkle_root.as_bytes().to_vec(),
            utxo_commitment: value.utxo_commitment.as_bytes().to_vec(),
            block_time: DateTime::<Utc>::from_timestamp_millis(value.timestamp as i64).unwrap(),
            bits: value.bits as i32,
            nonce: value.nonce as i64,
            daa_score: value.daa_score as i64,
            blue_work: value.blue_work.to_be_bytes().to_vec(),
            blue_score: value.blue_score as i64,
            pruning_point: value.pruning_point.as_bytes().to_vec(),
            difficulty: value.difficulty,
            selected_parent_hash: value.selected_parent_hash.as_bytes().to_vec(),
            is_chain_block: value.is_chain_block,
        }
    }
}

pub struct DbBlockParent {
    pub block_hash: Vec<u8>,
    pub parent_hash: Vec<u8>,
    pub block_time: DateTime<Utc>,
}

impl DbBlockParent {
    pub fn new(block_hash: Hash, parent_hash: Hash, block_time: u64) -> Self {
        Self {
            block_hash: block_hash.as_bytes().to_vec(),
            parent_hash: parent_hash.as_bytes().to_vec(),
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
        }
    }
}

pub struct DbBlockTransaction {
    pub block_hash: Vec<u8>,
    pub transaction_id: Vec<u8>,
    pub block_time: DateTime<Utc>,
}

impl DbBlockTransaction {
    pub fn new(block_hash: Hash, transaction_id: Hash, block_time: u64) -> Self {
        Self {
            block_hash: block_hash.as_bytes().to_vec(),
            transaction_id: transaction_id.as_bytes().to_vec(),
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
        }
    }
}

pub struct DbTransaction {
    pub block_time: DateTime<Utc>,
    pub transaction_id: Vec<u8>,
    pub version: i16,
    pub lock_time: i64,
    pub subnetwork_id: String,
    pub gas: i64,
    pub payload: Vec<u8>,
    pub mass: i64,
    pub compute_mass: i64,
    pub accepting_block_hash: Option<Vec<u8>>,
}

impl From<CacheTransaction> for DbTransaction {
    fn from(value: CacheTransaction) -> Self {
        DbTransaction {
            block_time: DateTime::<Utc>::from_timestamp_millis(value.block_time as i64).unwrap(),
            transaction_id: value.id.as_bytes().to_vec(),
            version: value.version as i16,
            lock_time: value.lock_time as i64,
            subnetwork_id: value.subnetwork_id.to_string(),
            gas: value.gas as i64,
            payload: value.payload.clone(),
            mass: value.mass as i64,
            compute_mass: value.compute_mass as i64,
            accepting_block_hash: value.accepting_block_hash.map(|h| h.as_bytes().to_vec()),
        }
    }
}

pub struct DbTransactionInput {
    pub block_time: DateTime<Utc>,
    pub transaction_id: Vec<u8>,
    pub index: i32,
    pub previous_outpoint_transaction_id: Vec<u8>,
    pub previous_outpoint_index: i32,
    pub signature_script: Vec<u8>,
    pub sig_op_count: i8,
    pub utxo_amount: Option<i64>,
    pub utxo_script_public_key: Option<Vec<u8>>,
    pub utxo_is_coinbase: Option<bool>,
    pub utxo_script_public_key_type: Option<i8>,
    pub utxo_script_public_key_address: Option<String>,
}

impl DbTransactionInput {
    pub fn new(
        block_time: u64,
        transaction_id: Hash,
        index: u32,
        input: &CacheTransactionInput,
    ) -> Self {
        let (
            utxo_amount,
            utxo_script_public_key,
            utxo_is_coinbase,
            utxo_script_public_key_type,
            utxo_script_public_key_address,
        ) = if let Some(cache_utxo) = input.utxo_entry.clone() {
            let script_public_key_type = cache_utxo.script_public_key_type.map(|spkt| spkt as i8);
            // let script_public_key_type =
            //     if let Some(script_public_key_type) = cache_utxo.script_public_key_type {
            //         Some(script_public_key_type as i8)
            //     } else {
            //         None
            //     };

            let script_public_key_address = cache_utxo
                .script_public_key_address
                .map(|spka| spka.to_string());
            // let script_public_key_address =
            //     if let Some(script_public_key_address) = cache_utxo.script_public_key_address {
            //         Some(script_public_key_address.to_string())
            //     } else {
            //         None
            //     };

            (
                Some(cache_utxo.amount as i64),
                Some(cache_utxo.script_public_key.script().into()),
                Some(cache_utxo.is_coinbase),
                script_public_key_type,
                script_public_key_address,
            )
        } else {
            (None, None, None, None, None)
        };

        DbTransactionInput {
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
            transaction_id: transaction_id.as_bytes().to_vec(),
            index: index as i32,
            previous_outpoint_transaction_id: input
                .previous_outpoint
                .transaction_id
                .as_bytes()
                .to_vec(),
            previous_outpoint_index: input.previous_outpoint.index as i32,
            signature_script: input.signature_script.clone(),
            sig_op_count: input.sig_op_count as i8,
            utxo_amount,
            utxo_script_public_key,
            utxo_is_coinbase,
            utxo_script_public_key_type,
            utxo_script_public_key_address,
        }
    }
}

pub struct DbTransactionOutput {
    pub block_time: DateTime<Utc>,
    pub transaction_id: Vec<u8>,
    pub index: i32,
    pub amount: i64,
    pub script_public_key: Vec<u8>,
    pub script_public_key_address: String,
}

impl DbTransactionOutput {
    pub fn new(
        block_time: u64,
        transaction_id: Hash,
        index: u32,
        output: &CacheTransactionOutput,
    ) -> Self {
        DbTransactionOutput {
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
            transaction_id: transaction_id.as_bytes().to_vec(),
            index: index as i32,
            amount: output.value as i64,
            script_public_key: output.script_public_key.script().to_vec(),
            script_public_key_address: output.script_public_key_address.clone(),
        }
    }
}
