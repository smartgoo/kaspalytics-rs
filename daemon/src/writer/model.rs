use crate::cache::model::{
    CacheTransaction, CacheTransactionInput, CacheTransactionOutput, PrunedBlock,
};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;

pub struct DbBlock {
    pub block_time: DateTime<Utc>,
    pub block_hash: Vec<u8>,
    pub daa_score: i64,
}

impl From<PrunedBlock> for DbBlock {
    fn from(value: PrunedBlock) -> Self {
        DbBlock {
            block_time: DateTime::<Utc>::from_timestamp_millis(value.timestamp as i64).unwrap(),
            block_hash: value.hash.as_bytes().to_vec(),
            daa_score: value.daa_score as i64,
        }
    }
}

pub struct DbTransaction {
    pub block_time: DateTime<Utc>,
    pub block_hash: Vec<u8>,
    pub transaction_id: Vec<u8>,
    pub subnetwork_id: String,
    pub payload: Vec<u8>,
    pub mass: i64,
    pub compute_mass: i64,
    pub accepting_block_hash: Option<Vec<u8>>,
}

impl DbTransaction {
    pub fn new(block_hash: Hash, transaction: &CacheTransaction) -> Self {
        DbTransaction {
            block_time: DateTime::<Utc>::from_timestamp_millis(transaction.block_time as i64)
                .unwrap(),
            block_hash: block_hash.as_bytes().to_vec(),
            transaction_id: transaction.id.as_bytes().to_vec(),
            subnetwork_id: transaction.subnetwork_id.to_string(),
            payload: transaction.payload.clone(),
            mass: transaction.mass as i64,
            compute_mass: transaction.compute_mass as i64,
            accepting_block_hash: transaction
                .accepting_block_hash
                .map(|h| h.as_bytes().to_vec()),
        }
    }
}

pub struct DbTransactionInput {
    pub block_time: DateTime<Utc>,
    pub block_hash: Vec<u8>,
    pub transaction_id: Vec<u8>,
    pub index: i32,
    pub previous_outpoint_transaction_id: Vec<u8>,
    pub previous_outpoint_index: i32,
    pub signature_script: Vec<u8>,
    pub sig_op_count: i8,
}

impl DbTransactionInput {
    pub fn new(
        block_hash: Hash,
        block_time: u64,
        transaction_id: Hash,
        index: u32,
        input: &CacheTransactionInput,
    ) -> Self {
        DbTransactionInput {
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
            block_hash: block_hash.as_bytes().to_vec(),
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
        }
    }
}

pub struct DbTransactionOutput {
    pub block_time: DateTime<Utc>,
    pub block_hash: Vec<u8>,
    pub transaction_id: Vec<u8>,
    pub index: i32,
    pub amount: i64,
    pub script_public_key: Vec<u8>,
    pub script_public_key_address: String,
}

impl DbTransactionOutput {
    pub fn new(
        block_hash: Hash,
        block_time: u64,
        transaction_id: Hash,
        index: u32,
        output: &CacheTransactionOutput,
    ) -> Self {
        DbTransactionOutput {
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
            block_hash: block_hash.as_bytes().to_vec(),
            transaction_id: transaction_id.as_bytes().to_vec(),
            index: index as i32,
            amount: output.value as i64,
            script_public_key: output.script_public_key.script().to_vec(),
            script_public_key_address: output.script_public_key_address.clone(),
        }
    }
}
