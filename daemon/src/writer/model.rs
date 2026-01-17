use crate::ingest::model::{
    CacheBlock, CacheTransaction, CacheTransactionInput, CacheTransactionOutput,
};
use chrono::{DateTime, Utc};
use kaspa_consensus_core::subnets::{SUBNETWORK_ID_COINBASE, SUBNETWORK_ID_NATIVE};
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
    pub index: i16,
    pub block_time: DateTime<Utc>,
}

impl DbBlockTransaction {
    pub fn new(block_hash: Hash, transaction_id: Hash, index: u16, block_time: u64) -> Self {
        Self {
            block_hash: block_hash.as_bytes().to_vec(),
            transaction_id: transaction_id.as_bytes().to_vec(),
            index: index as i16,
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
        }
    }
}

pub struct DbTransaction {
    pub transaction_id: Vec<u8>,
    pub version: i16,
    pub lock_time: i64,
    pub subnetwork_id: i64,
    pub gas: i64,
    pub mass: i64,
    pub compute_mass: i64,
    pub accepting_block_hash: Option<Vec<u8>>,
    pub block_time: DateTime<Utc>,
    pub protocol_id: Option<i64>,
    pub total_input_amount: i64,
    pub total_output_amount: i64,
    pub payload: Vec<u8>,
}

impl From<CacheTransaction> for DbTransaction {
    fn from(value: CacheTransaction) -> Self {
        let subnetwork_id = match value.subnetwork_id {
            SUBNETWORK_ID_NATIVE => 0,
            SUBNETWORK_ID_COINBASE => 1,
            _ => panic!("Unknown subnetwork ID {:?}", value.subnetwork_id),
        };

        let protocol_id = value.protocol.map(|v| v as i64);

        let total_input_amount = value
            .inputs
            .iter()
            .map(|input| {
                input
                    .utxo_entry
                    .as_ref()
                    .map(|utxo_entry| utxo_entry.amount)
                    .unwrap_or(0)
            })
            .sum::<u64>() as i64;

        let total_output_amount =
            value.outputs.iter().map(|output| output.value).sum::<u64>() as i64;

        DbTransaction {
            transaction_id: value.id.as_bytes().to_vec(),
            version: value.version as i16,
            lock_time: value.lock_time as i64,
            subnetwork_id: subnetwork_id as i64,
            gas: value.gas as i64,
            mass: value.mass as i64,
            compute_mass: value.compute_mass as i64,
            accepting_block_hash: value.accepting_block_hash.map(|h| h.as_bytes().to_vec()),
            block_time: DateTime::<Utc>::from_timestamp_millis(value.block_time as i64).unwrap(),
            protocol_id,
            total_input_amount,
            total_output_amount,
            payload: value.payload.clone(),
        }
    }
}

pub struct DbTransactionInput {
    pub transaction_id: Vec<u8>,
    pub index: i16,
    pub previous_outpoint_transaction_id: Vec<u8>,
    pub previous_outpoint_index: i16,
    pub signature_script: Vec<u8>,
    pub sig_op_count: i16,
    pub utxo_amount: Option<i64>,
    pub utxo_script_public_key: Option<Vec<u8>>,
    pub utxo_is_coinbase: Option<bool>,
    pub utxo_script_public_key_type: Option<i16>,
    pub utxo_script_public_key_address: Option<String>,
    pub block_time: DateTime<Utc>,
}

impl DbTransactionInput {
    pub fn new(
        transaction_id: Hash,
        index: u32,
        input: &CacheTransactionInput,
        block_time: u64,
    ) -> Self {
        let (
            utxo_amount,
            utxo_script_public_key,
            utxo_is_coinbase,
            utxo_script_public_key_type,
            utxo_script_public_key_address,
        ) = if let Some(cache_utxo) = input.utxo_entry.clone() {
            let script_public_key_type = cache_utxo.script_public_key_type.map(|spkt| spkt as i16);
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
            transaction_id: transaction_id.as_bytes().to_vec(),
            index: index as i16,
            previous_outpoint_transaction_id: input
                .previous_outpoint
                .transaction_id
                .unwrap()
                .as_bytes()
                .to_vec(),
            previous_outpoint_index: input.previous_outpoint.index.unwrap() as i16,
            signature_script: input.signature_script.clone(),
            sig_op_count: input.sig_op_count as i16,
            utxo_amount,
            utxo_script_public_key,
            utxo_is_coinbase,
            utxo_script_public_key_type,
            utxo_script_public_key_address,
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
        }
    }
}

pub struct DbTransactionOutput {
    pub transaction_id: Vec<u8>,
    pub index: i16,
    pub amount: i64,
    pub is_coinbase: bool,
    pub script_public_key: Vec<u8>,
    pub script_public_key_type: i16,
    pub script_public_key_address: String,
    pub block_time: DateTime<Utc>,
}

impl DbTransactionOutput {
    pub fn new(
        transaction_id: Hash,
        index: u32,
        output: &CacheTransactionOutput,
        block_time: u64,
        is_coinbase: bool,
    ) -> Self {
        DbTransactionOutput {
            transaction_id: transaction_id.as_bytes().to_vec(),
            index: index as i16,
            amount: output.value as i64,
            is_coinbase,
            script_public_key: output.script_public_key.script().to_vec(),
            script_public_key_type: output.script_public_key_type.clone() as i16,
            script_public_key_address: output.script_public_key_address.clone(),
            block_time: DateTime::<Utc>::from_timestamp_millis(block_time as i64).unwrap(),
        }
    }
}
