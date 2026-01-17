use super::super::super::AppState;
use crate::ingest::cache::Reader;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;
use kaspalytics_utils::log::LogTarget;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Serialize, Deserialize, Clone)]
pub struct TransactionResponse {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,
    #[serde(rename = "transactionData")]
    pub transaction_data: Option<TransactionData>,
    pub inputs: Vec<TransactionInput>,
    pub outputs: Vec<TransactionOutput>,
    pub blocks: Vec<BlockRef>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TransactionData {
    pub block_hash: String,
    pub transaction_id: String,
    pub block_time: DateTime<Utc>,
    pub version: i16,
    pub lock_time: i64,
    pub subnetwork_id: String,
    pub gas: i64,
    pub payload: String,
    pub mass: i64,
    pub compute_mass: i64,
    pub protocol_id: Option<i32>,
    pub accepting_block_hash: String,
    #[serde(rename = "isCoinbase")]
    pub is_coinbase: bool,
    #[serde(rename = "totalInputAmount")]
    pub total_input_amount: i64,
    #[serde(rename = "totalOutputAmount")]
    pub total_output_amount: i64,
    pub fee: i64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TransactionInput {
    pub index: i16,
    pub previous_outpoint_transaction_id: String,
    pub previous_outpoint_index: i16,
    pub signature_script: String,
    pub sequence: Option<i64>,
    pub sig_op_count: i16,
    pub utxo_amount: Option<i64>,
    pub utxo_script_public_key: Option<String>,
    pub utxo_is_coinbase: Option<bool>,
    pub utxo_script_public_key_type: Option<i16>,
    pub utxo_script_public_key_address: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TransactionOutput {
    pub index: i16,
    pub amount: i64,
    pub script_public_key: String,
    pub script_public_key_type: i16,
    pub script_public_key_address: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockRef {
    pub block_hash: String,
}

fn is_valid_transaction_id(tx_id: &str) -> bool {
    tx_id.len() == 64 && tx_id.chars().all(|c| c.is_ascii_hexdigit())
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex)
}

fn convert_cache_transaction_to_response(
    transaction_id: String,
    cache_tx: &crate::ingest::model::CacheTransaction,
) -> TransactionResponse {
    use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;

    let is_coinbase = cache_tx.subnetwork_id == SUBNETWORK_ID_COINBASE;

    // Convert inputs
    let mut total_input_amount = 0i64;
    let inputs: Vec<TransactionInput> = cache_tx
        .inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let utxo_amount = input.utxo_entry.as_ref().map(|utxo| utxo.amount as i64);
            if let Some(amount) = utxo_amount {
                total_input_amount += amount;
            }

            TransactionInput {
                index: index as i16,
                previous_outpoint_transaction_id: hex::encode(
                    input.previous_outpoint.transaction_id.unwrap().as_bytes(),
                ),
                previous_outpoint_index: input.previous_outpoint.index.unwrap() as i16,
                signature_script: hex::encode(&input.signature_script),
                sequence: Some(input.sequence as i64),
                sig_op_count: input.sig_op_count as i16,
                utxo_amount,
                utxo_script_public_key: input
                    .utxo_entry
                    .as_ref()
                    .map(|utxo| hex::encode(utxo.script_public_key.script())),
                utxo_is_coinbase: input.utxo_entry.as_ref().map(|utxo| utxo.is_coinbase),
                utxo_script_public_key_type: input.utxo_entry.as_ref().and_then(|utxo| {
                    utxo.script_public_key_type
                        .as_ref()
                        .map(|t| t.clone() as i16)
                }),
                utxo_script_public_key_address: input
                    .utxo_entry
                    .as_ref()
                    .and_then(|utxo| utxo.script_public_key_address.clone()),
            }
        })
        .collect();

    // Convert outputs
    let mut total_output_amount = 0i64;
    let outputs: Vec<TransactionOutput> = cache_tx
        .outputs
        .iter()
        .enumerate()
        .map(|(index, output)| {
            total_output_amount += output.value as i64;
            TransactionOutput {
                index: index as i16,
                amount: output.value as i64,
                script_public_key: hex::encode(output.script_public_key.script()),
                script_public_key_type: output.script_public_key_type.clone() as i16,
                script_public_key_address: Some(output.script_public_key_address.clone()),
            }
        })
        .collect();

    // Convert blocks
    let blocks: Vec<BlockRef> = cache_tx
        .blocks
        .iter()
        .map(|block_hash| BlockRef {
            block_hash: hex::encode(block_hash.as_bytes()),
        })
        .collect();

    // Calculate fee
    let fee = if is_coinbase {
        0
    } else {
        std::cmp::max(0, total_input_amount - total_output_amount)
    };

    // Format subnetwork ID
    let subnetwork_id_text =
        if cache_tx.subnetwork_id == kaspa_consensus_core::subnets::SUBNETWORK_ID_NATIVE {
            "0000000000000000000000000000000000000000".to_string()
        } else if cache_tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
            "0100000000000000000000000000000000000000".to_string()
        } else {
            format!("{:?}", cache_tx.subnetwork_id)
        };

    // Create transaction data
    let transaction_data = TransactionData {
        block_hash: cache_tx
            .blocks
            .first()
            .map_or_else(|| "".to_string(), |h| hex::encode(h.as_bytes())),
        transaction_id: hex::encode(cache_tx.id.as_bytes()),
        block_time: DateTime::from_timestamp_millis(cache_tx.block_time as i64).unwrap_or_default(),
        version: cache_tx.version as i16,
        lock_time: cache_tx.lock_time as i64,
        subnetwork_id: subnetwork_id_text,
        gas: cache_tx.gas as i64,
        payload: hex::encode(&cache_tx.payload),
        mass: cache_tx.mass as i64,
        compute_mass: cache_tx.compute_mass as i64,
        protocol_id: Some(0), // Cache doesn't store protocol_id, using default
        accepting_block_hash: cache_tx
            .accepting_block_hash
            .map_or_else(|| "".to_string(), |h| hex::encode(h.as_bytes())),
        is_coinbase,
        total_input_amount,
        total_output_amount,
        fee,
    };

    TransactionResponse {
        transaction_id,
        transaction_data: Some(transaction_data),
        inputs,
        outputs,
        blocks,
        status: "success".to_string(),
        error: None,
    }
}

pub async fn get_transaction(
    Path(transaction_id): Path<String>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<TransactionResponse>), (StatusCode, Json<TransactionResponse>)> {
    // Validate transaction ID format
    if !is_valid_transaction_id(&transaction_id) {
        let response = TransactionResponse {
            transaction_id: transaction_id.clone(),
            transaction_data: None,
            inputs: vec![],
            outputs: vec![],
            blocks: vec![],
            status: "invalid_format".to_string(),
            error: Some("Invalid transaction ID format. Transaction IDs must be 64-character hexadecimal strings.".to_string()),
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    // Convert hex string to bytes for database query
    let transaction_id_bytes = match hex_to_bytes(&transaction_id) {
        Ok(bytes) => bytes,
        Err(_) => {
            let response = TransactionResponse {
                transaction_id: transaction_id.clone(),
                transaction_data: None,
                inputs: vec![],
                outputs: vec![],
                blocks: vec![],
                status: "invalid_format".to_string(),
                error: Some("Invalid hexadecimal format".to_string()),
            };
            return Err((StatusCode::BAD_REQUEST, Json(response)));
        }
    };

    // Convert bytes to Hash for DagCache lookup
    let transaction_hash =
        Hash::from_bytes(transaction_id_bytes.clone().try_into().map_err(|_| {
            let response = TransactionResponse {
                transaction_id: transaction_id.clone(),
                transaction_data: None,
                inputs: vec![],
                outputs: vec![],
                blocks: vec![],
                status: "invalid_format".to_string(),
                error: Some("Invalid transaction ID length".to_string()),
            };
            (StatusCode::BAD_REQUEST, Json(response))
        })?);

    // First, check DagCache for the transaction
    if let Some(cache_tx) = state.context.dag_cache.get_transaction(&transaction_hash) {
        log::debug!(
            target: LogTarget::Web.as_str(),
            "Transaction {} found in DagCache",
            transaction_id
        );
        let response = convert_cache_transaction_to_response(transaction_id.clone(), &cache_tx);

        // Add cache headers for found transaction
        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            HeaderValue::from_static("public, max-age=300, s-maxage=300"),
        );

        return Ok((headers, Json(response)));
    }

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Transaction {} not found in DagCache, querying database",
        transaction_id,
    );

    // Database queries
    let transaction_query = r#"
        SELECT 
            encode(bt.block_hash, 'hex') as block_hash,
            encode(t.transaction_id, 'hex') as transaction_id,
            t.block_time,
            t.version,
            t.lock_time,
            t.subnetwork_id,
            t.gas,
            encode(t.payload, 'hex') as payload,
            t.mass,
            t.compute_mass,
            t.protocol_id,
            encode(t.accepting_block_hash, 'hex') as accepting_block_hash
        FROM kaspad.transactions t
        INNER JOIN kaspad.blocks_transactions bt ON t.transaction_id = bt.transaction_id
        WHERE t.transaction_id = $1
        LIMIT 1
    "#;

    let inputs_query = r#"
        SELECT 
            index,
            encode(previous_outpoint_transaction_id, 'hex') as previous_outpoint_transaction_id,
            previous_outpoint_index,
            encode(signature_script, 'hex') as signature_script,
            sequence,
            sig_op_count,
            utxo_amount,
            encode(utxo_script_public_key, 'hex') as utxo_script_public_key,
            utxo_is_coinbase,
            utxo_script_public_key_type,
            utxo_script_public_key_address
        FROM kaspad.transactions_inputs 
        WHERE transaction_id = $1
        ORDER BY index ASC
    "#;

    let outputs_query = r#"
        SELECT 
            index,
            amount,
            encode(script_public_key, 'hex') as script_public_key,
            script_public_key_type,
            script_public_key_address
        FROM kaspad.transactions_outputs 
        WHERE transaction_id = $1
        ORDER BY index ASC
    "#;

    let blocks_query = r#"
        SELECT 
            encode(bt.block_hash, 'hex') as block_hash
        FROM kaspad.blocks_transactions bt
        WHERE bt.transaction_id = $1
    "#;

    // Execute all queries concurrently
    let (transaction_result, inputs_result, outputs_result, blocks_result) = tokio::try_join!(
        sqlx::query(transaction_query)
            .bind(&transaction_id_bytes)
            .fetch_optional(&state.context.pg_pool),
        sqlx::query(inputs_query)
            .bind(&transaction_id_bytes)
            .fetch_all(&state.context.pg_pool),
        sqlx::query(outputs_query)
            .bind(&transaction_id_bytes)
            .fetch_all(&state.context.pg_pool),
        sqlx::query(blocks_query)
            .bind(&transaction_id_bytes)
            .fetch_all(&state.context.pg_pool)
    )
    .map_err(|e| {
        log::error!(
            target: LogTarget::WebErr.as_str(),
            "Database error fetching transaction {}: {}",
            transaction_id,
            e,
        );
        let response = TransactionResponse {
            transaction_id: transaction_id.clone(),
            transaction_data: None,
            inputs: vec![],
            outputs: vec![],
            blocks: vec![],
            status: "error".to_string(),
            error: Some("Failed to fetch transaction data".to_string()),
        };
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    })?;

    // Check if transaction exists
    let transaction_row = match transaction_result {
        Some(row) => row,
        None => {
            let response = TransactionResponse {
                transaction_id: transaction_id.clone(),
                transaction_data: None,
                inputs: vec![],
                outputs: vec![],
                blocks: vec![],
                status: "not_found".to_string(),
                error: Some("Transaction not found".to_string()),
            };
            return Err((StatusCode::NOT_FOUND, Json(response)));
        }
    };

    // Parse transaction data
    let subnetwork_id: i32 = transaction_row.get("subnetwork_id");
    let is_coinbase = subnetwork_id == 1;

    let subnetwork_id_text = match subnetwork_id {
        0 => "0000000000000000000000000000000000000000".to_string(),
        1 => "0100000000000000000000000000000000000000".to_string(),
        _ => subnetwork_id.to_string(),
    };

    // Parse inputs
    let mut inputs = Vec::new();
    let mut total_input_amount = 0i64;

    for row in inputs_result {
        let utxo_amount: Option<i64> = row.try_get("utxo_amount").ok();
        if let Some(amount) = utxo_amount {
            total_input_amount += amount;
        }

        inputs.push(TransactionInput {
            index: row.get("index"),
            previous_outpoint_transaction_id: row.get("previous_outpoint_transaction_id"),
            previous_outpoint_index: row.get("previous_outpoint_index"),
            signature_script: row.get("signature_script"),
            sequence: row.get("sequence"),
            sig_op_count: row.get("sig_op_count"),
            utxo_amount,
            utxo_script_public_key: row.try_get("utxo_script_public_key").ok(),
            utxo_is_coinbase: row.try_get("utxo_is_coinbase").ok(),
            utxo_script_public_key_type: row.try_get("utxo_script_public_key_type").ok(),
            utxo_script_public_key_address: row.try_get("utxo_script_public_key_address").ok(),
        });
    }

    // Parse outputs
    let mut outputs = Vec::new();
    let mut total_output_amount = 0i64;

    for row in outputs_result {
        let amount: i64 = row.get("amount");
        total_output_amount += amount;

        outputs.push(TransactionOutput {
            index: row.get("index"),
            amount,
            script_public_key: row.get("script_public_key"),
            script_public_key_type: row.get("script_public_key_type"),
            script_public_key_address: row.try_get("script_public_key_address").ok(),
        });
    }

    // Parse blocks
    let blocks: Vec<BlockRef> = blocks_result
        .into_iter()
        .map(|row| BlockRef {
            block_hash: row.get("block_hash"),
        })
        .collect();

    // Calculate fee
    let fee = if is_coinbase {
        0
    } else {
        std::cmp::max(0, total_input_amount - total_output_amount)
    };

    // Build transaction data
    let transaction_data = TransactionData {
        block_hash: transaction_row.get("block_hash"),
        transaction_id: transaction_row.get("transaction_id"),
        block_time: transaction_row.get("block_time"),
        version: transaction_row.get("version"),
        lock_time: transaction_row.get("lock_time"),
        subnetwork_id: subnetwork_id_text,
        gas: transaction_row.get("gas"),
        payload: transaction_row.get("payload"),
        mass: transaction_row.get("mass"),
        compute_mass: transaction_row.get("compute_mass"),
        protocol_id: transaction_row.try_get("protocol_id").ok(),
        accepting_block_hash: transaction_row.get("accepting_block_hash"),
        is_coinbase,
        total_input_amount,
        total_output_amount,
        fee,
    };

    // Build response
    let response = TransactionResponse {
        transaction_id: transaction_id.clone(),
        transaction_data: Some(transaction_data),
        inputs,
        outputs,
        blocks,
        status: "success".to_string(),
        error: None,
    };

    // Add cache headers for found transaction
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=300, s-maxage=300"),
    );

    Ok((headers, Json(response)))
}
