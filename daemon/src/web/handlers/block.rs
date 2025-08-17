use super::super::AppState;
use crate::ingest::cache::Reader;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;
use kaspalytics_utils::log::LogTarget;
use serde::Serialize;
use sqlx::Row;

#[derive(Serialize)]
pub struct BlockResponse {
    #[serde(rename = "blockHash")]
    pub block_hash: String,
    #[serde(rename = "blockData")]
    pub block_data: Option<BlockData>,
    pub transactions: Vec<BlockTransaction>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct BlockData {
    pub hash: String,
    pub version: i16,
    #[serde(rename = "merkleRoot")]
    pub merkle_root: String,
    #[serde(rename = "acceptedIdMerkleRoot")]
    pub accepted_id_merkle_root: String,
    #[serde(rename = "utxoCommitment")]
    pub utxo_commitment: String,
    pub timestamp: DateTime<Utc>,
    pub bits: u32,
    pub nonce: u64,
    #[serde(rename = "blueWork")]
    pub blue_work: String,
    #[serde(rename = "daaScore")]
    pub daa_score: u64,
    #[serde(rename = "blueScore")]
    pub blue_score: u64,
    pub difficulty: f64,
    #[serde(rename = "selectedParentHash")]
    pub selected_parent_hash: String,
    #[serde(rename = "isChainBlock")]
    pub is_chain_block: bool,
    #[serde(rename = "pruningPoint")]
    pub pruning_point: String,
    #[serde(rename = "parentHashes")]
    pub parent_hashes: Vec<String>,
    #[serde(rename = "childrenHashes")]
    pub children_hashes: Vec<String>,
    #[serde(rename = "transactionCount")]
    pub transaction_count: usize,
}

#[derive(Serialize)]
pub struct BlockTransaction {
    #[serde(rename = "txId")]
    pub tx_id: String,
    #[serde(rename = "blockTime")]
    pub block_time: DateTime<Utc>,
    pub mass: u64,
    #[serde(rename = "totalInputAmount")]
    pub total_input_amount: u64,
    #[serde(rename = "totalOutputAmount")]
    pub total_output_amount: u64,
    pub fee: u64,
    #[serde(rename = "isCoinbase")]
    pub is_coinbase: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

fn is_valid_block_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex)
}

fn protocol_id_to_string(protocol_id: Option<i64>) -> Option<String> {
    match protocol_id {
        Some(0) => Some("KRC".to_string()),
        Some(1) => Some("KNS".to_string()),
        Some(2) => Some("Kasia".to_string()),
        _ => None,
    }
}

fn convert_cache_block_to_response(
    block_hash: String,
    cache_block: &crate::ingest::model::CacheBlock,
    state: &AppState,
) -> BlockResponse {
    use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;

    // Get parent hashes
    let parent_hashes: Vec<String> = cache_block
        .parent_hashes
        .iter()
        .map(|h| hex::encode(h.as_bytes()))
        .collect();

    // Get children hashes - we'll need to query the cache for this
    let mut children_hashes = Vec::new();
    for entry in state.context.dag_cache.blocks_iter() {
        let hash = entry.key();
        let block = entry.value();
        if block.parent_hashes.contains(&cache_block.hash) {
            children_hashes.push(hex::encode(hash.as_bytes()));
        }
    }
    children_hashes.sort();

    // Get transactions from cache
    let mut transactions = Vec::new();
    for tx_id in &cache_block.transactions {
        if let Some(cache_tx) = state.context.dag_cache.get_transaction(tx_id) {
            let is_coinbase = cache_tx.subnetwork_id == SUBNETWORK_ID_COINBASE;

            // Calculate totals
            let total_input_amount: u64 = cache_tx
                .inputs
                .iter()
                .filter_map(|input| input.utxo_entry.as_ref().map(|utxo| utxo.amount))
                .sum();

            let total_output_amount: u64 = cache_tx.outputs.iter().map(|output| output.value).sum();

            let fee = if is_coinbase {
                0
            } else {
                total_input_amount.saturating_sub(total_output_amount)
            };

            transactions.push(BlockTransaction {
                tx_id: hex::encode(cache_tx.id.as_bytes()),
                block_time: DateTime::from_timestamp_millis(cache_tx.block_time as i64)
                    .unwrap_or_default(),
                mass: cache_tx.mass,
                total_input_amount,
                total_output_amount,
                fee,
                is_coinbase,
                protocol: cache_tx.protocol.as_ref().map(|p| format!("{:?}", p)),
            });
        }
    }

    let block_data = BlockData {
        hash: hex::encode(cache_block.hash.as_bytes()),
        version: cache_block.version as i16,
        merkle_root: hex::encode(cache_block.hash_merkle_root.as_bytes()),
        accepted_id_merkle_root: hex::encode(cache_block.accepted_id_merkle_root.as_bytes()),
        utxo_commitment: hex::encode(cache_block.utxo_commitment.as_bytes()),
        timestamp: DateTime::from_timestamp_millis(cache_block.timestamp as i64)
            .unwrap_or_default(),
        bits: cache_block.bits,
        nonce: cache_block.nonce,
        blue_work: hex::encode(&cache_block.blue_work.to_le_bytes()),
        daa_score: cache_block.daa_score,
        blue_score: cache_block.blue_score,
        difficulty: cache_block.difficulty,
        selected_parent_hash: hex::encode(cache_block.selected_parent_hash.as_bytes()),
        is_chain_block: cache_block.is_chain_block,
        pruning_point: hex::encode(cache_block.pruning_point.as_bytes()),
        parent_hashes,
        children_hashes,
        transaction_count: transactions.len(),
    };

    BlockResponse {
        block_hash,
        block_data: Some(block_data),
        transactions,
        status: "success".to_string(),
        error: None,
    }
}

pub async fn get_block(
    Path(block_hash): Path<String>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<BlockResponse>), (StatusCode, Json<BlockResponse>)> {
    // Validate block hash format
    if !is_valid_block_hash(&block_hash) {
        let response = BlockResponse {
            block_hash: block_hash.clone(),
            block_data: None,
            transactions: vec![],
            status: "invalid_format".to_string(),
            error: Some(
                "Invalid block hash format. Block hashes must be 64-character hexadecimal strings."
                    .to_string(),
            ),
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    // Convert hex string to bytes
    let block_hash_bytes = match hex_to_bytes(&block_hash) {
        Ok(bytes) => bytes,
        Err(_) => {
            let response = BlockResponse {
                block_hash: block_hash.clone(),
                block_data: None,
                transactions: vec![],
                status: "invalid_format".to_string(),
                error: Some("Invalid hexadecimal format".to_string()),
            };
            return Err((StatusCode::BAD_REQUEST, Json(response)));
        }
    };

    // Convert bytes to Hash for DagCache lookup
    let hash_obj = Hash::from_bytes(block_hash_bytes.clone().try_into().map_err(|_| {
        let response = BlockResponse {
            block_hash: block_hash.clone(),
            block_data: None,
            transactions: vec![],
            status: "invalid_format".to_string(),
            error: Some("Invalid block hash length".to_string()),
        };
        (StatusCode::BAD_REQUEST, Json(response))
    })?);

    // First, check DagCache for the block
    if let Some(cache_block) = state.context.dag_cache.get_block(&hash_obj) {
        log::debug!(
            target: LogTarget::Web.as_str(),
            "Block {} found in DagCache",
            block_hash
        );
        let response = convert_cache_block_to_response(block_hash.clone(), &cache_block, &state);

        // Add cache headers
        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            HeaderValue::from_static("public, max-age=300, s-maxage=300"),
        );
        headers.insert(
            "Vercel-CDN-Cache-Control",
            HeaderValue::from_static("public, max-age=300"),
        );

        return Ok((headers, Json(response)));
    }

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Block {} not found in DagCache, querying database",
        block_hash,
    );

    // Database queries
    let block_query = r#"
        SELECT 
            block_time,
            encode(block_hash, 'hex') as block_hash,
            daa_score,
            version,
            encode(hash_merkle_root, 'hex') as hash_merkle_root,
            encode(accepted_id_merkle_root, 'hex') as accepted_id_merkle_root,
            encode(utxo_commitment, 'hex') as utxo_commitment,
            bits,
            nonce,
            encode(blue_work, 'hex') as blue_work,
            blue_score,
            encode(pruning_point, 'hex') as pruning_point,
            difficulty,
            encode(selected_parent_hash, 'hex') as selected_parent_hash,
            is_chain_block
        FROM kaspad.blocks 
        WHERE block_hash = $1
    "#;

    let parents_query = r#"
        SELECT encode(parent_hash, 'hex') as parent_hash
        FROM kaspad.blocks_parents 
        WHERE block_hash = $1
        ORDER BY parent_hash
    "#;

    let children_query = r#"
        SELECT encode(block_hash, 'hex') as block_hash
        FROM kaspad.blocks_parents 
        WHERE parent_hash = $1
        ORDER BY block_hash
    "#;

    let transactions_query = r#"
        SELECT 
            t.block_time,
            encode(t.transaction_id, 'hex') as transaction_id,
            t.subnetwork_id,
            t.mass,
            t.total_input_amount,
            t.total_output_amount,
            t.protocol_id
        FROM kaspad.transactions t
        INNER JOIN kaspad.blocks_transactions bt ON t.transaction_id = bt.transaction_id
        WHERE bt.block_hash = $1
        ORDER BY bt.index
        LIMIT 1000
    "#;

    // Execute all queries concurrently
    let (block_result, parents_result, children_result, transactions_result) = tokio::try_join!(
        sqlx::query(block_query)
            .bind(&block_hash_bytes)
            .fetch_optional(&state.context.pg_pool),
        sqlx::query(parents_query)
            .bind(&block_hash_bytes)
            .fetch_all(&state.context.pg_pool),
        sqlx::query(children_query)
            .bind(&block_hash_bytes)
            .fetch_all(&state.context.pg_pool),
        sqlx::query(transactions_query)
            .bind(&block_hash_bytes)
            .fetch_all(&state.context.pg_pool)
    )
    .map_err(|e| {
        log::error!(
            target: LogTarget::WebErr.as_str(),
            "Database error fetching block {}: {}",
            block_hash,
            e,
        );
        let response = BlockResponse {
            block_hash: block_hash.clone(),
            block_data: None,
            transactions: vec![],
            status: "error".to_string(),
            error: Some("Failed to load block data".to_string()),
        };
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    })?;

    // Check if block exists
    let block_row = match block_result {
        Some(row) => row,
        None => {
            let response = BlockResponse {
                block_hash: block_hash.clone(),
                block_data: None,
                transactions: vec![],
                status: "not_found".to_string(),
                error: Some("Block not found".to_string()),
            };
            return Err((StatusCode::NOT_FOUND, Json(response)));
        }
    };

    // Parse parent hashes
    let parent_hashes: Vec<String> = parents_result
        .into_iter()
        .map(|row| row.get::<String, _>("parent_hash"))
        .collect();

    // Parse children hashes
    let children_hashes: Vec<String> = children_result
        .into_iter()
        .map(|row| row.get::<String, _>("block_hash"))
        .collect();

    // Parse transactions
    let transactions: Vec<BlockTransaction> = transactions_result
        .into_iter()
        .map(|tx| {
            let subnetwork_id: i32 = tx.get("subnetwork_id");
            let is_coinbase = subnetwork_id == 1;
            let total_input_amount: i64 = tx.try_get("total_input_amount").unwrap_or(0);
            let total_output_amount: i64 = tx.try_get("total_output_amount").unwrap_or(0);
            let fee = if is_coinbase {
                0
            } else {
                std::cmp::max(0, total_input_amount - total_output_amount) as u64
            };

            let protocol_id: Option<i64> = tx.try_get("protocol_id").ok();
            let protocol = protocol_id_to_string(protocol_id);

            BlockTransaction {
                tx_id: tx.get("transaction_id"),
                block_time: tx.get("block_time"),
                mass: tx.get::<i64, _>("mass") as u64,
                total_input_amount: total_input_amount as u64,
                total_output_amount: total_output_amount as u64,
                fee,
                is_coinbase,
                protocol,
            }
        })
        .collect();

    // Build block data
    let block_data = BlockData {
        hash: block_row.get("block_hash"),
        version: block_row.get::<i16, _>("version"),
        merkle_root: block_row.get("hash_merkle_root"),
        accepted_id_merkle_root: block_row.get("accepted_id_merkle_root"),
        utxo_commitment: block_row.get("utxo_commitment"),
        timestamp: block_row.get("block_time"),
        bits: block_row.get::<i32, _>("bits") as u32,
        nonce: block_row.get::<i64, _>("nonce") as u64,
        blue_work: block_row.get("blue_work"),
        daa_score: block_row.get::<i64, _>("daa_score") as u64,
        blue_score: block_row.get::<i64, _>("blue_score") as u64,
        difficulty: block_row.get("difficulty"),
        selected_parent_hash: block_row.get("selected_parent_hash"),
        is_chain_block: block_row.get("is_chain_block"),
        pruning_point: block_row.get("pruning_point"),
        parent_hashes,
        children_hashes,
        transaction_count: transactions.len(),
    };

    // Build response
    let response = BlockResponse {
        block_hash: block_hash.clone(),
        block_data: Some(block_data),
        transactions,
        status: "success".to_string(),
        error: None,
    };

    // Add cache headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=300, s-maxage=300"),
    );
    headers.insert(
        "Vercel-CDN-Cache-Control",
        HeaderValue::from_static("public, max-age=300"),
    );

    Ok((headers, Json(response)))
}
