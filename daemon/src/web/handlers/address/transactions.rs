use super::super::super::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use kaspa_addresses::Address as KaspaAddress;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspalytics_utils::log::LogTarget;

use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// Simple in-memory cache for address metadata
#[derive(Clone)]
struct CachedAddressData {
    data: AddressData,
    cached_at: Instant,
}

type AddressCache = Arc<RwLock<HashMap<String, CachedAddressData>>>;

// Raw transaction data from database queries
#[derive(Debug, Clone)]
struct TransactionInput {
    transaction_id: Vec<u8>,
    block_time: DateTime<Utc>,
    amount: i64,
}

#[derive(Debug, Clone)]
struct TransactionOutput {
    transaction_id: Vec<u8>,
    block_time: DateTime<Utc>,
    amount: i64,
}

#[derive(Debug)]
struct TransactionMetadata {
    protocol_id: Option<i32>,
    subnetwork_id: Option<i32>,
}

// Aggregated transaction for final response
#[derive(Debug)]
struct AggregatedTransaction {
    transaction_id: Vec<u8>,
    block_time: DateTime<Utc>,
    net_change: i64,
    protocol_id: Option<i32>,
    subnetwork_id: Option<i32>,
}

#[derive(Deserialize)]
pub struct TransactionQuery {
    timestamp: u64,  // Unix timestamp in milliseconds
    limit: Option<u32>,
}

#[derive(Serialize)]
pub struct AddressTransactionsResponse {
    pub address: String,
    #[serde(rename = "addressData")]
    pub address_data: AddressData,
    pub transactions: Vec<AddressTransaction>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct AddressData {
    pub address: String,
    pub balance: u64,
    #[serde(rename = "addressType")]
    pub address_type: String,
    pub known: Option<KnownAddress>,
}

#[derive(Serialize, Clone)]
pub struct KnownAddress {
    pub label: String,
    #[serde(rename = "type")]
    pub address_type: String,
}

#[derive(Serialize)]
pub struct AddressTransaction {
    pub transaction_id: String,
    pub block_time: DateTime<Utc>,
    pub amount_change: i64,
    pub protocol_id: Option<i32>,
    pub subnetwork_id: Option<i32>,
}


fn is_valid_kaspa_address(address: &str) -> bool {
    KaspaAddress::try_from(address).is_ok()
}





// Separate input query - optimized for TimescaleDB index scans
fn get_inputs_query() -> &'static str {
    r#"
    SELECT 
        transaction_id,
        block_time,
        COALESCE(utxo_amount, 0) as amount
    FROM kaspad.transactions_inputs
    WHERE utxo_script_public_key_address = $1
    AND block_time >= $2::timestamptz
    AND block_time < $3::timestamptz
    ORDER BY block_time DESC, transaction_id DESC
    "#
}

// Separate output query - optimized for TimescaleDB index scans  
fn get_outputs_query() -> &'static str {
    r#"
    SELECT 
            transaction_id,
            block_time,
        COALESCE(amount, 0) as amount
        FROM kaspad.transactions_outputs
        WHERE script_public_key_address = $1
    AND block_time >= $2::timestamptz
    AND block_time < $3::timestamptz
    ORDER BY block_time DESC, transaction_id DESC
    "#
}

// Transaction metadata query - fetch protocol and subnetwork info
fn get_transaction_metadata_query() -> &'static str {
    r#"
        SELECT 
            transaction_id,
            block_time,
        protocol_id,
        subnetwork_id
    FROM kaspad.transactions
    WHERE transaction_id = ANY($1)
    AND block_time >= $2::timestamptz
    AND block_time < $3::timestamptz
    "#
}







// Cache for address metadata (balance, known addresses, etc.)
static ADDRESS_CACHE: std::sync::OnceLock<AddressCache> = std::sync::OnceLock::new();

fn get_address_cache() -> &'static AddressCache {
    ADDRESS_CACHE.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
}

const CACHE_TTL: Duration = Duration::from_secs(30); // 30 second cache

async fn get_cached_address_data(
    address: &str,
    state: &AppState,
) -> Result<AddressData, sqlx::Error> {
    // Check cache first
    {
        let cache = get_address_cache().read().await;
        if let Some(cached) = cache.get(address) {
            if cached.cached_at.elapsed() < CACHE_TTL {
                return Ok(cached.data.clone());
            }
        }
    }

    // Fetch fresh data
    let balance_future = async {
        let parsed_address = KaspaAddress::try_from(address).map_err(|_| sqlx::Error::Protocol("Invalid address".into()))?;
        state.context.rpc_client.get_balance_by_address(parsed_address).await.map_err(|_| sqlx::Error::Protocol("RPC error".into()))
    };

    let known_future = sqlx::query("SELECT label, type FROM known_addresses WHERE address = $1 LIMIT 1")
        .bind(address)
        .fetch_optional(&state.context.pg_pool);

    let (balance_result, known_result) = tokio::try_join!(balance_future, known_future)?;

    let known = known_result.map(|row| KnownAddress {
        label: row.get("label"),
        address_type: row.get("type"),
    });

    let address_data = AddressData {
        address: address.to_string(),
        balance: balance_result,
        address_type: "P2PK".to_string(),
        known,
    };

    // Update cache
    {
        let mut cache = get_address_cache().write().await;
        cache.insert(address.to_string(), CachedAddressData {
            data: address_data.clone(),
            cached_at: Instant::now(),
        });
    }

    Ok(address_data)
}

// Aggregate inputs and outputs into distinct transactions with net changes
fn aggregate_transactions(
    inputs: Vec<TransactionInput>,
    outputs: Vec<TransactionOutput>,
    metadata: HashMap<Vec<u8>, TransactionMetadata>,
    limit: u32,
) -> Vec<AggregatedTransaction> {
    use std::collections::BTreeMap;
    
    // Use BTreeMap to maintain time ordering
    let mut tx_map: BTreeMap<(DateTime<Utc>, Vec<u8>), i64> = BTreeMap::new();
    
    // Process inputs (negative amounts)
    let input_processing_start = Instant::now();
    for input in inputs.iter() {
        let key = (input.block_time, input.transaction_id.clone());
        *tx_map.entry(key).or_insert(0) -= input.amount;
    }
    let input_processing_duration = input_processing_start.elapsed();
    
    // Process outputs (positive amounts)
    let output_processing_start = Instant::now();
    for output in outputs.iter() {
        let key = (output.block_time, output.transaction_id.clone());
        *tx_map.entry(key).or_insert(0) += output.amount;
    }
    let output_processing_duration = output_processing_start.elapsed();
    
    log::debug!(
        target: LogTarget::Web.as_str(),
        "Aggregation processing: inputs={}ms ({}rows), outputs={}ms ({}rows), unique_txs={}",
        input_processing_duration.as_millis(),
        inputs.len(),
        output_processing_duration.as_millis(),
        outputs.len(),
        tx_map.len()
    );
    
    // Convert to aggregated transactions, sorted by time (newest first)
    let mut aggregated: Vec<AggregatedTransaction> = tx_map
        .into_iter()
        .rev() // Reverse to get newest first
        .take(limit as usize) // Take up to the limit
        .map(|((block_time, transaction_id), net_change)| {
            let meta = metadata.get(&transaction_id);
            AggregatedTransaction {
                transaction_id,
                block_time,
                net_change,
                protocol_id: meta.and_then(|m| m.protocol_id),
                subnetwork_id: meta.and_then(|m| m.subnetwork_id),
            }
        })
        .collect();
    
    // Sort by time descending, then by transaction_id descending for deterministic ordering
    aggregated.sort_by(|a, b| {
        b.block_time.cmp(&a.block_time)
            .then_with(|| b.transaction_id.cmp(&a.transaction_id))
    });
    
    aggregated
}

pub async fn get_address_transactions(
    Path(address): Path<String>,
    Query(params): Query<TransactionQuery>,
    State(state): State<AppState>,
) -> Result<
    (HeaderMap, Json<AddressTransactionsResponse>),
    (StatusCode, Json<AddressTransactionsResponse>),
> {
    let request_start = Instant::now();
    
    log::info!(
        target: LogTarget::Web.as_str(),
        "Starting transaction query for address: {}, timestamp: {}, limit: {:?}",
        address,
        params.timestamp,
        params.limit
    );

    // Validate address format
    if !is_valid_kaspa_address(&address) {
        let response = AddressTransactionsResponse {
            address: address.clone(),
            address_data: AddressData {
                address: address.clone(),
                balance: 0,
                address_type: "P2PK".to_string(),
                known: None,
            },
            transactions: vec![],
            status: "invalid_format".to_string(),
            error: Some("Invalid address format".to_string()),
            message: None,
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    // Simple parameter processing
    let limit = params.limit.unwrap_or(50).min(100).max(1);
    
    // Convert timestamp from milliseconds to DateTime<Utc>
    let end_time = DateTime::from_timestamp(params.timestamp as i64 / 1000, ((params.timestamp % 1000) * 1_000_000) as u32)
        .ok_or_else(|| {
            let response = AddressTransactionsResponse {
                address: address.clone(),
                address_data: AddressData {
                    address: address.clone(),
                    balance: 0,
                    address_type: "P2PK".to_string(),
                    known: None,
                },
                transactions: vec![],
                status: "invalid_timestamp".to_string(),
                error: Some("Invalid timestamp format".to_string()),
                message: None,
            };
            (StatusCode::BAD_REQUEST, Json(response))
        })?;

    // Start address data fetch in parallel (cached)
    let address_data_future = get_cached_address_data(&address, &state);

    // Get oldest available timestamp to know when to stop looking back
    let oldest_timestamp_future = async {
        sqlx::query("SELECT MIN(block_time) as oldest_block_timestamp FROM kaspad.blocks")
            .fetch_optional(&state.context.pg_pool)
            .await
            .ok()
            .flatten()
            .and_then(|row| row.try_get::<DateTime<Utc>, _>("oldest_block_timestamp").ok())
    };

    // Execute address data and oldest timestamp queries in parallel
    let (address_data_result, oldest_timestamp_result) = tokio::join!(
        address_data_future,
        oldest_timestamp_future
    );

    let address_data = address_data_result.map_err(|e| {
        log::error!(
            target: LogTarget::Web.as_str(),
            "Failed to fetch address data for {}: {}",
            address,
            e
        );
        let response = AddressTransactionsResponse {
            address: address.clone(),
            address_data: AddressData {
                address: address.clone(),
                balance: 0,
                address_type: "P2PK".to_string(),
                known: None,
            },
            transactions: vec![],
            status: "error".to_string(),
            error: Some("Failed to fetch address data".to_string()),
            message: None,
        };
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    })?;

    // Determine the earliest time we should query (either oldest data or reasonable limit)
    let oldest_available = oldest_timestamp_result.unwrap_or_else(|| {
        // Fallback to 30 days ago if we can't determine oldest timestamp
        end_time - chrono::Duration::days(30)
    });
    
    // Set a reasonable maximum lookback period (30 days from end_time)
    let max_lookback_time = end_time - chrono::Duration::days(30);
    let earliest_query_time = oldest_available.max(max_lookback_time);

    log::info!(
        target: LogTarget::Web.as_str(),
        "Starting chunked iteration for address {}: end_time={}, earliest_query_time={}, chunk_size=6h",
        address,
        end_time.to_rfc3339(),
        earliest_query_time.to_rfc3339()
    );

    // Iteratively query in 6-hour chunks going backwards until we have enough transactions
    let chunk_duration = chrono::Duration::hours(6);
    let mut all_inputs: Vec<TransactionInput> = Vec::new();
    let mut all_outputs: Vec<TransactionOutput> = Vec::new();
    let mut current_end_time = end_time;
    let mut chunk_count = 0;
    let target_transactions = limit as usize;
    let queries_start = Instant::now();

    loop {
        let current_start_time = (current_end_time - chunk_duration).max(earliest_query_time);
        chunk_count += 1;
        
        log::info!(
            target: LogTarget::Web.as_str(),
            "Chunk {} for address {}: {} to {} (duration: {}min)",
            chunk_count,
            address,
            current_start_time.to_rfc3339(),
            current_end_time.to_rfc3339(),
            current_end_time.signed_duration_since(current_start_time).num_minutes()
        );

        // Query this chunk
        let chunk_start = Instant::now();
        let (chunk_inputs_result, chunk_outputs_result) = match tokio::try_join!(
            async {
                let input_start = Instant::now();
                let result = sqlx::query(get_inputs_query())
                    .bind(&address)
                    .bind(current_start_time)
                    .bind(current_end_time)
                    .fetch_all(&state.context.pg_pool)
                    .await;
                let input_duration = input_start.elapsed();
                log::debug!(
                    target: LogTarget::Web.as_str(),
                    "Chunk {} input query for address {} completed in {}ms, rows: {}",
                    chunk_count,
                    address,
                    input_duration.as_millis(),
                    result.as_ref().map(|r| r.len()).unwrap_or(0)
                );
                result
            },
            async {
                let output_start = Instant::now();
                let result = sqlx::query(get_outputs_query())
                    .bind(&address)
                    .bind(current_start_time)
                    .bind(current_end_time)
                    .fetch_all(&state.context.pg_pool)
                    .await;
                let output_duration = output_start.elapsed();
                log::debug!(
                    target: LogTarget::Web.as_str(),
                    "Chunk {} output query for address {} completed in {}ms, rows: {}",
                    chunk_count,
                    address,
                    output_duration.as_millis(),
                    result.as_ref().map(|r| r.len()).unwrap_or(0)
                );
                result
            }
        ) {
            Ok((inputs_result, outputs_result)) => (inputs_result, outputs_result),
            Err(e) => {
                log::error!(
                    target: LogTarget::Web.as_str(),
                    "Chunk {} queries failed for address {}: {}",
                    chunk_count,
                    address,
                    e
                );
                let response = AddressTransactionsResponse {
                    address: address.clone(),
                    address_data,
                    transactions: vec![],
                    status: "error".to_string(),
                    error: Some("Failed to fetch transaction data".to_string()),
                    message: None,
                };
                return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
            }
        };

        let chunk_duration_ms = chunk_start.elapsed();
        
        // Parse and accumulate results from this chunk
        let chunk_inputs: Vec<TransactionInput> = chunk_inputs_result
            .iter()
            .map(|row| TransactionInput {
                transaction_id: row.get("transaction_id"),
                block_time: row.get("block_time"),
                amount: row.get("amount"),
            })
            .collect();

        let chunk_outputs: Vec<TransactionOutput> = chunk_outputs_result
            .iter()
            .map(|row| TransactionOutput {
                transaction_id: row.get("transaction_id"),
                block_time: row.get("block_time"),
                amount: row.get("amount"),
            })
            .collect();

        let chunk_input_count = chunk_inputs.len();
        let chunk_output_count = chunk_outputs.len();
        
        all_inputs.extend(chunk_inputs);
        all_outputs.extend(chunk_outputs);

        // Quick aggregation check to see if we have enough transactions
        let quick_agg_start = Instant::now();
        let aggregated = aggregate_transactions(
            all_inputs.clone(), 
            all_outputs.clone(), 
            HashMap::new(), 
            (target_transactions * 2) as u32  // Get extra for filtering
        );
        
        // Filter by end_time and count
        let filtered_count = aggregated.iter()
            .filter(|tx| tx.block_time <= end_time)
            .count();
        let quick_agg_duration = quick_agg_start.elapsed();

        log::info!(
            target: LogTarget::Web.as_str(),
            "Chunk {} completed for address {} in {}ms: +{} inputs, +{} outputs, total_aggregated={}, filtered_count={} (agg_check: {}ms)",
            chunk_count,
            address,
            chunk_duration_ms.as_millis(),
            chunk_input_count,
            chunk_output_count,
            aggregated.len(),
            filtered_count,
            quick_agg_duration.as_millis()
        );

        // Check termination conditions
        let should_continue = filtered_count < target_transactions 
            && current_start_time > earliest_query_time
            && chunk_count < 120; // Safety limit: max 120 chunks (30 days worth)

        if !should_continue {
            if filtered_count >= target_transactions {
                log::info!(
                    target: LogTarget::Web.as_str(),
                    "Found enough transactions for address {} after {} chunks: {} >= {}",
                    address, chunk_count, filtered_count, target_transactions
                );
            } else if current_start_time <= earliest_query_time {
                log::info!(
                    target: LogTarget::Web.as_str(),
                    "Reached data availability limit for address {} after {} chunks at {}",
                    address, chunk_count, earliest_query_time.to_rfc3339()
                );
            } else {
                log::warn!(
                    target: LogTarget::Web.as_str(),
                    "Hit safety limit for address {} after {} chunks",
                    address, chunk_count
                );
            }
            break;
        }

        // Move to the next chunk (going backwards in time)
        current_end_time = current_start_time;
    }
    
    let queries_duration = queries_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Chunked iteration completed for address {} in {}ms: {} chunks, {} total inputs, {} total outputs",
        address,
        queries_duration.as_millis(),
        chunk_count,
        all_inputs.len(),
        all_outputs.len()
    );

    // Process accumulated query results with Rust-side aggregation
    let aggregation_start = Instant::now();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Starting final aggregation for address {}: {} input rows, {} output rows from {} chunks",
        address,
        all_inputs.len(),
        all_outputs.len(),
        chunk_count
    );

    // Pre-aggregate to identify top transactions before fetching metadata
    let pre_agg_start = Instant::now();
    let pre_aggregated = aggregate_transactions(all_inputs, all_outputs, HashMap::new(), limit * 3); // Get extra for filtering
    
    // Filter transactions to only include those BEFORE the given timestamp,
    // sort by time descending, and take only the requested limit
    let mut filtered_txs = pre_aggregated;
    filtered_txs.retain(|tx| tx.block_time <= end_time);
    filtered_txs.sort_by(|a, b| b.block_time.cmp(&a.block_time));
    let top_txs: Vec<AggregatedTransaction> = filtered_txs.into_iter().take(limit as usize).collect();
    
    let pre_agg_duration = pre_agg_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Pre-aggregation for address {} completed in {}ms: {} top transactions identified",
        address,
        pre_agg_duration.as_millis(),
        top_txs.len()
    );

    // Collect unique transaction IDs from only the top transactions for metadata fetch
    let dedup_start = Instant::now();
    let tx_ids_vec: Vec<Vec<u8>> = top_txs.iter()
        .map(|tx| tx.transaction_id.clone())
        .collect();
    
    let dedup_duration = dedup_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Transaction ID collection for address {} completed in {}ms: {} transactions for metadata fetch",
        address,
        dedup_duration.as_millis(),
        tx_ids_vec.len()
    );

    // Fetch metadata for only the top transactions
    let metadata_start = Instant::now();
    let metadata_rows = if !tx_ids_vec.is_empty() {
        let result = sqlx::query(get_transaction_metadata_query())
            .bind(&tx_ids_vec)
            .bind(earliest_query_time)
            .bind(end_time)
            .fetch_all(&state.context.pg_pool)
            .await
            .unwrap_or_default();
        
        let metadata_duration = metadata_start.elapsed();
        log::info!(
            target: LogTarget::Web.as_str(),
            "Metadata query for address {} completed in {}ms: {} metadata rows (optimized)",
            address,
            metadata_duration.as_millis(),
            result.len()
        );
        result
    } else {
        log::info!(
            target: LogTarget::Web.as_str(),
            "No transactions found for address {}, skipping metadata fetch",
            address
        );
        vec![]
    };

    let metadata: HashMap<Vec<u8>, TransactionMetadata> = metadata_rows
        .iter()
        .map(|row| {
            let tx_id: Vec<u8> = row.get("transaction_id");
            let meta = TransactionMetadata {
                protocol_id: row.try_get("protocol_id").ok(),
                subnetwork_id: row.try_get("subnetwork_id").ok(),
            };
            (tx_id, meta)
        })
        .collect();

    // Apply metadata to the top transactions
    let final_agg_start = Instant::now();
    let final_txs: Vec<AggregatedTransaction> = top_txs.into_iter()
        .map(|mut tx| {
            if let Some(meta) = metadata.get(&tx.transaction_id) {
                tx.protocol_id = meta.protocol_id;
                tx.subnetwork_id = meta.subnetwork_id;
            }
            tx
        })
        .collect();
    
    let final_agg_duration = final_agg_start.elapsed();
    let total_aggregation_duration = aggregation_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Final aggregation for address {} completed in {}ms (total aggregation: {}ms): {} transactions",
        address,
        final_agg_duration.as_millis(),
        total_aggregation_duration.as_millis(),
        final_txs.len()
    );

    // Convert to final response format
    let transactions: Vec<AddressTransaction> = final_txs
        .iter()
        .map(|tx| AddressTransaction {
            transaction_id: hex::encode(&tx.transaction_id),
            block_time: tx.block_time,
            amount_change: tx.net_change,
            protocol_id: tx.protocol_id,
            subnetwork_id: tx.subnetwork_id,
        })
        .collect();

    // Build response with cached address data
    let transaction_count = transactions.len();
    let response = AddressTransactionsResponse {
        address: address.clone(),
        address_data,
        transactions,
        status: "success".to_string(),
        error: None,
        message: None,
    };

    // Add cache headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=10, s-maxage=10"),
    );

    let total_request_duration = request_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Transaction query completed for address {}: total_duration={}ms, transactions_returned={}, status={}",
        address,
        total_request_duration.as_millis(),
        transaction_count,
        response.status
    );

    Ok((headers, Json(response)))
}
