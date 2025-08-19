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
#[derive(Debug)]
struct TransactionInput {
    transaction_id: Vec<u8>,
    block_time: DateTime<Utc>,
    amount: i64,
}

#[derive(Debug)]
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

    // Define time window - we want transactions BEFORE the given timestamp
    // Use a reasonable search window (e.g., 24 hours) to find the most recent transactions
    let start_time = end_time - chrono::Duration::hours(24);

    log::info!(
        target: LogTarget::Web.as_str(),
        "Time window for address {}: {} to {} (duration: {}min) - timestamp-based query",
        address,
        start_time.to_rfc3339(),
        end_time.to_rfc3339(),
        end_time.signed_duration_since(start_time).num_minutes()
    );

    // Execute separate concurrent queries for inputs and outputs
    let queries_start = Instant::now();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Starting concurrent queries for address {}: inputs, outputs, and address data",
        address
    );
    
    let (address_data, inputs_result, outputs_result) = match tokio::try_join!(
        address_data_future,
        async {
            let input_start = Instant::now();
            let result = sqlx::query(get_inputs_query())
                .bind(&address)
                .bind(start_time)
                .bind(end_time)
                .fetch_all(&state.context.pg_pool)
                .await;
            let input_duration = input_start.elapsed();
            log::info!(
                target: LogTarget::Web.as_str(),
                "Input query for address {} completed in {}ms, rows: {}",
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
                .bind(start_time)
                .bind(end_time)
                .fetch_all(&state.context.pg_pool)
                .await;
            let output_duration = output_start.elapsed();
            log::info!(
                target: LogTarget::Web.as_str(),
                "Output query for address {} completed in {}ms, rows: {}",
                address,
                output_duration.as_millis(),
                result.as_ref().map(|r| r.len()).unwrap_or(0)
            );
            result
        }
    ) {
        Ok((address_data, inputs_result, outputs_result)) => {
            (address_data, inputs_result, outputs_result)
        },
        Err(e) => {
            log::error!(
                target: LogTarget::Web.as_str(),
                "Parallel time-window queries failed for address {}: {}",
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
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
        }
    };
    
    let queries_duration = queries_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "All concurrent queries completed for address {} in {}ms",
        address,
        queries_duration.as_millis()
    );

    // Process query results with Rust-side aggregation
    let aggregation_start = Instant::now();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Starting Rust-side aggregation for address {}: {} input rows, {} output rows",
        address,
        inputs_result.len(),
        outputs_result.len()
    );
    
    // Parse inputs and outputs from separate queries
    let parse_start = Instant::now();
    let inputs: Vec<TransactionInput> = inputs_result
        .iter()
        .map(|row| TransactionInput {
            transaction_id: row.get("transaction_id"),
            block_time: row.get("block_time"),
            amount: row.get("amount"),
        })
        .collect();

    let outputs: Vec<TransactionOutput> = outputs_result
        .iter()
        .map(|row| TransactionOutput {
            transaction_id: row.get("transaction_id"),
            block_time: row.get("block_time"),
            amount: row.get("amount"),
        })
        .collect();
    
    let parse_duration = parse_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Row parsing for address {} completed in {}ms",
        address,
        parse_duration.as_millis()
    );

    // Collect unique transaction IDs for metadata fetch
    let dedup_start = Instant::now();
    let mut tx_ids: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for input in &inputs {
        tx_ids.insert(input.transaction_id.clone());
    }
    for output in &outputs {
        tx_ids.insert(output.transaction_id.clone());
    }

    let tx_ids_vec: Vec<Vec<u8>> = tx_ids.into_iter().collect();
    let dedup_duration = dedup_start.elapsed();
    log::info!(
        target: LogTarget::Web.as_str(),
        "Transaction deduplication for address {} completed in {}ms: {} unique transactions",
        address,
        dedup_duration.as_millis(),
        tx_ids_vec.len()
    );

    // Fetch metadata for all transactions
    let metadata_start = Instant::now();
    let metadata_rows = if !tx_ids_vec.is_empty() {
        let result = sqlx::query(get_transaction_metadata_query())
            .bind(&tx_ids_vec)
            .bind(start_time)
            .bind(end_time)
            .fetch_all(&state.context.pg_pool)
            .await
            .unwrap_or_default();
        
        let metadata_duration = metadata_start.elapsed();
        log::info!(
            target: LogTarget::Web.as_str(),
            "Metadata query for address {} completed in {}ms: {} metadata rows",
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

    // Aggregate in Rust and get most recent transactions before the timestamp
    let final_agg_start = Instant::now();
    let mut aggregated = aggregate_transactions(inputs, outputs, metadata, limit * 3); // Get extra for filtering
    
    // Filter transactions to only include those BEFORE the given timestamp,
    // sort by time descending, and take only the requested limit
    aggregated.retain(|tx| tx.block_time <= end_time);
    aggregated.sort_by(|a, b| b.block_time.cmp(&a.block_time));
    let final_txs: Vec<AggregatedTransaction> = aggregated.into_iter().take(limit as usize).collect();
    
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
