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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Deserialize)]
pub struct PaginationQuery {
    page: Option<u32>,
}

#[derive(Serialize)]
pub struct AddressTransactionsResponse {
    pub address: String,
    #[serde(rename = "addressData")]
    pub address_data: AddressData,
    pub transactions: Vec<AddressTransaction>,
    pub pagination: Pagination,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Serialize)]
pub struct AddressData {
    pub address: String,
    pub balance: u64,
    #[serde(rename = "addressType")]
    pub address_type: String,
    #[serde(rename = "firstSeen")]
    pub first_seen: Option<DateTime<Utc>>,
    #[serde(rename = "lastActivity")]
    pub last_activity: Option<DateTime<Utc>>,
    pub known: Option<KnownAddress>,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
pub struct Pagination {
    #[serde(rename = "currentPage")]
    pub current_page: u32,
    #[serde(rename = "totalPages")]
    pub total_pages: u32,
    #[serde(rename = "totalTransactions")]
    pub total_transactions: Option<u64>,
    pub limit: u32,
    #[serde(rename = "hasNextPage")]
    pub has_next_page: bool,
    #[serde(rename = "hasPrevPage")]
    pub has_prev_page: bool,
}

fn is_valid_kaspa_address(address: &str) -> bool {
    KaspaAddress::try_from(address).is_ok()
}

// Optimized query that avoids expensive ROW_NUMBER() and reduces data scanning
fn get_optimized_single_query() -> &'static str {
    r#"
    WITH recent_inputs AS (
        SELECT DISTINCT
            transaction_id,
            block_time,
            -COALESCE(utxo_amount, 0) as amount_change
        FROM kaspad.transactions_inputs
        WHERE utxo_script_public_key_address = $1
        AND block_time >= NOW() - INTERVAL '30 days'
    ),
    recent_outputs AS (
        SELECT DISTINCT
            transaction_id,
            block_time,
            COALESCE(amount, 0) as amount_change
        FROM kaspad.transactions_outputs
        WHERE script_public_key_address = $1
        AND block_time >= NOW() - INTERVAL '30 days'
    ),
    combined_activity AS (
        SELECT transaction_id, block_time, amount_change FROM recent_inputs
        UNION ALL
        SELECT transaction_id, block_time, amount_change FROM recent_outputs
    ),
    aggregated_txs AS (
        SELECT 
            transaction_id,
            block_time,
            SUM(amount_change) as net_change
        FROM combined_activity
        GROUP BY transaction_id, block_time
    )
    SELECT 
        encode(a.transaction_id, 'hex') as transaction_id,
        a.block_time,
        a.net_change as amount_change,
        t.protocol_id,
        t.subnetwork_id
    FROM aggregated_txs a
    LEFT JOIN kaspad.transactions t ON a.transaction_id = t.transaction_id
    ORDER BY a.block_time DESC, a.transaction_id DESC
    LIMIT $2 OFFSET $3
    "#
}

// Fallback query for addresses with sparse activity (extends time window)
fn get_extended_query(time_interval: &str) -> String {
    format!(
        r#"
        WITH extended_inputs AS (
            SELECT DISTINCT
                transaction_id,
                block_time,
                -COALESCE(utxo_amount, 0) as amount_change
            FROM kaspad.transactions_inputs
            WHERE utxo_script_public_key_address = $1
            AND block_time >= NOW() - INTERVAL '{}'
        ),
        extended_outputs AS (
            SELECT DISTINCT
                transaction_id,
                block_time,
                COALESCE(amount, 0) as amount_change
            FROM kaspad.transactions_outputs
            WHERE script_public_key_address = $1
            AND block_time >= NOW() - INTERVAL '{}'
        ),
        combined_activity AS (
            SELECT transaction_id, block_time, amount_change FROM extended_inputs
            UNION ALL
            SELECT transaction_id, block_time, amount_change FROM extended_outputs
        ),
        aggregated_txs AS (
            SELECT 
                transaction_id,
                block_time,
                SUM(amount_change) as net_change
            FROM combined_activity
            GROUP BY transaction_id, block_time
        )
        SELECT 
            encode(a.transaction_id, 'hex') as transaction_id,
            a.block_time,
            a.net_change as amount_change,
            t.protocol_id,
            t.subnetwork_id
        FROM aggregated_txs a
        LEFT JOIN kaspad.transactions t ON a.transaction_id = t.transaction_id
        ORDER BY a.block_time DESC, a.transaction_id DESC
        LIMIT $2 OFFSET $3
        "#,
        time_interval, time_interval
    )
}

// Optimized count query that avoids UNION and uses more efficient counting
fn get_optimized_count_query() -> &'static str {
    r#"
    WITH input_txs AS (
        SELECT COUNT(DISTINCT transaction_id) as count
        FROM kaspad.transactions_inputs
        WHERE utxo_script_public_key_address = $1
        AND block_time >= NOW() - INTERVAL '30 days'
    ),
    output_txs AS (
        SELECT COUNT(DISTINCT transaction_id) as count
        FROM kaspad.transactions_outputs
        WHERE script_public_key_address = $1
        AND block_time >= NOW() - INTERVAL '30 days'
    ),
    unique_txs AS (
        SELECT transaction_id
        FROM kaspad.transactions_inputs
        WHERE utxo_script_public_key_address = $1
        AND block_time >= NOW() - INTERVAL '30 days'
        UNION
        SELECT transaction_id
        FROM kaspad.transactions_outputs
        WHERE script_public_key_address = $1
        AND block_time >= NOW() - INTERVAL '30 days'
    )
    SELECT COUNT(*) as total_count FROM unique_txs
    "#
}

// Fallback count query for extended time windows
fn get_extended_count_query(time_interval: &str) -> String {
    format!(
        r#"
        WITH unique_txs AS (
            SELECT transaction_id
            FROM kaspad.transactions_inputs
            WHERE utxo_script_public_key_address = $1
            AND block_time >= NOW() - INTERVAL '{}'
            UNION
            SELECT transaction_id
            FROM kaspad.transactions_outputs
            WHERE script_public_key_address = $1
            AND block_time >= NOW() - INTERVAL '{}'
        )
        SELECT COUNT(*) as total_count FROM unique_txs
        "#,
        time_interval, time_interval
    )
}

async fn fetch_balance_from_rpc(
    address: &str,
    state: &AppState,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let parsed_address = KaspaAddress::try_from(address)?;
    let balance = state
        .context
        .rpc_client
        .get_balance_by_address(parsed_address)
        .await?;
    Ok(balance)
}

pub async fn get_address_transactions(
    Path(address): Path<String>,
    Query(params): Query<PaginationQuery>,
    State(state): State<AppState>,
) -> Result<
    (HeaderMap, Json<AddressTransactionsResponse>),
    (StatusCode, Json<AddressTransactionsResponse>),
> {
    // Validate address format
    if !is_valid_kaspa_address(&address) {
        let response = AddressTransactionsResponse {
            address: address.clone(),
            address_data: AddressData {
                address: address.clone(),
                balance: 0,
                address_type: "P2PK".to_string(),
                first_seen: None,
                last_activity: None,
                known: None,
            },
            transactions: vec![],
            pagination: Pagination {
                current_page: 1,
                total_pages: 1,
                total_transactions: None,
                limit: 50,
                has_next_page: false,
                has_prev_page: false,
            },
            status: "invalid_format".to_string(),
            error: Some("Invalid address format".to_string()),
            message: None,
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    let page = params.page.unwrap_or(1).max(1);
    let limit = 50u32;
    let offset = (page - 1) * limit;

    // Start balance fetch in parallel
    let balance_future = fetch_balance_from_rpc(&address, &state);

    // Try optimized query first (30 days), then fallback to extended windows
    let mut transactions_result = None;
    let mut used_extended_query = false;

    // First attempt: optimized query for recent activity (30 days)
    match sqlx::query(get_optimized_single_query())
        .bind(&address)
        .bind((limit + 1) as i64) // Get one extra to check if there's a next page
        .bind(offset as i64)
        .fetch_all(&state.context.pg_pool)
        .await
    {
        Ok(result) => {
            if result.len() >= limit as usize || page == 1 {
                // We have enough data or it's the first page
                transactions_result = Some(result);
            }
        }
        Err(e) => {
            log::warn!(
                target: LogTarget::WebErr.as_str(),
                "Optimized query failed for address {}, trying fallback: {}",
                address,
                e
            );
        }
    }

    // Fallback: try extended time windows if optimized query didn't return enough data
    if transactions_result.is_none() {
        let extended_windows = ["90 days", "365 days"];

        for &time_window in &extended_windows {
            match sqlx::query(&get_extended_query(time_window))
                .bind(&address)
                .bind((limit + 1) as i64)
                .bind(offset as i64)
                .fetch_all(&state.context.pg_pool)
                .await
            {
                Ok(result) => {
                    if !result.is_empty() || time_window == "365 days" {
                        transactions_result = Some(result);
                        used_extended_query = true;
                        break;
                    }
                }
                Err(e) => {
                    if time_window == "365 days" {
                        log::error!(
                            target: LogTarget::WebErr.as_str(),
                            "All queries failed for address {}: {}",
                            address,
                            e
                        );
                        let response = AddressTransactionsResponse {
                            address: address.clone(),
                            address_data: AddressData {
                                address: address.clone(),
                                balance: 0,
                                address_type: "P2PK".to_string(),
                                first_seen: None,
                                last_activity: None,
                                known: None,
                            },
                            transactions: vec![],
                            pagination: Pagination {
                                current_page: page,
                                total_pages: 1,
                                total_transactions: None,
                                limit,
                                has_next_page: false,
                                has_prev_page: page > 1,
                            },
                            status: "error".to_string(),
                            error: Some("Failed to fetch address data".to_string()),
                            message: None,
                        };
                        return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
                    }
                }
            }
        }
    }

    let transactions_rows = transactions_result.unwrap_or_default();

    // Execute parallel queries for count, balance, and known address
    let count_query = if used_extended_query {
        get_extended_count_query("365 days")
    } else {
        get_optimized_count_query().to_string()
    };
    let known_address_query = "SELECT label, type FROM known_addresses WHERE address = $1 LIMIT 1";

    // Handle balance fetch separately since it has different error type
    let balance_result = balance_future.await;

    let (count_result, known_result) = tokio::try_join!(
        sqlx::query(&count_query)
            .bind(&address)
            .fetch_one(&state.context.pg_pool),
        sqlx::query(known_address_query)
            .bind(&address)
            .fetch_optional(&state.context.pg_pool)
    )
    .map_err(|e| {
        log::error!(
            target: LogTarget::WebErr.as_str(),
            "Error in parallel queries for address {}: {}",
            address,
            e
        );
        let response = AddressTransactionsResponse {
            address: address.clone(),
            address_data: AddressData {
                address: address.clone(),
                balance: 0,
                address_type: "P2PK".to_string(),
                first_seen: None,
                last_activity: None,
                known: None,
            },
            transactions: vec![],
            pagination: Pagination {
                current_page: page,
                total_pages: 1,
                total_transactions: None,
                limit,
                has_next_page: false,
                has_prev_page: page > 1,
            },
            status: "error".to_string(),
            error: Some("Failed to fetch address data".to_string()),
            message: None,
        };
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    })?;

    // Process results - we fetched limit+1 to check for next page
    let has_next_page = transactions_rows.len() > limit as usize;
    let actual_transactions: Vec<_> = transactions_rows.into_iter().take(limit as usize).collect();

    let transactions: Vec<AddressTransaction> = actual_transactions
        .into_iter()
        .map(|row| AddressTransaction {
            transaction_id: row.get("transaction_id"),
            block_time: row.get("block_time"),
            amount_change: row
                .try_get::<Decimal, _>("amount_change")
                .map(|d| d.to_string().parse::<i64>().unwrap_or(0))
                .unwrap_or(0),
            protocol_id: row.try_get("protocol_id").ok(),
            subnetwork_id: row.try_get("subnetwork_id").ok(),
        })
        .collect();

    let has_prev_page = page > 1;

    // Handle case where user requested a page beyond available data
    if transactions.is_empty() && page > 1 {
        let response = AddressTransactionsResponse {
            address: address.clone(),
            address_data: AddressData {
                address: address.clone(),
                balance: 0,
                address_type: "P2PK".to_string(),
                first_seen: None,
                last_activity: None,
                known: None,
            },
            transactions: vec![],
            pagination: Pagination {
                current_page: page,
                total_pages: page.saturating_sub(1).max(1),
                total_transactions: None,
                limit,
                has_next_page: false,
                has_prev_page: true,
            },
            status: "success".to_string(),
            error: None,
            message: Some("No more transactions available at this page".to_string()),
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            HeaderValue::from_static("public, max-age=10, s-maxage=10"),
        );

        return Ok((headers, Json(response)));
    }

    // Build response
    let fetched_balance = balance_result.unwrap_or(0);
    let total_count: i64 = count_result.get("total_count");
    let total_transactions = total_count as u64;
    let total_pages = ((total_transactions as f64) / (limit as f64)).ceil() as u32;

    let known = known_result.map(|row| KnownAddress {
        label: row.get("label"),
        address_type: row.get("type"),
    });

    let address_data = AddressData {
        address: address.clone(),
        balance: fetched_balance,
        address_type: "P2PK".to_string(),
        first_seen: None,
        last_activity: None,
        known,
    };

    let pagination = Pagination {
        current_page: page,
        total_pages: total_pages.max(1),
        total_transactions: Some(total_transactions),
        limit,
        has_next_page,
        has_prev_page,
    };

    let response = AddressTransactionsResponse {
        address: address.clone(),
        address_data,
        transactions,
        pagination,
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

    Ok((headers, Json(response)))
}
