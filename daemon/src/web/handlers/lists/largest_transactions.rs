use super::super::super::AppState;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use kaspalytics_utils::log::LogTarget;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Deserialize)]
pub struct LargestTransactionsQuery {
    timeframe: Option<String>,
}

#[derive(Serialize)]
pub struct LargestTransactionsResponse {
    pub status: String,
    pub transactions: Vec<TransactionAmountData>,
    #[serde(rename = "timeFrame")]
    pub time_frame: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct TransactionAmountData {
    pub transaction_id: String,
    pub block_time: DateTime<Utc>,
    pub protocol_id: Option<i32>,
    pub total_output_amount: i64,
}

fn get_interval_expression(timeframe: &str) -> Option<&'static str> {
    match timeframe {
        "15m" => Some("INTERVAL '15 MINUTES'"),
        "1h" => Some("INTERVAL '1 HOUR'"),
        "24h" => Some("INTERVAL '24 HOURS'"),
        "3d" => Some("INTERVAL '3 DAYS'"),
        "7d" => Some("INTERVAL '7 DAYS'"),
        _ => None,
    }
}

fn build_today_query() -> String {
    r#"
        SELECT encode(transaction_id, 'hex') AS transaction_id, block_time, protocol_id, total_output_amount
        FROM kaspad.transactions
        WHERE subnetwork_id = 0
          AND accepting_block_hash IS NOT NULL
          AND block_time >= date_trunc('day', now())
        ORDER BY total_output_amount DESC, block_time DESC
        LIMIT 100
    "#
    .to_string()
}

fn build_yesterday_query() -> String {
    r#"
        SELECT encode(transaction_id, 'hex') AS transaction_id, block_time, protocol_id, total_output_amount
        FROM kaspad.transactions
        WHERE subnetwork_id = 0
          AND accepting_block_hash IS NOT NULL
          AND block_time >= (date_trunc('day', now()) - INTERVAL '1 day')
          AND block_time < date_trunc('day', now())
        ORDER BY total_output_amount DESC, block_time DESC
        LIMIT 100
    "#
    .to_string()
}

fn build_interval_query(interval_expression: &str) -> String {
    format!(
        r#"
        SELECT encode(transaction_id, 'hex') AS transaction_id, block_time, protocol_id, total_output_amount
        FROM kaspad.transactions
        WHERE subnetwork_id = 0
          AND accepting_block_hash IS NOT NULL
          AND block_time >= (NOW() - {})
        ORDER BY total_output_amount DESC, block_time DESC
        LIMIT 100
        "#,
        interval_expression
    )
}

pub async fn get_largest_transactions(
    Query(params): Query<LargestTransactionsQuery>,
    State(state): State<AppState>,
) -> Result<
    (HeaderMap, Json<LargestTransactionsResponse>),
    (StatusCode, Json<LargestTransactionsResponse>),
> {
    let timeframe = params.timeframe.as_deref().unwrap_or("24h");

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Largest transactions request: timeframe={}",
        timeframe
    );

    // Build appropriate query
    let sql_query = if timeframe == "today" {
        build_today_query()
    } else if timeframe == "yesterday" {
        build_yesterday_query()
    } else {
        let interval_expression =
            get_interval_expression(timeframe).unwrap_or("INTERVAL '24 HOURS'");
        build_interval_query(interval_expression)
    };

    // Execute query
    let result = sqlx::query(&sql_query)
        .fetch_all(&state.context.pg_pool)
        .await;

    let rows = match result {
        Ok(rows) => rows,
        Err(e) => {
            log::error!(
                target: LogTarget::WebErr.as_str(),
                "Database error fetching largest transactions: {}",
                e,
            );
            let response = LargestTransactionsResponse {
                status: "error".to_string(),
                transactions: vec![],
                time_frame: timeframe.to_string(),
                error: Some(e.to_string()),
            };
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
        }
    };

    // Process results
    let transactions: Vec<TransactionAmountData> = rows
        .into_iter()
        .map(|row| TransactionAmountData {
            transaction_id: row.get("transaction_id"),
            block_time: row.get("block_time"),
            protocol_id: row.try_get("protocol_id").ok(),
            total_output_amount: row.get("total_output_amount"),
        })
        .collect();

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Largest transactions query completed: {} transactions found",
        transactions.len()
    );

    let response = LargestTransactionsResponse {
        status: "success".to_string(),
        transactions,
        time_frame: timeframe.to_string(),
        error: None,
    };

    // Add cache headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=5, s-maxage=5"),
    );

    Ok((headers, Json(response)))
}
