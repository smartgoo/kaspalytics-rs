use super::super::super::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use kaspa_addresses::Address as KaspaAddress;
use kaspalytics_utils::log::LogTarget;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct ChartQuery {
    range: Option<String>,
    include_coinbase: Option<String>,
}

#[derive(Serialize)]
pub struct TransactionCountChartResponse {
    pub status: String,
    #[serde(rename = "chartData")]
    pub chart_data: Option<ChartData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ChartData {
    pub datasets: Vec<Dataset>,
    pub labels: Vec<String>,
}

#[derive(Serialize)]
pub struct Dataset {
    pub label: String,
    pub data: Vec<u32>,
    #[serde(rename = "backgroundColor")]
    pub background_color: String,
    #[serde(rename = "borderColor")]
    pub border_color: String,
    #[serde(rename = "borderWidth")]
    pub border_width: u32,
    pub fill: bool,
    pub tension: f32,
}

#[derive(Serialize)]
pub struct BucketData {
    pub timestamp: String,
    #[serde(rename = "Transactions")]
    pub transactions: u32,
}

fn is_valid_kaspa_address(address: &str) -> bool {
    KaspaAddress::try_from(address).is_ok()
}

fn parse_include_coinbase(param: &str) -> bool {
    let lower = param.to_lowercase();
    !(lower == "0" || lower == "false" || lower == "no")
}

fn get_time_config(range: &str) -> (&str, &str, Duration) {
    match range {
        "1h" => ("minute", "1 hour", Duration::hours(1)),
        "7d" | "7days" => ("hour", "7 days", Duration::days(7)),
        _ => ("hour", "24 hours", Duration::days(1)), // Default to 24h
    }
}

fn truncate_to_bucket(datetime: DateTime<Utc>, unit: &str) -> DateTime<Utc> {
    match unit {
        "minute" => Utc
            .with_ymd_and_hms(
                datetime.year(),
                datetime.month(),
                datetime.day(),
                datetime.hour(),
                datetime.minute(),
                0,
            )
            .unwrap(),
        _ => Utc
            .with_ymd_and_hms(
                datetime.year(),
                datetime.month(),
                datetime.day(),
                datetime.hour(),
                0,
                0,
            )
            .unwrap(),
    }
}

fn generate_time_buckets(range: &str, unit: &str) -> Vec<DateTime<Utc>> {
    let now = Utc::now();
    let (_, _, duration) = get_time_config(range);
    let start_time = truncate_to_bucket(now - duration, unit);
    let mut buckets = Vec::new();

    let increment = match unit {
        "minute" => Duration::minutes(1),
        _ => Duration::hours(1), // "hour"
    };

    let mut current = start_time;
    while current <= now {
        buckets.push(current);
        current += increment;
    }

    buckets
}

fn pg_to_chart_js(buckets: Vec<BucketData>) -> ChartData {
    let labels: Vec<String> = buckets.iter().map(|b| b.timestamp.clone()).collect();
    let data: Vec<u32> = buckets.iter().map(|b| b.transactions).collect();

    ChartData {
        datasets: vec![Dataset {
            label: "Transactions".to_string(),
            data,
            background_color: "rgba(54, 162, 235, 0.2)".to_string(),
            border_color: "rgba(54, 162, 235, 1)".to_string(),
            border_width: 2,
            fill: false,
            tension: 0.1,
        }],
        labels,
    }
}

pub async fn get_transaction_count_chart(
    Path(address): Path<String>,
    Query(params): Query<ChartQuery>,
    State(state): State<AppState>,
) -> Result<
    (HeaderMap, Json<TransactionCountChartResponse>),
    (StatusCode, Json<TransactionCountChartResponse>),
> {
    // Validate address format
    if !is_valid_kaspa_address(&address) {
        let response = TransactionCountChartResponse {
            status: "invalid_format".to_string(),
            chart_data: None,
            error: Some("Invalid address format".to_string()),
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    let range = params.range.as_deref().unwrap_or("24h");
    let include_coinbase = params
        .include_coinbase
        .as_deref()
        .map(parse_include_coinbase)
        .unwrap_or(true);

    let (date_trunc_unit, interval_expr, _) = get_time_config(range);

    // Build queries
    let coinbase_filter = if include_coinbase {
        "".to_string()
    } else {
        "AND to2.is_coinbase = false".to_string()
    };

    let inputs_query = format!(
        r#"
        SELECT 
            ti.transaction_id,
            ti.block_time
        FROM kaspad.transactions_inputs ti
        WHERE ti.utxo_script_public_key_address = $1 
            AND ti.block_time >= (now() at time zone 'utc') - interval '{}'
            AND ti.block_time <= (now() at time zone 'utc')
        ORDER BY ti.block_time DESC
        "#,
        interval_expr
    );

    let outputs_query = format!(
        r#"
        SELECT 
            to2.transaction_id,
            to2.block_time
        FROM kaspad.transactions_outputs to2
        WHERE to2.script_public_key_address = $1 
            AND to2.block_time >= (now() at time zone 'utc') - interval '{}'
            AND to2.block_time <= (now() at time zone 'utc')
            {}
        ORDER BY to2.block_time DESC
        "#,
        interval_expr, coinbase_filter
    );

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Executing parallel queries for transaction count chart: address={}, range={}, include_coinbase={}",
        address, range, include_coinbase
    );

    // Execute both queries in parallel
    let (inputs_result, outputs_result) = tokio::try_join!(
        sqlx::query(&inputs_query)
            .bind(&address)
            .fetch_all(&state.context.pg_pool),
        sqlx::query(&outputs_query)
            .bind(&address)
            .fetch_all(&state.context.pg_pool)
    )
    .map_err(|e| {
        log::error!(
            target: LogTarget::WebErr.as_str(),
            "Database error fetching transaction count chart for {}: {}",
            address,
            e,
        );
        let response = TransactionCountChartResponse {
            status: "error".to_string(),
            chart_data: None,
            error: Some("Internal Server Error".to_string()),
        };
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    })?;

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Parallel queries completed: inputs={}, outputs={}",
        inputs_result.len(),
        outputs_result.len()
    );

    // JavaScript-style aggregation: combine and deduplicate transactions by time bucket
    let mut transaction_map: HashMap<String, DateTime<Utc>> = HashMap::new();

    // Process inputs - deduplicate by transaction_id first
    let mut input_tx_map: HashMap<String, DateTime<Utc>> = HashMap::new();
    for row in inputs_result {
        let tx_id = hex::encode(row.get::<Vec<u8>, _>("transaction_id"));
        let block_time: DateTime<Utc> = row.get("block_time");

        input_tx_map.entry(tx_id).or_insert(block_time);
    }

    // Add deduplicated input transactions to main map
    for (tx_id, block_time) in input_tx_map {
        transaction_map.insert(tx_id, block_time);
    }

    // Process outputs - deduplicate by transaction_id first
    let mut output_tx_map: HashMap<String, DateTime<Utc>> = HashMap::new();
    for row in outputs_result {
        let tx_id = hex::encode(row.get::<Vec<u8>, _>("transaction_id"));
        let block_time: DateTime<Utc> = row.get("block_time");

        output_tx_map.entry(tx_id).or_insert(block_time);
    }

    // Add deduplicated output transactions to main map (don't overwrite existing inputs)
    for (tx_id, block_time) in output_tx_map {
        if !transaction_map.contains_key(&tx_id) {
            transaction_map.insert(tx_id.clone(), block_time);
        }
        transaction_map.entry(tx_id).or_insert(block_time);
    }

    // Group by time buckets
    let mut bucket_counts: HashMap<String, u32> = HashMap::new();

    for block_time in transaction_map.values() {
        let bucket_key = truncate_to_bucket(*block_time, date_trunc_unit);
        let bucket_str = bucket_key.to_rfc3339();

        *bucket_counts.entry(bucket_str).or_insert(0) += 1;
    }

    // Generate time buckets and fill in data
    let time_buckets = generate_time_buckets(range, date_trunc_unit);
    let mut buckets = Vec::new();

    for bucket_time in time_buckets {
        let bucket_key = bucket_time.to_rfc3339();
        let count = bucket_counts.get(&bucket_key).copied().unwrap_or(0);

        buckets.push(BucketData {
            timestamp: bucket_key,
            transactions: count,
        });
    }

    log::debug!(
        target: LogTarget::Web.as_str(),
        "JavaScript aggregation completed: {} unique transactions across {} buckets",
        transaction_map.len(),
        buckets.len()
    );

    let chart_data = pg_to_chart_js(buckets);

    let response = TransactionCountChartResponse {
        status: "success".to_string(),
        chart_data: Some(chart_data),
        error: None,
    };

    // Add cache headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=30, s-maxage=30"),
    );
    headers.insert(
        "Vercel-CDN-Cache-Control",
        HeaderValue::from_static("public, max-age=30"),
    );

    Ok((headers, Json(response)))
}
