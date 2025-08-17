use super::super::super::AppState;
use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use kaspalytics_utils::log::LogTarget;
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Deserialize)]
pub struct ActiveAddressesQuery {
    timeframe: Option<String>,
}

#[derive(Serialize)]
pub struct ActiveAddressesResponse {
    pub status: String,
    pub addresses: Vec<AddressActivityData>,
    #[serde(rename = "timeFrame")]
    pub time_frame: String,
    #[serde(rename = "type")]
    pub address_type: String,
    pub optimization: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct AddressActivityData {
    pub address: String,
    pub transaction_count: i64,
    pub total_spent: i64,
    pub known_label: Option<String>,
    pub known_type: Option<String>,
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
        SELECT agg.address,
               SUM(agg.transaction_count)::bigint AS transaction_count,
               SUM(agg.total_spent)::bigint AS total_spent,
               ka.label AS known_label,
               ka.type AS known_type
        FROM kaspad.address_spending_per_minute agg
        LEFT JOIN known_addresses ka ON ka.address = agg.address
        WHERE agg.minute_bucket >= date_trunc('day', now() at time zone 'utc')
          AND agg.minute_bucket < date_trunc('day', now() at time zone 'utc') + INTERVAL '1 day'
        GROUP BY agg.address, ka.label, ka.type
        ORDER BY transaction_count DESC
        LIMIT 100
    "#
    .to_string()
}

fn build_yesterday_query() -> String {
    r#"
        SELECT agg.address,
               SUM(agg.transaction_count)::bigint AS transaction_count,
               SUM(agg.total_spent)::bigint AS total_spent,
               ka.label AS known_label,
               ka.type AS known_type
        FROM kaspad.address_spending_per_minute agg
        LEFT JOIN known_addresses ka ON ka.address = agg.address
        WHERE agg.minute_bucket >= (date_trunc('day', now() at time zone 'utc') - INTERVAL '1 day')
          AND agg.minute_bucket < date_trunc('day', now() at time zone 'utc')
        GROUP BY agg.address, ka.label, ka.type
        ORDER BY transaction_count DESC
        LIMIT 100
    "#
    .to_string()
}

fn build_interval_query(interval_expression: &str) -> String {
    format!(
        r#"
        SELECT agg.address,
               SUM(agg.transaction_count)::bigint AS transaction_count,
               SUM(agg.total_spent)::bigint AS total_spent,
               ka.label AS known_label,
               ka.type AS known_type
        FROM kaspad.address_spending_per_minute agg
        LEFT JOIN known_addresses ka ON ka.address = agg.address
        WHERE agg.minute_bucket >= (now() at time zone 'utc' - {})
          AND agg.minute_bucket <= (now() at time zone 'utc')
        GROUP BY agg.address, ka.label, ka.type
        ORDER BY transaction_count DESC
        LIMIT 100
        "#,
        interval_expression
    )
}

pub async fn get_most_active_addresses(
    Query(params): Query<ActiveAddressesQuery>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<ActiveAddressesResponse>), (StatusCode, Json<ActiveAddressesResponse>)>
{
    let timeframe = params.timeframe.as_deref().unwrap_or("24h");

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Most active addresses request: timeframe={}",
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
                "Database error fetching most active addresses: {}",
                e,
            );
            let response = ActiveAddressesResponse {
                status: "error".to_string(),
                addresses: vec![],
                time_frame: timeframe.to_string(),
                address_type: "spending".to_string(),
                optimization: "database_aggregate".to_string(),
                error: Some(e.to_string()),
            };
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
        }
    };

    // Process results
    let addresses: Vec<AddressActivityData> = rows
        .into_iter()
        .map(|row| AddressActivityData {
            address: row.get("address"),
            transaction_count: row.get("transaction_count"),
            total_spent: row.get("total_spent"),
            known_label: row.try_get("known_label").ok(),
            known_type: row.try_get("known_type").ok(),
        })
        .collect();

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Most active addresses query completed: {} addresses found",
        addresses.len()
    );

    let response = ActiveAddressesResponse {
        status: "success".to_string(),
        addresses,
        time_frame: timeframe.to_string(),
        address_type: "spending".to_string(),
        optimization: "database_aggregate".to_string(),
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
