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
pub struct ProtocolActivityQuery {
    timeframe: Option<String>,
    start: Option<String>,
    end: Option<String>,
}

#[derive(Serialize)]
pub struct ProtocolActivityResponse {
    pub status: String,
    pub protocols: Vec<ProtocolData>,
    #[serde(rename = "timeFrame")]
    pub time_frame: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    pub optimization: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Serialize)]
pub struct ProtocolData {
    pub protocol_id: i32,
    pub transaction_count: i64,
    pub fees_generated: i64,
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

fn build_custom_query() -> String {
    r#"
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_minutely
        WHERE minute_bucket >= $1::timestamptz AND minute_bucket < $2::timestamptz
            AND protocol_id is not null
        GROUP BY protocol_id
        ORDER BY transaction_count DESC
    "#
    .to_string()
}

fn build_today_query() -> String {
    r#"
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_minutely
        WHERE minute_bucket >= date_trunc('day', now() at time zone 'utc')
          AND minute_bucket < date_trunc('day', now() at time zone 'utc') + INTERVAL '1 day'
          AND protocol_id is not null
        GROUP BY protocol_id
        ORDER BY transaction_count DESC
    "#
    .to_string()
}

fn build_yesterday_query() -> String {
    r#"
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_minutely
        WHERE minute_bucket >= (date_trunc('day', now() at time zone 'utc') - INTERVAL '1 day')
          AND minute_bucket < date_trunc('day', now() at time zone 'utc')
          AND protocol_id is not null
        GROUP BY protocol_id
        ORDER BY transaction_count DESC
    "#
    .to_string()
}

fn build_interval_query(interval_expression: &str) -> String {
    format!(
        r#"
        SELECT
          protocol_id,
          SUM(transaction_count)::bigint AS transaction_count,
          SUM(fees_generated)::bigint AS fees_generated
        FROM kaspad.protocol_activity_minutely
        WHERE minute_bucket >= (now() at time zone 'utc' - {})
          AND minute_bucket <= (now() at time zone 'utc')
          AND protocol_id is not null
        GROUP BY protocol_id
        ORDER BY transaction_count DESC
        "#,
        interval_expression
    )
}

pub async fn get_protocol_activity(
    Query(params): Query<ProtocolActivityQuery>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<ProtocolActivityResponse>), (StatusCode, Json<ProtocolActivityResponse>)>
{
    let timeframe = params.timeframe.as_deref().unwrap_or("24h");
    let custom_start_iso = params.start.as_deref();
    let custom_end_iso = params.end.as_deref();

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Protocol activity request: timeframe={}, start={:?}, end={:?}",
        timeframe,
        custom_start_iso,
        custom_end_iso
    );

    // Determine query type and build SQL
    let (sql_query, query_params, start_param, end_param) =
        if timeframe == "custom" && custom_start_iso.is_some() && custom_end_iso.is_some() {
            // Custom date range
            let start_str = custom_start_iso.unwrap();
            let end_str = custom_end_iso.unwrap();

            // Parse and validate dates
            let custom_start = match DateTime::parse_from_rfc3339(start_str) {
                Ok(dt) => dt.with_timezone(&Utc),
                Err(_) => {
                    let response = ProtocolActivityResponse {
                        status: "error".to_string(),
                        protocols: vec![],
                        time_frame: timeframe.to_string(),
                        start: Some(start_str.to_string()),
                        end: Some(end_str.to_string()),
                        optimization: "database_aggregate".to_string(),
                        error: Some("Invalid start date format".to_string()),
                    };
                    return Err((StatusCode::BAD_REQUEST, Json(response)));
                }
            };

            let custom_end = match DateTime::parse_from_rfc3339(end_str) {
                Ok(dt) => dt.with_timezone(&Utc),
                Err(_) => {
                    let response = ProtocolActivityResponse {
                        status: "error".to_string(),
                        protocols: vec![],
                        time_frame: timeframe.to_string(),
                        start: Some(start_str.to_string()),
                        end: Some(end_str.to_string()),
                        optimization: "database_aggregate".to_string(),
                        error: Some("Invalid end date format".to_string()),
                    };
                    return Err((StatusCode::BAD_REQUEST, Json(response)));
                }
            };

            (
                build_custom_query(),
                vec![custom_start, custom_end],
                Some(start_str.to_string()),
                Some(end_str.to_string()),
            )
        } else if timeframe == "today" {
            (build_today_query(), vec![], None, None)
        } else if timeframe == "yesterday" {
            (build_yesterday_query(), vec![], None, None)
        } else {
            // Standard time intervals
            let interval_expression =
                get_interval_expression(timeframe).unwrap_or("INTERVAL '24 HOURS'");
            (
                build_interval_query(interval_expression),
                vec![],
                None,
                None,
            )
        };

    // Execute query
    let result = if query_params.is_empty() {
        sqlx::query(&sql_query)
            .fetch_all(&state.context.pg_pool)
            .await
    } else {
        sqlx::query(&sql_query)
            .bind(query_params[0])
            .bind(query_params[1])
            .fetch_all(&state.context.pg_pool)
            .await
    };

    let rows = match result {
        Ok(rows) => rows,
        Err(e) => {
            log::error!(
                target: LogTarget::WebErr.as_str(),
                "Database error fetching protocol activity: {}",
                e,
            );
            let response = ProtocolActivityResponse {
                status: "error".to_string(),
                protocols: vec![],
                time_frame: timeframe.to_string(),
                start: start_param,
                end: end_param,
                optimization: "database_aggregate".to_string(),
                error: Some(e.to_string()),
            };
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
        }
    };

    // Process results
    let protocols: Vec<ProtocolData> = rows
        .into_iter()
        .map(|row| ProtocolData {
            protocol_id: row.get("protocol_id"),
            transaction_count: row.get("transaction_count"),
            fees_generated: row.get("fees_generated"),
        })
        .collect();

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Protocol activity query completed: {} protocols found",
        protocols.len()
    );

    let response = ProtocolActivityResponse {
        status: "success".to_string(),
        protocols,
        time_frame: timeframe.to_string(),
        start: start_param,
        end: end_param,
        optimization: "database_aggregate".to_string(),
        error: None,
    };

    // Add cache headers - short TTL for real-time data
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=5, s-maxage=5"),
    );

    Ok((headers, Json(response)))
}
