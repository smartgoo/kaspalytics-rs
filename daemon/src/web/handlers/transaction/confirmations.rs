use super::super::super::AppState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use kaspalytics_utils::log::LogTarget;
use serde::Serialize;
use sqlx::Row;

#[derive(Serialize)]
pub struct ConfirmationResponse {
    #[serde(rename = "confirmationCount")]
    pub confirmation_count: i64,
    #[serde(rename = "acceptingBlockHash")]
    pub accepting_block_hash: String,
    #[serde(rename = "acceptingBlockBlueScore")]
    pub accepting_block_blue_score: i64,
    #[serde(rename = "sinkBlueScore")]
    pub sink_blue_score: i64,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

fn is_valid_transaction_id(tx_id: &str) -> bool {
    tx_id.len() == 64 && tx_id.chars().all(|c| c.is_ascii_hexdigit())
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex)
}

pub async fn get_confirmation_count(
    Path(transaction_id): Path<String>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<ConfirmationResponse>), (StatusCode, Json<ErrorResponse>)> {
    // Validate transaction ID format
    if !is_valid_transaction_id(&transaction_id) {
        let response = ErrorResponse {
            message: "Invalid transaction ID format. Transaction IDs must be 64-character hexadecimal strings.".to_string(),
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    // Convert hex string to bytes
    let transaction_id_bytes = match hex_to_bytes(&transaction_id) {
        Ok(bytes) => bytes,
        Err(_) => {
            let response = ErrorResponse {
                message: "Invalid hexadecimal format".to_string(),
            };
            return Err((StatusCode::BAD_REQUEST, Json(response)));
        }
    };

    // Database queries
    let transaction_query = r#"
        SELECT 
            encode(t.accepting_block_hash, 'hex') as accepting_block_hash,
            b.blue_score as accepting_block_blue_score
        FROM kaspad.transactions t
        LEFT JOIN kaspad.blocks b ON t.accepting_block_hash = b.block_hash
        WHERE t.transaction_id = $1
        AND t.accepting_block_hash IS NOT NULL
    "#;

    let sink_blue_score_query = r#"
        SELECT value::bigint as sink_blue_score
        FROM key_value 
        WHERE key = 'SinkBlueScore'
    "#;

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Executing parallel queries for confirmation count: transaction_id={}",
        transaction_id
    );

    // Execute both queries in parallel
    let (transaction_result, sink_result) = tokio::try_join!(
        sqlx::query(transaction_query)
            .bind(&transaction_id_bytes)
            .fetch_optional(&state.context.pg_pool),
        sqlx::query(sink_blue_score_query).fetch_optional(&state.context.pg_pool)
    )
    .map_err(|e| {
        log::error!(
            target: LogTarget::WebErr.as_str(),
            "Database error fetching confirmation count for {}: {}",
            transaction_id,
            e,
        );
        let response = ErrorResponse {
            message: "Failed to fetch confirmation count".to_string(),
        };
        (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
    })?;

    // Check if transaction exists and is accepted
    let transaction_row = match transaction_result {
        Some(row) => row,
        None => {
            let response = ErrorResponse {
                message: "Transaction not found or not accepted".to_string(),
            };
            return Err((StatusCode::NOT_FOUND, Json(response)));
        }
    };

    // Check if we have sink blue score
    let sink_row = match sink_result {
        Some(row) => row,
        None => {
            let response = ErrorResponse {
                message: "Unable to fetch current virtual blue score".to_string(),
            };
            return Err((StatusCode::INTERNAL_SERVER_ERROR, Json(response)));
        }
    };

    // Extract data
    let accepting_block_hash: String = transaction_row.get("accepting_block_hash");
    let accepting_block_blue_score: i64 = transaction_row.get("accepting_block_blue_score");
    let sink_blue_score: i64 = sink_row.get("sink_blue_score");

    // Calculate confirmation count
    let confirmation_count = sink_blue_score - accepting_block_blue_score;

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Confirmation calculation: sink_blue_score={}, accepting_block_blue_score={}, confirmations={}",
        sink_blue_score,
        accepting_block_blue_score,
        confirmation_count
    );

    let response = ConfirmationResponse {
        confirmation_count,
        accepting_block_hash,
        accepting_block_blue_score,
        sink_blue_score,
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
