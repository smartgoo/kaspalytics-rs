use crate::{ingest::cache::Reader, storage::Reader as StorageReader};

use super::super::super::AppState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use kaspa_hashes::Hash;
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

fn http_response(
    accepting_block_hash: String,
    accepting_block_blue_score: i64,
    sink_blue_score: i64,
) -> (HeaderMap, Json<ConfirmationResponse>) {
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
        HeaderValue::from_static("public, max-age=1, s-maxage=1"),
    );

    (headers, Json(response))
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

    let sink_blue_score = state.context.storage.get_sink_blue_score().await;

    let transaction_hash = Hash::from_bytes(transaction_id_bytes.clone().try_into().unwrap());

    let accepting_block_hash = state
        .context
        .dag_cache
        .get_transaction(&transaction_hash)
        .and_then(|tx| tx.accepting_block_hash);

    let accepting_block_blue_score = accepting_block_hash.and_then(|abh| {
        state
            .context
            .dag_cache
            .get_block(&abh)
            .map(|block| block.blue_score)
    });

    // Try cache first - if both accepting block hash and blue score are available
    if let (Some(accepting_block_hash), Some(accepting_block_blue_score)) =
        (accepting_block_hash, accepting_block_blue_score)
    {
        return Ok(http_response(
            accepting_block_hash.to_string(),
            accepting_block_blue_score as i64,
            sink_blue_score.value as i64,
        ));
    }

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

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Executing parallel queries for confirmation count: transaction_id={}",
        transaction_id
    );

    // Execute both queries in parallel
    let transaction_result = sqlx::query(transaction_query)
        .bind(&transaction_id_bytes)
        .fetch_optional(&state.context.pg_pool)
        .await
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

    // Extract data
    let accepting_block_hash: String = transaction_row.get("accepting_block_hash");
    let accepting_block_blue_score: i64 = transaction_row.get("accepting_block_blue_score");

    // Calculate confirmation count
    // let confirmation_count = sink_blue_score.value as i64 - accepting_block_blue_score;

    // log::debug!(
    //     target: LogTarget::Web.as_str(),
    //     "Confirmation calculation: sink_blue_score={}, accepting_block_blue_score={}, confirmations={}",
    //     sink_blue_score.value,
    //     accepting_block_blue_score,
    //     confirmation_count
    // );

    // let response = ConfirmationResponse {
    //     confirmation_count,
    //     accepting_block_hash,
    //     accepting_block_blue_score,
    //     sink_blue_score: sink_blue_score.value as i64,
    // };

    // // Add cache headers
    // let mut headers = HeaderMap::new();
    // headers.insert(
    //     "Cache-Control",
    //     HeaderValue::from_static("public, max-age=1, s-maxage=1"),
    // );

    // Ok((headers, Json(response)))

    Ok(http_response(
        accepting_block_hash.to_string(),
        accepting_block_blue_score,
        sink_blue_score.value as i64,
    ))
}
