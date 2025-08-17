use super::super::AppState;
use crate::ingest::cache::Reader;
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
pub struct ExplorerResponse {
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

fn is_valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit())
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex)
}

pub async fn search_value(
    Path(value): Path<String>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<ExplorerResponse>), (StatusCode, Json<ExplorerResponse>)> {
    // Validate hash format
    if !is_valid_hash(&value) {
        let response = ExplorerResponse {
            found: false,
            r#type: None,
        };
        return Err((StatusCode::BAD_REQUEST, Json(response)));
    }

    // Convert hex string to bytes
    let value_bytes = match hex_to_bytes(&value) {
        Ok(bytes) => bytes,
        Err(_) => {
            let response = ExplorerResponse {
                found: false,
                r#type: None,
            };
            return Err((StatusCode::BAD_REQUEST, Json(response)));
        }
    };

    // Convert bytes to Hash for DagCache lookup
    let hash_obj = Hash::from_bytes(value_bytes.clone().try_into().map_err(|_| {
        let response = ExplorerResponse {
            found: false,
            r#type: None,
        };
        (StatusCode::BAD_REQUEST, Json(response))
    })?);

    // First, check DagCache for transaction
    if let Some(_cache_tx) = state.context.dag_cache.get_transaction(&hash_obj) {
        log::debug!(
            target: LogTarget::Web.as_str(),
            "Searched value {} found as transaction in DagCache",
            value
        );

        let response = ExplorerResponse {
            found: true,
            r#type: Some("transaction".to_string()),
        };

        // Add cache headers for found hash
        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            HeaderValue::from_static("public, max-age=300, s-maxage=300"),
        );

        return Ok((headers, Json(response)));
    }

    // Check DagCache for block
    if let Some(_cache_block) = state.context.dag_cache.get_block(&hash_obj) {
        log::debug!(
            target: LogTarget::Web.as_str(),
            "Searched value {} found as block in DagCache",
            value
        );

        let response = ExplorerResponse {
            found: true,
            r#type: Some("block".to_string()),
        };

        // Add cache headers for found hash
        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            HeaderValue::from_static("public, max-age=300, s-maxage=300"),
        );

        return Ok((headers, Json(response)));
    }

    log::debug!(
        target: LogTarget::Web.as_str(),
        "Searched value {} not found in DagCache, querying database",
        value,
    );

    // Database search query - matches JavaScript implementation exactly
    let search_query = r#"
        (SELECT 'transaction' as type, 1 as found
        FROM kaspad.transactions 
        WHERE transaction_id = $1 
        LIMIT 1)
        
        UNION ALL
        
        (SELECT 'block' as type, 1 as found
        FROM kaspad.blocks 
        WHERE block_hash = $1 
        LIMIT 1)
    "#;

    let result = sqlx::query(search_query)
        .bind(&value_bytes)
        .fetch_all(&state.context.pg_pool)
        .await
        .map_err(|e| {
            log::error!(
                target: LogTarget::WebErr.as_str(),
                "Database error searching for hash {}: {}",
                value,
                e,
            );
            let response = ExplorerResponse {
                found: false,
                r#type: None,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        })?;

    if result.is_empty() {
        let response = ExplorerResponse {
            found: false,
            r#type: None,
        };
        // Cache headers will be set by middleware for not found responses
        let mut headers = HeaderMap::new();
        headers.insert(
            "Cache-Control",
            HeaderValue::from_static("public, max-age=300, s-maxage=300"),
        );
        return Ok((headers, Json(response)));
    }

    // Return the first match (transaction takes precedence if both exist)
    let match_row = &result[0];
    let hash_type: String = match_row.get("type");

    let response = ExplorerResponse {
        found: true,
        r#type: Some(hash_type),
    };

    // Add cache headers for found hash
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=300, s-maxage=300"),
    );
    Ok((headers, Json(response)))
}
