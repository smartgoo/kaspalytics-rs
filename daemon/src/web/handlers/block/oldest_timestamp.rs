use super::super::super::AppState;
use axum::{
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use chrono::{DateTime, Utc};
use kaspalytics_utils::log::LogTarget;
use serde::Serialize;
use sqlx::Row;

#[derive(Serialize)]
pub struct OldestTimestampResponse {
    oldest_block_timestamp: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    error: String,
}

pub async fn get_oldest_timestamp(
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<OldestTimestampResponse>), (StatusCode, Json<ErrorResponse>)> {
    let query = "SELECT MIN(block_time) as oldest_block_timestamp FROM kaspad.blocks";

    match sqlx::query(query).fetch_one(&state.context.pg_pool).await {
        Ok(row) => {
            if let Ok(timestamp) = row.try_get::<DateTime<Utc>, _>("oldest_block_timestamp") {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "Cache-Control",
                    HeaderValue::from_static("public, max-age=300, s-maxage=300"),
                );

                Ok((
                    headers,
                    Json(OldestTimestampResponse {
                        oldest_block_timestamp: timestamp,
                    }),
                ))
            } else {
                Err((
                    StatusCode::NOT_FOUND,
                    Json(ErrorResponse {
                        error: "No blocks found in database".to_string(),
                    }),
                ))
            }
        }
        Err(e) => {
            log::error!(
                target: LogTarget::WebErr.as_str(),
                "Error fetching oldest block timestamp: {}",
                e
            );
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to fetch oldest block timestamp".to_string(),
                }),
            ))
        }
    }
}
