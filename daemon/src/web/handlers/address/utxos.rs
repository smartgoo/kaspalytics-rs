use super::super::super::AppState;
use crate::web::cache::{get_cached_json, set_cached_json};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri},
    Json,
};
use kaspa_rpc_core::api::rpc::RpcApi;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct UtxoCountResponse {
    address: String,
    count: usize,
}

pub async fn get_utxos_by_address(
    Path(address): Path<String>,
    State(state): State<AppState>,
    uri: Uri,
) -> Result<(HeaderMap, Json<UtxoCountResponse>), (StatusCode, Json<UtxoCountResponse>)> {
    // Check web cache first - use actual request URI as cache key
    let key = uri.to_string();
    if let Some(cached_json) = get_cached_json(&state.context.web_cache, &key).await {
        // Try to deserialize the cached JSON
        if let Ok(cached_response) = serde_json::from_str::<UtxoCountResponse>(&cached_json) {
            // Add cache headers to indicate this is a cached response
            let mut headers = HeaderMap::new();
            headers.insert(
                "Cache-Control",
                HeaderValue::from_static("public, max-age=15, s-maxage=15"),
            );
            headers.insert("X-Cache", HeaderValue::from_static("HIT"));

            return Ok((headers, Json(cached_response)));
        }
    }

    let parsed_address = kaspa_addresses::Address::try_from(address.as_str()).map_err(|_e| {
        let response = UtxoCountResponse {
            address: address.clone(),
            count: 0,
        };
        (StatusCode::BAD_REQUEST, Json(response))
    })?;

    let data = state
        .context
        .rpc_client
        .get_utxos_by_addresses(vec![parsed_address])
        .await
        .map_err(|_e| {
            let response = UtxoCountResponse {
                address: address.clone(),
                count: 0,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        })?;

    let count = data.len();
    let response = UtxoCountResponse { address, count };

    // Store successful response in web cache
    if let Ok(json) = serde_json::to_string(&response) {
        set_cached_json(&state.context.web_cache, key, json).await;
    }

    // Add cache headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=15, s-maxage=15"),
    );
    headers.insert("X-Cache", HeaderValue::from_static("MISS"));

    Ok((headers, Json(response)))
}
