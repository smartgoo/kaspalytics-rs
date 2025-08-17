use super::super::super::AppState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
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
) -> Result<(HeaderMap, Json<UtxoCountResponse>), (StatusCode, Json<UtxoCountResponse>)> {
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

    // Add cache headers
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=10, s-maxage=10"),
    );

    Ok((headers, Json(response)))
}
