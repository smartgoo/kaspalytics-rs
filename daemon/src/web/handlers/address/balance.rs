use super::super::super::AppState;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    Json,
};
use kaspa_rpc_core::api::rpc::RpcApi;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct BalanceResponse {
    address: String,
    balance: u64,
}

pub async fn get_balance(
    Path(address): Path<String>,
    State(state): State<AppState>,
) -> Result<(HeaderMap, Json<BalanceResponse>), (StatusCode, Json<BalanceResponse>)> {
    let parsed_address = kaspa_addresses::Address::try_from(address.as_str()).map_err(|_e| {
        let response = BalanceResponse {
            address: address.clone(),
            balance: 0,
        };
        (StatusCode::BAD_REQUEST, Json(response))
    })?;

    let balance = state
        .context
        .rpc_client
        .get_balance_by_address(parsed_address)
        .await
        .map_err(|_e| {
            let response = BalanceResponse {
                address: address.clone(),
                balance: 0,
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        })?;

    let response = BalanceResponse { address, balance };

    // Add cache headers for found balance
    let mut headers = HeaderMap::new();
    headers.insert(
        "Cache-Control",
        HeaderValue::from_static("public, max-age=10, s-maxage=10"),
    );

    Ok((headers, Json(response)))
}
