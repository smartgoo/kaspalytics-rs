use super::super::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use kaspa_rpc_core::api::rpc::RpcApi;
use serde::Serialize;

#[derive(Serialize)]
pub struct BalanceResponse {
    address: String,
    balance: u64,
}

#[derive(Serialize)]
pub struct UtxoCountResponse {
    address: String,
    count: usize,
}

pub async fn get_balance(
    Path(address): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<BalanceResponse>, (StatusCode, String)> {
    let parsed_address = kaspa_addresses::Address::try_from(address.as_str())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid address: {}", e)))?;

    let balance = state
        .context
        .rpc_client
        .get_balance_by_address(parsed_address)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(BalanceResponse { address, balance }))
}

pub async fn get_utxos_by_address(
    Path(address): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<UtxoCountResponse>, (StatusCode, String)> {
    let parsed_address = kaspa_addresses::Address::try_from(address.as_str())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid address: {}", e)))?;

    let data = state
        .context
        .rpc_client
        .get_utxos_by_addresses(vec![parsed_address])
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let count = data.len();
    Ok(Json(UtxoCountResponse { address, count }))
}
