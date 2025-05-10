use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use sqlx::PgPool;
use std::sync::Arc;

pub async fn snapshot_hash_rate(rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) {
    let GetBlockDagInfoResponse {
        past_median_time,
        difficulty,
        // tip_hashes,
        ..
    } = rpc_client.get_block_dag_info().await.unwrap();
    let timestamp = chrono::DateTime::from_timestamp((past_median_time / 1000) as i64, 0).unwrap();

    let hash_rate = (difficulty * 2f64) as u64;
    let hash_rate_10bps = hash_rate * 10u64;
    // let hash_rate = rpc_client.estimate_network_hashes_per_second(1000, None).await.unwrap();

    kaspalytics_utils::database::sql::hash_rate::insert(
        &pg_pool,
        timestamp,
        hash_rate_10bps,
        difficulty as u64,
    )
    .await
    .unwrap();
}
