use chrono::{DateTime, Utc};
use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use sqlx::PgPool;
use std::sync::Arc;

pub async fn insert_to_db(
    pg_pool: &PgPool,
    timestamp: DateTime<Utc>,
    hash_rate: u64,
    difficulty: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO hash_rate
        (timestamp, hash_rate, difficulty)
        VALUES ($1, $2, $3)
    "#,
    )
    .bind(timestamp)
    .bind(hash_rate as i64)
    .bind(difficulty as i64)
    .execute(pg_pool)
    .await?;

    Ok(())
}

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

    insert_to_db(&pg_pool, timestamp, hash_rate_10bps, difficulty as u64)
        .await
        .unwrap();
}
