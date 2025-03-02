use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use sqlx::PgPool;
use std::sync::Arc;

pub async fn insert_to_db(
    pg_pool: &PgPool,
    daa_score: u64,
    block_timestamp: u64,
) -> Result<(), sqlx::Error> {
    let block_dt = chrono::DateTime::from_timestamp((block_timestamp / 1000) as i64, 0).unwrap();

    sqlx::query(
        r#"
        INSERT INTO daa_snapshot
        (daa_score, block_timestamp_milliseconds, block_timestamp)
        VALUES ($1, $2, $3)
    "#,
    )
    .bind(daa_score as i64)
    .bind(block_timestamp as i64)
    .bind(block_dt)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn snapshot_daa_timestamp(rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) {
    let GetBlockDagInfoResponse { tip_hashes, .. } = rpc_client.get_block_dag_info().await.unwrap();

    let block = rpc_client.get_block(tip_hashes[0], false).await.unwrap();

    insert_to_db(&pg_pool, block.header.daa_score, block.header.timestamp)
        .await
        .unwrap();
}
