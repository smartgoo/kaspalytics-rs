use kaspa_rpc_core::{api::rpc::RpcApi, GetBlockDagInfoResponse};
use kaspa_wrpc_client::KaspaRpcClient;
use sqlx::PgPool;
use std::sync::Arc;

pub async fn snapshot_daa_timestamp(rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) {
    let GetBlockDagInfoResponse { tip_hashes, .. } = rpc_client.get_block_dag_info().await.unwrap();

    let block = rpc_client.get_block(tip_hashes[0], false).await.unwrap();

    kaspalytics_utils::database::sql::daa_snapshot::insert(
        &pg_pool,
        block.header.daa_score,
        block.header.timestamp,
    )
    .await
    .unwrap();
}
