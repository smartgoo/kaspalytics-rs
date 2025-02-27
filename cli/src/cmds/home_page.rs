use chrono::Utc;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_wrpc_client::KaspaRpcClient;
use sqlx::PgPool;
use std::sync::Arc;

pub async fn home_page_data_refresh(rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) {
    // TODO this fn is gross, getting in place quickly. Needs love
    let coin_response = kaspalytics_utils::coingecko::get_coin_data().await.unwrap();
    let date = Utc::now();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('price_usd', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(coin_response.market_data.current_price.usd)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('price_btc', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(coin_response.market_data.current_price.btc)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('market_cap', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(coin_response.market_data.market_cap.usd)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('volume', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(coin_response.market_data.total_volume.usd)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();

    let block_dag = rpc_client.get_block_dag_info().await.unwrap();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('daa_score', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(block_dag.virtual_daa_score as i64)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('pruning_point', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(block_dag.pruning_point_hash.to_string())
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();

    let coin_supply = rpc_client.get_coin_supply().await.unwrap();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('cs_sompi', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(coin_supply.circulating_sompi as i64)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();
}
