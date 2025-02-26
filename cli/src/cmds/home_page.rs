use chrono::Utc;
use sqlx::PgPool;

pub async fn home_page_data_refresh(pg_pool: PgPool) {
    // TODO this fn is gross, getting in place quickly. Needs love

    let coin_response = kaspalytics_utils::coingecko::get_coin_data().await.unwrap();

    let date = Utc::now();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('price_usd', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#
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
        "#
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
        "#
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
        "#
    )
    .bind(coin_response.market_data.total_volume.usd)
    .bind(date)
    .execute(&pg_pool)
    .await
    .unwrap();
}