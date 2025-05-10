use chrono::{DateTime, Utc};
use kaspa_consensus_core::Hash;
use sqlx::PgPool;

pub async fn upsert_price_usd(
    pg_pool: &PgPool,
    price: f64,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('price_usd', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(price)
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn upsert_price_btc(
    pg_pool: &PgPool,
    price: f64,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('price_btc', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(price)
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn upsert_market_cap(
    pg_pool: &PgPool,
    market_cap: f64,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('market_cap', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(market_cap)
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn upsert_volume(
    pg_pool: &PgPool,
    volume: f64,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('volume', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(volume)
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn upsert_daa_score(
    pg_pool: &PgPool,
    daa_score: u64,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('daa_score', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(daa_score as i64)
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn upsert_pruning_point(
    pg_pool: &PgPool,
    pruning_point: Hash,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('pruning_point', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(pruning_point.to_string())
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn upsert_cs_sompi(
    pg_pool: &PgPool,
    cs_sompi: u64,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('cs_sompi', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(cs_sompi as i64)
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}
