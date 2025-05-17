use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{prelude::FromRow, PgPool};

pub async fn insert(
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

#[derive(FromRow)]
pub struct HashRate {
    pub hash_rate: Decimal,
    pub difficulty: Decimal,
    pub timestamp: DateTime<Utc>,
}

pub async fn get(pg_pool: &PgPool) -> Result<HashRate, sqlx::Error> {
    let hash_rate = sqlx::query_as(
        r#"
            SELECT hash_rate, difficulty, timestamp
            FROM hash_rate
            ORDER BY timestamp desc
            LIMIT 1;
        "#,
    )
    .fetch_one(pg_pool)
    .await?;

    Ok(hash_rate)
}

pub async fn get_x_days_ago(pg_pool: &PgPool, days: u64) -> Result<HashRate, sqlx::Error> {
    let hash_rate = sqlx::query_as(
        r#"
            SELECT hash_rate, difficulty, timestamp
            FROM hash_rate
            ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - (CURRENT_TIMESTAMP - INTERVAL '$1 days'))))
            LIMIT 1;
        "#,
    )
    .bind(days as i64)
    .fetch_one(pg_pool)
    .await?;

    Ok(hash_rate)
}