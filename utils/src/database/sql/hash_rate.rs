use chrono::{DateTime, Utc};
use sqlx::PgPool;

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
