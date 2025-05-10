use chrono::DateTime;
use sqlx::PgPool;

pub async fn insert(
    pg_pool: &PgPool,
    daa_score: u64,
    block_timestamp: u64,
) -> Result<(), sqlx::Error> {
    let block_dt = DateTime::from_timestamp_millis(block_timestamp as i64).unwrap();

    sqlx::query(
        r#"
        INSERT INTO daa_snapshot
        (daa_score, block_timestamp)
        VALUES ($1, $2)
    "#,
    )
    .bind(daa_score as i64)
    .bind(block_dt)
    .execute(pg_pool)
    .await?;

    Ok(())
}
