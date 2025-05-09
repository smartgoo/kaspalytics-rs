use crate::cache::Cache;
use chrono::Utc;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

async fn coinbase_transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: &str,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_transaction_count)
        .sum();

    let sql = format!(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('{}', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
        key
    );

    sqlx::query(&sql)
        .bind(count as i64)
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

    Ok(())
}

async fn coinbase_accepted_transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: &str,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_accepted_transaction_count)
        .sum();

    let sql = format!(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('{}', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
        key
    );

    sqlx::query(&sql)
        .bind(count as i64)
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

    Ok(())
}

async fn transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: &str,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.transaction_count)
        .sum();

    let sql = format!(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('{}', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
        key
    );

    sqlx::query(&sql)
        .bind(count as i64)
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

    Ok(())
}

async fn unique_transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: &str,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_count)
        .sum();

    let sql = format!(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('{}', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
        key
    );

    sqlx::query(&sql)
        .bind(count as i64)
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

    Ok(())
}

async fn unique_transaction_accepted_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: &str,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_accepted_count)
        .sum();

    let sql = format!(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('{}', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
        key
    );

    sqlx::query(&sql)
        .bind(count as i64)
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

    Ok(())
}

async fn accepted_count_per_hour_24h(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
) -> Result<(), sqlx::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let current_hour = now - (now % 3600);
    let cutoff = current_hour - (23 * 3600);
    let mut effective_count_per_hour = HashMap::<u64, u64>::new();

    cache
        .seconds
        .iter()
        .map(|entry| {
            let second = *entry.key();
            let hour = second - (second % 3600);
            (
                hour,
                entry.value().coinbase_accepted_transaction_count
                    + entry.value().unique_transaction_accepted_count,
            )
        })
        .filter(|(hour, _)| *hour >= cutoff)
        .for_each(|(hour, count)| {
            *effective_count_per_hour.entry(hour).or_insert(0) += count;
        });

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('effective_transaction_count_per_hour_24h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(serde_json::to_string(&effective_count_per_hour).unwrap())
    .bind(Utc::now())
    .execute(pg_pool)
    .await?;

    Ok(())
}

async fn accepted_count_per_minute_60m(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
) -> Result<(), sqlx::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let current_minute = now - (now % 60);
    let cutoff = current_minute - (59 * 60);
    let mut effective_count_per_minute = HashMap::<u64, u64>::new();

    cache
        .seconds
        .iter()
        .map(|entry| {
            let second = *entry.key();
            let minute = second - (second % 60);
            (
                minute,
                entry.value().coinbase_accepted_transaction_count
                    + entry.value().unique_transaction_accepted_count,
            )
        })
        .filter(|(minute, _)| *minute >= cutoff)
        .for_each(|(minute, count)| {
            *effective_count_per_minute.entry(minute).or_insert(0) += count;
        });

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('effective_transaction_count_per_minute_60m', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(serde_json::to_string(&effective_count_per_minute).unwrap())
    .bind(Utc::now())
    .execute(pg_pool)
    .await?;

    Ok(())
}

async fn accepted_count_per_second_60s(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
) -> Result<(), sqlx::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let threshold = now - 60;
    let mut effective_count_per_second = HashMap::<u64, u64>::new();

    cache
        .seconds
        .iter()
        .map(|entry| {
            let second = *entry.key();
            (
                second,
                entry.value().coinbase_accepted_transaction_count
                    + entry.value().unique_transaction_accepted_count,
            )
        })
        .filter(|(second, _)| *second >= threshold)
        .for_each(|(second, count)| {
            *effective_count_per_second.entry(second).or_insert(0) += count;
        });

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('effective_transaction_count_per_second_60s', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(serde_json::to_string(&effective_count_per_second).unwrap())
    .bind(Utc::now())
    .execute(pg_pool)
    .await?;

    Ok(())
}

pub async fn run(cache: Arc<Cache>, pg_pool: &PgPool) -> Result<(), sqlx::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let threshold = now - 86400;

    coinbase_transaction_count(
        &cache,
        pg_pool,
        "coinbase_transaction_count_86400s",
        threshold,
    )
    .await?;

    coinbase_accepted_transaction_count(
        &cache,
        pg_pool,
        "coinbase_accepted_transaction_count_86400s",
        threshold,
    )
    .await?;

    transaction_count(&cache, pg_pool, "transaction_count_86400s", threshold).await?;

    unique_transaction_count(
        &cache,
        pg_pool,
        "unique_transaction_count_86400s",
        threshold,
    )
    .await?;

    unique_transaction_accepted_count(
        &cache,
        pg_pool,
        "unique_transaction_accepted_count_86400s",
        threshold,
    )
    .await?;

    accepted_count_per_hour_24h(&cache, pg_pool).await.unwrap();
    accepted_count_per_minute_60m(&cache, pg_pool)
        .await
        .unwrap();
    accepted_count_per_second_60s(&cache, pg_pool)
        .await
        .unwrap();

    Ok(())
}
