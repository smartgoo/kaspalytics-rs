use crate::cache::Cache;
use chrono::Utc;
use kaspalytics_utils::database::sql::{key_value, key_value::KeyRegistry};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

async fn coinbase_transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: KeyRegistry,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_transaction_count)
        .sum();

    key_value::upsert(pg_pool, key, count, Utc::now()).await?;

    Ok(())
}

async fn coinbase_accepted_transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: KeyRegistry,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_accepted_transaction_count)
        .sum();

    key_value::upsert(pg_pool, key, count, Utc::now()).await?;

    Ok(())
}

async fn transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: KeyRegistry,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.transaction_count)
        .sum();

    key_value::upsert(pg_pool, key, count, Utc::now()).await?;

    Ok(())
}

async fn unique_transaction_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: KeyRegistry,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_count)
        .sum();

    key_value::upsert(pg_pool, key, count, Utc::now()).await?;

    Ok(())
}

async fn unique_transaction_accepted_count(
    cache: &Arc<Cache>,
    pg_pool: &PgPool,
    key: KeyRegistry,
    threshold: u64,
) -> Result<(), sqlx::Error> {
    let count: u64 = cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_accepted_count)
        .sum();

    key_value::upsert(pg_pool, key, count, Utc::now()).await?;

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

    key_value::upsert(
        pg_pool,
        KeyRegistry::AcceptedTransactionCountPerHour24h,
        serde_json::to_string(&effective_count_per_hour).unwrap(),
        Utc::now(),
    )
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

    key_value::upsert(
        pg_pool,
        KeyRegistry::AcceptedTransactionCountPerMinute60m,
        serde_json::to_string(&effective_count_per_minute).unwrap(),
        Utc::now(),
    )
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

    key_value::upsert(
        pg_pool,
        KeyRegistry::AcceptedTransactionCountPerSecond60s,
        serde_json::to_string(&effective_count_per_second).unwrap(),
        Utc::now(),
    )
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
        KeyRegistry::CoinbaseTransactionCount86400s,
        threshold,
    )
    .await?;

    coinbase_accepted_transaction_count(
        &cache,
        pg_pool,
        KeyRegistry::CoinbaseAcceptedTransactionCount86400s,
        threshold,
    )
    .await?;

    transaction_count(&cache, pg_pool, KeyRegistry::TransactionCount, threshold).await?;

    unique_transaction_count(
        &cache,
        pg_pool,
        KeyRegistry::UniqueTransactionCount86400s,
        threshold,
    )
    .await?;

    unique_transaction_accepted_count(
        &cache,
        pg_pool,
        KeyRegistry::UniqueTransactionAcceptedCount86400s,
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
