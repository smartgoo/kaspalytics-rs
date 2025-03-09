use chrono::Utc;
use log::debug;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::Cache;

pub async fn run(cache: Arc<Cache>, pg_pool: PgPool) -> Result<(), sqlx::Error> {
    // TODO break this function out into it's own service and modularize analysis performed
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let threshold = now - 86400;

    let effective_count: u64 = cache
        .per_second
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.effective_transaction_count)
        .sum();

    let count: u64 = cache
        .per_second
        .iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.transaction_count)
        .sum();

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('transaction_count_24h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(count as i64)
    .bind(Utc::now())
    .execute(&pg_pool)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('effective_transaction_count_24h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(effective_count as i64)
    .bind(Utc::now())
    .execute(&pg_pool)
    .await?;

    let current_hour = now - (now % 3600);
    let cutoff = current_hour - (23 * 3600);
    let mut effective_count_per_hour = HashMap::<u64, u64>::new();

    cache
        .per_second
        .iter()
        .map(|entry| {
            let second = *entry.key();
            let hour = second - (second % 3600);
            (hour, entry.value().effective_transaction_count)
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
    .execute(&pg_pool)
    .await?;

    debug!("txs: {} | effective txs: {}", count, effective_count);
    debug!("per hour effective tx count {:?}", effective_count_per_hour);

    Ok(())
}
