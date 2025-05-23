use crate::cache::Cache;
use chrono::Utc;
use kaspalytics_utils::database::sql::{key_value, key_value::KeyRegistry};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn run(cache: Arc<Cache>, pg_pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut version_counts = HashMap::<String, u64>::new();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cutoff = now - 3600;

    cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= cutoff)
        .map(|entry| entry.mining_node_version_block_counts.clone())
        .for_each(|second_map| {
            second_map.iter().for_each(|entry| {
                let version = entry.key();
                let second_count = entry.value();

                version_counts
                    .entry(version.clone())
                    .and_modify(|overall_count| *overall_count += second_count)
                    .or_insert(*second_count);
            });
        });

    let total_blocks = version_counts.values().sum::<u64>();

    let version_share: HashMap<String, f64> = version_counts
        .into_iter()
        .map(|(version, count)| {
            let share = (count as f64 / total_blocks as f64) * 100.0;
            (version, share)
        })
        .collect();

    key_value::upsert(
        pg_pool,
        KeyRegistry::MinerNodeVersions1h,
        serde_json::to_string(&version_share).unwrap(),
        Utc::now(),
    )
    .await
    .unwrap();

    Ok(())
}
