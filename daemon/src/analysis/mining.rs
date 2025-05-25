use crate::ingest::cache::{DagCache, Reader};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn mining_node_version_share_60m(dag_cache: &Arc<DagCache>) -> HashMap<String, Decimal> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cutoff = now - 3600;

    let mut version_counts = HashMap::<String, Decimal>::new();

    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= cutoff)
        .map(|entry| entry.mining_node_version_block_counts.clone())
        .for_each(|second_map| {
            second_map.iter().for_each(|entry| {
                let version = entry.key();
                let second_count = Decimal::from_u64(*entry.value()).unwrap();

                version_counts
                    .entry(version.clone())
                    .and_modify(|overall_count| *overall_count += second_count)
                    .or_insert(second_count);
            });
        });

    let total_blocks = version_counts.values().sum::<Decimal>();

    version_counts
        .into_iter()
        .map(|(version, count)| {
            let share = ((count / total_blocks) * dec!(100)).round_dp(2);
            (version, share)
        })
        .collect()
}
