use crate::analysis::transactions::counter::unique_accepted_transaction_count;
use crate::ingest::cache::{DagCache, Reader};

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn median_fee(dag_cache: &Arc<DagCache>, period: u64) -> Decimal {
    let threshold = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - period;

    let total_fees = dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.total_fees)
        .sum();

    let tx_count = unique_accepted_transaction_count(dag_cache, threshold);

    if total_fees == 0 || tx_count == 0 {
        return Decimal::from_u64(0).unwrap();
    }

    Decimal::from_u64(total_fees).unwrap() / Decimal::from_u64(tx_count).unwrap()
}

pub fn total_fees(dag_cache: &Arc<DagCache>, period: u64) -> u64 {
    let threshold = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - period;

    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.total_fees)
        .sum()
}

/// Average fee by time bucket
/// E.g. average fee per 60 seconds, per 5 minutes, etc.
/// Returns a vector of (timestamp, average_fee) tuples sorted by timestamp
/// suitable for time series charting
/// Only includes complete buckets that are entirely within the lookback period
pub fn average_fee_by_time_bucket(
    dag_cache: &Arc<DagCache>,
    bucket: u64,
    lookback_period: u64,
) -> Vec<(u64, Decimal)> {
    if bucket == 0 {
        return Vec::new();
    }

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let start_time = current_time.saturating_sub(lookback_period);

    // Align start_time to bucket boundary (round down to nearest bucket)
    let aligned_start = (start_time / bucket) * bucket;

    // Collect all bucket data
    let mut bucket_data: HashMap<u64, (u64, u64)> = HashMap::new(); // (bucket_timestamp, (total_fees, tx_count))

    // Minimum realistic timestamp (January 1, 2020) to filter out invalid/test data
    let min_realistic_timestamp = 1577836800u64; // 2020-01-01 00:00:00 UTC

    // Iterate through all seconds in the cache
    for entry in dag_cache.seconds_iter() {
        let timestamp = *entry.key();

        // Skip unrealistic timestamps (likely test data or uninitialized values)
        if timestamp < min_realistic_timestamp {
            continue;
        }

        // Skip if outside our time window
        if timestamp < start_time || timestamp > current_time {
            continue;
        }

        // Determine which bucket this timestamp belongs to
        let bucket_timestamp = (timestamp / bucket) * bucket;

        // Only include complete buckets (bucket end time must be <= current_time)
        let bucket_end = bucket_timestamp + bucket;
        if bucket_end > current_time {
            continue;
        }

        // Only include buckets that start at or after our aligned start
        if bucket_timestamp < aligned_start {
            continue;
        }

        let total_fees = entry.total_fees;
        let tx_count = entry.unique_accepted_transaction_count;

        // Accumulate data for this bucket
        let bucket_entry = bucket_data.entry(bucket_timestamp).or_insert((0, 0));
        bucket_entry.0 += total_fees;
        bucket_entry.1 += tx_count;
    }

    // Calculate averages and prepare result
    let mut result: Vec<(u64, Decimal)> = bucket_data
        .into_iter()
        .map(|(bucket_timestamp, (total_fees, tx_count))| {
            if tx_count == 0 {
                // If no transactions, average fee is 0
                (bucket_timestamp, Decimal::from_u64(0).unwrap())
            } else {
                // Calculate average fee
                let average_fee =
                    Decimal::from_u64(total_fees).unwrap() / Decimal::from_u64(tx_count).unwrap();
                (bucket_timestamp, average_fee)
            }
        })
        .collect();

    // Sort by timestamp
    result.sort_by_key(|&(timestamp, _)| timestamp);

    result
}
