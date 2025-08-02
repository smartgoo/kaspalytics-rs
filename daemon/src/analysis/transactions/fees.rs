use crate::analysis::transactions::counter::unique_accepted_transaction_count;
use crate::ingest::cache::{DagCache, Reader};

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::{collections::HashMap, sync::{atomic::Ordering, Arc}, time::{SystemTime, UNIX_EPOCH}};

pub fn median_fee(dag_cache: &Arc<DagCache>, period: u64) -> Decimal {
    let threshold = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - period;

    let total_fees = dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.total_fees.load(Ordering::Relaxed))
        .sum();

    let tx_count = unique_accepted_transaction_count(dag_cache, threshold);

    if total_fees == 0 || tx_count == 0 {
        return Decimal::from_u64(0).unwrap()
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
        .map(|entry| entry.total_fees.load(Ordering::Relaxed))
        .sum()
}

/// Average fee by time bucket
/// E.g. average fee per 60 seconds, per 5 minutes, etc.
/// Returns a vector of (timestamp, average_fee) tuples sorted by timestamp
/// suitable for time series charting
pub fn average_fee_by_time_bucket(dag_cache: &Arc<DagCache>, bucket: u64, lookback_period: u64) -> Vec<(u64, Decimal)> {
    let threshold = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - lookback_period;
    
    let mut bucket_data: HashMap<u64, (u64, u64)> = HashMap::new();
    
    // Iterate through all seconds and group them into buckets
    for entry in dag_cache.seconds_iter() {
        let timestamp = *entry.key();
        
        // Only include data within the lookback period
        if timestamp < threshold {
            continue;
        }
        
        let bucket_start = (timestamp / bucket) * bucket; // Floor to bucket boundary
        
        let total_fees = entry.total_fees.load(Ordering::Relaxed);
        let tx_count = entry.unique_accepted_transaction_count.load(Ordering::Relaxed);
        
        let bucket_entry = bucket_data.entry(bucket_start).or_insert((0, 0));
        bucket_entry.0 += total_fees;
        bucket_entry.1 += tx_count;
    }
    
    // Calculate averages and create result
    let mut result: Vec<(u64, Decimal)> = bucket_data
        .into_iter()
        .filter_map(|(bucket_start, (total_fees, tx_count))| {
            if tx_count > 0 {
                let avg_fee = Decimal::from_u64(total_fees).unwrap() / Decimal::from_u64(tx_count).unwrap();
                Some((bucket_start, avg_fee))
            } else {
                // Include buckets with no transactions as zero average fee
                Some((bucket_start, Decimal::from_u64(0).unwrap()))
            }
        })
        .collect();
    
    // Sort by timestamp for time series
    result.sort_by_key(|&(timestamp, _)| timestamp);
    result
}