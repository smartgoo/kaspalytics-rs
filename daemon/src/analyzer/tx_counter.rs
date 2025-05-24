use crate::ingest::cache::{DagCache, Reader};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn coinbase_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_transaction_count)
        .sum()
}

pub fn coinbase_accepted_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_accepted_transaction_count)
        .sum()
}

pub fn transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.transaction_count)
        .sum()
}

pub fn unique_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_count)
        .sum()
}

pub fn unique_transaction_accepted_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_accepted_count)
        .sum()
}

pub fn accepted_count_per_hour_24h(dag_cache: &Arc<DagCache>) -> HashMap<u64, u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let current_hour = now - (now % 3600);
    let cutoff = current_hour - (23 * 3600);
    let mut effective_count_per_hour = HashMap::<u64, u64>::new();

    dag_cache
        .seconds_iter()
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

    effective_count_per_hour
}

pub fn accepted_count_per_minute_60m(dag_cache: &Arc<DagCache>) -> HashMap<u64, u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let current_minute = now - (now % 60);
    let cutoff = current_minute - (59 * 60);
    let mut effective_count_per_minute = HashMap::<u64, u64>::new();

    dag_cache
        .seconds_iter()
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

    effective_count_per_minute
}

// pub async fn run(dag_cache: Arc<DagCache>, storage: Arc<Storage>) -> Result<(), sqlx::Error> {
//     let now = SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .unwrap()
//         .as_secs();

//     let threshold = now - 86400;

//     storage.set_coinbase_transaction_count_24h(
//         coinbase_transaction_count(&dag_cache, threshold),
//         None,
//     )
//     .await
//     .unwrap();

//     storage.set_coinbase_accepted_transaction_count_24h(
//         coinbase_accepted_transaction_count(&dag_cache, threshold),
//         None,
//     )
//     .await
//     .unwrap();

//     storage.set_transaction_count_24h(
//         transaction_count(&dag_cache, threshold),
//         None
//     )
//     .await
//     .unwrap();

//     storage.set_unique_transaction_count_24h(
//         unique_transaction_count(&dag_cache, threshold),
//         None
//     )
//     .await
//     .unwrap();

//     storage.set_unique_transaction_accepted_count_24h(
//         unique_transaction_accepted_count(&dag_cache, threshold),
//         None
//     )
//     .await
//     .unwrap();

//     storage.set_transaction_count_per_hour_24h(
//         accepted_count_per_hour_24h(&dag_cache),
//         None,
//     )
//     .await
//     .unwrap();

//     storage.set_transaction_count_per_minute_1h(
//         accepted_count_per_minute_60m(&dag_cache),
//         None,
//     )
//     .await
//     .unwrap();

//     Ok(())
// }
