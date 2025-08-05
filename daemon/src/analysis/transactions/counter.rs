#![allow(dead_code)]
use crate::ingest::cache::{DagCache, Reader};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.transaction_count)
        .sum()
}

pub fn coinbase_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_transaction_count)
        .sum()
}

pub fn coinbase_transaction_accepted_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_accepted_transaction_count)
        .sum()
}

pub fn unique_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_transaction_count)
        .sum()
}

pub fn unique_accepted_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.unique_accepted_transaction_count)
        .sum()
}

pub fn unique_accepted_count_per_hour_24h(dag_cache: &Arc<DagCache>) -> HashMap<u64, u64> {
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
            (hour, entry.value().unique_accepted_transaction_count)
        })
        .filter(|(hour, _)| *hour >= cutoff)
        .for_each(|(hour, count)| {
            *effective_count_per_hour.entry(hour).or_insert(0) += count;
        });

    effective_count_per_hour
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
                    + entry.value().unique_accepted_transaction_count,
            )
        })
        .filter(|(hour, _)| *hour >= cutoff)
        .for_each(|(hour, count)| {
            *effective_count_per_hour.entry(hour).or_insert(0) += count;
        });

    effective_count_per_hour
}

pub fn kasia_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.kasia_transaction_count)
        .sum()
}

pub fn krc_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.krc_transaction_count)
        .sum()
}

pub fn kns_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.kns_transaction_count)
        .sum()
}
