use crate::analysis::transactions::protocol::TransactionProtocol;
use crate::ingest::cache::{DagCache, Reader};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(dead_code)]
pub fn transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.transaction_count)
        .sum()
}

#[allow(dead_code)]
pub fn coinbase_transaction_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_transaction_count)
        .sum()
}

#[allow(dead_code)]
pub fn coinbase_transaction_accepted_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.coinbase_accepted_transaction_count)
        .sum()
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

pub fn protocol_transaction_count(
    dag_cache: &Arc<DagCache>,
    protocol: TransactionProtocol,
    threshold: u64,
) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.get_protocol_transaction_count(&protocol))
        .sum()
}

/// Counts of accepted transaction outputs by script class (per output) since
/// `threshold`. Keys: `pubkey`, `pubkeyecdsa`, `scripthash`, `nonstandard`.
pub fn output_script_class_counts(
    dag_cache: &Arc<DagCache>,
    threshold: u64,
) -> HashMap<String, u64> {
    let mut counts = HashMap::new();
    let mut pubkey = 0u64;
    let mut pubkey_ecdsa = 0u64;
    let mut script_hash = 0u64;
    let mut nonstandard = 0u64;

    for entry in dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
    {
        pubkey += entry.output_count_pubkey;
        pubkey_ecdsa += entry.output_count_pubkey_ecdsa;
        script_hash += entry.output_count_script_hash;
        nonstandard += entry.output_count_nonstandard;
    }

    counts.insert("pubkey".to_string(), pubkey);
    counts.insert("pubkeyecdsa".to_string(), pubkey_ecdsa);
    counts.insert("scripthash".to_string(), script_hash);
    counts.insert("nonstandard".to_string(), nonstandard);
    counts
}

/// Count of accepted transactions using a covenant / introspection opcode since `threshold`.
pub fn introspection_opcode_tx_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.introspection_opcode_tx_count)
        .sum()
}

/// Count of accepted transactions creating at least one covenant-bound output since `threshold`.
pub fn covenant_creating_tx_count(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.covenant_creating_tx_count)
        .sum()
}

/// Count of covenant-bound outputs created since `threshold`.
pub fn covenant_outputs_created(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.covenant_outputs_created)
        .sum()
}

/// Count of covenant-bound outputs spent since `threshold`.
pub fn covenant_outputs_spent(dag_cache: &Arc<DagCache>, threshold: u64) -> u64 {
    dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
        .map(|entry| entry.covenant_outputs_spent)
        .sum()
}
