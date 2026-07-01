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

/// All covenant / introspection / ZK-tag / chainblock 24h SSE counters, computed
/// in a single filtered pass over the seconds cache. Each field mirrors the
/// like-named [`SecondMetrics`](crate::ingest::second::SecondMetrics) field summed
/// over entries whose second `>= threshold`. Folding them into one pass avoids the
/// dozen independent full-map scans the SSE broadcast hot path would otherwise do.
#[derive(Default)]
pub struct CovenantWindowSums {
    /// Accepted transactions using a covenant / introspection opcode.
    pub introspection_tx: u64,
    /// P2SH outputs spent whose revealed redeem script uses a covenant /
    /// introspection opcode.
    pub introspection_outputs_spent: u64,
    /// Accepted transactions using the ZK precompile opcode (any tag).
    pub zk_tx: u64,
    /// P2SH outputs spent whose revealed redeem script uses the ZK precompile
    /// opcode (any tag).
    pub zk_outputs_spent: u64,
    /// Accepted transactions using a Groth16-tagged ZK precompile (overlapping
    /// subset of `zk_tx`).
    pub zk_groth16_tx: u64,
    /// P2SH outputs spent revealing a Groth16-tagged ZK precompile.
    pub zk_groth16_outputs_spent: u64,
    /// Accepted transactions using an R0Succinct-tagged ZK precompile (overlapping
    /// subset of `zk_tx`).
    pub zk_r0succinct_tx: u64,
    /// P2SH outputs spent revealing an R0Succinct-tagged ZK precompile.
    pub zk_r0succinct_outputs_spent: u64,
    /// Accepted transactions using a ZK precompile with an unrecognized tag
    /// (overlapping subset of `zk_tx`; expected to be ~0).
    pub zk_unknown_tag_tx: u64,
    /// P2SH outputs spent revealing a ZK precompile with an unrecognized tag.
    pub zk_unknown_tag_outputs_spent: u64,
    /// Accepted transactions using the chain-block sequencing-commitment opcode
    /// (`OpChainblockSeqCommit`; subset of `introspection_tx`).
    pub chainblock_seqcommit_tx: u64,
    /// P2SH outputs spent revealing the chain-block sequencing-commitment opcode.
    pub chainblock_seqcommit_outputs_spent: u64,
    /// Accepted transactions creating at least one covenant-bound output.
    pub covenant_creating_tx: u64,
    /// Covenant-bound outputs created.
    pub covenant_outputs_created: u64,
    /// Covenant-bound outputs spent.
    pub covenant_outputs_spent: u64,
}

/// Computes every covenant / introspection / ZK-tag / chainblock 24h counter in a
/// single filtered pass over the seconds cache. Values are identical to summing
/// each field independently; this exists to avoid a dozen separate full-map scans
/// on the SSE broadcast hot path.
pub fn covenant_window_sums(dag_cache: &Arc<DagCache>, threshold: u64) -> CovenantWindowSums {
    let mut s = CovenantWindowSums::default();
    for entry in dag_cache
        .seconds_iter()
        .filter(|entry| *entry.key() >= threshold)
    {
        s.introspection_tx += entry.introspection_opcode_tx_count;
        s.introspection_outputs_spent += entry.introspection_opcode_outputs_spent;
        s.zk_tx += entry.zk_precompile_tx_count;
        s.zk_outputs_spent += entry.zk_precompile_outputs_spent;
        s.zk_groth16_tx += entry.zk_precompile_groth16_tx_count;
        s.zk_groth16_outputs_spent += entry.zk_precompile_groth16_outputs_spent;
        s.zk_r0succinct_tx += entry.zk_precompile_r0succinct_tx_count;
        s.zk_r0succinct_outputs_spent += entry.zk_precompile_r0succinct_outputs_spent;
        s.zk_unknown_tag_tx += entry.zk_precompile_unknown_tag_tx_count;
        s.zk_unknown_tag_outputs_spent += entry.zk_precompile_unknown_tag_outputs_spent;
        s.chainblock_seqcommit_tx += entry.chainblock_seqcommit_tx_count;
        s.chainblock_seqcommit_outputs_spent += entry.chainblock_seqcommit_outputs_spent;
        s.covenant_creating_tx += entry.covenant_creating_tx_count;
        s.covenant_outputs_created += entry.covenant_outputs_created;
        s.covenant_outputs_spent += entry.covenant_outputs_spent;
    }
    s
}
