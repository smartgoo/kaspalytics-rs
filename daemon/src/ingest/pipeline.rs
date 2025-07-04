use super::cache::DagCache;
use super::model::{CacheBlock, CacheTransaction};
use crate::analysis::transactions::protocol::inscription::parse_signature_script;
use crate::analysis::transactions::protocol::TransactionProtocol;
use crate::ingest::cache::Reader;
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcAcceptedTransactionIds, RpcBlock, RpcTransaction};
use kaspalytics_utils::log::LogTarget;
use log::warn;
use std::sync::Arc;

pub fn block_add_pipeline(dag_cache: &Arc<DagCache>, block: &RpcBlock) {
    // Add block to DagCache Blocks
    // If it already exists, return early, block has already been processed
    match dag_cache.blocks.entry(block.header.hash) {
        dashmap::Entry::Occupied(_) => return,
        dashmap::Entry::Vacant(entry) => {
            entry.insert(CacheBlock::from(block.clone()));
        }
    }

    // Increment block count for the block's second
    dag_cache
        .seconds
        .entry(block.header.timestamp / 1000)
        .or_default()
        .add_block(block.transactions[0].payload.clone());

    // Process transactions in the block
    for tx in block.transactions.iter() {
        transaction_add_pipeline(dag_cache, tx);
    }
}

fn payload_to_string(payload: Vec<u8>) -> String {
    payload.iter().map(|&b| b as char).collect::<String>()
}

fn detect_transaction_protocol(
    dag_cache: &Arc<DagCache>,
    transaction: &RpcTransaction,
) -> Option<TransactionProtocol> {
    // Check for Kasia protocol in transactionpayload
    if payload_to_string(transaction.payload.clone()).contains("ciph_msg") {
        return Some(TransactionProtocol::Kasia);
    }

    // Check inputs for Kasplex or KNS inscription
    for input in transaction.inputs.iter() {
        let parsed_script = parse_signature_script(&input.signature_script);

        for (opcode, data) in parsed_script {
            if opcode.as_str() == "OP_PUSH" && ["kasplex", "kspr"].contains(&data.as_str()) {
                if let Some(mut previous_transaction) = dag_cache
                    .transactions
                    .get_mut(&input.previous_outpoint.transaction_id)
                {
                    previous_transaction.protocol = Some(TransactionProtocol::Krc);

                    if previous_transaction.accepting_block_hash.is_some() {
                        dag_cache
                            .seconds
                            .entry(previous_transaction.block_time / 1000)
                            .and_modify(|v| v.increment_krc_transaction_count());
                    }
                } else {
                    warn!(
                        target: LogTarget::Daemon.as_str(),
                        "KRC tx detected, but previous transaction not found in cache: {}",
                        transaction.verbose_data.as_ref().unwrap().transaction_id,
                    );
                }
                return Some(TransactionProtocol::Krc);
            }

            if opcode.as_str() == "OP_PUSH" && ["kns"].contains(&data.as_str()) {
                if let Some(mut previous_transaction) = dag_cache
                    .transactions
                    .get_mut(&input.previous_outpoint.transaction_id)
                {
                    previous_transaction.protocol = Some(TransactionProtocol::Kns);

                    if previous_transaction.accepting_block_hash.is_some() {
                        dag_cache
                            .seconds
                            .entry(previous_transaction.block_time / 1000)
                            .and_modify(|v| v.increment_kns_transaction_count());
                    }
                } else {
                    warn!(
                        target: LogTarget::Daemon.as_str(),
                        "KNS tx detected, but previous transaction not found in cache: {}",
                        transaction.verbose_data.as_ref().unwrap().transaction_id,
                    );
                }
                return Some(TransactionProtocol::Kns);
            }
        }
    }

    None
}

pub fn transaction_add_pipeline(dag_cache: &Arc<DagCache>, transaction: &RpcTransaction) {
    let tx_id = transaction.verbose_data.as_ref().unwrap().transaction_id;
    let block_hash = transaction.verbose_data.as_ref().unwrap().block_hash;
    let block_time = transaction.verbose_data.as_ref().unwrap().block_time;

    // Add Transaction to DagCache Transactions
    // If already in cache, add block_hash
    if let Some(mut tx) = dag_cache.transactions.get_mut(&tx_id) {
        tx.blocks.push(block_hash);
    } else {
        let mut cache_transaction = CacheTransaction::from(transaction.clone());

        if transaction.subnetwork_id == SUBNETWORK_ID_COINBASE {
            dag_cache
                .seconds
                .entry(block_time / 1000)
                .and_modify(|v| v.increment_coinbase_transaction_count());
        } else {
            dag_cache
                .seconds
                .entry(block_time / 1000)
                .and_modify(|v| v.increment_unique_transaction_count());

            // Transaction Protocol
            cache_transaction.protocol = detect_transaction_protocol(dag_cache, transaction);
        }

        dag_cache.transactions.insert(tx_id, cache_transaction);
    }

    // Increase total transaction count
    dag_cache
        .seconds
        .entry(block_time / 1000)
        .and_modify(|v| v.increment_transaction_count());
}

fn add_transaction_acceptance(
    dag_cache: &Arc<DagCache>,
    transaction_id: Hash,
    accepting_block_hash: Hash,
) {
    // Set transactions accepting block hash
    dag_cache
        .transactions
        .entry(transaction_id)
        .and_modify(|v| v.accepting_block_hash = Some(accepting_block_hash));

    let Some(tx) = dag_cache.transactions.get(&transaction_id) else {
        warn!(
            target: LogTarget::Daemon.as_str(),
            "Failed to add transaction acceptance status for tx {}, tx not in cache",
            transaction_id
        );
        return;
    };

    let tx_timestamp = tx.block_time;

    // Increment transaction counts
    if tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| v.increment_coinbase_accepted_transaction_count());
    } else {
        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| v.increment_unique_accepted_transaction_count());

        match tx.protocol {
            Some(TransactionProtocol::Kasia) => {
                dag_cache
                    .seconds
                    .entry(tx_timestamp / 1000)
                    .and_modify(|v| v.increment_kasia_transaction_count());
            }
            Some(TransactionProtocol::Krc) => {
                dag_cache
                    .seconds
                    .entry(tx_timestamp / 1000)
                    .and_modify(|v| v.increment_krc_transaction_count());
            }
            Some(TransactionProtocol::Kns) => {
                dag_cache
                    .seconds
                    .entry(tx_timestamp / 1000)
                    .and_modify(|v| v.increment_kns_transaction_count());
            }
            None => {}
        }
    }
}

pub fn add_chain_block_acceptance_pipeline(
    dag_cache: &Arc<DagCache>,
    acceptance_data: RpcAcceptedTransactionIds,
) {
    // Set block's chain block status to true
    dag_cache
        .blocks
        .entry(acceptance_data.accepting_block_hash)
        .and_modify(|v| v.is_chain_block = true);

    // Add chain block and it's accepted transactions
    dag_cache.accepting_block_transactions.insert(
        acceptance_data.accepting_block_hash,
        acceptance_data.accepted_transaction_ids.clone(),
    );

    // Process transactions
    for tx_id in acceptance_data.accepted_transaction_ids {
        add_transaction_acceptance(dag_cache, tx_id, acceptance_data.accepting_block_hash);
    }
}

fn remove_transaction_acceptance(dag_cache: &Arc<DagCache>, transaction_id: Hash) {
    // Remove former accepting block hash from transaction
    dag_cache
        .transactions
        .entry(transaction_id)
        .and_modify(|v| v.accepting_block_hash = None);

    let Some(tx) = dag_cache.transactions.get(&transaction_id) else {
        warn!(
            target: LogTarget::Daemon.as_str(),
            "Failed to remove transaction acceptance status for tx {}, tx not in cache",
            transaction_id
        );
        return;
    };

    let tx_timestamp = tx.block_time;

    // Increment transaction counts
    if tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| v.decrement_coinbase_accepted_transaction_count());
    } else {
        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| v.decrement_unique_accepted_transaction_count());

        match tx.protocol {
            Some(TransactionProtocol::Kasia) => {
                dag_cache
                    .seconds
                    .entry(tx_timestamp / 1000)
                    .and_modify(|v| v.decrement_kasia_transaction_count());
            }
            Some(TransactionProtocol::Krc) => {
                dag_cache
                    .seconds
                    .entry(tx_timestamp / 1000)
                    .and_modify(|v| v.decrement_krc_transaction_count());
            }
            Some(TransactionProtocol::Kns) => {
                dag_cache
                    .seconds
                    .entry(tx_timestamp / 1000)
                    .and_modify(|v| v.decrement_kns_transaction_count());
            }
            None => {}
        }
    }
}

pub fn remove_chain_block_pipeline(dag_cache: &Arc<DagCache>, removed_chain_block: &Hash) {
    // Temporary while working thru bug...
    {
        let removed_chain_block_data = dag_cache.blocks.get(removed_chain_block);

        if removed_chain_block_data.is_none() {
            warn!(
                target: LogTarget::Daemon.as_str(),
                "Removed chain block {} not found in blocks cache",
                removed_chain_block
            );
            return;
        }

        // TODO I think since removed_chain_blocks are returned in high to low order,
        // there are situations where removed_chain_block is not in
        // accepting_block_transactions map yet
        if removed_chain_block_data.unwrap().timestamp > dag_cache.tip_timestamp() {
            warn!(
                target: LogTarget::Daemon.as_str(),
                "Removed chain block {} timestamp greater than tip_timestamp",
                removed_chain_block
            );
        }
    }

    // Set block's chain block status to false
    dag_cache
        .blocks
        .entry(*removed_chain_block)
        .and_modify(|v| v.is_chain_block = false);

    // Remove former chain blocks accepted transactions from accepting_block_transactions map
    if let Some((_, removed_transactions)) = dag_cache
        .accepting_block_transactions
        .remove(removed_chain_block)
    {
        for tx_id in removed_transactions {
            remove_transaction_acceptance(dag_cache, tx_id);
        }
    } else {
        warn!(
            target: LogTarget::Daemon.as_str(),
            "Removed chain block {} is below cache tip timestamp, and does not exist in cache accepting_block_transactions map",
            removed_chain_block
        );
    }
}
