use super::cache::DagCache;
use super::model::{CacheBlock, CacheTransaction};
use crate::analysis::transactions::protocol::inscription::parse_signature_script;
use crate::analysis::transactions::protocol::TransactionProtocol;
use crate::ingest::cache::Reader;
use crate::ingest::model::CacheUtxoEntry;
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcAcceptanceData, RpcBlock, RpcTransaction};
use kaspalytics_utils::log::LogTarget;
use log::warn;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum PayloadParseError {
    #[error("Payload less than 19 bytes")]
    InsufficientPayloadLength,

    #[error("First byte 0xaa indicates address payload")]
    InvalidFirstByte,

    #[error("Payload split error")]
    SplitError,
}

fn parse_coinbase_tx_payload(payload: &[u8]) -> Result<(String, String), PayloadParseError> {
    if payload.len() < 19 {
        return Err(PayloadParseError::InsufficientPayloadLength);
    }

    let length = payload[18] as usize;

    if payload.len() < 19 + length {
        return Err(PayloadParseError::InsufficientPayloadLength);
    }

    let script = &payload[19..19 + length];
    if script.is_empty() || script[0] == 0xaa {
        return Err(PayloadParseError::InvalidFirstByte);
    }

    let payload_str = payload[19 + length..]
        .iter()
        .map(|&b| b as char)
        .collect::<String>();

    let parts: Vec<&str> = payload_str.splitn(2, '/').collect();
    match parts.len() {
        1 => Ok((parts[0].to_string(), "".to_string())),
        2 => {
            // TODO miner_prefix needs work....
            let miner_parts: Vec<&str> = parts[1].splitn(2, '/').collect();
            let miner_prefix = miner_parts[0].to_string();
            Ok((parts[0].to_string(), miner_prefix))
        }
        _ => Err(PayloadParseError::SplitError),
    }
}

pub fn block_add_pipeline(dag_cache: Arc<DagCache>, block: &RpcBlock) {
    // Add block to DagCache Blocks
    // If it already exists, return early, block has already been processed
    match dag_cache.blocks.entry(block.header.hash) {
        dashmap::Entry::Occupied(_) => return,
        dashmap::Entry::Vacant(entry) => {
            entry.insert(CacheBlock::from(block.clone()));
        }
    }

    // debug!(
    //     target: LogTarget::Daemon.as_str(),
    //     "block_add_pipeline {}",
    //     block.header.hash,
    // );

    let (node_version, _) = parse_coinbase_tx_payload(&block.transactions[0].payload).unwrap();

    // Increment second counters
    dag_cache
        .seconds
        .entry(block.header.timestamp / 1000)
        .or_default()
        .map(|e| {
            // Increment block count for the block's second
            e.increment_block_count();

            // Increment mining node version count
            e.mining_node_version_block_counts
                .entry(node_version)
                .and_modify(|v| *v += 1)
                .or_insert(1);
            e
        });

    // Process transactions in the block
    for tx in block.transactions.iter() {
        transaction_add_pipeline(dag_cache.clone(), tx);
    }
}

fn payload_to_string(payload: Vec<u8>) -> String {
    payload.iter().map(|&b| b as char).collect::<String>()
}

fn detect_transaction_protocol(
    dag_cache: Arc<DagCache>,
    transaction: &RpcTransaction,
) -> Option<TransactionProtocol> {
    let payload_str = payload_to_string(transaction.payload.clone());

    // Check for Kasia protocol in transaction payload
    if payload_str.contains("ciph_msg") {
        return Some(TransactionProtocol::Kasia);
    }

    // Check for Kasplex L2 protocol in transaction payload
    if payload_str.contains("kasplex") {
        return Some(TransactionProtocol::Kasplex);
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

fn transaction_add_pipeline(dag_cache: Arc<DagCache>, transaction: &RpcTransaction) {
    let tx_id = transaction.verbose_data.as_ref().unwrap().transaction_id;
    let block_hash = transaction.verbose_data.as_ref().unwrap().block_hash;
    let block_time = transaction.verbose_data.as_ref().unwrap().block_time;

    // debug!(
    //     target: LogTarget::Daemon.as_str(),
    //     "transaction_add_pipeline {}",
    //     tx_id,
    // );

    // Increase total transaction count
    dag_cache
        .seconds
        .entry(block_time / 1000)
        .and_modify(|v| v.increment_transaction_count());

    // If TX already in cache, add block_hash
    if let Some(mut tx) = dag_cache.transactions.get_mut(&tx_id) {
        tx.blocks.push(block_hash);
        return;
    }

    // Transaction is not in cache
    // Run process to determine TX type, protocol, and add to cache
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
        cache_transaction.protocol = detect_transaction_protocol(dag_cache.clone(), transaction);
    }

    dag_cache.transactions.insert(tx_id, cache_transaction);
}

fn add_transaction_acceptance(
    dag_cache: Arc<DagCache>,
    transaction_id: Hash,
    accepting_block_hash: Hash,
) {
    // Set transaction's accepting block hash
    dag_cache
        .transactions
        .entry(transaction_id)
        .and_modify(|v| v.accepting_block_hash = Some(accepting_block_hash));

    let Some(mut tx) = dag_cache.transactions.get_mut(&transaction_id) else {
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
        // Cacluate fee paid
        let total_input_amount = tx
            .inputs
            .iter()
            .map(|input| {
                input
                    .utxo_entry
                    .as_ref()
                    .map(|utxo_entry| utxo_entry.amount)
                    .unwrap_or(0)
            })
            .sum::<u64>();

        let total_output_amount = tx.outputs.iter().map(|output| output.value).sum::<u64>();

        tx.fee = Some(total_input_amount - total_output_amount);

        // Update metrics for given second
        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| {
                v.increment_unique_accepted_transaction_count();
                v.increment_total_fees(tx.fee.unwrap());

                // Increment Protocol count for given second
                match tx.protocol {
                    Some(TransactionProtocol::Krc) => {
                        v.increment_krc_transaction_count();
                    }
                    Some(TransactionProtocol::Kns) => {
                        v.increment_kns_transaction_count();
                    }
                    Some(TransactionProtocol::Kasia) => {
                        v.increment_kasia_transaction_count();
                    }
                    Some(TransactionProtocol::Kasplex) => {
                        v.increment_kasplex_transaction_count();
                    }
                    None => {}
                }
            });

        // Increment Protocol count for given second
        // match tx.protocol {
        //     Some(TransactionProtocol::Kasia) => {
        //         dag_cache
        //             .seconds
        //             .entry(tx_timestamp / 1000)
        //             .and_modify(|v| v.increment_kasia_transaction_count());
        //     }
        //     Some(TransactionProtocol::Krc) => {
        //         dag_cache
        //             .seconds
        //             .entry(tx_timestamp / 1000)
        //             .and_modify(|v| v.increment_krc_transaction_count());
        //     }
        //     Some(TransactionProtocol::Kns) => {
        //         dag_cache
        //             .seconds
        //             .entry(tx_timestamp / 1000)
        //             .and_modify(|v| v.increment_kns_transaction_count());
        //     }
        //     None => {}
        // }
    }
}

pub fn add_chain_block_acceptance_pipeline(
    dag_cache: Arc<DagCache>,
    acceptance_data: RpcAcceptanceData,
) {
    // Set block's chain block status to true
    dag_cache
        .blocks
        .entry(acceptance_data.accepting_chain_block_header.hash)
        .and_modify(|v| v.is_chain_block = true);

    // Popuplate UTXO on Transaction
    for merged_block in acceptance_data.mergeset_block_acceptance_data.iter() {
        for transaction in merged_block.accepted_transactions.iter() {
            let tx_id = transaction.verbose_data.as_ref().unwrap().transaction_id;

            for (idx, input) in transaction.inputs.iter().enumerate() {
                if input.verbose_data.as_ref().unwrap().utxo_entry.is_none() {
                    warn!(
                        target: LogTarget::Daemon.as_str(),
                        "input.verbose_data.utxo_entry is none: {}-{}",
                        tx_id, idx
                    );
                    continue;
                }

                dag_cache.transactions.entry(tx_id).and_modify(|cache_tx| {
                    let rpc_utxo = input
                        .verbose_data
                        .as_ref()
                        .unwrap()
                        .utxo_entry
                        .clone()
                        .unwrap();
                    let cache_utxo = CacheUtxoEntry::from(rpc_utxo);
                    if cache_utxo.script_public_key_type.is_none() {
                        warn!(
                            target: LogTarget::Daemon.as_str(),
                            "utxo_entry.script_public_key_type is none: {}-{}",
                            tx_id, idx
                        );
                    }
                    cache_tx.inputs[idx].utxo_entry = Some(cache_utxo);
                });
            }
        }
    }

    // Add chain block and it's accepted transactions
    let mut accepted_transactions = acceptance_data
        .mergeset_block_acceptance_data
        .into_iter()
        .flat_map(|block| block.accepted_transactions)
        .map(|tx| tx.verbose_data.unwrap().transaction_id)
        .collect::<Vec<_>>();
    accepted_transactions.sort();
    accepted_transactions.dedup();

    dag_cache.accepting_block_transactions.insert(
        acceptance_data.accepting_chain_block_header.hash,
        accepted_transactions.clone(),
    );

    // Process transactions
    for tx_id in accepted_transactions {
        add_transaction_acceptance(
            dag_cache.clone(),
            tx_id,
            acceptance_data.accepting_chain_block_header.hash,
        );
    }
}

fn remove_transaction_acceptance(dag_cache: Arc<DagCache>, transaction_id: Hash) {
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
            .and_modify(|v| {
                v.decrement_unique_accepted_transaction_count();
                v.decrement_total_fees(tx.fee.unwrap());

                match tx.protocol {
                    Some(TransactionProtocol::Krc) => {
                        v.decrement_krc_transaction_count();
                    }
                    Some(TransactionProtocol::Kns) => {
                        v.decrement_kns_transaction_count();
                    }
                    Some(TransactionProtocol::Kasia) => {
                        v.decrement_kasia_transaction_count();
                    }
                    Some(TransactionProtocol::Kasplex) => {
                        v.decrement_kasplex_transaction_count();
                    }
                    None => {}
                }
            });
    }
}

pub fn remove_chain_block_pipeline(dag_cache: Arc<DagCache>, removed_chain_block: &Hash) {
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
            remove_transaction_acceptance(dag_cache.clone(), tx_id);
        }
    } else {
        warn!(
            target: LogTarget::Daemon.as_str(),
            "Removed chain block {} is below cache tip timestamp, and does not exist in cache accepting_block_transactions map",
            removed_chain_block
        );
    }
}
