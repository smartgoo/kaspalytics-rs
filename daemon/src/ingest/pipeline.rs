use super::cache::DagCache;
use super::model::{CacheBlock, CacheTransaction, RevealedOpcodeCredit};
use crate::analysis::transactions::protocol::inscription::parse_signature_script;
use crate::analysis::transactions::protocol::TransactionProtocol;
use crate::ingest::cache::Reader;
use crate::ingest::model::CacheUtxoEntry;
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcBlock, RpcChainBlockAcceptedTransactions};
use kaspa_txscript::script_class::ScriptClass;
use kaspalytics_utils::covenant::{
    tally_covenant_metrics, CovenantTally, ResolvedInputView, TxOutputView,
};
use kaspalytics_utils::log::LogTarget;
use kaspalytics_utils::script::{
    signature_script_reveals_introspection, signature_script_reveals_zk_precompile,
};
use log::warn;
use std::collections::HashMap;
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

    let node_version = match parse_coinbase_tx_payload(&block.transactions[0].payload) {
        Ok((version, _)) => Some(version),
        Err(err) => {
            warn!(
                target: LogTarget::Daemon.as_str(),
                "Failed to parse coinbase transaction payload for block {}: {}",
                block.header.hash,
                err
            );
            None
        }
    };

    // Increment second counters
    dag_cache
        .seconds
        .entry(block.header.timestamp / 1000)
        .or_default()
        .map(|e| {
            // Increment block count for the block's second
            e.increment_block_count();

            // Increment mining node version count
            if let Some(version) = node_version {
                e.mining_node_version_block_counts
                    .entry(version)
                    .and_modify(|v| *v += 1)
                    .or_insert(1);
            }
            e
        });

    // Process transactions in the block
    for tx in block.transactions.iter() {
        let tx = CacheTransaction::from(tx.clone());
        transaction_add_pipeline(dag_cache.clone(), block.header.hash, tx);
    }
}

fn payload_to_string(payload: Vec<u8>) -> String {
    payload.iter().map(|&b| b as char).collect::<String>()
}

fn detect_transaction_protocol(
    dag_cache: Arc<DagCache>,
    transaction: &CacheTransaction,
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

    // Check for K Social payload prefix
    if payload_str.starts_with("k:") {
        return Some(TransactionProtocol::KSocial);
    }

    // Check for Igra Labs L2: first byte upper nibble = 0x9, lower nibble = 0x1–0x7
    if let Some(&first_byte) = transaction.payload.first() {
        if (0x91..=0x97).contains(&first_byte) {
            return Some(TransactionProtocol::Igra);
        }
    }

    // Check inputs for Kasplex or KNS inscription
    for input in transaction.inputs.iter() {
        let parsed_script = parse_signature_script(&input.signature_script);

        for (opcode, data) in parsed_script {
            if opcode.as_str() == "OP_PUSH" && ["kasplex", "kspr"].contains(&data.as_str()) {
                if let Some(mut previous_transaction) = dag_cache
                    .transactions
                    .get_mut(&input.previous_outpoint.transaction_id.unwrap())
                {
                    previous_transaction.protocol = Some(TransactionProtocol::Krc);

                    if previous_transaction.accepting_block_hash.is_some() {
                        dag_cache
                            .seconds
                            .entry(previous_transaction.block_time / 1000)
                            .and_modify(|v| {
                                v.increment_protocol_transaction_count(TransactionProtocol::Krc)
                            });
                    }
                } else {
                    warn!(
                        target: LogTarget::Daemon.as_str(),
                        "KRC tx detected, but previous transaction not found in cache: {}",
                        transaction.id,
                    );
                }
                return Some(TransactionProtocol::Krc);
            }

            if opcode.as_str() == "OP_PUSH" && ["kns"].contains(&data.as_str()) {
                if let Some(mut previous_transaction) = dag_cache
                    .transactions
                    .get_mut(&input.previous_outpoint.transaction_id.unwrap())
                {
                    previous_transaction.protocol = Some(TransactionProtocol::Kns);

                    if previous_transaction.accepting_block_hash.is_some() {
                        dag_cache
                            .seconds
                            .entry(previous_transaction.block_time / 1000)
                            .and_modify(|v| {
                                v.increment_protocol_transaction_count(TransactionProtocol::Kns)
                            });
                    }
                } else {
                    warn!(
                        target: LogTarget::Daemon.as_str(),
                        "KNS tx detected, but previous transaction not found in cache: {}",
                        transaction.id,
                    );
                }
                return Some(TransactionProtocol::Kns);
            }
        }
    }

    None
}

/// Computes a transaction's contribution to the per-second script-class and
/// covenant metrics that are attributed to the transaction's own timestamp, using
/// the shared tally so the daemon and CLI stay in sync. Output classes and
/// covenant-bound outputs are counted per output; covenant spends and
/// P2SH-revealed introspection / ZK precompile opcodes are counted from each
/// input's resolved UTXO entry (unresolved inputs contribute nothing).
///
/// P2SH opcode reveals are credited to the spending transaction's second here.
/// They are *additionally* credited to the second of the transaction that
/// *created* the spent output via [`collect_revealed_opcode_attributions`] (when
/// that creating transaction is still cached), mirroring how inscription
/// protocols are also credited to the commit transaction.
fn script_covenant_delta(transaction: &CacheTransaction) -> CovenantTally {
    let outputs = transaction.outputs.iter().map(|output| TxOutputView {
        script_public_key: &output.script_public_key,
        is_covenant: output.covenant.is_some(),
    });

    let resolved_inputs = transaction.inputs.iter().filter_map(|input| {
        input.utxo_entry.as_ref().map(|utxo_entry| ResolvedInputView {
            spent_script_public_key: &utxo_entry.script_public_key,
            spent_is_covenant: utxo_entry.covenant_id.is_some(),
            signature_script: &input.signature_script,
        })
    });

    tally_covenant_metrics(outputs, resolved_inputs)
}

/// Per-creating-transaction record of which covenant opcodes a spending
/// transaction revealed when spending that creating transaction's P2SH outputs.
#[derive(Default)]
struct RevealedOpcodes {
    /// At least one revealed redeem script used a covenant / introspection opcode.
    introspection: bool,
    /// At least one revealed redeem script used the ZK precompile opcode.
    zk_precompile: bool,
}

/// Scans a spending transaction's resolved P2SH inputs and groups the covenant
/// opcodes they reveal by the transaction that created each spent output. Only
/// pay-to-script-hash spends can reveal these opcodes (they live in the redeem
/// script committed by the P2SH output), and unresolved inputs are skipped.
fn collect_revealed_opcode_attributions(
    transaction: &CacheTransaction,
) -> HashMap<Hash, RevealedOpcodes> {
    let mut attributions: HashMap<Hash, RevealedOpcodes> = HashMap::new();

    for input in transaction.inputs.iter() {
        let Some(utxo) = input.utxo_entry.as_ref() else {
            continue;
        };

        if utxo.script_public_key_type != Some(ScriptClass::ScriptHash) {
            continue;
        }

        let introspection = signature_script_reveals_introspection(&input.signature_script);
        let zk_precompile = signature_script_reveals_zk_precompile(&input.signature_script);
        if !introspection && !zk_precompile {
            continue;
        }

        let Some(previous_tx_id) = input.previous_outpoint.transaction_id else {
            continue;
        };

        let entry = attributions.entry(previous_tx_id).or_default();
        entry.introspection |= introspection;
        entry.zk_precompile |= zk_precompile;
    }

    attributions
}

/// Credits the *transaction counts* for revealed covenant opcodes to the second
/// of the transaction that *created* each spent output, so a creating transaction
/// is counted as introspection / ZK-precompile activity at its own timestamp.
/// Output-spent counts are not attributed here: the spend happens once, at the
/// spending transaction, and is recorded against that transaction's second via
/// the shared covenant tally.
///
/// Returns a record of every credit actually applied so the identical credits can
/// be reversed on reorg via [`reverse_revealed_opcode_credits`]. When a creating
/// transaction is still cached and accepted we use its timestamp; when it has aged
/// out of cache, or is not yet accepted, we skip it (warning on eviction, mirroring
/// inscription handling) and record nothing for it, so there is nothing to reverse.
fn apply_revealed_opcode_credits(
    dag_cache: &Arc<DagCache>,
    attributions: HashMap<Hash, RevealedOpcodes>,
) -> Vec<RevealedOpcodeCredit> {
    let mut credits = Vec::new();

    for (previous_tx_id, revealed) in attributions {
        let second = {
            let Some(previous_tx) = dag_cache.transactions.get(&previous_tx_id) else {
                warn!(
                    target: LogTarget::Daemon.as_str(),
                    "Covenant opcode reveal detected, but previous transaction not in cache: {}",
                    previous_tx_id
                );
                continue;
            };

            // Only attribute once the creating transaction is itself accepted.
            if previous_tx.accepting_block_hash.is_none() {
                continue;
            }

            previous_tx.block_time / 1000
        };

        // Record the credit only if the target second actually exists (and was
        // therefore modified), so the reversal never subtracts a credit that was
        // never applied.
        if let Some(mut second_metrics) = dag_cache.seconds.get_mut(&second) {
            if revealed.introspection {
                second_metrics.introspection_opcode_tx_count += 1;
            }
            if revealed.zk_precompile {
                second_metrics.zk_precompile_tx_count += 1;
            }
            credits.push(RevealedOpcodeCredit {
                second,
                introspection: revealed.introspection,
                zk_precompile: revealed.zk_precompile,
            });
        }
    }

    credits
}

/// Reverses credits previously recorded by [`apply_revealed_opcode_credits`],
/// subtracting from the exact same seconds. Because the seconds are taken from the
/// recorded credits rather than re-derived from the creating transactions, the
/// reversal stays correct even if those transactions have since been evicted from
/// the cache or had their own acceptance removed.
fn reverse_revealed_opcode_credits(dag_cache: &Arc<DagCache>, credits: &[RevealedOpcodeCredit]) {
    for credit in credits {
        dag_cache.seconds.entry(credit.second).and_modify(|v| {
            if credit.introspection {
                v.introspection_opcode_tx_count =
                    v.introspection_opcode_tx_count.saturating_sub(1);
            }
            if credit.zk_precompile {
                v.zk_precompile_tx_count = v.zk_precompile_tx_count.saturating_sub(1);
            }
        });
    }
}

fn transaction_add_pipeline(
    dag_cache: Arc<DagCache>,
    block_hash: Hash,
    mut transaction: CacheTransaction,
) {
    let tx_id = transaction.id;
    let block_time = transaction.block_time;

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
    // Run process to determine TX type, protocol
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
        transaction.protocol = detect_transaction_protocol(dag_cache.clone(), &transaction);
    }

    dag_cache.transactions.insert(tx_id, transaction);
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
        // Calculate fee paid. A fee can only be computed when every input's
        // spent (previous) output is resolved; otherwise total_input_amount is
        // understated and the subtraction would underflow. When any input is
        // unresolved we record no fee for this transaction rather than a wrong
        // one — downstream consumers treat a None fee as zero.
        let all_inputs_resolved = tx.inputs.iter().all(|input| input.utxo_entry.is_some());

        let total_input_amount = tx
            .inputs
            .iter()
            .filter_map(|input| input.utxo_entry.as_ref().map(|utxo_entry| utxo_entry.amount))
            .sum::<u64>();

        let total_output_amount = tx.outputs.iter().map(|output| output.value).sum::<u64>();

        tx.fee = if all_inputs_resolved {
            total_input_amount.checked_sub(total_output_amount)
        } else {
            None
        };

        let delta = script_covenant_delta(&tx);
        let reveal_attributions = collect_revealed_opcode_attributions(&tx);

        // Update metrics for given second
        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| {
                v.increment_unique_accepted_transaction_count();

                // Only accumulate a fee for transactions with a fully-resolved
                // input set. Covenant-spend counts inside the delta are likewise
                // only derived from resolved inputs.
                if let Some(fee) = tx.fee {
                    v.increment_total_fees(fee);
                }

                // Increment Protocol count for given second
                if let Some(ref protocol) = tx.protocol {
                    v.increment_protocol_transaction_count(protocol.clone());
                }

                // Increment script-class and covenant counts for given second
                v.apply_script_covenant_delta(&delta);
            });

        // Release the transaction guard before looking up creating transactions,
        // to avoid a re-entrant borrow on the transactions DashMap.
        drop(tx);

        // Additionally credit any P2SH-revealed introspection / ZK precompile
        // opcodes to the second of the transaction that created each spent output
        // (the spending transaction's second was already credited above). Record
        // the applied credits on this transaction so a reorg can reverse the exact
        // same seconds even if those creating transactions are later evicted.
        let credits = apply_revealed_opcode_credits(&dag_cache, reveal_attributions);
        if !credits.is_empty() {
            dag_cache
                .transactions
                .entry(transaction_id)
                .and_modify(|v| v.revealed_opcode_credits = credits);
        }
    }
}

pub fn add_chain_block_acceptance_pipeline(
    dag_cache: Arc<DagCache>,
    accepting_chain_block_hash: Hash,
    acceptance_data: &RpcChainBlockAcceptedTransactions,
) {
    // Set block's chain block status to true
    dag_cache
        .blocks
        .entry(accepting_chain_block_hash)
        .and_modify(|v| v.is_chain_block = true);

    // Popuplate UTXO on Transaction
    for transaction in acceptance_data.accepted_transactions.iter() {
        // for transaction in merged_block.accepted_transactions.iter() {
        let tx_id = transaction
            .verbose_data
            .as_ref()
            .unwrap()
            .transaction_id
            .unwrap();

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
        // }
    }

    // Add chain block and it's accepted transactions
    let mut accepted_transactions = acceptance_data
        .accepted_transactions
        .iter()
        // .flat_map(|block| block.accepted_transactions)
        .map(|tx| tx.verbose_data.as_ref().unwrap().transaction_id.unwrap())
        .collect::<Vec<_>>();
    accepted_transactions.sort();
    accepted_transactions.dedup();

    dag_cache
        .accepting_block_transactions
        .insert(accepting_chain_block_hash, accepted_transactions.clone());

    // Process transactions
    for tx_id in accepted_transactions {
        add_transaction_acceptance(dag_cache.clone(), tx_id, accepting_chain_block_hash);
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
        let delta = script_covenant_delta(&tx);

        dag_cache
            .seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| {
                v.decrement_unique_accepted_transaction_count();

                if let Some(fee) = tx.fee {
                    v.decrement_total_fees(fee);
                }

                if let Some(ref protocol) = tx.protocol {
                    v.decrement_protocol_transaction_count(protocol.clone());
                }

                v.remove_script_covenant_delta(&delta);
            });

        // Release the transaction guard before looking up creating transactions,
        // to avoid a re-entrant borrow on the transactions DashMap.
        drop(tx);

        // Reverse the exact creating-transaction-second credits recorded when this
        // spending transaction was accepted (its own second is reversed via the
        // delta above). Replaying the recorded credits — rather than recomputing
        // them from the creating transactions — keeps the reversal correct even if
        // those transactions have since been evicted or had acceptance removed.
        let credits = dag_cache
            .transactions
            .get_mut(&transaction_id)
            .map(|mut v| std::mem::take(&mut v.revealed_opcode_credits))
            .unwrap_or_default();
        reverse_revealed_opcode_credits(&dag_cache, &credits);
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
