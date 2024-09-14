use crate::service::stats::Stats;
use chrono::{Duration, Utc};
use clap::Args;
use kaspa_consensus::consensus::storage::ConsensusStorage;
use kaspa_consensus::model::stores::acceptance_data::AcceptanceDataStoreReader;
use kaspa_consensus::model::stores::block_transactions::BlockTransactionsStoreReader;
use kaspa_consensus::model::stores::headers::HeaderStoreReader;
use kaspa_consensus::model::stores::selected_chain::SelectedChainStoreReader;
use kaspa_consensus::model::stores::utxo_diffs::UtxoDiffsStoreReader;
use kaspa_consensus_core::header::Header;
use kaspa_consensus_core::network::NetworkId;
use kaspa_consensus_core::tx::{Transaction, TransactionId, TransactionOutpoint, UtxoEntry};
use kaspa_consensus_core::utxo::utxo_diff::ImmutableUtxoDiff;
use kaspa_consensus_core::Hash;
use kaspa_database::prelude::StoreError;
use kaspa_txscript::standard::extract_script_pub_key_address;
use log::info;
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::sync::Arc;

use super::Granularity;

pub struct Analysis {
    storage: Arc<ConsensusStorage>,
    network_id: NetworkId,
    window_start_time: u64,
    window_end_time: u64,
    chain_blocks: BTreeMap<u64, Hash>,
    stats: BTreeMap<u64, Stats>,
}

impl Analysis {
    pub fn new_from_time_window(storage: Arc<ConsensusStorage>, network_id: NetworkId) -> Self {
        // Temporarily hardcoded to always run for yesterday
        let start_of_today = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
        let start_of_yesterday = start_of_today - Duration::days(1);
        let end_of_yesterday = start_of_today - Duration::milliseconds(1);
        let window_start_time = start_of_yesterday.and_utc().timestamp_millis() as u64;
        let window_end_time = end_of_yesterday.and_utc().timestamp_millis() as u64;

        Self {
            storage,
            network_id,
            window_start_time,
            window_end_time,
            chain_blocks: BTreeMap::<u64, Hash>::new(),
            stats: BTreeMap::<u64, Stats>::new(),
        }
    }

    // pub fn new_from_low_hash(storage: Arc<ConsensusStorage>, network_id: NetworkId, low_hash: Hash) -> Self {
    //     // TODO ensure is chain block. Else walk back until we get one
    //     let block_header = storage
    //         .headers_store
    //         .get_header(low_hash)
    //         .unwrap();

    //     let window_start_time = block_header.timestamp;
    //     let window_end_time = Utc::now().timestamp_millis() as u64;
    //     let stats = Stats::new(window_start_time, window_end_time);

    //     Self {
    //         storage,
    //         network_id,
    //         window_start_time: window_start_time,
    //         window_end_time: window_end_time,
    //         chain_blocks: BTreeMap::<u64, Hash>::new(),
    //         stats,
    //     }
    // }
}

impl Analysis {
    fn load_chain_blocks(&mut self) {
        for (key, hash) in self
            .storage
            .selected_chain_store
            .read()
            .access_hash_by_index
            .iterator()
            .map(|p| p.unwrap())
        {
            let key = u64::from_le_bytes((*key).try_into().unwrap());
            let header = self.storage.headers_store.get_header(hash).unwrap();

            if self.window_start_time <= header.timestamp
                && header.timestamp <= self.window_end_time
            {
                self.chain_blocks.insert(key, hash);
            }
        }

        info!(
            "{} chain blocks loaded from DbSelectedChainStore for target window",
            self.chain_blocks.len()
        );
    }
}

impl Analysis {
    fn get_utxos_from_utxo_diffs_store(
        &self,
        hash: Hash,
    ) -> HashMap<TransactionOutpoint, UtxoEntry> {
        let utxo_diffs = self.storage.utxo_diffs_store.get(hash).unwrap();
        let mut utxos = HashMap::<TransactionOutpoint, UtxoEntry>::new();

        utxo_diffs.removed().iter().for_each(|(outpoint, utxo)| {
            utxos.insert(*outpoint, utxo.clone());
        });

        utxo_diffs.added().iter().for_each(|(outpoint, utxo)| {
            utxos.insert(*outpoint, utxo.clone());
        });

        utxos
    }

    fn get_block_data(&self, hash: Hash) -> (Arc<Header>, Arc<Vec<Transaction>>, bool) {
        let header = self.storage.headers_store.get_header(hash).unwrap();

        let transactions = self.storage.block_transactions_store.get(hash).unwrap();

        let is_chain_block = match self.storage.selected_chain_store.read().get_by_hash(hash) {
            Ok(_) => true,
            Err(StoreError::KeyNotFound(_)) => false,
            Err(_) => panic!(),
        };

        (header, transactions, is_chain_block)
    }
}

impl Analysis {
    fn tx_analysis(&mut self) {
        let mut transaction_cache = std::collections::HashSet::<TransactionId>::new();
        let mut tx_iter_order = std::collections::VecDeque::<Vec<TransactionId>>::new();

        // Iterate chain blocks
        for (i, (_, hash)) in self.chain_blocks.iter().skip(1).enumerate() {
            let mut this_chain_blocks_merged_transactions = Vec::<TransactionId>::new();

            // Get acceptance data
            let acceptances = self.storage.acceptance_data_store.get(*hash).unwrap();

            // Load UTXOs from utxo diffs store
            let utxos = self.get_utxos_from_utxo_diffs_store(*hash);

            // Iterate blocks in current chain block's mergeset
            for mergeset_data in acceptances.iter() {
                // Get block header, transactions, if chain block
                let (header, transactions, is_chain_block) =
                    self.get_block_data(mergeset_data.block_hash);
                let block_time_s = header.timestamp / 1000;

                // Ensure stats entry for this second exists
                self.stats
                    .entry(block_time_s)
                    .or_insert(Stats::new(block_time_s, Granularity::Second));

                // Iterate transactions in the merged block
                let mut accepted_transactions_in_this_block = 0;
                for (tx_index, tx) in transactions.iter().enumerate() {
                    // Skip transactions we already processed
                    // This is a lazy (inefficient) approach to handle when a TX is in multiple blocks, and those blocks are not merged by same chain block
                    if transaction_cache.contains(&tx.id()) {
                        continue;
                    }

                    match (is_chain_block, tx_index) {
                        (true, 0) => {
                            // Coinbase transaction of chain block
                            // Add to counters
                            self.stats
                                .entry(block_time_s)
                                .and_modify(|stats| stats.coinbase_tx_count += 1);

                            self.stats.entry(block_time_s).and_modify(|stats| {
                                stats.output_count_coinbase_tx += tx.outputs.len() as u64
                            });

                            self.stats
                                .entry(block_time_s)
                                .and_modify(|stats| stats.spc_block_count += 1);

                            accepted_transactions_in_this_block += 1;

                            // Continue skips fee analysis since this is coinbase tx
                            continue;
                        }
                        (false, 0) => {
                            // Coinbase transaction of non-chain block
                            // Skip processing as these are paid by chain block
                            continue;
                        }
                        (_, _) => {
                            // A regular transaction
                            // Either part of chain block (at index 1+)
                            // Or part of non-chain block (at index 1+)
                            self.stats
                                .entry(block_time_s)
                                .and_modify(|stats| stats.regular_tx_count += 1);

                            accepted_transactions_in_this_block += 1;
                        }
                    }

                    // Count inputs of current transaction
                    self.stats
                        .entry(block_time_s)
                        .and_modify(|stats| stats.input_count += tx.inputs.len() as u64);

                    // Count outputs of current transaction
                    self.stats.entry(block_time_s).and_modify(|stats| {
                        stats.output_count_regular_tx += tx.outputs.len() as u64
                    });

                    let mut all_outpoints_resolved = true;
                    let mut tx_fee = 0;
                    for input in tx.inputs.iter() {
                        let previous_outpoint = utxos.get(&input.previous_outpoint);
                        match previous_outpoint {
                            Some(previous_outpoint) => {
                                tx_fee += previous_outpoint.amount;

                                let address = extract_script_pub_key_address(
                                    &previous_outpoint.script_public_key,
                                    self.network_id.into(),
                                )
                                .unwrap();

                                self.stats.entry(block_time_s).and_modify(|stats| {
                                    stats.unique_senders.insert(address);
                                });
                            }
                            None => {
                                self.stats.entry(block_time_s).and_modify(|stats| {
                                    stats.input_count_missing_previous_outpoints += 1
                                });

                                all_outpoints_resolved = false;
                            }
                        }
                    }

                    if !all_outpoints_resolved {
                        self.stats
                            .entry(block_time_s)
                            .and_modify(|stats| stats.skipped_tx_count_cannot_resolve_inputs += 1);
                        continue;
                    }

                    for output in tx.outputs.iter() {
                        tx_fee -= output.value;
                        let address = extract_script_pub_key_address(
                            &output.script_public_key,
                            self.network_id.into(),
                        )
                        .unwrap();
                        self.stats.entry(block_time_s).and_modify(|stats| {
                            stats.unique_recipients.insert(address);
                        });
                    }

                    self.stats
                        .entry(block_time_s)
                        .and_modify(|stats| stats.fees.push(tx_fee));

                    transaction_cache.insert(tx.id());
                    this_chain_blocks_merged_transactions.push(tx.id());
                }

                self.stats.entry(block_time_s).and_modify(|stats| {
                    stats
                        .transaction_count_per_block
                        .push(accepted_transactions_in_this_block)
                });
            }

            tx_iter_order.push_back(this_chain_blocks_merged_transactions);

            if i >= 2700 {
                if let Some(tx_ids) = tx_iter_order.pop_front() {
                    for tx_id in tx_ids {
                        transaction_cache.remove(&tx_id);
                    }
                }
            }
        }
    }
}

impl Analysis {
    pub async fn run(&mut self, pool: &PgPool) {
        self.load_chain_blocks();

        self.tx_analysis();
        info!("{}", self.stats.len());

        let per_day = Stats::rollup(&self.stats.clone(), Granularity::Day);
        for (_, stats) in per_day {
            info!("{:?}", stats);
            stats.save(pool).await;

            crate::utils::email::send_email(
                format!("{} | kaspalytics-rs stats results", env::var("ENV").unwrap()),
                format!("{:?}", stats)
            );
        }
    }
}
