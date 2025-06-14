use crate::cmds::blocks::stats::Stats;
use kaspa_consensus::consensus::storage::ConsensusStorage;
use kaspa_consensus::model::stores::acceptance_data::AcceptanceDataStoreReader;
use kaspa_consensus::model::stores::block_transactions::BlockTransactionsStoreReader;
use kaspa_consensus::model::stores::headers::HeaderStoreReader;
use kaspa_consensus::model::stores::selected_chain::SelectedChainStoreReader;
use kaspa_consensus::model::stores::utxo_diffs::UtxoDiffsStoreReader;
use kaspa_consensus_core::tx::{TransactionId, TransactionOutpoint, UtxoEntry};
use kaspa_consensus_core::utxo::utxo_diff::ImmutableUtxoDiff;
use kaspa_consensus_core::Hash;
use kaspa_database::prelude::StoreError;
use kaspa_txscript::standard::extract_script_pub_key_address;
use kaspalytics_utils::config::Config;
use kaspalytics_utils::kaspad::db::ConsensusStorageSecondary;
use kaspalytics_utils::log::LogTarget;
use log::{debug, error};
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::time::sleep;

use kaspalytics_utils::granularity::Granularity;

pub struct BlockAnalysis {
    config: Config,
    storage: Arc<ConsensusStorage>,
    window_start_time: u64,
    window_end_time: u64,
    chain_blocks: BTreeMap<u64, Hash>,
    stats: BTreeMap<u64, Stats>,
}

impl BlockAnalysis {
    pub fn new_for_yesterday(config: Config, storage: Arc<ConsensusStorage>) -> Self {
        let start_of_today = chrono::Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let start_of_yesterday = start_of_today - chrono::Duration::days(1);
        let end_of_yesterday = start_of_today - chrono::Duration::milliseconds(1);

        Self {
            config,
            storage,
            window_start_time: start_of_yesterday.and_utc().timestamp_millis() as u64,
            window_end_time: end_of_yesterday.and_utc().timestamp_millis() as u64,
            chain_blocks: BTreeMap::<u64, Hash>::new(),
            stats: BTreeMap::<u64, Stats>::new(),
        }
    }

    #[allow(dead_code)]
    pub fn new_from_time_window(
        config: Config,
        storage: Arc<ConsensusStorage>,
        start_time: u64,
        end_time: u64,
    ) -> Self {
        Self {
            config,
            storage,
            window_start_time: start_time,
            window_end_time: end_time,
            chain_blocks: BTreeMap::<u64, Hash>::new(),
            stats: BTreeMap::<u64, Stats>::new(),
        }
    }

    // pub fn new_from_low_hash()
}

impl BlockAnalysis {
    fn load_chain_blocks(&mut self) -> Result<(), StoreError> {
        for entry in self
            .storage
            .selected_chain_store
            .read()
            .access_hash_by_index
            .iterator()
        {
            let (key, hash) = entry.map_err(|err| {
                if let Some(rocksdb_err) = err.downcast_ref::<rocksdb::Error>() {
                    StoreError::DbError(rocksdb_err.clone())
                } else if let Ok(bincode_err) = err.downcast::<Box<bincode::ErrorKind>>() {
                    StoreError::DeserializationError(*bincode_err)
                } else {
                    unreachable!()
                }
            })?;

            let key = u64::from_le_bytes((*key).try_into().unwrap());
            let header = self.storage.headers_store.get_header(hash)?;

            if self.window_start_time <= header.timestamp
                && header.timestamp <= self.window_end_time
            {
                self.chain_blocks.insert(key, hash);
            }
        }

        debug!(
            target: LogTarget::Cli.as_str(),
            "{} chain blocks loaded from DbSelectedChainStore for target window",
            self.chain_blocks.len()
        );

        Ok(())
    }

    // Reads utxo_diffs_store for given chain block
    // Returns a single map of all UTXOs affected (created or removed) for chain block
    fn get_utxos_for_chain_block(
        &self,
        hash: Hash,
    ) -> Result<HashMap<TransactionOutpoint, UtxoEntry>, StoreError> {
        let utxo_diffs = self.storage.utxo_diffs_store.get(hash)?;

        let mut utxos = HashMap::<TransactionOutpoint, UtxoEntry>::new();

        utxo_diffs.removed().iter().for_each(|(outpoint, utxo)| {
            utxos.insert(*outpoint, utxo.clone());
        });

        utxo_diffs.added().iter().for_each(|(outpoint, utxo)| {
            utxos.insert(*outpoint, utxo.clone());
        });

        Ok(utxos)
    }
}

impl BlockAnalysis {
    fn tx_analysis(&mut self) -> Result<(), StoreError> {
        let mut transaction_cache = HashSet::<TransactionId>::new();
        let mut tx_iter_order = VecDeque::<Vec<TransactionId>>::new();

        // Iterate chain blocks
        for (chain_block_index, (_, chain_block_hash)) in
            self.chain_blocks.iter().skip(1).enumerate()
        {
            if chain_block_index % 1000 == 0 {
                debug!(
                    target: LogTarget::Cli.as_str(),
                    "tx_analysis processed {} chain blocks",
                    chain_block_index
                );
            }

            let mut this_chain_blocks_accepted_transactions = Vec::<TransactionId>::new();

            // Get acceptance data
            let acceptances = self.storage.acceptance_data_store.get(*chain_block_hash)?;

            // Load UTXOs from utxo diffs store
            let utxos = self.get_utxos_for_chain_block(*chain_block_hash)?;

            // Iterate blocks in current chain block's mergeset
            for mergeset_data in acceptances.iter() {
                let header = self
                    .storage
                    .headers_store
                    .get_header(mergeset_data.block_hash)?;

                let transactions = self
                    .storage
                    .block_transactions_store
                    .get(mergeset_data.block_hash)?;

                let is_chain_block = match self
                    .storage
                    .selected_chain_store
                    .read()
                    .get_by_hash(mergeset_data.block_hash)
                {
                    Ok(_) => true,
                    Err(StoreError::KeyNotFound(_)) => false,
                    Err(_) => panic!(),
                };

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
                                .and_modify(|stats| stats.chain_block_count += 1);

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
                                    self.config.network_id.into(),
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
                            self.config.network_id.into(),
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
                    this_chain_blocks_accepted_transactions.push(tx.id());
                }

                self.stats.entry(block_time_s).and_modify(|stats| {
                    stats
                        .transaction_count_per_block
                        .push(accepted_transactions_in_this_block)
                });
            }

            tx_iter_order.push_back(this_chain_blocks_accepted_transactions);

            if chain_block_index >= 2700 {
                if let Some(tx_ids) = tx_iter_order.pop_front() {
                    for tx_id in tx_ids {
                        transaction_cache.remove(&tx_id);
                    }
                }
            }
        }

        Ok(())
    }
}

impl BlockAnalysis {
    async fn run_inner(&mut self, pool: &PgPool) -> Result<(), StoreError> {
        // TODO custom error that wraps StoreError, other error types...

        debug!(
            target: LogTarget::Cli.as_str(),
            "Loading chain blocks from DbSelectedChainStore for target window..."
        );
        self.load_chain_blocks()?;

        debug!(target: LogTarget::Cli.as_str(), "Running tx_analysis...");
        self.tx_analysis()?;

        let per_day = Stats::rollup(&self.stats.clone(), Granularity::Day);
        for (time, stats) in per_day {
            // Skip stat entries outside of time window
            // Sometimes, due to block relations, there are entries for the day prior
            if time * 1000 < self.window_start_time || self.window_end_time < time * 1000 {
                continue;
            }

            debug!(target: LogTarget::Cli.as_str(), "{:?}", stats);
            stats.save(pool).await.unwrap(); // TODO handle

            let _ = kaspalytics_utils::email::send_email(
                &self.config,
                "block-pipeline completed".to_string(),
                format!("{:?}", stats),
            );
        }

        Ok(())
    }

    pub async fn run(config: Config, pool: PgPool) {
        // Sporadically, a RocksDB error will be raised about missing SST file
        // The readonly conn creates a point in time view of database
        // But SST files are being deleted by primary
        // New rdb connection uses a readonly over a checkpoint to attempt to address this
        // If checkpoint fixes the issue, might be able to remove below retry loop
        let mut retries = 0;
        let max_retries = 120;
        let retry_delay = std::time::Duration::from_secs(60);

        loop {
            let storage = ConsensusStorageSecondary::new(config.clone());

            let mut process =
                BlockAnalysis::new_for_yesterday(config.clone(), storage.inner.clone());

            match process.run_inner(&pool).await {
                Ok(_) => break,
                Err(StoreError::DbError(err)) if retries < max_retries => {
                    // Close database connection before sleeping
                    // Inside retries window. Sleep and try again
                    drop(process);
                    drop(storage);

                    retries += 1;
                    error!(target: LogTarget::Cli.as_str(), "{}", err);
                    error!(
                        target: LogTarget::Cli.as_str(),
                        "Database error during tx_analysis attempt {}/{}. Retrying in {:?}...\n",
                        retries, max_retries, retry_delay
                    );
                    sleep(retry_delay).await;
                }
                Err(StoreError::DbError(_)) => {
                    // After max retries, send alert email and exit
                    error!(
                        target: LogTarget::Cli.as_str(),
                        "Analysis::tx_analysis failed after {} attempts. Exiting...",
                        retries
                    );
                    let _ = kaspalytics_utils::email::send_email(
                        &config,
                        "block-pipeline alert".to_string(),
                        "Analysis::tx_analysis reached max retries due to database error."
                            .to_string(),
                    );
                    break;
                }
                Err(e) => {
                    // Handle other errors and exit
                    error!(
                        target: LogTarget::Cli.as_str(),
                        "Analysis::tx_analysis failed with error: {:?}",
                        e
                    );
                    let _ = kaspalytics_utils::email::send_email(
                        &config,
                        "block-pipeline alert".to_string(),
                        format!("Analysis::tx_analysis failed with error: {:?}", e),
                    );
                    break;
                }
            }
        }
    }
}
