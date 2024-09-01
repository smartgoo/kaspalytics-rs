use crate::service::stats::Stats;
use chrono::{Duration, Utc};
use kaspa_consensus::consensus::storage::ConsensusStorage;
use kaspa_consensus::model::stores::acceptance_data::AcceptanceDataStoreReader;
use kaspa_consensus::model::stores::block_transactions::BlockTransactionsStoreReader;
use kaspa_consensus::model::stores::headers::HeaderStoreReader;
use kaspa_consensus::model::stores::selected_chain::SelectedChainStoreReader;
use kaspa_consensus::model::stores::utxo_diffs::UtxoDiffsStoreReader;
use kaspa_consensus_core::network::NetworkId;
use kaspa_consensus_core::tx::{TransactionId, TransactionOutpoint, UtxoEntry};
use kaspa_consensus_core::utxo::utxo_diff::ImmutableUtxoDiff;
use kaspa_consensus_core::Hash;
use kaspa_database::prelude::StoreError;
use kaspa_txscript::standard::extract_script_pub_key_address;
use log::info;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct Analysis {
    storage: Arc<ConsensusStorage>,
    network_id: NetworkId,

    window_start_time: u64,
    window_end_time: u64,

    chain_blocks: BTreeMap<u64, Hash>,

    stats: Stats,
}

impl Analysis {
    pub fn new(storage: Arc<ConsensusStorage>, network_id: NetworkId) -> Self {
        // Temporarily hardcoding target window to yesterday
        // TODO Should expose as fn args, so it can be ran for any window
        // Should also give ability to start it from a low hash and run to virtual? Or virtual down to low hash
        let start_of_today = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
        let start_of_yesterday = start_of_today - Duration::days(1);
        let end_of_yesterday = start_of_today - Duration::milliseconds(1);
        let start_of_yesterday_epoch = start_of_yesterday.and_utc().timestamp_millis() as u64;
        let end_of_yesterday_epoch = end_of_yesterday.and_utc().timestamp_millis() as u64;

        info!("window_start_time: {}", start_of_yesterday_epoch);
        info!("window_end_time: {}", end_of_yesterday_epoch);

        let stats = Stats::new(start_of_yesterday_epoch, end_of_yesterday_epoch);

        Self {
            storage,
            network_id,

            window_start_time: start_of_yesterday_epoch,
            window_end_time: end_of_yesterday_epoch,

            chain_blocks: BTreeMap::<u64, Hash>::new(),

            stats,
        }
    }

    // Load chain blocks from database into BTreeMap, so we can iterate ordered
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

    fn get_accepted_transaction_counts_per_accepting_block(&mut self) -> Vec<u64> {
        let mut counts = Vec::<u64>::new();

        for (_, hash) in self.chain_blocks.iter() {
            let acceptance_data = self.storage.acceptance_data_store.get(*hash).unwrap();

            let mut chain_block_count = 0;
            for acceptance_obj in acceptance_data.iter() {
                chain_block_count += acceptance_obj.accepted_transactions.len();
            }
            counts.push(chain_block_count as u64);
        }

        counts
    }

    fn get_accepted_transaction_counts_per_block(&mut self) -> Vec<u64> {
        let mut counts = Vec::<u64>::new();

        for (_, hash) in self.chain_blocks.iter() {
            let acceptance_data = self.storage.acceptance_data_store.get(*hash).unwrap();

            for acceptance_obj in acceptance_data.iter() {
                counts.push(acceptance_obj.accepted_transactions.len() as u64);
            }
        }

        counts
    }

    fn tx_analysis(&mut self) {
        let mut processed_transactions = std::collections::HashSet::<TransactionId>::new();

        for (_, hash) in self.chain_blocks.iter() {
            // Get acceptance data
            let acceptances = self.storage.acceptance_data_store.get(*hash).unwrap();

            // Load UTXO diffs to assist with analyzing fees
            let utxo_diffs = self.storage.utxo_diffs_store.get(*hash).unwrap();
            let mut utxos = std::collections::HashMap::<TransactionOutpoint, UtxoEntry>::new();
            for (outpoint, utxo) in utxo_diffs.removed().iter() {
                utxos.insert(*outpoint, utxo.clone());
            }
            for (outpoint, utxo) in utxo_diffs.added().iter() {
                utxos.insert(*outpoint, utxo.clone());
            }

            for obj in acceptances.iter() {
                let header = self
                    .storage
                    .headers_store
                    .get_header(obj.block_hash)
                    .unwrap();
                let transactions = self
                    .storage
                    .block_transactions_store
                    .get(obj.block_hash)
                    .unwrap();
                let is_chain_block = match self
                    .storage
                    .selected_chain_store
                    .read()
                    .get_by_hash(obj.block_hash)
                {
                    Ok(_) => true,
                    Err(StoreError::KeyNotFound(_)) => false,
                    Err(_) => panic!(),
                };

                for (tx_index, tx) in transactions.iter().enumerate() {
                    let mut outpoints_resolved = true;
                    let mut tx_fee = 0;

                    // Skip transactions we already processed
                    // This is a lazy (inefficient) approach to handle when a TX is in mutliple blocks, and those blocks are not in same DAA
                    // TODO find better way of handling
                    if processed_transactions.contains(&tx.id()) {
                        continue;
                    }

                    match (is_chain_block, tx_index) {
                        (true, 0) => {
                            // Coinbase transaction of chain block
                            // Add to counters
                            // Continue to skip fee analysis
                            self.stats.coinbase_tx_count += 1;
                            self.stats.output_count_coinbase_tx += tx.outputs.len() as u64;

                            self.stats
                                .transactions_per_second
                                .entry(header.timestamp / 1000)
                                .and_modify(|v| *v += 1)
                                .or_insert(1);

                            continue;
                        }
                        (false, 0) => {
                            // Coinbase transaction of non-chain block
                            // Skip as these are technically not accepted
                            continue;
                        }
                        (_, _) => {
                            // A regular transaction
                            // Either part of chain block (at index 1+)
                            // Or part of non-chain block (at index 1+)
                            self.stats.regular_tx_count += 1;

                            self.stats
                                .transactions_per_second
                                .entry(header.timestamp / 1000)
                                .and_modify(|v| *v += 1)
                                .or_insert(1);
                        }
                    }

                    // Count inputs/outputs
                    self.stats.input_count += tx.inputs.len() as u64;
                    self.stats.output_count_regular_tx += tx.outputs.len() as u64;

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
                                self.stats.unique_senders.insert(address);
                            }
                            None => {
                                self.stats.input_count_missing_previous_outpoints += 1;
                                outpoints_resolved = false;
                            }
                        }
                    }

                    if !outpoints_resolved {
                        self.stats.skipped_tx_count_cannot_resolve_inputs += 1;
                        continue;
                    }

                    for output in tx.outputs.iter() {
                        tx_fee -= output.value;
                        let address = extract_script_pub_key_address(
                            &output.script_public_key,
                            self.network_id.into(),
                        )
                        .unwrap();
                        self.stats.unique_recipients.insert(address);
                    }

                    self.stats.fees.push(tx_fee);

                    processed_transactions.insert(tx.id());
                }
            }
        }

        self.stats.log();
    }

    pub fn run(&mut self) {
        // load chain blocks into cache
        self.load_chain_blocks();

        // count chain blocks
        self.stats.spc_block_count = self.chain_blocks.len() as u64;

        // txs per accepting block - mean, median, min, max
        self.stats.transaction_count_per_spc_block =
            self.get_accepted_transaction_counts_per_accepting_block();

        // txs per block - mean, median, min, max
        self.stats.transaction_count_per_block = self.get_accepted_transaction_counts_per_block();

        // Transaction analysis
        self.tx_analysis();
    }
}
