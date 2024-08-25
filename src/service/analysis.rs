use chrono::{DateTime, NaiveDate};
use kaspa_consensus::consensus::storage::ConsensusStorage;
use kaspa_consensus::model::stores::acceptance_data::AcceptanceDataStoreReader;
use kaspa_consensus::model::stores::block_transactions::BlockTransactionsStoreReader;
use kaspa_consensus::model::stores::headers::HeaderStoreReader;
use kaspa_consensus::model::stores::utxo_diffs::UtxoDiffsStoreReader;
use kaspa_consensus_core::tx::{TransactionId, TransactionOutpoint, UtxoEntry};
use kaspa_consensus_core::utxo::utxo_diff::ImmutableUtxoDiff;
use kaspa_consensus_core::Hash;
use std::sync::Arc;

pub struct Analysis {
    storage: Arc<ConsensusStorage>,
}

impl Analysis {
    pub fn new(storage: Arc<ConsensusStorage>) -> Self {
        Self { storage }
    }

    pub fn run(&self) {
        let mut count = 0;

        let mut chain_block_map = std::collections::BTreeMap::<u64, Hash>::new();

        // Load chain block hashes into ordered map
        for (key, chain_block_hash) in self
            .storage
            .selected_chain_store
            .read()
            .access_hash_by_index
            .iterator()
            .map(|p| p.unwrap())
        {
            count += 1;
            let key = u64::from_le_bytes((*key).try_into().unwrap());
            chain_block_map.insert(key, chain_block_hash);
        }

        println!("{}", count);
        println!("{}", chain_block_map.len());

        let mut block_date_map = std::collections::HashMap::<NaiveDate, u64>::new();
        let mut tx_date_map = std::collections::HashMap::<NaiveDate, u64>::new();
        let mut fees_date_map = std::collections::HashMap::<NaiveDate, u64>::new();

        count = 0;

        let mut processed_transactions = std::collections::HashSet::<TransactionId>::new();

        // Iterate over ordered chain block map
        let max_key = chain_block_map.iter().next().unwrap().0 - 1;
        for (idx, (key, hash)) in chain_block_map.iter().enumerate() {
            if idx < 2048 {
                continue;
            }

            if key <= &max_key {
                panic!()
            }

            // Get block header and extract date from timestamp
            let header = self.storage.headers_store.get_header(hash.clone()).unwrap();
            let datetime = DateTime::from_timestamp_millis(header.timestamp as i64)
                .expect("Invalid timestamp");
            let date = datetime.date_naive();

            // Get acceptance data
            let acceptances = self
                .storage
                .acceptance_data_store
                .get(hash.clone())
                .unwrap();
            for obj in acceptances.iter() {
                block_date_map
                    .entry(date)
                    .and_modify(|e| *e += 1) // If the key exists, add the value
                    .or_insert(1);

                tx_date_map
                    .entry(date)
                    .and_modify(|e| *e += obj.accepted_transactions.len() as u64) // If the key exists, add the value
                    .or_insert(obj.accepted_transactions.len() as u64);
            }

            // For analyzing fees
            let utxo_diffs = self.storage.utxo_diffs_store.get(hash.clone()).unwrap();
            let mut utxos = std::collections::HashMap::<TransactionOutpoint, UtxoEntry>::new();
            for (outpoint, utxo) in utxo_diffs.removed().iter() {
                utxos.insert(outpoint.clone(), utxo.clone());
            }
            for (outpoint, utxo) in utxo_diffs.added().iter() {
                utxos.insert(outpoint.clone(), utxo.clone());
            }

            let mut fees = 0;
            for obj in acceptances.iter() {
                let block_body = self.storage.block_transactions_store.get(obj.block_hash);
                if block_body.is_err() {
                    continue;
                }

                for (idx, tx) in block_body.unwrap().iter().enumerate() {
                    // Skip coinbase transactions
                    if idx == 0 {
                        continue;
                    }

                    // Skip transactions we already processed
                    // This is a lazy approach to handle when a TX is in mutliple blocks, and those blocks are not in same DAA
                    if processed_transactions.contains(&tx.id()) {
                        continue;
                    }

                    for input in tx.inputs.iter() {
                        let previous_outpoint = utxos.get(&input.previous_outpoint).unwrap();
                        fees += previous_outpoint.amount;
                    }

                    for output in tx.outputs.iter() {
                        fees -= output.value;
                    }

                    processed_transactions.insert(tx.id());
                }
            }

            fees_date_map
                .entry(date)
                .and_modify(|e| *e += fees) // If the key exists, add the value
                .or_insert(fees);
        }

        for (date, value) in &block_date_map {
            println!("Blocks | Date: {}, Value: {}", date, value);
        }

        for (date, value) in &tx_date_map {
            println!("Txs | Date: {}, Value: {}", date, value);
        }

        for (date, value) in &fees_date_map {
            println!("Fees | Date: {}, Value: {}", date, value / 100_000_000);
        }
    }
}
