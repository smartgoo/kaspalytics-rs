pub mod model;
pub mod second;

use dashmap::DashMap;
use kaspa_consensus_core::blockhash::BlockHashExtensions;
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcAcceptedTransactionIds, RpcBlock, RpcTransaction};
use kaspalytics_utils::config::Config;
use log::{info, warn};
use model::*;
use rocksdb::DB;
use second::SecondMetrics;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Default)]
pub struct Cache {
    synced: AtomicBool,

    last_known_chain_block: Mutex<Option<Hash>>,

    tip_timestamp: AtomicU64,

    pub blocks: DashMap<Hash, CacheBlock>,
    transactions: DashMap<CacheTransactionId, CacheTransaction>,
    accepting_block_transactions: DashMap<Hash, Vec<CacheTransactionId>>,

    pub seconds: DashMap<u64, SecondMetrics>,
}

impl Cache {
    pub fn set_last_known_chain_block(&self, hash: Hash) {
        *self.last_known_chain_block.lock().unwrap() = Some(hash)
    }

    pub fn last_known_chain_block(&self) -> Option<Hash> {
        *self.last_known_chain_block.lock().unwrap()
    }

    pub fn set_synced(&self, state: bool) {
        self.synced.store(state, Ordering::Relaxed);
    }

    pub fn synced(&self) -> bool {
        self.synced.load(Ordering::Relaxed)
    }

    pub fn set_tip_timestamp(&self, timestamp: u64) {
        self.tip_timestamp.store(timestamp, Ordering::Relaxed);
    }

    pub fn tip_timestamp(&self) -> u64 {
        self.tip_timestamp.load(Ordering::Relaxed)
    }

    fn add_transaction(&self, transaction: &RpcTransaction) {
        let tx_id = transaction.verbose_data.as_ref().unwrap().transaction_id;
        let block_hash = transaction.verbose_data.as_ref().unwrap().block_hash;
        let block_time = transaction.verbose_data.as_ref().unwrap().block_time;

        // Add transaction to map
        // If already in map, add block hash to CacheTransaction.blocks vec
        self.transactions
            .entry(tx_id)
            .and_modify(|entry| entry.blocks.push(block_hash))
            .or_insert(CacheTransaction::from(transaction.clone()));

        // Add to seconds map
        // Always increase transaction count even if tx already in map
        // Since effective tx count handled elsewhere
        self.seconds
            .entry(block_time / 1000)
            .and_modify(|v| v.increment_trnasaction_count());
    }

    pub fn add_block(&self, block: &RpcBlock) {
        // Add block to map if it does not yet exist
        // Otherwise return, to prevent seconds map fields from increasing on duplicate data
        match self.blocks.entry(block.header.hash) {
            dashmap::Entry::Occupied(_) => return,
            dashmap::Entry::Vacant(e) => {
                e.insert(CacheBlock::from(block.clone()));
            }
        }

        // Increment per second stats block count
        self.seconds
            .entry(block.header.timestamp / 1000)
            .or_default()
            .add_block(block.transactions[0].payload.clone());

        // Process transactions in the block
        for tx in block.transactions.iter() {
            self.add_transaction(tx);
        }
    }

    fn remove_transaction_acceptance(&self, transaction_id: Hash) {
        // Remove former accepting block hash from transaction
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = None);

        let tx = self.transactions.get(&transaction_id).unwrap();

        let tx_timestamp = tx.block_time;

        // Decrement effective transaction counts
        if tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
            self.seconds
                .entry(tx_timestamp / 1000)
                .and_modify(|v| v.decrement_effective_coinbase_transaction());
        } else {
            self.seconds
                .entry(tx_timestamp / 1000)
                .and_modify(|v| v.decrement_effective_non_coinbase_transaction());
        };
    }

    pub fn remove_chain_block(&self, removed_chain_block: &Hash) {
        // Temporary while working thru bug...
        {
            let removed_chain_block_data = self.blocks.get(removed_chain_block);

            if removed_chain_block.is_none() {
                warn!(
                    "Removed chain block {} not found in blocks cache",
                    removed_chain_block
                );
                return;
            }

            // TODO I think since removed_chain_blocks are returned in high to low order,
            // there are situations where removed_chain_block is not in
            // accepting_block_transactions map yet
            if removed_chain_block_data.unwrap().timestamp > self.tip_timestamp() {
                info!(
                    "Removed chain block {} timestamp greater than tip_timestamp",
                    removed_chain_block
                );
            }
        }

        // Set block's chain block status to false
        self.blocks
            .entry(*removed_chain_block)
            .and_modify(|v| v.is_chain_block = false);

        // Remove former chain blocks accepted transactions from accepting_block_transactions map
        if let Some((_, removed_transactions)) = self
            .accepting_block_transactions
            .remove(removed_chain_block)
        {
            for tx_id in removed_transactions {
                self.remove_transaction_acceptance(tx_id);
            }
        } else {
            warn!(
                "Removed chain block {} is below cache tip timestamp, and does not exist in cache accepting_block_transactions map",
                removed_chain_block
            );
        }
    }

    fn add_transaction_acceptance(&self, transaction_id: Hash, accepting_block_hash: Hash) {
        // Set transactions accepting block hash
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = Some(accepting_block_hash));

        let tx = self.transactions.get(&transaction_id).unwrap();
        let tx_timestamp = tx.block_time;

        // Increment effective transaction counts
        if tx.subnetwork_id == SUBNETWORK_ID_COINBASE {
            self.seconds
                .entry(tx_timestamp / 1000)
                .and_modify(|v| v.increment_effective_coinbase_transaction());
        } else {
            self.seconds
                .entry(tx_timestamp / 1000)
                .and_modify(|v| v.increment_effective_non_coinbase_transaction());
        };
    }

    pub fn add_chain_block_acceptance_data(&self, acceptance_data: RpcAcceptedTransactionIds) {
        // Set block's chain block status to true
        self.blocks
            .entry(acceptance_data.accepting_block_hash)
            .and_modify(|v| v.is_chain_block = true);

        // Add chain block and it's accepted transactions
        self.accepting_block_transactions.insert(
            acceptance_data.accepting_block_hash,
            acceptance_data.accepted_transaction_ids.clone(),
        );

        // Process transactions
        for tx_id in acceptance_data.accepted_transaction_ids {
            self.add_transaction_acceptance(tx_id, acceptance_data.accepting_block_hash);
        }
    }
}

impl Cache {
    pub fn prune(&self) -> Vec<PrunedBlock> {
        let prune_timestamp = self.tip_timestamp() - 30 * 1000;

        // Identify blocks older than pruning timestamp
        // Store these block hashes
        let mut candidate_blocks: Vec<Hash> = vec![];
        for block in self.blocks.iter() {
            if block.timestamp < prune_timestamp {
                candidate_blocks.push(*block.key());
            }
        }

        // Var to store pruned blocks
        // That will later be returned at end of function
        let mut pruned_blocks = Vec::new();

        for block_hash in candidate_blocks {
            // Remove block from blocks cache
            let (_, removed_block) = self.blocks.remove(&block_hash).unwrap();

            // Var to store current block's transactions
            // Since all transactions are not removed from cache until all containing blocks are removed,
            // need to clone all transactions into this var
            let mut transactions = Vec::new();

            // Var to store transactions in this block that no longer have any containing blocks
            // These transactions can be fully removed from cache at end
            let mut transactions_to_remove = Vec::new();

            for tx_id in removed_block.transactions {
                let mut cache_tx = self.transactions.get_mut(&tx_id).unwrap();
                transactions.push(cache_tx.value().clone());

                cache_tx.blocks.retain(|b| *b != block_hash);
                if cache_tx.blocks.is_empty() {
                    transactions_to_remove.push(tx_id);
                }
            }

            // Remove transactions whose containing blocks are no longer in cache
            for tx_id in transactions_to_remove.into_iter() {
                self.transactions.remove(&tx_id).unwrap();
            }

            // Remove block from accepting_block_transaction
            self.accepting_block_transactions.remove(&block_hash);

            pruned_blocks.push(PrunedBlock {
                hash: removed_block.hash,
                timestamp: removed_block.timestamp,
                daa_score: removed_block.daa_score,
                transactions,
            });
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let threshold = now - (86_400f64 * 1.1) as u64;
        self.seconds.retain(|second, _| *second > threshold);

        pruned_blocks
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum CacheStateError {
    #[error("{0}")]
    DbError(#[from] rocksdb::Error),

    #[error("{0}")]
    DeserializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Missing data at {0}")]
    MissingKeyError(String),
}

impl Cache {
    pub async fn load_cache_state(config: Config) -> Result<Cache, CacheStateError> {
        // TODO refactor store and load function to better handle DB
        info!("Loading cache state...");

        let db = DB::open_default(config.kaspalytics_dirs.cache_dir)?;

        // Get vspc_low_hash
        let last_known_chain_block_bytes = db
            .get(b"last_known_chain_block")?
            .ok_or(CacheStateError::MissingKeyError("low_hash".to_string()))?;
        let last_known_chain_block = bincode::deserialize::<Hash>(&last_known_chain_block_bytes)?;

        // Get tip_timestamp
        let tip_timestamp_bytes =
            db.get(b"tip_timestamp")?
                .ok_or(CacheStateError::MissingKeyError(
                    "tip_timestamp".to_string(),
                ))?;
        let tip_timestamp = bincode::deserialize::<u64>(&tip_timestamp_bytes)?;

        // Get blocks map
        let blocks_bytes = db
            .get(b"blocks")?
            .ok_or(CacheStateError::MissingKeyError("blocks".to_string()))?;
        let blocks = bincode::deserialize::<DashMap<Hash, CacheBlock>>(&blocks_bytes)?;

        // Get transactions map
        let transactions_bytes = db
            .get(b"transactions")?
            .ok_or(CacheStateError::MissingKeyError("transactions".to_string()))?;
        let transactions = bincode::deserialize::<DashMap<CacheTransactionId, CacheTransaction>>(
            &transactions_bytes,
        )?;

        // Get accepting_block_transactions map
        let accepting_block_transactions_bytes =
            db.get(b"accepting_block_transactions")?
                .ok_or(CacheStateError::MissingKeyError(
                    "accepting_block_transactions".to_string(),
                ))?;
        let accepting_block_transactions = bincode::deserialize::<
            DashMap<Hash, Vec<CacheTransactionId>>,
        >(&accepting_block_transactions_bytes)?;

        // Get seconds map
        let per_second_bytes = db
            .get(b"seconds")?
            .ok_or(CacheStateError::MissingKeyError("seconds".to_string()))?;
        let seconds = bincode::deserialize::<DashMap<u64, SecondMetrics>>(&per_second_bytes)?;

        Ok(Cache {
            synced: AtomicBool::new(false),
            last_known_chain_block: Mutex::new(Some(last_known_chain_block)),
            tip_timestamp: AtomicU64::new(tip_timestamp as u64),
            blocks,
            transactions,
            accepting_block_transactions,
            seconds,
        })
    }

    pub async fn store_cache_state(&self, config: Config) -> Result<(), CacheStateError> {
        info!("Storing cache state... ");

        let db = DB::open_default(config.kaspalytics_dirs.cache_dir)?;

        // Store vspc_low_hash
        db.put(
            b"last_known_chain_block",
            bincode::serialize(&self.last_known_chain_block().unwrap())?,
        )?;

        // Insert tip_timestamp to db
        db.put(b"tip_timestamp", bincode::serialize(&self.tip_timestamp())?)?;

        // Insert blocks to db
        db.put(b"blocks", bincode::serialize(&self.blocks)?)?;

        // Insert transactions to db
        db.put(b"transactions", bincode::serialize(&self.transactions)?)?;

        // Insert accepting_block_transactions to db
        db.put(
            b"accepting_block_transactions",
            bincode::serialize(&self.accepting_block_transactions)?,
        )?;

        // Insert seconds to db
        db.put(b"seconds", bincode::serialize(&self.seconds)?)?;

        Ok(())
    }
}
