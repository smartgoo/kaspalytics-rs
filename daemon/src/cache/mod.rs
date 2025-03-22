pub mod model;
pub mod per_second;

use dashmap::DashMap;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcAcceptedTransactionIds, RpcBlock, RpcTransaction};
use kaspalytics_utils::config::Config;
use log::{info, warn};
use model::*;
use per_second::*;
use rocksdb::DB;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Default)]
pub struct Cache {
    // Synced to DAG tip
    synced: AtomicBool,

    last_known_chain_block: Mutex<Option<Hash>>,

    tip_timestamp: AtomicU64,

    pub blocks: DashMap<Hash, CacheBlock>,
    pub transactions: DashMap<CacheTransactionId, CacheTransaction>,
    pub accepting_block_transactions: DashMap<Hash, Vec<CacheTransactionId>>,

    pub seconds: DashMap<u64, SecondMetrics>,
    // pruning_point
    // pruning_point_timestamp
}

impl Cache {
    pub async fn set_last_known_chain_block(&self, hash: Hash) {
        *self.last_known_chain_block.lock().await = Some(hash)
    }

    pub async fn last_known_chain_block(&self) -> Option<Hash> {
        *self.last_known_chain_block.lock().await
    }

    pub fn set_synced(&self, state: bool) {
        self.synced.store(state, Ordering::SeqCst);
    }

    pub fn synced(&self) -> bool {
        self.synced.load(Ordering::SeqCst)
    }

    pub fn set_tip_timestamp(&self, timestamp: u64) {
        self.tip_timestamp.store(timestamp, Ordering::SeqCst);
    }

    pub fn tip_timestamp(&self) -> u64 {
        self.tip_timestamp.load(Ordering::SeqCst)
    }

    fn add_transaction(&self, transaction: RpcTransaction) {
        let tx_id = transaction.verbose_data.as_ref().unwrap().transaction_id;
        let block_hash = transaction.verbose_data.as_ref().unwrap().block_hash;
        let block_time = transaction.verbose_data.as_ref().unwrap().block_time;

        // Add transaction to map
        // If already in map, add block hash to CacheTransaction.blocks vec
        self.transactions
            .entry(tx_id)
            .and_modify(|entry| entry.blocks.push(block_hash))
            .or_insert(CacheTransaction::from(transaction));

        // Add to seconds map
        // Always increase transaction count even if tx already in map
        // Effective tx count handled elsewhere
        self.seconds
            .entry(block_time / 1000)
            .and_modify(|v| v.add_transaction());
    }

    pub fn add_block(&self, block: RpcBlock) {
        // Add block to map if it does not yet exist
        // Otherwise return, to prevent seconds map fields from increasing on duplicate data
        match self.blocks.entry(block.header.hash) {
            dashmap::Entry::Occupied(_) => {
                return;
            }
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
        for tx in block.transactions {
            self.add_transaction(tx);
        }
    }

    fn remove_transaction_acceptance(&self, transaction_id: Hash) {
        // Remove former accepting block hash from transaction
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = None);

        let tx_timestamp = self.transactions.get(&transaction_id).unwrap().block_time;

        // Decrement per second effective tx count
        self.seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| v.remove_transaction_acceptance());
    }

    pub fn remove_chain_block(&self, removed_chain_block: Hash) {
        // Temporary while working thru bug...
        {
            let removed_chain_block_data = match self.blocks.get(&removed_chain_block) {
                Some(block) => {
                    info!(
                        "Removed chain block {} found in blocks cache, block timestamp {}",
                        removed_chain_block,
                        block.value().timestamp
                    );
                    block
                }
                None => {
                    warn!(
                        "Removed chain block {} not found in blocks cache",
                        removed_chain_block
                    );
                    return;
                }
            };

            // TODO find better way of handling.
            // I think since removed_chain_blocks are returned in high to low order,
            // there are situations where removed_chain_block is not in
            // accepting_block_transactions map yet
            if removed_chain_block_data.timestamp > self.tip_timestamp() {
                warn!(
                    "Removed chain block {} timestamp greater than tip_timestamp",
                    removed_chain_block
                );
                return;
            }
        }

        // Set block's chain block status to false
        self.blocks
            .entry(removed_chain_block)
            .and_modify(|v| v.is_chain_block = false);

        // Remove chain block and all accepted txs from map
        // let (_, removed_transactions) = self
        //     .accepting_block_transactions
        //     .remove(&removed_chain_block)
        //     .unwrap();

        if let Some((_, removed_transactions)) = self
            .accepting_block_transactions
            .remove(&removed_chain_block)
        {
            // Process each removed tx
            for tx_id in removed_transactions {
                self.remove_transaction_acceptance(tx_id);
            }
        } else {
            warn!(
                "Removed chain block {} does not exist in cache accepting_block_transactions map",
                removed_chain_block
            );
        }
    }

    fn add_transaction_acceptance(&self, transaction_id: Hash, accepting_block_hash: Hash) {
        // Set transactions accepting block hash
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = Some(accepting_block_hash));

        let tx_timestamp = self.transactions.get(&transaction_id).unwrap().block_time;

        // Increment effective transaction count
        self.seconds
            .entry(tx_timestamp / 1000)
            .and_modify(|v| v.add_transaction_acceptance());
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
    pub fn log_size(&self) {
        let tt = self.tip_timestamp() / 1000;
        let behind = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - tt;

        info!(
            "tip_timestamp: {} ({}s behind) | blocks: {} | transactions {} | accepting_blocks_transactions {} | per_second {}",
            tt,
            behind,
            self.blocks.len(),
            self.transactions.len(),
            self.accepting_block_transactions.len(),
            self.seconds.len(),
        );
    }
}

impl Cache {
    pub fn prune(&self) {
        // TODO refactor this

        // TODO better handling of window size
        // Currently targeting 5 mins of block data
        let window = 5 * 60 * 1000;
        // let pruning_timestamp = self.tip_timestamp.fetch_sub(window, Ordering::SeqCst) - window;
        let pruning_timestamp = self.tip_timestamp() - window;

        // Prune blocks
        let mut candidate_blocks: Vec<Hash> = vec![];
        for block in self.blocks.iter() {
            if block.timestamp < pruning_timestamp {
                candidate_blocks.push(*block.key());
            }
        }

        let mut candidate_txs = Vec::new();
        for hash in candidate_blocks {
            // Remove block from blocks cache
            let (_, removed_block) = self.blocks.remove(&hash).unwrap();

            // Remove removed block from CachedTransaction.blocks
            // Capture transactions that have no blocks in cache
            for tx in removed_block.transactions {
                if let Some(mut cached_tx) = self.transactions.get_mut(&tx) {
                    cached_tx.blocks.retain(|b| *b != hash);

                    if cached_tx.blocks.is_empty() {
                        candidate_txs.push(tx);
                    }
                }
            }

            // Remove transactions
            for tx in candidate_txs.iter() {
                self.transactions.remove(tx);
            }

            // Remove block from accepting_block_transaction
            self.accepting_block_transactions.remove(&hash);
        }

        // ------
        // Prune per second stats
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let threshold = now - (86_400f64 * 1.1) as u64;
        self.seconds.retain(|second, _| *second > threshold);
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
        info!("Attempting to load cache state...");

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

        // Get per_second map
        let per_second_bytes = db
            .get(b"per_second")?
            .ok_or(CacheStateError::MissingKeyError("per_second".to_string()))?;
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
            bincode::serialize(&self.last_known_chain_block().await.unwrap())?,
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

        // Insert per_second to db
        db.put(b"per_second", bincode::serialize(&self.seconds)?)?;

        Ok(())
    }
}
