use super::model::*;
use super::second::SecondMetrics;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use kaspa_hashes::Hash;
use kaspalytics_utils::{config::Config, log::LogTarget};
use log::info;
use rocksdb::DB;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Default)]
pub struct DagCache {
    synced: AtomicBool,
    last_known_chain_block: Mutex<Option<Hash>>,
    tip_timestamp: AtomicU64,
    pub blocks: DashMap<Hash, CacheBlock>,
    pub transactions: DashMap<CacheTransactionId, CacheTransaction>,
    pub accepting_block_transactions: DashMap<Hash, Vec<CacheTransactionId>>,
    pub seconds: DashMap<u64, SecondMetrics>,
}

pub trait Writer {
    fn set_last_known_chain_block(&self, hash: Hash);

    fn set_synced(&self, state: bool);

    fn set_tip_timestamp(&self, timestamp: u64);

    fn prune(&self) -> Vec<PrunedBlock>;
}

impl Writer for DagCache {
    fn set_last_known_chain_block(&self, hash: Hash) {
        *self.last_known_chain_block.lock().unwrap() = Some(hash)
    }

    fn set_synced(&self, state: bool) {
        self.synced.store(state, Ordering::Relaxed);
    }

    fn set_tip_timestamp(&self, timestamp: u64) {
        self.tip_timestamp.store(timestamp, Ordering::Relaxed);
    }

    fn prune(&self) -> Vec<PrunedBlock> {
        let prune_timestamp = self.tip_timestamp() - 60 * 1000;

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

pub trait Reader {
    fn last_known_chain_block(&self) -> Option<Hash>;

    fn synced(&self) -> bool;

    fn tip_timestamp(&self) -> u64;

    fn contains_block_hash(&self, block_hash: &Hash) -> bool;

    fn blocks_iter(&self) -> impl Iterator<Item = RefMulti<'_, Hash, CacheBlock>>;

    fn seconds_iter(&self) -> impl Iterator<Item = RefMulti<'_, u64, SecondMetrics>>;

    fn log_cache_size(&self);
}

impl Reader for DagCache {
    fn last_known_chain_block(&self) -> Option<Hash> {
        *self.last_known_chain_block.lock().unwrap()
    }

    fn synced(&self) -> bool {
        self.synced.load(Ordering::Relaxed)
    }

    fn tip_timestamp(&self) -> u64 {
        self.tip_timestamp.load(Ordering::Relaxed)
    }

    fn contains_block_hash(&self, block_hash: &Hash) -> bool {
        self.blocks.contains_key(block_hash)
    }

    fn blocks_iter(&self) -> impl Iterator<Item = RefMulti<'_, Hash, CacheBlock>> {
        self.blocks.iter()
    }

    fn seconds_iter(&self) -> impl Iterator<Item = RefMulti<'_, u64, SecondMetrics>> {
        self.seconds.iter()
    }

    fn log_cache_size(&self) {
        // Estimate blocks size
        let estimated_blocks_size = self
            .blocks
            .iter()
            .map(|entry| std::mem::size_of::<Hash>() + entry.value().estimate_size())
            .sum::<usize>();

        // Estimate transactions size
        let estimated_transactions_size = self
            .transactions
            .iter()
            .map(|entry| std::mem::size_of::<CacheTransactionId>() + entry.value().estimate_size())
            .sum::<usize>();

        // Estimate accepting_block_transactions size
        let estimated_accepting_size = self
            .accepting_block_transactions
            .iter()
            .map(|entry| {
                std::mem::size_of::<Hash>() + // key size
                std::mem::size_of::<Vec<CacheTransactionId>>() +
                (entry.value().len() * std::mem::size_of::<CacheTransactionId>())
            })
            .sum::<usize>();

        // Estimate seconds size
        // TODO

        let total_size =
            estimated_blocks_size + estimated_transactions_size + estimated_accepting_size;

        info!(
            target: LogTarget::Daemon.as_str(),
            "DagCache est memory size: {:.2} MB | {} Blocks ({:.2} MB) | {} Transactions ({:.2} MB) | {} Accepting Blocks ({:.2} MB)",
            total_size as f64 / (1024.0 * 1024.0),
            self.blocks.len(),
            estimated_blocks_size as f64 / (1024.0 * 1024.0),
            self.transactions.len(),
            estimated_transactions_size as f64 / (1024.0 * 1024.0),
            self.accepting_block_transactions.len(),
            estimated_accepting_size as f64 / (1024.0 * 1024.0),
        );
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

impl DagCache {
    pub async fn load_cache_state(config: Config) -> Result<DagCache, CacheStateError> {
        // TODO refactor store and load function to better handle DB
        info!(target: LogTarget::Daemon.as_str(), "Loading cache state...");

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

        Ok(DagCache {
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
        info!(target: LogTarget::Daemon.as_str(), "Storing cache state... ");

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

        info!(target: LogTarget::Daemon.as_str(), "Storing cache state complete");

        Ok(())
    }
}
