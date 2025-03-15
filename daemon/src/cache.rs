use dashmap::DashMap;
use kaspa_consensus_core::subnets::SubnetworkId;
use kaspa_consensus_core::tx::{ScriptPublicKey, TransactionId};
use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    RpcAcceptedTransactionIds, RpcBlock, RpcTransaction, RpcTransactionInput,
    RpcTransactionOutpoint, RpcTransactionOutput,
};
use kaspa_txscript::script_class::ScriptClass;
use kaspalytics_utils::config::Config;
use log::{info, warn};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::RwLock;

pub type CacheTransactionId = TransactionId;
pub type CacheScriptPublicKey = ScriptPublicKey;
pub type CacheScriptClass = ScriptClass;

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct CacheBlock {
    pub hash: Hash,
    pub timestamp: u64,
    pub daa_score: u64,
    pub transactions: Vec<CacheTransactionId>,
    pub selected_parent_hash: Hash,
    pub is_chain_block: bool,
}

impl From<RpcBlock> for CacheBlock {
    fn from(value: RpcBlock) -> Self {
        CacheBlock {
            hash: value.header.hash,
            timestamp: value.header.timestamp,
            daa_score: value.header.daa_score,
            transactions: value
                .transactions
                .iter()
                .map(|tx| tx.verbose_data.clone().unwrap().transaction_id)
                .collect(),
            selected_parent_hash: value.verbose_data.clone().unwrap().selected_parent_hash,
            is_chain_block: value.verbose_data.unwrap().is_chain_block,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheTransactionOutput {
    pub value: u64,
    pub script_public_key: CacheScriptPublicKey,
    pub script_public_key_type: CacheScriptClass,
    pub script_public_key_address: String,
}

impl From<RpcTransactionOutput> for CacheTransactionOutput {
    fn from(value: RpcTransactionOutput) -> Self {
        Self {
            value: value.value,
            script_public_key: value.script_public_key,
            script_public_key_type: value.verbose_data.clone().unwrap().script_public_key_type,
            script_public_key_address: value
                .verbose_data
                .unwrap()
                .script_public_key_address
                .to_string(),
        }
    }
}

pub type CacheTransactionOutpoint = RpcTransactionOutpoint;

#[derive(Clone, Serialize, Deserialize)]
pub struct CacheTransactionInput {
    pub previous_outpoint: CacheTransactionOutpoint,
    pub signature_script: Vec<u8>,
    pub sequence: u64,
    pub sig_op_count: u8,
}

impl From<RpcTransactionInput> for CacheTransactionInput {
    fn from(value: RpcTransactionInput) -> Self {
        Self {
            previous_outpoint: value.previous_outpoint,
            signature_script: value.signature_script,
            sequence: value.sequence,
            sig_op_count: value.sig_op_count,
        }
    }
}

pub type CacheSubnetworkId = SubnetworkId;

// TODO clean up and standardize Rpc* vs. Cache*
#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct CacheTransaction {
    pub id: CacheTransactionId,
    pub inputs: Vec<CacheTransactionInput>,
    pub outputs: Vec<CacheTransactionOutput>,
    lock_time: u64,
    pub subnetwork_id: CacheSubnetworkId,
    pub gas: u64,
    pub payload: Vec<u8>,
    pub mass: u64,
    pub compute_mass: u64,
    // block_time: u64,
    pub blocks: Vec<Hash>,
    pub block_time: u64,
    pub accepting_block_hash: Option<Hash>,
}

impl From<RpcTransaction> for CacheTransaction {
    fn from(value: RpcTransaction) -> Self {
        CacheTransaction {
            id: value.verbose_data.clone().unwrap().transaction_id,
            inputs: value
                .inputs
                .iter()
                .map(|o| CacheTransactionInput::from(o.clone()))
                .collect(),
            outputs: value
                .outputs
                .iter()
                .map(|o| CacheTransactionOutput::from(o.clone()))
                .collect(),
            lock_time: value.lock_time,
            subnetwork_id: value.subnetwork_id,
            gas: value.gas,
            payload: value.payload,
            mass: value.mass,
            compute_mass: value.verbose_data.clone().unwrap().compute_mass,
            blocks: vec![value.verbose_data.clone().unwrap().block_hash],
            block_time: value.verbose_data.clone().unwrap().block_time,
            accepting_block_hash: None,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SecondMetrics {
    pub block_count: u64,
    pub transaction_count: u64,
    pub effective_transaction_count: u64,
}

#[derive(Default)]
pub struct Cache {
    // Synced to DAG tip
    synced: AtomicBool,

    low_hash: RwLock<Option<Hash>>, // use std

    tip_timestamp: AtomicU64,

    pub blocks: DashMap<Hash, CacheBlock>,
    pub transactions: DashMap<CacheTransactionId, CacheTransaction>,
    pub accepting_block_transactions: DashMap<Hash, Vec<CacheTransactionId>>,

    pub per_second: DashMap<u64, SecondMetrics>,
}

impl Cache {
    pub async fn set_low_hash(&self, hash: Hash) {
        let mut h = self.low_hash.write().await;
        *h = Some(hash);
    }

    pub async fn low_hash(&self) -> Option<Hash> {
        *self.low_hash.read().await
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

    fn insert_transaction(&self, transaction: RpcTransaction) {
        let tx_id = transaction.verbose_data.as_ref().unwrap().transaction_id;
        let block_hash = transaction.verbose_data.clone().unwrap().block_hash;
        let block_time = transaction.verbose_data.clone().unwrap().block_time;

        // Add transaction to map
        self.transactions
            .entry(tx_id)
            .and_modify(|entry| entry.blocks.push(block_hash))
            .or_insert(CacheTransaction::from(transaction.clone()));

        // Increment per second stats TOTAL (not effective) tx count
        self.per_second
            .entry(block_time / 1000)
            .and_modify(|ps| ps.transaction_count += 1);
    }

    pub fn insert_block(&self, block: RpcBlock) {
        // Add block to map
        self.blocks
            .insert(block.header.hash, CacheBlock::from(block.clone()));

        // Increment per second stats block count
        self.per_second
            .entry(block.header.timestamp / 1000)
            .and_modify(|v| v.block_count += 1)
            .or_default();

        // Process transactions in the block
        for tx in block.transactions.iter() {
            self.insert_transaction(tx.clone());
        }
    }

    fn remove_transaction_acceptance(&self, transaction_id: Hash) {
        // Remove former accepting block hash from transaction
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = None);

        let tx_timestamp = self.transactions.get(&transaction_id).unwrap().block_time / 1000;

        // Decrement per second effective tx count
        self.per_second
            .entry(tx_timestamp)
            .and_modify(|v| v.effective_transaction_count -= 1);
    }

    pub fn remove_chain_block(&self, removed_chain_block: Hash) {
        // Temporary while working thru bug...
        match self.blocks.get(&removed_chain_block) {
            Some(block) => {
                info!(
                    "Removed chain block {} found in blocks cache, block timestamp {}",
                    removed_chain_block,
                    block.value().timestamp
                );
            }
            None => {
                warn!(
                    "Removed chain block {} not found in blocks cache",
                    removed_chain_block
                );
            }
        }

        // Set block's chain block status to false
        self.blocks
            .entry(removed_chain_block)
            .and_modify(|v| v.is_chain_block = false);

        // Remove chain block and all accepted txs from map
        let (_, removed_transactions) = self
            .accepting_block_transactions
            .remove(&removed_chain_block)
            .unwrap();

        // Process each removed tx
        for tx_id in removed_transactions {
            self.remove_transaction_acceptance(tx_id);
        }
    }

    fn add_transaction_acceptance(&self, transaction_id: Hash, accepting_block_hash: Hash) {
        // Set transactions accepting block hash
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = Some(accepting_block_hash));

        let tx_timestamp = self.transactions.get(&transaction_id).unwrap().block_time / 1000;

        // Increment effective transaction count
        self.per_second
            .entry(tx_timestamp)
            .and_modify(|v| v.effective_transaction_count += 1);
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
        info!(
            "tip_timestamp: {} | blocks: {} | transactions {} | accepting_blocks_transactions {} | per_second {}",
            self.tip_timestamp() / 1000,
            self.blocks.len(),
            self.transactions.len(),
            self.accepting_block_transactions.len(),
            self.per_second.len(),
        );
    }
}

impl Cache {
    pub fn prune(&self) {
        // TODO refactor this

        // TODO better handling of window size
        // Currently targeting 1 hour of blocks
        let window = 3600 * 1000;
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
        let threshold = now - (86400f64 * 1.1) as u64;
        self.per_second.retain(|second, _| *second > threshold);
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

        let db = DB::open_default(config.kaspalytics_dir.join("cache_state"))?;

        // Get low_hash
        let low_hash_bytes = db
            .get(b"low_hash")?
            .ok_or(CacheStateError::MissingKeyError("low_hash".to_string()))?;
        let low_hash = bincode::deserialize::<Hash>(&low_hash_bytes)?;

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
        let per_second = bincode::deserialize::<DashMap<u64, SecondMetrics>>(&per_second_bytes)?;

        Ok(Cache {
            synced: AtomicBool::new(false),
            low_hash: RwLock::new(Some(low_hash)),
            tip_timestamp: AtomicU64::new(tip_timestamp as u64),
            blocks,
            transactions,
            accepting_block_transactions,
            per_second,
        })
    }

    pub async fn store_cache_state(&self, config: Config) -> Result<(), CacheStateError> {
        info!("Storing cache state... ");

        let db = DB::open_default(config.kaspalytics_dir.join("cache_state"))?;

        // Store low_hash
        db.put(
            b"low_hash",
            bincode::serialize(&self.low_hash().await.unwrap())?,
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
        db.put(b"per_second", bincode::serialize(&self.per_second)?)?;

        Ok(())
    }
}
