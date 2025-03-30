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
use thiserror::Error;
use tokio::sync::Mutex;

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

#[derive(Default)]
pub struct Cache {
    // Synced to DAG tip
    synced: AtomicBool,

    last_known_chain_block: Mutex<Option<Hash>>,

    tip_timestamp: AtomicU64,

    pub blocks: DashMap<Hash, CacheBlock>,
    transactions: DashMap<CacheTransactionId, CacheTransaction>,
    accepting_block_transactions: DashMap<Hash, Vec<CacheTransactionId>>,
}

impl Cache {
    pub async fn set_last_known_chain_block(&self, hash: Hash) {
        *self.last_known_chain_block.lock().await = Some(hash)
    }

    pub async fn last_known_chain_block(&self) -> Option<Hash> {
        *self.last_known_chain_block.lock().await
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

        // Add transaction to map
        // If already in map, add block hash to CacheTransaction.blocks vec
        self.transactions
            .entry(tx_id)
            .and_modify(|entry| entry.blocks.push(block_hash))
            .or_insert(CacheTransaction::from(transaction.clone()));
    }

    pub fn add_block(&self, block: &RpcBlock) -> bool {
        // Add block to map if it does not yet exist
        // Otherwise return, to prevent seconds map fields from increasing on duplicate data
        match self.blocks.entry(block.header.hash) {
            dashmap::Entry::Occupied(_) => {
                return false;
            }
            dashmap::Entry::Vacant(e) => {
                e.insert(CacheBlock::from(block.clone()));
            }
        }

        // Process transactions in the block
        for tx in block.transactions.iter() {
            self.add_transaction(tx);
        }

        true
    }

    fn remove_transaction_acceptance(&self, transaction_id: Hash) {
        // Remove former accepting block hash from transaction
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = None);
    }

    pub fn remove_chain_block(&self, removed_chain_block: Hash) -> bool {
        // Temporary while working thru bug...
        {
            let removed_chain_block_data = match self.blocks.get(&removed_chain_block) {
                Some(block) => block,
                None => {
                    warn!(
                        "Removed chain block {} not found in blocks cache",
                        removed_chain_block
                    );
                    return false;
                }
            };

            // TODO I think since removed_chain_blocks are returned in high to low order,
            // there are situations where removed_chain_block is not in
            // accepting_block_transactions map yet
            if removed_chain_block_data.timestamp > self.tip_timestamp() {
                info!(
                    "Removed chain block {} timestamp greater than tip_timestamp",
                    removed_chain_block
                );
                return false;
            }
        }

        // Set block's chain block status to false
        self.blocks
            .entry(removed_chain_block)
            .and_modify(|v| v.is_chain_block = false);

        // Remove former chain blocks accepted transactions from accepting_block_transactions map
        if let Some((_, removed_transactions)) = self
            .accepting_block_transactions
            .remove(&removed_chain_block)
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

        true
    }

    fn add_transaction_acceptance(&self, transaction_id: Hash, accepting_block_hash: Hash) {
        // Set transactions accepting block hash
        self.transactions
            .entry(transaction_id)
            .and_modify(|v| v.accepting_block_hash = Some(accepting_block_hash));
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
    pub fn prune(&self) {
        // TODO refactor this

        // TODO better handling of window size
        // Currently 1 min of DAG data
        let window = 60 * 1000;
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

        Ok(Cache {
            synced: AtomicBool::new(false),
            last_known_chain_block: Mutex::new(Some(last_known_chain_block)),
            tip_timestamp: AtomicU64::new(tip_timestamp as u64),
            blocks,
            transactions,
            accepting_block_transactions,
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

        Ok(())
    }
}
