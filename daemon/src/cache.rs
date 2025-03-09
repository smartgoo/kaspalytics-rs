use chrono::Utc;
use dashmap::DashMap;
use kaspa_hashes::Hash;
use kaspa_rpc_core::{
    RpcBlock, RpcSubnetworkId, RpcTransaction, RpcTransactionId, RpcTransactionInput,
    RpcTransactionOutput,
};
use log::info;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct CacheBlock {
    pub hash: Hash,

    // RpcBlockHeader fields
    pub timestamp: u64,
    pub daa_score: u64,

    pub transactions: Vec<RpcTransactionId>,

    // RpcBlockVerboseData
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

#[allow(dead_code)]
#[derive(Serialize, Deserialize)]
pub struct CacheTransaction {
    pub id: RpcTransactionId,
    pub inputs: Vec<RpcTransactionInput>,
    pub outputs: Vec<RpcTransactionOutput>,
    // lock_time: u64,
    pub subnetwork_id: RpcSubnetworkId,
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
            inputs: value.inputs,
            outputs: value.outputs,
            // lock_time: value.lock_time,
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

    low_hash: RwLock<Option<Hash>>,

    pub tip_timestamp: AtomicU64,

    pub blocks: DashMap<Hash, CacheBlock>,
    pub transactions: DashMap<RpcTransactionId, CacheTransaction>,
    pub accepting_block_transactions: DashMap<Hash, Vec<RpcTransactionId>>,

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
}

impl Cache {
    pub fn log_size(&self) {
        info!(
            "tip_timestamp: {} | blocks: {} | transactions {} | accepting_blocks_transactions {} | per_second {}",
            self.tip_timestamp.load(Ordering::SeqCst) / 1000,
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
        let pruning_timestamp = self.tip_timestamp.fetch_sub(window, Ordering::SeqCst) - window;

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
        let threshold = now - 86400 * 2;
        self.per_second.retain(|second, _| *second > threshold);
    }
}

impl Cache {
    // pub async fn load_cache_state_to_cache(pg_pool: &PgPool) -> Result<Cache, sqlx::Error> {
    //     Ok(())
    // }

    pub async fn store_cache_state(&self, pg_pool: &PgPool) -> Result<(), sqlx::Error> {
        info!("Storing cache state... ");

        // Store synced status
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_bool", updated_timestamp)
            VALUES('synced', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_bool" = $1, updated_timestamp = $2
            "#,
        )
        .bind(self.synced())
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        // Store low_hash
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_char", updated_timestamp)
            VALUES('low_hash', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_char" = $1, updated_timestamp = $2
            "#,
        )
        .bind(self.low_hash().await.unwrap().to_string())
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        // Insert tip_timestamp to db
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_int", updated_timestamp)
            VALUES('tip_timestamp', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_int" = $1, updated_timestamp = $2
            "#,
        )
        .bind(self.tip_timestamp.load(Ordering::SeqCst) as i64)
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        // Insert blocks to db
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_bytea", updated_timestamp)
            VALUES('blocks', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_bytea" = $1, updated_timestamp = $2
            "#,
        )
        .bind(bincode::serialize(&self.blocks).unwrap())
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        // Insert transactions to db
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_bytea", updated_timestamp)
            VALUES('transactions', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_bytea" = $1, updated_timestamp = $2
            "#,
        )
        .bind(bincode::serialize(&self.transactions).unwrap())
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        // Insert accepting_block_transactions to db
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_bytea", updated_timestamp)
            VALUES('accepting_block_transactions', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_bytea" = $1, updated_timestamp = $2
            "#,
        )
        .bind(bincode::serialize(&self.accepting_block_transactions).unwrap())
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        // Insert per_second to db
        sqlx::query(
            r#"INSERT INTO cache_state ("key", "value_bytea", updated_timestamp)
            VALUES('per_second', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value_bytea" = $1, updated_timestamp = $2
            "#,
        )
        .bind(bincode::serialize(&self.per_second).unwrap())
        .bind(Utc::now())
        .execute(pg_pool)
        .await?;

        Ok(())
    }
}
