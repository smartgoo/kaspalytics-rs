use crate::cache::Cache;
use chrono::Utc;
use kaspa_hashes::Hash;
use log::{debug, info};
use sqlx::{self, PgPool};
use std::collections::HashMap;
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[derive(thiserror::Error, Debug)]
pub enum PayloadParseError {
    #[error("First byte 0xaa indicates address payload")]
    InvalidFirstByte,

    #[error("Payload split error")]
    SplitError,
}

#[derive(thiserror::Error, Debug)]
pub enum BlockMinerProcessError {
    #[error("{0}")]
    DbError(#[from] sqlx::Error),

    #[error("Unable to get coinbase transaction {0} for block {1} from cache")]
    MissingCoinbaseTransaction(String, String),

    #[error("{0}")]
    PayloadParseError(#[from] PayloadParseError),
}

fn parse_payload_node_version(payload: Vec<u8>) -> Result<String, PayloadParseError> {
    // let mut version = payload[16];
    let length = payload[18];
    let script = &payload[19_usize..(19 + length as usize)];

    if script[0] == 0xaa {
        return Err(PayloadParseError::InvalidFirstByte);
    }
    // if script[0] < 0x76 { ... }

    let payload_str = payload[19_usize + (length as usize)..]
        .iter()
        .map(|&b| b as char)
        .collect::<String>();

    let node_version = &payload_str
        .split("/")
        .next()
        .ok_or(PayloadParseError::SplitError)?;

    Ok(String::from(*node_version))
}

#[allow(dead_code)]
struct BlockMiner {
    hash: Hash,
    timestamp: u64,
    node_version: String,
    // address: RpcAddress,
    // miner
}

impl BlockMiner {
    fn try_new(hash: Hash, timestamp: u64, payload: Vec<u8>) -> Result<Self, PayloadParseError> {
        let node_version = parse_payload_node_version(payload)?;
        Ok(Self {
            hash,
            timestamp,
            node_version,
        })
    }
}

pub struct Analyzer {
    cache: Arc<Cache>,
    pg_pool: PgPool,
}

impl Analyzer {
    pub fn new(cache: Arc<Cache>, pg_pool: PgPool) -> Self {
        Analyzer { cache, pg_pool }
    }
}

impl Analyzer {
    async fn rolling_tx_count(&self) -> Result<(), sqlx::Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let threshold = now - 86400;

        let effective_count: u64 = self
            .cache
            .per_second
            .iter()
            .filter(|entry| *entry.key() >= threshold)
            .map(|entry| entry.effective_transaction_count)
            .sum();

        let count: u64 = self
            .cache
            .per_second
            .iter()
            .filter(|entry| *entry.key() >= threshold)
            .map(|entry| entry.transaction_count)
            .sum();

        sqlx::query(
            r#"
            INSERT INTO key_value ("key", "value", updated_timestamp)
            VALUES('transaction_count_24h', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value" = $1, updated_timestamp = $2
            "#,
        )
        .bind(count as i64)
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO key_value ("key", "value", updated_timestamp)
            VALUES('effective_transaction_count_24h', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value" = $1, updated_timestamp = $2
            "#,
        )
        .bind(effective_count as i64)
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await?;

        let mut effective_count_per_hour = HashMap::<u64, u64>::new();
        for entry in self.cache.per_second.iter() {
            let (second, second_metrics) = (entry.key(), entry.value());
            let hour = second - (second % 3600);
            *effective_count_per_hour.entry(hour).or_insert(0) += second_metrics.effective_transaction_count;
        }

        sqlx::query(
            r#"
            INSERT INTO key_value ("key", "value", updated_timestamp)
            VALUES('effective_transaction_count_per_hour_24h', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value" = $1, updated_timestamp = $2
            "#,
        )
        .bind(serde_json::to_string(&effective_count_per_hour).unwrap())
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await?;

        debug!("txs: {} | effective txs: {}", count, effective_count);
        debug!("per hour effective tx count {:?}", effective_count_per_hour);

        Ok(())
    }

    async fn rolling_miner_node_versions(&self) -> Result<(), BlockMinerProcessError> {
        let mut version_counts = HashMap::<String, u64>::new();

        for block in &self.cache.blocks {
            let coinbase_tx_id = block.transactions.first().unwrap();
            let coinbase_tx = self.cache.transactions.get(coinbase_tx_id).ok_or(
                BlockMinerProcessError::MissingCoinbaseTransaction(
                    coinbase_tx_id.to_string(),
                    block.key().to_string(),
                ),
            )?;

            let block_miner =
                BlockMiner::try_new(*block.key(), block.timestamp, coinbase_tx.payload.clone())?;

            *version_counts.entry(block_miner.node_version).or_insert(0) += 1;
        }

        let total_blocks = version_counts.values().sum::<u64>();

        let version_share: HashMap<String, f64> = version_counts
            .into_iter()
            .map(|(version, count)| {
                let share = (count as f64 / total_blocks as f64) * 100.0;
                (version, share)
            })
            .collect();

        sqlx::query(
            r#"INSERT INTO key_value ("key", "value", updated_timestamp)
            VALUES('miner_node_versions_1h', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value" = $1, updated_timestamp = $2
            "#,
        )
        .bind(serde_json::to_string(&version_share).unwrap())
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await?;

        debug!("Version share: {:?}", version_share);

        Ok(())
    }

    pub async fn run(&self) {
        // TODO error handling for everything in here

        loop {
            // Skip until cache is at DAG tip
            if !self.cache.synced.load(Ordering::SeqCst) {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            // Intentionally not prop or handling errors yet. TODO
            let _ = self.rolling_tx_count().await;
            let _ = self.rolling_miner_node_versions().await;

            info!("Analyzer completed, sleeping");
            sleep(Duration::from_secs(30)).await;
        }
    }
}
