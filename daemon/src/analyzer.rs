use crate::cache::Cache;
use chrono::Utc;
use kaspa_hashes::Hash;
use log::{debug, info};
use sqlx::{self, PgPool};
use std::collections::HashMap;
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

fn parse_payload_node_version(payload: Vec<u8>) -> String {
    // let mut version = payload[16];
    let length = payload[18];
    let script = &payload[19_usize..(19 + length as usize)];

    if script[0] == 0xaa {
        panic!("test");
    }

    // Assuming script[0] < 0x76 is true
    // if script[0] < 0x76 { ... }

    let payload_str = payload[19_usize + (length as usize)..]
        .iter()
        .map(|&b| b as char)
        .collect::<String>();

    let node_version = &payload_str.split("/").next().unwrap();

    String::from(*node_version)
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
    fn new(hash: Hash, timestamp: u64, payload: Vec<u8>) -> Self {
        let node_version = parse_payload_node_version(payload);
        Self {
            hash,
            timestamp,
            node_version,
        }
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
    async fn rolling_tx_count(&self) {
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
            "#
        )
        .bind(count as i64)
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await
        .unwrap();

        sqlx::query(
            r#"
            INSERT INTO key_value ("key", "value", updated_timestamp)
            VALUES('effective_transaction_count_24h', $1, $2)
            ON CONFLICT ("key") DO UPDATE
                SET "value" = $1, updated_timestamp = $2
            "#
        )
        .bind(effective_count as i64)
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await
        .unwrap();

        debug!("txs: {} | effective txs: {}", count, effective_count);
    }

    async fn rolling_miner_node_versions(&self) {
        let mut version_counts = HashMap::<String, u64>::new();

        for block in &self.cache.blocks {
            let coinbase_tx_id = block.transactions.first().unwrap();
            let coinbase_tx = self.cache.transactions.get(coinbase_tx_id).unwrap();

            let block_miner =
                BlockMiner::new(*block.key(), block.timestamp, coinbase_tx.payload.clone());

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
            "#
        )
        .bind(serde_json::to_string(&version_share).unwrap())
        .bind(Utc::now())
        .execute(&self.pg_pool)
        .await
        .unwrap();

        debug!("Version share: {:?}", version_share);
    }

    pub async fn run(&self) {
        // TODO refactor entire struct
        loop {
            // Skip if cache is at DAG tip
            if !self.cache.synced.load(Ordering::SeqCst) {
                continue;
            }

            self.rolling_tx_count().await;
            self.rolling_miner_node_versions().await;

            info!("Analyzer completed, sleeping");
            sleep(Duration::from_secs(30)).await;
        }
    }
}
