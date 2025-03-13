use crate::cache::Cache;
use chrono::Utc;
use kaspa_hashes::Hash;
use log::debug;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum PayloadParseError {
    #[error("First byte 0xaa indicates address payload")]
    InvalidFirstByte,

    #[error("Payload split error")]
    SplitError,
}

#[derive(thiserror::Error, Debug)]
pub enum MiningAnalyzerError {
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
    address: String,
    // miner
}

impl BlockMiner {
    fn try_new(
        hash: Hash,
        timestamp: u64,
        payload: Vec<u8>,
        address: String,
    ) -> Result<Self, PayloadParseError> {
        let node_version = parse_payload_node_version(payload)?;
        Ok(Self {
            hash,
            timestamp,
            node_version,
            address,
        })
    }
}

pub async fn run(cache: Arc<Cache>, pg_pool: PgPool) -> Result<(), MiningAnalyzerError> {
    // TODO store mining by address
    let mut version_counts = HashMap::<String, u64>::new();
    let mut miner_counts = HashMap::<String, u64>::new();

    for block in &cache.blocks {
        let coinbase_tx_id = block.transactions.first().unwrap();
        let coinbase_tx = cache.transactions.get(coinbase_tx_id).ok_or(
            MiningAnalyzerError::MissingCoinbaseTransaction(
                coinbase_tx_id.to_string(),
                block.key().to_string(),
            ),
        )?;

        let block_miner = BlockMiner::try_new(
            *block.key(),
            block.timestamp,
            coinbase_tx.payload.clone(),
            coinbase_tx.outputs[0].script_public_key_address.clone(),
        )?;

        *version_counts.entry(block_miner.node_version).or_insert(0) += 1;
        *miner_counts.entry(block_miner.address).or_insert(0) += 1;
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
    .execute(&pg_pool)
    .await?;

    sqlx::query(
        r#"INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('miner_block_counts_1h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(serde_json::to_string(&miner_counts).unwrap())
    .bind(Utc::now())
    .execute(&pg_pool)
    .await?;

    debug!("Version share: {:?}", version_share);
    debug!("Miner block counts: {:?}", miner_counts);

    Ok(())
}
