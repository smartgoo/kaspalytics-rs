use crate::cache::Cache;
use chrono::Utc;
use log::debug;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn run(cache: Arc<Cache>, pg_pool: &PgPool) -> Result<(), sqlx::Error> {
    // TODO store mining by address
    let mut version_counts = HashMap::<String, u64>::new();
    // let mut miner_counts = HashMap::<String, u64>::new();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let cutoff = now - 3600;

    cache
        .seconds
        .iter()
        .filter(|entry| *entry.key() >= cutoff)
        .map(|entry| entry.mining_node_version_block_counts.clone())
        .for_each(|second_map| {
            second_map.iter().for_each(|entry| {
                let version = entry.key();
                let second_count = entry.value();

                version_counts
                    .entry(version.clone())
                    .and_modify(|overall_count| *overall_count += second_count)
                    .or_insert(*second_count);
            });
        });

    // for block in &cache.blocks {
    //     let coinbase_tx_id = block.transactions.first().unwrap();
    //     let coinbase_tx = cache.transactions.get(coinbase_tx_id).ok_or(
    //         MiningAnalyzerError::MissingCoinbaseTransaction(
    //             coinbase_tx_id.to_string(),
    //             block.key().to_string(),
    //         ),
    //     )?;

    //     let block_miner = BlockMiner::try_new(
    //         *block.key(),
    //         block.timestamp,
    //         coinbase_tx.payload.clone(),
    //         coinbase_tx.outputs[0].script_public_key_address.clone(),
    //     )?;

    //     *version_counts.entry(block_miner.node_version).or_insert(0) += 1;
    //     *miner_counts.entry(block_miner.address).or_insert(0) += 1;
    // }

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
    .execute(pg_pool)
    .await?;

    // sqlx::query(
    //     r#"INSERT INTO key_value ("key", "value", updated_timestamp)
    //     VALUES('miner_block_counts_1h', $1, $2)
    //     ON CONFLICT ("key") DO UPDATE
    //         SET "value" = $1, updated_timestamp = $2
    //     "#,
    // )
    // .bind(serde_json::to_string(&miner_counts).unwrap())
    // .bind(Utc::now())
    // .execute(&pg_pool)
    // .await?;

    debug!("Version share: {:?}", version_share);
    // debug!("Miner block counts: {:?}", miner_counts);

    Ok(())
}
