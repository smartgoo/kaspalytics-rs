use super::aging::UtxoAgeAnalysis;
use super::kas_bucket::DistributionByKASBucketAnalysis;
use super::percentile::AddressPercentileAnalysis;
use kaspa_addresses::{Address, Prefix};
use kaspa_consensus_core::tx::ScriptPublicKey;
use kaspa_consensus_core::Hash;
use kaspa_database::prelude::DB;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_txscript::extract_script_pub_key_address;
use kaspa_utxoindex::stores::indexed_utxos::{
    ScriptPublicKeyBucket, TRANSACTION_OUTPOINT_KEY_SIZE,
};
use kaspa_utxoindex::stores::store_manager::Store;
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::config::Config;
use kaspalytics_utils::kaspad::db::UtxoIndexSecondary;
use log::debug;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Default)]
struct UtxoSetLoadResults {
    address_balances: HashMap<Address, u64>,

    utxo_count: u64,

    // Total sompi held by addresses with a dust balance
    dust_address_sompi_total: u64,

    // Count of addresses that hold a dust balance
    dust_address_count: u64,

    pub_key_format_errors: u64,
}

#[allow(dead_code)]
struct UtxoSnapshotHeader {
    pg_pool: PgPool,
    id: i32,
    block: Hash,
    block_timestamp: u64,
    daa_score: u64,
    kas_price_usd: f64,
    circulating_supply: u64,
    utxo_count: u64,
    unique_address_count: u64,
    unique_address_count_dust: u64,
    sompi_held_by_dust_addresses: u64,
    percentile_analysis_completed: Option<bool>,
    kas_last_moved_by_age_bucket_complete: Option<bool>,
    distribution_by_kas_bucket_complete: Option<bool>,
}

impl UtxoSnapshotHeader {
    async fn new(
        pg_pool: PgPool,
        block: Hash,
        block_timestamp: u64,
        daa_score: u64,
        kas_price_usd: f64,
        circulating_supply: u64,
    ) -> Self {
        let dt = chrono::DateTime::from_timestamp_millis(block_timestamp as i64).unwrap();

        let r = sqlx::query(
            r#"
                INSERT INTO utxo_snapshot_header
                (block, block_timestamp, daa_score, kas_price_usd, circulating_supply)
                VALUES
                ($1, $2, $3, $4, $5)
                RETURNING id
            "#,
        )
        .bind(block.to_string())
        .bind(dt)
        .bind(daa_score as i64)
        .bind(kas_price_usd)
        .bind(circulating_supply as i64)
        .fetch_one(&pg_pool)
        .await
        .unwrap();

        let id = r.try_get::<i32, &str>("id").unwrap();

        UtxoSnapshotHeader {
            pg_pool,
            id,
            block,
            block_timestamp,
            daa_score,
            kas_price_usd,
            circulating_supply,
            utxo_count: 0,
            unique_address_count: 0,
            unique_address_count_dust: 0,
            sompi_held_by_dust_addresses: 0,
            percentile_analysis_completed: Some(false),
            kas_last_moved_by_age_bucket_complete: Some(false),
            distribution_by_kas_bucket_complete: Some(false),
        }
    }

    async fn set_utxo_count(&self, count: u64) {
        let sql = r#"
            UPDATE utxo_snapshot_header
            SET utxo_count = $1
            WHERE id = $2
        "#;

        sqlx::query(sql)
            .bind(count as i64)
            .bind(self.id)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    async fn set_unique_address_count(&self, count: u64) {
        let sql = r#"
            UPDATE utxo_snapshot_header
            SET unique_address_count = $1
            WHERE id = $2
        "#;

        sqlx::query(sql)
            .bind(count as i64)
            .bind(self.id)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    async fn set_unique_address_count_dust(&self, count: u64) {
        let sql = r#"
            UPDATE utxo_snapshot_header
            SET unique_address_count_dust = $1
            WHERE id = $2
        "#;

        sqlx::query(sql)
            .bind(count as i64)
            .bind(self.id)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    async fn set_sompi_held_by_dust_addresses(&self, count: u64) {
        let sql = r#"
            UPDATE utxo_snapshot_header
            SET sompi_held_by_dust_addresses = $1
            WHERE id = $2
        "#;

        sqlx::query(sql)
            .bind(count as i64)
            .bind(self.id)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    async fn set_percentile_analysis_complete(&mut self, pg_pool: PgPool) {
        self.percentile_analysis_completed = Some(true);

        let sql = r#"
            UPDATE utxo_snapshot_header
            SET percentile_analysis_completed = true
            WHERE id = $1
        "#;

        sqlx::query(sql)
            .bind(self.id)
            .execute(&pg_pool)
            .await
            .unwrap();
    }

    async fn set_distribution_by_kas_bucket_complete(&mut self, pg_pool: PgPool) {
        self.distribution_by_kas_bucket_complete = Some(true);

        let sql = r#"
            UPDATE utxo_snapshot_header
            SET distribution_by_kas_bucket_complete = true
            WHERE id = $1
        "#;

        sqlx::query(sql)
            .bind(self.id)
            .execute(&pg_pool)
            .await
            .unwrap();
    }

    async fn set_kas_last_moved_by_age_bucket_complete(&mut self, pg_pool: PgPool) {
        self.distribution_by_kas_bucket_complete = Some(true);

        let sql = r#"
            UPDATE utxo_snapshot_header
            SET kas_last_moved_by_age_bucket_complete = true
            WHERE id = $1
        "#;

        sqlx::query(sql)
            .bind(self.id)
            .execute(&pg_pool)
            .await
            .unwrap();
    }
}

pub struct UtxoBasedPipeline {
    config: Config,
    rpc_client: Arc<KaspaRpcClient>,
    pg_pool: PgPool,
}

impl UtxoBasedPipeline {
    pub fn new(config: Config, rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) -> Self {
        UtxoBasedPipeline {
            config,
            rpc_client,
            pg_pool,
        }
    }
}

impl UtxoBasedPipeline {
    fn load_from_utxo_set(&mut self, db: Arc<DB>) -> UtxoSetLoadResults {
        let store = Store::new(db);

        let mut results = UtxoSetLoadResults::default();

        for c in store.utxos_by_script_public_key_store.access.iterator() {
            results.utxo_count += 1;

            let (key, utxo) = c.unwrap();

            if utxo.amount <= 1000 {
                results.dust_address_count += 1;
                results.dust_address_sompi_total += utxo.amount;
                continue;
            }

            let script_public_key_bucket =
                ScriptPublicKeyBucket(key[..key.len() - TRANSACTION_OUTPOINT_KEY_SIZE].to_vec());
            let script_public_key = ScriptPublicKey::from(script_public_key_bucket);

            let address = match extract_script_pub_key_address(&script_public_key, Prefix::Mainnet)
            {
                Ok(addr) => addr,
                Err(_) => {
                    results.pub_key_format_errors += 1;
                    continue;
                }
            };

            *results.address_balances.entry(address).or_insert(0) += utxo.amount;
        }

        debug!(
            "dust_address_sompi_total: {}",
            results.dust_address_sompi_total / 100_000_000
        );
        debug!("dust_address_count: {}", results.dust_address_count);

        results
    }

    fn get_utxo_tip(&self, db: Arc<DB>) -> Hash {
        let store = Store::new(db);

        // Return a single utxo tip (order doens't really matter)
        *store.get_tips().unwrap().iter().next().unwrap()
    }

    fn get_circulating_supply(&self, db: Arc<DB>) -> u64 {
        let store = Store::new(db);
        store.get_circulating_supply().unwrap()
    }
}

impl UtxoBasedPipeline {
    async fn insert_address_balances(
        &self,
        utxo_snapshot_id: i32,
        address_data: Rc<HashMap<Address, u64>>,
    ) -> Result<(), sqlx::Error> {
        let db_addresses: Vec<(String, i32, u64)> = address_data
            .iter()
            .map(|(address, sompi)| (address.to_string(), utxo_snapshot_id, *sompi))
            .collect();

        let mut conn = self.pg_pool.acquire().await?;

        let mut copy_in = conn
            .copy_in_raw(
                "COPY address_balance_snapshot (address, utxo_snapshot_id, amount_sompi) FROM STDIN WITH (FORMAT csv)",
            )
            .await?;

        for (address, snapshot_id, amount_sompi) in db_addresses {
            let line = format!("{},{},{}\n", address, snapshot_id, amount_sompi);
            copy_in.send(line.as_bytes()).await?;
        }

        copy_in.finish().await?;
        Ok(())
    }
}

impl UtxoBasedPipeline {
    pub async fn run(&mut self) {
        // Get KAS/USD price
        debug!("Retrieving KAS/USD price...");
        let kas_price_usd = kaspalytics_utils::coingecko::get_simple_price()
            .await
            .unwrap()
            .kaspa
            .usd;

        // Get UTXO tips from utxoindex db
        debug!("Loading UTXO tips from RocksDB...");
        let db = UtxoIndexSecondary::new(self.config.clone());

        let utxo_tip_block = self.get_utxo_tip(db.db.clone());
        let utxo_tip_circulating_supply = self.get_circulating_supply(db.db.clone());

        // Get block timestamp
        debug!("Getting UTXO tip timestamp...");
        let block_data = self
            .rpc_client
            .get_block(utxo_tip_block, false)
            .await
            .unwrap();
        let utxo_tip_block_timestamp = block_data.header.timestamp;
        let utxo_tip_daa_score = block_data.header.daa_score;

        // Create initial record in utxo_snapshot_header
        debug!("Inserting UTXO snapshot header record...");
        let mut utxo_snapshot_header = UtxoSnapshotHeader::new(
            self.pg_pool.clone(),
            utxo_tip_block,
            utxo_tip_block_timestamp,
            utxo_tip_daa_score,
            kas_price_usd,
            utxo_tip_circulating_supply,
        )
        .await;

        // Snapshot DAA score and timestamp
        debug!("Saving DAA score and timestamp...");
        kaspalytics_utils::database::sql::daa_snapshot::insert(
            &self.pg_pool,
            block_data.header.daa_score,
            block_data.header.timestamp,
        )
        .await
        .unwrap();

        // Iterate over UTXOs in utxoindex db, loading address balance data into memory
        debug!("Loading address balances from UTXO index...");
        let utxo_set_results = self.load_from_utxo_set(db.db.clone());

        let address_balances = Rc::new(utxo_set_results.address_balances.clone());

        utxo_snapshot_header
            .set_utxo_count(utxo_set_results.utxo_count)
            .await;
        utxo_snapshot_header
            .set_unique_address_count(
                address_balances.clone().len() as u64 + utxo_set_results.dust_address_count,
            )
            .await;
        utxo_snapshot_header
            .set_unique_address_count_dust(utxo_set_results.dust_address_count)
            .await;
        utxo_snapshot_header
            .set_sompi_held_by_dust_addresses(utxo_set_results.dust_address_sompi_total)
            .await;

        // Store address balances in DB
        debug!("Saving address balance snapshots...");
        self.insert_address_balances(utxo_snapshot_header.id, address_balances.clone())
            .await
            .unwrap();

        // Address percentile analysis
        debug!("Starting address percentile analysis...");
        AddressPercentileAnalysis::new(
            self.pg_pool.clone(),
            utxo_snapshot_header.id,
            address_balances.clone(),
            utxo_snapshot_header.circulating_supply,
        )
        .run()
        .await;

        utxo_snapshot_header
            .set_percentile_analysis_complete(self.pg_pool.clone())
            .await;

        // Distribution by KAS Bucket
        debug!("Starting distribution by KAS bucket analysis...");
        DistributionByKASBucketAnalysis::new(
            self.pg_pool.clone(),
            utxo_snapshot_header.id,
            address_balances.clone(),
            utxo_set_results.dust_address_sompi_total,
            utxo_set_results.dust_address_count,
            utxo_snapshot_header.circulating_supply,
            utxo_snapshot_header.kas_price_usd,
        )
        .run()
        .await;

        utxo_snapshot_header
            .set_distribution_by_kas_bucket_complete(self.pg_pool.clone())
            .await;

        // UTXO Aging
        debug!("Starting UTXO aging analysis...");
        UtxoAgeAnalysis::new(
            self.pg_pool.clone(),
            utxo_snapshot_header.id,
            db.db.clone(),
            self.rpc_client.clone(),
            utxo_snapshot_header.circulating_supply,
        )
        .run()
        .await;

        utxo_snapshot_header
            .set_kas_last_moved_by_age_bucket_complete(self.pg_pool.clone())
            .await;

        let _ = kaspalytics_utils::email::send_email(
            &self.config,
            "utxo-pipeline completed".to_string(),
            "".to_string(),
        );
    }
}
