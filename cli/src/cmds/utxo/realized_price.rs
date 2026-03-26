use chrono::{DateTime, Utc};
use kaspa_database::db::DB;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_utxoindex::model::CompactUtxoEntry;
use kaspa_utxoindex::stores::store_manager::Store;
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::kaspad::SOMPI_PER_KAS;
use kaspalytics_utils::log::LogTarget;
use log::debug;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;

/// A (timestamp_ms, price_usd) entry from coin_market_history, sorted by timestamp.
struct PricePoint {
    timestamp_ms: i64,
    price_usd: f64,
}

/// URPD bucket accumulator keyed by the bucket's lower bound in cents.
/// e.g. key 7 = $0.07–$0.08
#[derive(Default)]
struct UrpdBucket {
    sompi: u64,
    utxo_count: u64,
}

pub struct RealizedPriceAnalysis {
    pg_pool: PgPool,
    utxo_snapshot_id: i32,
    db: Arc<DB>,
    rpc_client: Arc<KaspaRpcClient>,
    circulating_supply: u64,
    kas_price_usd: Decimal,

    // Price history loaded from DB (sorted ascending by timestamp)
    price_history: Vec<PricePoint>,

    // Aggregates
    realized_cap_usd: f64,
    sompi_in_profit: u64,
    sompi_in_loss: u64,
    utxos_processed: u64,

    // URPD buckets keyed by cent value (e.g. 0 = $0.00-$0.01, 7 = $0.07-$0.08)
    urpd_buckets: HashMap<i64, UrpdBucket>,
}

impl RealizedPriceAnalysis {
    pub fn new(
        pg_pool: PgPool,
        utxo_snapshot_id: i32,
        db: Arc<DB>,
        rpc_client: Arc<KaspaRpcClient>,
        circulating_supply: u64,
        kas_price_usd: Decimal,
    ) -> Self {
        Self {
            pg_pool,
            utxo_snapshot_id,
            db,
            rpc_client,
            circulating_supply,
            kas_price_usd,
            price_history: Vec::new(),
            realized_cap_usd: 0.0,
            sompi_in_profit: 0,
            sompi_in_loss: 0,
            utxos_processed: 0,
            urpd_buckets: HashMap::new(),
        }
    }

    /// Load coin_market_history from Postgres into a sorted vec for binary search.
    async fn load_price_history(&mut self) {
        let rows: Vec<(DateTime<Utc>, f64)> = sqlx::query_as(
            r#"
            SELECT "timestamp", price
            FROM coin_market_history
            WHERE symbol = 'KAS'
            ORDER BY "timestamp" ASC
            "#,
        )
        .fetch_all(&self.pg_pool)
        .await
        .unwrap();

        self.price_history = rows
            .into_iter()
            .map(|(dt, price)| PricePoint {
                timestamp_ms: dt.timestamp_millis(),
                price_usd: price,
            })
            .collect();

        debug!(
            target: LogTarget::Cli.as_str(),
            "Loaded {} price history entries",
            self.price_history.len()
        );
    }

    /// Find the price at or just before the given timestamp (floor lookup).
    /// Returns 0.0 if the timestamp is before any known price data.
    fn lookup_price(&self, timestamp_ms: u64) -> f64 {
        let ts = timestamp_ms as i64;

        // Binary search for the rightmost entry with timestamp_ms <= ts
        let idx = self.price_history.partition_point(|p| p.timestamp_ms <= ts);

        if idx == 0 {
            // UTXO is before any price data
            0.0
        } else {
            self.price_history[idx - 1].price_usd
        }
    }

    /// Determine the URPD bucket key (in cents) for a given price.
    /// $0.0723 → bucket key 7 (representing $0.07–$0.08)
    fn price_to_bucket_key(price_usd: f64) -> i64 {
        (price_usd * 100.0).floor() as i64
    }

    async fn process_batch(&mut self, utxos: Vec<CompactUtxoEntry>) {
        let mut daas: Vec<u64> = utxos.iter().map(|utxo| utxo.block_daa_score).collect();
        daas.sort_unstable();
        daas.dedup();

        let timestamps = self
            .rpc_client
            .get_daa_score_timestamp_estimate(daas.clone())
            .await
            .unwrap();

        let daa_timestamps: HashMap<u64, u64> =
            daas.into_iter().zip(timestamps.into_iter()).collect();

        let current_price = self.kas_price_usd.to_f64().unwrap();

        for utxo in utxos.iter() {
            let timestamp_ms = daa_timestamps.get(&utxo.block_daa_score).unwrap();
            let realized_price = self.lookup_price(*timestamp_ms);

            let kas_amount = utxo.amount as f64 / SOMPI_PER_KAS as f64;

            // Realized cap: sum of (KAS amount * price when UTXO was created)
            self.realized_cap_usd += kas_amount * realized_price;

            // Supply in profit vs loss (breakeven counts as profit)
            if realized_price <= current_price {
                self.sompi_in_profit += utxo.amount;
            } else {
                self.sompi_in_loss += utxo.amount;
            }

            // URPD bucket
            let bucket_key = Self::price_to_bucket_key(realized_price);
            let bucket = self.urpd_buckets.entry(bucket_key).or_default();
            bucket.sompi += utxo.amount;
            bucket.utxo_count += 1;
        }

        self.utxos_processed += utxos.len() as u64;
        debug!(
            target: LogTarget::Cli.as_str(),
            "Realized price analysis: {} UTXOs processed",
            self.utxos_processed
        );
    }

    async fn insert_to_db(&self) {
        let mut tx = self.pg_pool.begin().await.unwrap();

        // Delete any existing URPD rows for this snapshot (idempotent re-runs)
        sqlx::query(
            r#"
            DELETE FROM utxo_realized_price_distribution
            WHERE utxo_snapshot_id = $1
            "#,
        )
        .bind(self.utxo_snapshot_id)
        .execute(&mut *tx)
        .await
        .unwrap();

        // Update snapshot header with aggregate metrics
        sqlx::query(
            r#"
            UPDATE utxo_snapshot_header
            SET realized_cap_usd = $1,
                sompi_in_profit = $2,
                sompi_in_loss = $3,
                realized_price_analysis_complete = true
            WHERE id = $4
            "#,
        )
        .bind(self.realized_cap_usd)
        .bind(self.sompi_in_profit as i64)
        .bind(self.sompi_in_loss as i64)
        .bind(self.utxo_snapshot_id)
        .execute(&mut *tx)
        .await
        .unwrap();

        // Insert URPD rows
        let cs = self.circulating_supply as f64;

        for (bucket_key, bucket) in &self.urpd_buckets {
            let price_low = *bucket_key as f64 / 100.0;
            let price_high = (*bucket_key + 1) as f64 / 100.0;
            let cs_percent = bucket.sompi as f64 / cs * 100.0;

            sqlx::query(
                r#"
                INSERT INTO utxo_realized_price_distribution
                (utxo_snapshot_id, price_bucket_low, price_bucket_high, sompi, utxo_count, cs_percent)
                VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            )
            .bind(self.utxo_snapshot_id)
            .bind(price_low)
            .bind(price_high)
            .bind(bucket.sompi as i64)
            .bind(bucket.utxo_count as i64)
            .bind(cs_percent)
            .execute(&mut *tx)
            .await
            .unwrap();
        }

        tx.commit().await.unwrap();
    }

    pub async fn run(&mut self) {
        // Load price history from DB
        self.load_price_history().await;

        // Init UTXO store
        let store = Store::new(self.db.clone());

        // Iterate over UTXOs
        let mut utxos = vec![];

        for c in store.utxos_by_script_public_key_store.access.iterator() {
            let (_, utxo) = c.unwrap();

            // Skip dust UTXOs
            if utxo.amount <= 1000 {
                continue;
            }

            utxos.push(utxo);

            if utxos.len() >= 100_000 {
                self.process_batch(utxos.clone()).await;
                utxos.clear();
            }
        }

        if !utxos.is_empty() {
            self.process_batch(utxos.clone()).await;
            utxos.clear();
        }

        debug!(
            target: LogTarget::Cli.as_str(),
            "Realized cap: ${:.2}, In profit: {}, In loss: {}, URPD buckets: {}",
            self.realized_cap_usd,
            self.sompi_in_profit,
            self.sompi_in_loss,
            self.urpd_buckets.len()
        );

        self.insert_to_db().await;
    }
}
