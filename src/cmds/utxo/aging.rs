use crate::kaspad::SOMPI_PER_KAS;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use kaspa_database::db::DB;
use kaspa_utxoindex::model::CompactUtxoEntry;
use kaspa_utxoindex::stores::store_manager::Store;
use kaspa_wrpc_client::prelude::RpcApi;
use kaspa_wrpc_client::KaspaRpcClient;
use log::info;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use sqlx::postgres::{PgArguments, PgPool};
use sqlx::Arguments;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the exact number of *fully elapsed* months between `start` and `end`,
/// accounting for variable month lengths.
fn diff_in_full_months(start: DateTime<Utc>, end: DateTime<Utc>) -> i32 {
    let (start_year, start_month, start_day) = (start.year(), start.month(), start.day());
    let (end_year, end_month, end_day) = (end.year(), end.month(), end.day());

    let start_total_months = start_year * 12 + start_month as i32;
    let end_total_months = end_year * 12 + end_month as i32;

    let mut months_diff = end_total_months - start_total_months;

    // If the ending day-of-month is less than the starting day-of-month,
    // we haven't reached the 'full' month boundary. Subtract 1.
    if end_day < start_day {
        months_diff -= 1;
    }

    months_diff
}

/// Returns the number of fully elapsed years between `start` and `end`,
/// correctly accounting for leap years (e.g., birthdays on Feb 29).
///
/// - If `end` is before `start`, this could return a negative number.
/// - For "age" calculations, typically one ensures `end >= start`.
fn diff_in_full_years(start: DateTime<Utc>, end: DateTime<Utc>) -> i32 {
    // Extract year, month, day
    let (start_year, start_month, start_day) = (start.year(), start.month(), start.day());
    let (end_year, end_month, end_day) = (end.year(), end.month(), end.day());

    // Preliminary difference in calendar years
    let mut years_diff = end_year - start_year;

    // If the (month, day) of `end` is before the (month, day) of `start`,
    // we haven't hit the "anniversary" yet in the end year, so subtract 1.
    if (end_month, end_day) < (start_month, start_day) {
        years_diff -= 1;
    }

    years_diff
}

#[derive(Debug)]
struct Data {
    kas_lt_1d: u64,
    kas_1d_to_1w: u64,
    kas_1w_to_1m: u64,
    kas_1m_to_3m: u64,
    kas_3m_to_6m: u64,
    kas_6m_to_1y: u64,
    kas_1y_to_2y: u64,
    kas_2y_to_3y: u64,
    kas_3y_to_5y: u64,
    kas_5y_to_7y: u64,
    kas_7y_to_10y: u64,

    cs_percent_lt_1d: Decimal,
    cs_percent_1d_to_1w: Decimal,
    cs_percent_1w_to_1m: Decimal,
    cs_percent_1m_to_3m: Decimal,
    cs_percent_3m_to_6m: Decimal,
    cs_percent_6m_to_1y: Decimal,
    cs_percent_1y_to_2y: Decimal,
    cs_percent_2y_to_3y: Decimal,
    cs_percent_3y_to_5y: Decimal,
    cs_percent_5y_to_7y: Decimal,
    cs_percent_7y_to_10y: Decimal,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            kas_lt_1d: 0,
            kas_1d_to_1w: 0,
            kas_1w_to_1m: 0,
            kas_1m_to_3m: 0,
            kas_3m_to_6m: 0,
            kas_6m_to_1y: 0,
            kas_1y_to_2y: 0,
            kas_2y_to_3y: 0,
            kas_3y_to_5y: 0,
            kas_5y_to_7y: 0,
            kas_7y_to_10y: 0,

            cs_percent_lt_1d: Decimal::default(),
            cs_percent_1d_to_1w: Decimal::default(),
            cs_percent_1w_to_1m: Decimal::default(),
            cs_percent_1m_to_3m: Decimal::default(),
            cs_percent_3m_to_6m: Decimal::default(),
            cs_percent_6m_to_1y: Decimal::default(),
            cs_percent_1y_to_2y: Decimal::default(),
            cs_percent_2y_to_3y: Decimal::default(),
            cs_percent_3y_to_5y: Decimal::default(),
            cs_percent_5y_to_7y: Decimal::default(),
            cs_percent_7y_to_10y: Decimal::default(),
        }
    }
}

impl Data {
    fn set_cs_percent(&mut self, circulating_supply: u64) {
        let cs = Decimal::from_u64(circulating_supply).unwrap();
        let m = Decimal::new(100i64, 0);

        self.cs_percent_lt_1d = (Decimal::from_u64(self.kas_lt_1d).unwrap() / cs) * m;
        self.cs_percent_1d_to_1w = (Decimal::from_u64(self.kas_1d_to_1w).unwrap() / cs) * m;
        self.cs_percent_1w_to_1m = (Decimal::from_u64(self.kas_1w_to_1m).unwrap() / cs) * m;
        self.cs_percent_1m_to_3m = (Decimal::from_u64(self.kas_1m_to_3m).unwrap() / cs) * m;
        self.cs_percent_3m_to_6m = (Decimal::from_u64(self.kas_3m_to_6m).unwrap() / cs) * m;
        self.cs_percent_6m_to_1y = (Decimal::from_u64(self.kas_6m_to_1y).unwrap() / cs) * m;
        self.cs_percent_1y_to_2y = (Decimal::from_u64(self.kas_1y_to_2y).unwrap() / cs) * m;
        self.cs_percent_2y_to_3y = (Decimal::from_u64(self.kas_2y_to_3y).unwrap() / cs) * m;
        self.cs_percent_3y_to_5y = (Decimal::from_u64(self.kas_3y_to_5y).unwrap() / cs) * m;
        self.cs_percent_5y_to_7y = (Decimal::from_u64(self.kas_5y_to_7y).unwrap() / cs) * m;
        self.cs_percent_7y_to_10y = (Decimal::from_u64(self.kas_7y_to_10y).unwrap() / cs) * m;
    }
}

pub struct UtxoAgeAnalysis {
    pg_pool: PgPool,
    utxo_snapshot_id: i32,
    db: Arc<DB>,
    rpc_client: Arc<KaspaRpcClient>,
    circulating_supply: u64,
    utxos_processed: u64,
    data: Data,
}

impl UtxoAgeAnalysis {
    pub fn new(
        pg_pool: PgPool,
        utxo_snapshot_id: i32,
        db: Arc<DB>,
        rpc_client: Arc<KaspaRpcClient>,
        circulating_supply: u64,
    ) -> Self {
        Self {
            pg_pool,
            utxo_snapshot_id,
            db,
            rpc_client,
            circulating_supply,
            utxos_processed: 0,
            data: Data::default(),
        }
    }
}

impl UtxoAgeAnalysis {
    async fn process_batch(&mut self, utxos: Vec<CompactUtxoEntry>) {
        let mut daas: Vec<u64> = utxos.iter().map(|utxo| utxo.block_daa_score).collect();
        daas.sort_unstable();
        daas.dedup();

        // Get estimated timestamps for DAAs via RPC
        let timestamps = self
            .rpc_client
            .get_daa_score_timestamp_estimate(daas.clone())
            .await
            .unwrap();

        // Zip into map of Daa: Timestamp
        let daa_timestamps: HashMap<u64, u64> =
            daas.into_iter().zip(timestamps.into_iter()).collect();

        // Get current ms since epoch
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // Iterate UTXOs, calculate age, add to buckets
        for utxo in utxos.iter() {
            let timestamp = daa_timestamps.get(&utxo.block_daa_score).unwrap();

            let age = now as u64 - timestamp;
            let age_seconds = age / 1000;

            // Less than 1 day
            if age_seconds < 86400 {
                self.data.kas_lt_1d += utxo.amount;
                continue;
            }

            // 1 day to 1 Week Ago
            // let age_days = age_seconds / 86400;
            // 604800 = 7 days (86400 * 7)
            if 86400 <= age_seconds && age_seconds < 604800 {
                self.data.kas_1d_to_1w += utxo.amount;
                continue;
            }

            let start_dt = Utc.timestamp_millis_opt(timestamp.clone() as i64).unwrap();
            let end_dt = Utc.timestamp_millis_opt(now.clone() as i64).unwrap();
            let age_months = diff_in_full_months(start_dt, end_dt);

            // 1 Week to 1 Month Ago
            if 604800 <= age_seconds && age_months == 0 {
                self.data.kas_1w_to_1m += utxo.amount;
                continue;
            }

            // 1 Month to 3 Months Ago
            if 1 <= age_months && age_months < 3 {
                self.data.kas_1m_to_3m += utxo.amount;
                continue;
            }

            // 3 Months to 6 Months Ago
            if 3 <= age_months && age_months < 6 {
                self.data.kas_3m_to_6m += utxo.amount;
                continue;
            }

            // 6 Months to 12 Months Ago
            if 6 <= age_months && age_months < 12 {
                self.data.kas_6m_to_1y += utxo.amount;
                continue;
            }

            let age_years = diff_in_full_years(start_dt, end_dt);

            // 1 Year to 2 Years Ago
            if 1 <= age_years && age_years < 2 {
                self.data.kas_1y_to_2y += utxo.amount;
                continue;
            }

            // 2 Years to 3 Years Ago
            if 2 <= age_years && age_years < 3 {
                self.data.kas_2y_to_3y += utxo.amount;
                continue;
            }

            // 3 Years to 5 Years Ago
            if 3 <= age_years && age_years < 5 {
                self.data.kas_3y_to_5y += utxo.amount;
                continue;
            }

            // 5 Years to 7 Years Ago
            if 5 <= age_years && age_years < 7 {
                self.data.kas_5y_to_7y += utxo.amount;
                continue;
            }

            // 7 Years to 10 Years Ago
            if 7 <= age_years && age_years < 10 {
                self.data.kas_7y_to_10y += utxo.amount;
                continue;
            }

            info!("timestamp {}", timestamp);
        }

        self.utxos_processed += utxos.len() as u64;
        info!("{}", self.utxos_processed);
    }

    async fn insert_to_db(&self) {
        let sql = "
            INSERT INTO kas_last_moved_by_age_bucket (
                utxo_snapshot_id,
                qty_kas_lt_1d,
                qty_kas_1d_to_1w,
                qty_kas_1w_to_1m,
                qty_kas_1m_to_3m,
                qty_kas_3m_to_6m,
                qty_kas_6m_to_1y,
                qty_kas_1y_to_2y,
                qty_kas_2y_to_3y,
                qty_kas_3y_to_5y,
                qty_kas_5y_to_7y,
                qty_kas_7y_to_10y,
                cs_percent_lt_1d,
                cs_percent_1d_to_1w,
                cs_percent_1w_to_1m,
                cs_percent_1m_to_3m,
                cs_percent_3m_to_6m,
                cs_percent_6m_to_1y,
                cs_percent_1y_to_2y,
                cs_percent_2y_to_3y,
                cs_percent_3y_to_5y,
                cs_percent_5y_to_7y,
                cs_percent_7y_to_10y
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23
            );
        ";

        let mut args = PgArguments::default();
        args.add(self.utxo_snapshot_id);

        sqlx::query(sql)
            .bind(self.utxo_snapshot_id)
            .bind((self.data.kas_lt_1d as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_1d_to_1w as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_1w_to_1m as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_1m_to_3m as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_3m_to_6m as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_6m_to_1y as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_1y_to_2y as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_2y_to_3y as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_3y_to_5y as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_5y_to_7y as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind((self.data.kas_7y_to_10y as f64 / SOMPI_PER_KAS as f64) as i64)
            .bind(self.data.cs_percent_lt_1d)
            .bind(self.data.cs_percent_1d_to_1w)
            .bind(self.data.cs_percent_1w_to_1m)
            .bind(self.data.cs_percent_1m_to_3m)
            .bind(self.data.cs_percent_3m_to_6m)
            .bind(self.data.cs_percent_6m_to_1y)
            .bind(self.data.cs_percent_1y_to_2y)
            .bind(self.data.cs_percent_2y_to_3y)
            .bind(self.data.cs_percent_3y_to_5y)
            .bind(self.data.cs_percent_5y_to_7y)
            .bind(self.data.cs_percent_7y_to_10y)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    pub async fn run(&mut self) {
        // Init Store
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

        self.data.set_cs_percent(self.circulating_supply);

        info!("{:?}", self.data);

        self.insert_to_db().await;
    }
}
