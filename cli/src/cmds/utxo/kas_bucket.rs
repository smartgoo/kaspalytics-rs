use kaspa_addresses::Address;
use kaspalytics_utils::kaspad::SOMPI_PER_KAS;
use log::debug;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sqlx::Arguments;
use sqlx::PgPool;
use std::collections::HashMap;
use std::rc::Rc;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Clone, Debug, EnumIter, PartialEq)]
enum Bucket {
    // 0.0 - 0.01 KAS
    Kas0ToP01,

    // 0.01 - 1 KAS
    KasP01To1,

    // 1 - 100 KAS
    Kas1To100,

    // 100 - 1,000 KAS
    Kas100To1k,

    // 1,000 - 10,000 KAS
    Kas1kTo10k,

    // 10,000 - 100,000 KAS
    Kas10kTo100k,

    // 100,000 - 1,000,000 KAS
    Kas100kTo1m,

    // 1,000,000 - 10,000,000 KAS
    Kas1mTo10m,

    // 10,000,000 - 100,000,000 KAS
    Kas10mTo100m,

    // 100,000,000 - 1,000,000,000 KAS
    Kas100mTo1b,

    // 1,000,000,000 - 10,000,000,000 KAS
    Kas1bTo10b,
}

impl Bucket {
    fn range_in_kas(self) -> (f64, f64) {
        match self {
            Bucket::Kas0ToP01 => (0.0, 0.01),
            Bucket::KasP01To1 => (0.01, 1.0),
            Bucket::Kas1To100 => (1.0, 100.0),
            Bucket::Kas100To1k => (100.0, 1_000.0),
            Bucket::Kas1kTo10k => (1_000.0, 10_000.0),
            Bucket::Kas10kTo100k => (10_000.0, 100_000.0),
            Bucket::Kas100kTo1m => (100_000.0, 1_000_000.0),
            Bucket::Kas1mTo10m => (1_000_000.0, 10_000_000.0),
            Bucket::Kas10mTo100m => (10_000_000.0, 100_000_000.0),
            Bucket::Kas100mTo1b => (100_000_000.0, 1_000_000_000.0),
            Bucket::Kas1bTo10b => (1_000_000_000.0, 10_000_000_000.0),
        }
    }

    fn range_in_sompi(self) -> (u64, u64) {
        let in_kas = self.range_in_kas();

        let lower = (in_kas.0 * SOMPI_PER_KAS as f64) as u64;
        let upper = (in_kas.1 * SOMPI_PER_KAS as f64) as u64;
        (lower, upper)
    }
}

#[derive(Debug, Clone)]
struct BucketData {
    bucket: Bucket,
    address_count: u64,
    percent_of_addresses: f64,
    sompi_total: u64,
    percent_of_sompi: f64,
    usd_value: Decimal,
}

impl BucketData {
    // TODO convert this to default impl, same for PercentileData in `percentile.rs`
    fn new(bucket: Bucket) -> Self {
        Self {
            bucket,
            address_count: 0,
            percent_of_addresses: 0.0,
            sompi_total: 0,
            percent_of_sompi: 0.0,
            usd_value: dec!(0),
        }
    }

    fn set_percent_of_addresses(&mut self, total_address_count: u64) {
        self.percent_of_addresses = self.address_count as f64 / total_address_count as f64;
    }

    fn set_percent_of_circulating_supply(&mut self, circulating_supply: u64) {
        self.percent_of_sompi = self.sompi_total as f64 / circulating_supply as f64;
    }

    fn set_usd_value(&mut self, price_usd: Decimal) {
        self.usd_value = (Decimal::from_u64(self.sompi_total).unwrap()
            / Decimal::from_u64(SOMPI_PER_KAS).unwrap())
            * price_usd;
    }
}

pub struct DistributionByKASBucketAnalysis {
    pg_pool: PgPool,
    utxo_snapshot_id: i32,
    address_balances: Rc<HashMap<Address, u64>>,
    dust_address_sompi_total: u64,
    dust_address_count: u64,
    circulating_supply: u64,
    kas_price_usd: Decimal,
    buckets: Vec<BucketData>,
}

impl DistributionByKASBucketAnalysis {
    pub fn new(
        pg_pool: PgPool,
        utxo_snapshot_id: i32,
        address_balances: Rc<HashMap<Address, u64>>,
        dust_address_sompi_total: u64,
        dust_address_count: u64,
        circulating_supply: u64,
        kas_price_usd: Decimal,
    ) -> Self {
        let buckets = Bucket::iter()
            .map(|bucket| BucketData::new(bucket.clone()))
            .collect();

        Self {
            pg_pool,
            utxo_snapshot_id,
            address_balances,
            dust_address_sompi_total,
            dust_address_count,
            circulating_supply,
            kas_price_usd,
            buckets,
        }
    }

    async fn insert_to_db(&self) {
        let sql = "
            INSERT INTO distribution_by_kas_bucket (
                utxo_snapshot_id,

                addr_qty_0_to_p01,
                pct_addr_0_to_p01,
                sompi_0_to_p01,
                cs_percent_0_to_p01,
                tot_usd_0_to_p01,

                addr_qty_p01_to_1,
                pct_addr_p01_to_1,
                sompi_p01_to_1,
                cs_percent_p01_to_1,
                tot_usd_p01_to_1,

                addr_qty_1_to_100,
                pct_addr_1_to_100,
                sompi_1_to_100,
                cs_percent_1_to_100,
                tot_usd_1_to_100,

                addr_qty_100_to_1k,
                pct_addr_100_to_1k,
                sompi_100_to_1k,
                cs_percent_100_to_1k,
                tot_usd_100_to_1k,

                addr_qty_1k_to_10k,
                pct_addr_1k_to_10k,
                sompi_1k_to_10k,
                cs_percent_1k_to_10k,
                tot_usd_1k_to_10k,

                addr_qty_10k_to_100k,
                pct_addr_10k_to_100k,
                sompi_10k_to_100k,
                cs_percent_10k_to_100k,
                tot_usd_10k_to_100k,

                addr_qty_100k_to_1m,
                pct_addr_100k_to_1m,
                sompi_100k_to_1m,
                cs_percent_100k_to_1m,
                tot_usd_100k_to_1m,

                addr_qty_1m_to_10m,
                pct_addr_1m_to_10m,
                sompi_1m_to_10m,
                cs_percent_1m_to_10m,
                tot_usd_1m_to_10m,

                addr_qty_10m_to_100m,
                pct_addr_10m_to_100m,
                sompi_10m_to_100m,
                cs_percent_10m_to_100m,
                tot_usd_10m_to_100m,

                addr_qty_100m_to_1b,
                pct_addr_100m_to_1b,
                sompi_100m_to_1b,
                cs_percent_100m_to_1b,
                tot_usd_100m_to_1b,

                addr_qty_1b_to_10b,
                pct_addr_1b_to_10b,
                sompi_1b_to_10b,
                cs_percent_1b_to_10b,                
                tot_usd_1b_to_10b
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
                $51, $52, $53, $54, $55, $56
            )
        ";

        let mut args = sqlx::postgres::PgArguments::default();
        args.add(self.utxo_snapshot_id).unwrap();

        // TODO move x100 from below into bucket.set functions
        for bucket in &self.buckets {
            // Total addresses in bucket
            args.add(bucket.address_count as i64).unwrap();

            // Address percent of bucket
            args.add(bucket.percent_of_addresses * 100f64).unwrap();

            // Total sompi hled by bucket
            args.add(bucket.sompi_total as i64).unwrap();

            // CS Percent held by bucket
            args.add(bucket.percent_of_sompi * 100f64).unwrap();

            // Total USD held by bucket
            args.add(bucket.usd_value).unwrap();
        }

        sqlx::query_with(sql, args)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }

    pub async fn run(&mut self) {
        for (_addr, &sompi_balance) in self.address_balances.iter() {
            for bucket_data in self.buckets.iter_mut() {
                let (low_sompi, high_sompi) = bucket_data.bucket.clone().range_in_sompi();
                if sompi_balance >= low_sompi && sompi_balance < high_sompi {
                    bucket_data.address_count += 1;
                    bucket_data.sompi_total += sompi_balance;
                    break;
                }
            }

            // TODO catch when no bucket
        }

        self.buckets[0].address_count += self.dust_address_count;
        self.buckets[0].sompi_total += self.dust_address_sompi_total;

        let total_address_count = self.address_balances.len() as u64 + self.dust_address_count;
        for bucket in self.buckets.iter_mut() {
            bucket.set_percent_of_addresses(total_address_count);
            bucket.set_percent_of_circulating_supply(self.circulating_supply);
            bucket.set_usd_value(self.kas_price_usd);
        }

        self.insert_to_db().await;

        for bucket_data in &self.buckets {
            debug!("{:?}", bucket_data);
        }
    }
}
