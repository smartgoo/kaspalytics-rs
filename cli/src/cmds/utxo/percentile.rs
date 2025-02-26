use kaspa_addresses::Address;
use kaspalytics_utils::kaspad::SOMPI_PER_KAS;
use log::info;
use rust_decimal::{prelude::FromPrimitive, prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;
use sqlx::Arguments;
use sqlx::PgPool;
use std::{collections::HashMap, rc::Rc};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Clone, Debug, EnumIter, Eq, Hash, PartialEq)]
enum Percentile {
    Top0_01,
    Top0_1,
    Top1,
    Top5,
    Top10,
    Top25,
    Top50,
    Top75,
}

impl Percentile {
    pub fn fraction(self) -> f64 {
        match self {
            Percentile::Top0_01 => 0.0001,
            Percentile::Top0_1 => 0.001,
            Percentile::Top1 => 0.01,
            Percentile::Top5 => 0.05,
            Percentile::Top10 => 0.1,
            Percentile::Top25 => 0.25,
            Percentile::Top50 => 0.5,
            Percentile::Top75 => 0.75,
        }
    }
}

#[derive(Debug)]
struct PercentileData {
    // TODO switch uom from kas to sompi
    percentile: Percentile,
    min_kas: Decimal,
    max_kas: Decimal,
    average_kas: Decimal,
    total_kas: Decimal,
    circulating_supply_percent: Decimal,
    address_count: u64,
}

impl PercentileData {
    pub fn new(percentile: Percentile) -> Self {
        Self {
            percentile,
            min_kas: dec!(0),
            max_kas: dec!(0),
            average_kas: dec!(0),
            total_kas: dec!(0),
            circulating_supply_percent: dec!(0),
            address_count: 0,
        }
    }
}

pub struct AddressPercentileAnalysis {
    pg_pool: PgPool,
    utxo_snapshot_id: i32,
    address_balances: Rc<HashMap<Address, u64>>,
    circulating_supply: u64,
    percentiles: Vec<PercentileData>,
}

impl AddressPercentileAnalysis {
    pub fn new(
        pg_pool: PgPool,
        utxo_snapshot_id: i32,
        address_balances: Rc<HashMap<Address, u64>>,
        circulating_supply: u64,
    ) -> Self {
        let percentiles = Percentile::iter()
            .map(|percentile| PercentileData::new(percentile.clone()))
            .collect();

        Self {
            pg_pool,
            utxo_snapshot_id,
            address_balances,
            circulating_supply,
            percentiles,
        }
    }

    pub async fn run(&mut self) {
        let mut balances: Vec<u64> = self.address_balances.values().cloned().collect();
        balances.sort_unstable_by(|a, b| b.cmp(a));

        let address_count = balances.len();

        for percentile in self.percentiles.iter_mut() {
            let fraction = percentile.percentile.clone().fraction();
            let top_count = ((address_count as f64) * fraction).ceil() as usize;

            let slice = &balances[0..top_count];

            // TODO temporarily the following are in KAS, instead of sompi.
            // convert back to sompi. requires update to all existing data in db first though
            percentile.min_kas =
                Decimal::from_f64(*slice.iter().min().unwrap() as f64 / SOMPI_PER_KAS as f64)
                    .unwrap();
            percentile.max_kas =
                Decimal::from_f64(*slice.iter().max().unwrap() as f64 / SOMPI_PER_KAS as f64)
                    .unwrap();

            let total_kas = slice.iter().sum::<u64>() as f64 / SOMPI_PER_KAS as f64;
            let address_count = slice.len() as u64;
            let circulating_supply_kas = self.circulating_supply / SOMPI_PER_KAS;

            percentile.average_kas = Decimal::from_f64(total_kas / address_count as f64)
                .unwrap()
                .round_dp(4);
            percentile.total_kas = Decimal::from_f64(total_kas).unwrap();
            percentile.circulating_supply_percent =
                Decimal::from_f64((total_kas / circulating_supply_kas as f64) * 100f64)
                    .unwrap()
                    .round_dp(2);
            percentile.address_count = address_count;

            info!("{:?}", percentile);
        }

        self.insert_to_db().await.unwrap();
    }

    async fn insert_to_db(&self) -> Result<(), sqlx::Error> {
        let sql = "
            INSERT INTO percentile_analysis (
                utxo_snapshot_id,

                min_kas_top_point01_percent,
                avg_kas_top_point01_percent,
                total_kas_top_point01_percent,
                addr_count_top_point01_percent,
                cs_percent_top_point01_percent,

                min_kas_top_point10_percent,
                avg_kas_top_point10_percent,
                total_kas_top_point10_percent,
                addr_count_top_point10_percent,
                cs_percent_top_point10_percent,

                min_kas_top_1_percent,
                avg_kas_top_1_percent,
                total_kas_top_1_percent,
                addr_count_top_1_percent,
                cs_percent_top_1_percent,

                min_kas_top_5_percent,
                avg_kas_top_5_percent,
                total_kas_top_5_percent,
                addr_count_top_5_percent,
                cs_percent_top_5_percent,

                min_kas_top_10_percent,
                avg_kas_top_10_percent,
                total_kas_top_10_percent,
                addr_count_top_10_percent,
                cs_percent_top_10_percent,

                min_kas_top_25_percent,
                avg_kas_top_25_percent,
                total_kas_top_25_percent,
                addr_count_top_25_percent,
                cs_percent_top_25_percent,

                min_kas_top_50_percent,
                avg_kas_top_50_percent,
                total_kas_top_50_percent,
                addr_count_top_50_percent,
                cs_percent_top_50_percent,

                min_kas_top_75_percent,
                avg_kas_top_75_percent,
                total_kas_top_75_percent,
                addr_count_top_75_percent,
                cs_percent_top_75_percent
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, 
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41
            )
        ";

        let mut args = sqlx::postgres::PgArguments::default();
        args.add(self.utxo_snapshot_id).unwrap();

        for p in &self.percentiles {
            args.add(p.min_kas.to_f64().unwrap()).unwrap();
            args.add(p.average_kas.to_f64().unwrap()).unwrap();
            args.add(p.total_kas.to_f64().unwrap()).unwrap();
            args.add(p.address_count as i64).unwrap();
            args.add(p.circulating_supply_percent.to_f64().unwrap())
                .unwrap();
        }

        sqlx::query_with(sql, args).execute(&self.pg_pool).await?;

        Ok(())
    }
}
