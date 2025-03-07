use chrono::DateTime;
use kaspa_addresses::Address;
use sqlx::PgPool;
use std::collections::{BTreeMap, HashSet};
use std::fmt;

use kaspalytics_utils::granularity::Granularity;

#[allow(dead_code)]
#[derive(Clone)]
pub struct Stats {
    // Second, Minute, Hour, Day
    granularity: Granularity,

    // Timestamp of analysis window
    pub epoch_second: u64,

    // -----------------------------------
    // Block Summary
    pub chain_block_count: u64,
    // non_chain_block_count: u64 TODO-FUTURE
    // blue_block_count: u64 TODO-FUTURE
    // red_block_count: u64 TODO-FUTURE
    // daa_count: u64 TODO-FUTURE
    // blocks_per_daa - mean, median, min, max TODO-FUTURE
    // blue_block_interval - mean, median, min, max TODO-FUTURE
    // blue_blocks_per_second - mean, median, min, max TODO-FUTURE

    // Accepted transactions per block
    pub transaction_count_per_block: Vec<u64>,

    // -----------------------------------
    // Transaction Summary
    // Transactions related stats all include only accepted transactions
    pub coinbase_tx_count: u64,
    pub regular_tx_count: u64,
    pub input_count: u64,
    pub output_count_coinbase_tx: u64,
    pub output_count_regular_tx: u64,
    pub fees: Vec<u64>,

    // tps_max is not currently populated on per second records
    // only calculater on higher granularities. stores max tps inside the granularity
    pub tps_max: u64,

    // Count of inputs that didn't resolve to previous output
    pub input_count_missing_previous_outpoints: u64,

    // Count of transactions skipped due to an input not resolving to previous output
    pub skipped_tx_count_cannot_resolve_inputs: u64,

    pub unique_senders: HashSet<Address>,
    pub unique_recipients: HashSet<Address>,
    pub unique_addresses: HashSet<Address>,
}

impl Stats {
    pub fn new(epoch_second: u64, granularity: Granularity) -> Self {
        Self {
            granularity,
            epoch_second,
            chain_block_count: 0,
            transaction_count_per_block: Vec::<u64>::new(),
            coinbase_tx_count: 0,
            regular_tx_count: 0,
            input_count: 0,
            output_count_coinbase_tx: 0,
            output_count_regular_tx: 0,
            fees: Vec::<u64>::new(),
            tps_max: 0,
            input_count_missing_previous_outpoints: 0,
            skipped_tx_count_cannot_resolve_inputs: 0,
            unique_senders: HashSet::<Address>::new(),
            unique_recipients: HashSet::<Address>::new(),
            unique_addresses: HashSet::<Address>::new(),
        }
    }
}

impl Stats {
    fn vec_stats(&self, values: &[u64]) -> (u64, f64, f64, u64, u64) {
        let sum: u64 = values.iter().sum();
        let mean = (sum as f64) / (values.len() as f64);
        if values.is_empty() {
            return (0, 0.0, 0.0, 0, 0);
        }

        let min = *values.iter().min().unwrap_or(&0);
        let max = *values.iter().max().unwrap_or(&0);

        let median = {
            let mut sorted = values.to_owned();
            sorted.sort_unstable();
            let mid = sorted.len() / 2;
            if sorted.len() % 2 == 0 {
                ((sorted[mid - 1] + sorted[mid]) as f64) / 2.0
            } else {
                sorted[mid] as f64
            }
        };

        (sum, mean, median, min, max)
    }

    fn tps_mean(&self) -> f64 {
        match self.granularity {
            Granularity::Second => (self.coinbase_tx_count + self.regular_tx_count) as f64,
            Granularity::Minute => (self.coinbase_tx_count + self.regular_tx_count) as f64 / 60f64,
            Granularity::Hour => (self.coinbase_tx_count + self.regular_tx_count) as f64 / 3600f64,
            Granularity::Day => (self.coinbase_tx_count + self.regular_tx_count) as f64 / 86400f64,
        }
    }

    // fn tps_median - TODO, requires storing more data I think
    // fn tps_min - TODO, requires storing more data I think

    fn unique_sender_count(&self) -> u64 {
        self.unique_senders.len() as u64
    }

    fn unique_recipient_count(&self) -> u64 {
        self.unique_recipients.len() as u64
    }

    fn unique_address_count(&self) -> u64 {
        self.unique_senders
            .union(&self.unique_recipients)
            .collect::<HashSet<_>>()
            .len() as u64
    }
}

impl Stats {
    fn calculate_granularity_epoch(epoch_second: u64, granularity: &Granularity) -> u64 {
        match granularity {
            Granularity::Second => epoch_second,
            Granularity::Minute => (epoch_second / 60) * 60,
            Granularity::Hour => (epoch_second / 3600) * 3600,
            Granularity::Day => (epoch_second / 86400) * 86400,
        }
    }

    // "Rolls up" per second stats into target granularity
    // At this time, `per_second_stats` must be per second.
    // No other source granularity is supported.
    pub fn rollup(
        per_second_stats: &BTreeMap<u64, Stats>,
        target_granularity: Granularity,
    ) -> BTreeMap<u64, Stats> {
        let mut rolled_up: BTreeMap<u64, Stats> = BTreeMap::new();

        for (epoch_second, per_second_stats) in per_second_stats {
            let key = Self::calculate_granularity_epoch(*epoch_second, &target_granularity);

            rolled_up
                .entry(key)
                .and_modify(|new_stats| {
                    new_stats.chain_block_count += per_second_stats.chain_block_count;

                    new_stats
                        .transaction_count_per_block
                        .extend(per_second_stats.transaction_count_per_block.clone());

                    new_stats.coinbase_tx_count += per_second_stats.coinbase_tx_count;
                    new_stats.regular_tx_count += per_second_stats.regular_tx_count;
                    new_stats.input_count += per_second_stats.input_count;
                    new_stats.output_count_coinbase_tx += per_second_stats.output_count_coinbase_tx;
                    new_stats.output_count_regular_tx += per_second_stats.output_count_regular_tx;
                    new_stats.fees.extend(per_second_stats.fees.clone());

                    if per_second_stats.coinbase_tx_count + per_second_stats.regular_tx_count
                        > new_stats.tps_max
                    {
                        new_stats.tps_max =
                            per_second_stats.coinbase_tx_count + per_second_stats.regular_tx_count;
                    }

                    new_stats.input_count_missing_previous_outpoints +=
                        per_second_stats.input_count_missing_previous_outpoints;
                    new_stats.skipped_tx_count_cannot_resolve_inputs +=
                        per_second_stats.skipped_tx_count_cannot_resolve_inputs;

                    new_stats
                        .unique_senders
                        .extend(per_second_stats.unique_senders.clone());
                    new_stats
                        .unique_recipients
                        .extend(per_second_stats.unique_recipients.clone());
                    new_stats
                        .unique_addresses
                        .extend(per_second_stats.unique_addresses.clone());
                })
                .or_insert_with(|| {
                    let mut new_stats = per_second_stats.clone();
                    new_stats.granularity = target_granularity;
                    new_stats.epoch_second = key;
                    new_stats.tps_max =
                        per_second_stats.coinbase_tx_count + per_second_stats.regular_tx_count;
                    new_stats
                });
        }

        rolled_up
    }
}

impl Stats {
    async fn save_block_summary(&self, pool: &PgPool) {
        // TODO make sure date record isn't already in table
        let sql = r#"
            INSERT INTO block_summary
            (
                date, 
                chain_block_count, 
                txs_per_block_mean, txs_per_block_median, txs_per_block_min, txs_per_block_max
            )
            VALUES
            ($1, $2, $3, $4, $5, $6)
        "#;

        let date = DateTime::from_timestamp(self.epoch_second as i64, 0)
            .unwrap()
            .date_naive();

        let tpb = self.vec_stats(&self.transaction_count_per_block);

        sqlx::query(sql)
            .bind(date)
            .bind(self.chain_block_count as i64)
            .bind(tpb.1)
            .bind(tpb.2)
            .bind(tpb.3 as i64)
            .bind(tpb.4 as i64)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn save_transaction_summary(&self, pool: &PgPool) {
        // TODO make sure date record isn't already in table
        let sql = r#"
            INSERT INTO transaction_summary
            (   
                date, 
                coinbase_tx_qty, tx_qty, input_qty_total, output_qty_total_coinbase, output_qty_total, 
                fees_total, fees_mean, fees_median, fees_min, fees_max,
                skipped_tx_missing_inputs, inputs_missing_previous_outpoint,
                unique_senders, unique_recipients, unique_addresses, 
                tx_per_second_mean, tx_per_second_max
            )
            VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        "#;

        let date = DateTime::from_timestamp(self.epoch_second as i64, 0)
            .unwrap()
            .date_naive();
        let fees = self.vec_stats(&self.fees);
        let tps_mean = self.tps_mean();

        sqlx::query(sql)
            .bind(date)
            .bind(self.coinbase_tx_count as i64)
            .bind(self.regular_tx_count as i64)
            .bind(self.input_count as i64)
            .bind(self.output_count_coinbase_tx as i64)
            .bind(self.output_count_regular_tx as i64)
            .bind(fees.0 as i64)
            .bind(fees.1)
            .bind(fees.2)
            .bind(fees.3 as i64)
            .bind(fees.4 as i64)
            .bind(self.skipped_tx_count_cannot_resolve_inputs as i64)
            .bind(self.input_count_missing_previous_outpoints as i64)
            .bind(self.unique_senders.len() as i64)
            .bind(self.unique_recipients.len() as i64)
            .bind(self.unique_address_count() as i64)
            .bind(tps_mean)
            .bind(self.tps_max as i64)
            .execute(pool)
            .await
            .unwrap();
    }

    pub async fn save(&self, pool: &PgPool) {
        self.save_block_summary(pool).await;
        self.save_transaction_summary(pool).await;
    }
}

impl fmt::Debug for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tpb = self.vec_stats(&self.transaction_count_per_block);
        let fees = self.vec_stats(&self.fees);

        f.debug_struct("Stats")
            .field("epoch_second", &self.epoch_second)
            .field("granularity", &self.granularity)
            .field("chain_block_count", &self.chain_block_count)
            .field("transaction_count_per_block - mean", &tpb.1)
            .field("transaction_count_per_block - median", &tpb.2)
            .field("transaction_count_per_block - min", &tpb.3)
            .field("transaction_count_per_block - max", &tpb.4)
            .field("tps - mean", &self.tps_mean())
            .field("tps - max", &self.tps_max)
            .field("coinbase_tx_count", &self.coinbase_tx_count)
            .field("regular_tx_count", &self.regular_tx_count)
            .field("input_count", &self.input_count)
            .field("output_count_coinbase_tx", &self.output_count_coinbase_tx)
            .field("output_count_regular_tx", &self.output_count_regular_tx)
            .field("fees - total", &(fees.0 / 100_000_000))
            .field("fees - mean", &(fees.1 / 100_000_000.0))
            .field("fees - median", &(fees.2 / 100_000_000.0))
            .field("fees - min", &(fees.3 as f64 / 100_000_000.0))
            .field("fees - max", &(fees.4 as f64 / 100_000_000.0))
            .field(
                "input_count_missing_previous_outpoints",
                &self.input_count_missing_previous_outpoints,
            )
            .field(
                "skipped_tx_count_cannot_resolve_inputs",
                &self.skipped_tx_count_cannot_resolve_inputs,
            )
            .field("unique_senders", &self.unique_sender_count())
            .field("unique_recipients", &self.unique_recipient_count())
            .field("unique_addresses", &self.unique_address_count())
            .finish()
    }
}
