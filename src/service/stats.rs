use kaspa_addresses::Address;
use log::info;
use num_format::{Locale, ToFormattedString};
use std::collections::{BTreeMap, HashSet};

#[allow(dead_code)]
pub struct Stats {
    // Timestamps (in ms) of analysis window
    pub window_start_time: u64,
    pub window_end_time: u64,

    pub spc_block_count: u64,
    // non_spc_block_count: u64 TODO
    // blue_block_count: u64 TODO
    // red_block_count: u64 TODO
    // daa_count: u64 TODO
    // blocks_per_daa - mean, median, min, max TODO
    // blue_block_interval - mean, median, min, max TODO
    // blue_blocks_per_second - mean, median, min, max TODO

    // Accepted transactions per accepting (SPC) block
    pub transaction_count_per_spc_block: Vec<u64>,

    // Accepted transactions per block
    pub transaction_count_per_block: Vec<u64>,

    // Transactions related stats all include only accepted transactions
    pub coinbase_tx_count: u64,
    pub regular_tx_count: u64,
    pub input_count: u64,
    pub output_count_coinbase_tx: u64,
    pub output_count_regular_tx: u64,
    pub fees: Vec<u64>,

    // Count of inputs that didn't resolve to previous output
    pub input_count_missing_previous_outpoints: u64,

    // Count of transactions skipped due to an input not resolving to previous output
    pub skipped_tx_count_cannot_resolve_inputs: u64,

    pub unique_senders: HashSet<Address>,
    pub unique_sender_count: u64,
    pub unique_recipients: HashSet<Address>,
    pub unique_receipient_count: u64,
    pub unique_address_count: HashSet<Address>,

    pub transactions_per_second: BTreeMap<u64, u64>,
}

impl Stats {
    pub fn new(window_start_time: u64, window_end_time: u64) -> Self {
        Self {
            window_start_time,
            window_end_time,

            spc_block_count: 0,

            transaction_count_per_spc_block: Vec::<u64>::new(),
            transaction_count_per_block: Vec::<u64>::new(),

            coinbase_tx_count: 0,
            regular_tx_count: 0,
            input_count: 0,
            output_count_coinbase_tx: 0,
            output_count_regular_tx: 0,
            fees: Vec::<u64>::new(),

            input_count_missing_previous_outpoints: 0,
            skipped_tx_count_cannot_resolve_inputs: 0,

            unique_senders: HashSet::<Address>::new(),
            unique_sender_count: 0,
            unique_recipients: HashSet::<Address>::new(),
            unique_receipient_count: 0,
            unique_address_count: HashSet::<Address>::new(),

            transactions_per_second: BTreeMap::<u64, u64>::new(),
        }
    }
}

impl Stats {
    fn vec_stats(&self, values: &[u64]) -> (u64, f64, f64, u64, u64) {
        let sum: u64 = values.iter().sum();
        let mean = sum as f64 / values.len() as f64;

        let min = *values.iter().min().unwrap();
        let max = *values.iter().max().unwrap();

        let median = {
            let mut sorted = values.to_owned();
            sorted.sort_unstable();
            let mid = sorted.len() / 2;
            if sorted.len() % 2 == 0 {
                (sorted[mid - 1] + sorted[mid]) as f64 / 2.0
            } else {
                sorted[mid] as f64
            }
        };

        (sum, mean, median, min, max)
    }

    fn per_second_stats(&self, map: &BTreeMap<u64, u64>) -> (f64, f64, u64, u64) {
        // Find the range of timestamps
        let min_time = *map.keys().next().unwrap();
        let max_time = *map.keys().last().unwrap();

        // Calculate the total number of seconds
        let total_seconds = (max_time - min_time + 1) as usize;

        // Generate a list of record counts per second
        let mut records_per_second = Vec::with_capacity(total_seconds);
        for timestamp in min_time..=max_time {
            records_per_second.push(*map.get(&timestamp).unwrap_or(&0));
        }

        // Sort the record counts to calculate median
        records_per_second.sort_unstable();

        // Calculate the total number of records
        let total_records: u64 = records_per_second.iter().sum();

        // Calculate mean (average)
        let mean = total_records as f64 / total_seconds as f64;

        // Calculate median
        let median = if total_seconds % 2 == 0 {
            let mid1 = total_seconds / 2;
            let mid2 = mid1 - 1;
            (records_per_second[mid1] + records_per_second[mid2]) as f64 / 2.0
        } else {
            records_per_second[total_seconds / 2] as f64
        };

        // Calculate min and max
        let min = *records_per_second.first().unwrap();
        let max = *records_per_second.last().unwrap();

        (mean, median, min, max)
    }

    pub fn unique_address_count(&self) -> u64 {
        self.unique_senders
            .union(&self.unique_recipients)
            .collect::<HashSet<_>>()
            .len() as u64
    }
}

impl Stats {
    #[rustfmt::skip]
    pub fn log(&self) {
        info!("window_start_time: {}", self.window_start_time);
        info!("window_end_time: {}", self.window_end_time);

        // info!("spc_block_count: {}", self.spc_block_count.to_formatted_string(&Locale::en));

        // info!("transaction_count_per_spc_block - mean: {}", self.transaction_count_per_spc_block.iter().sum::<u64>() / self.transaction_count_per_spc_block.len() as u64);
        // info!("transaction_count_per_block - mean: {:?}", self.transaction_count_per_block.iter().sum::<u64>() / self.transaction_count_per_block.len() as u64);

        info!("coinbase_tx_count: {}", self.coinbase_tx_count.to_formatted_string(&Locale::en));
        info!("regular_tx_count: {}", self.regular_tx_count.to_formatted_string(&Locale::en));
        info!("input_count: {}", self.input_count.to_formatted_string(&Locale::en));
        info!("output_count_coinbase_tx: {}", self.output_count_coinbase_tx.to_formatted_string(&Locale::en));
        info!("output_count_regular_tx: {}", self.output_count_regular_tx.to_formatted_string(&Locale::en));

        let fees = self.vec_stats(&self.fees);
        info!("fees - total: {}", (fees.0 / 100_000_000).to_formatted_string(&Locale::en));
        info!("fees - mean: {}", (fees.1 / 100_000_000.0));
        info!("fees - median: {}", (fees.2 / 100_000_000.0));
        info!("fees - min: {}", (fees.3 as f64 / 100_000_000.0));
        info!("fees - max: {}", (fees.4 as f64 / 100_000_000.0));

        info!("input_count_missing_previous_outpoints: {}", self.input_count_missing_previous_outpoints.to_formatted_string(&Locale::en));
        info!("skipped_tx_count_cannot_resolve_inputs: {}", self.skipped_tx_count_cannot_resolve_inputs.to_formatted_string(&Locale::en));

        info!("unique_sender_count: {}", self.unique_senders.len().to_formatted_string(&Locale::en));
        info!("unique_receipient_count: {}", self.unique_recipients.len().to_formatted_string(&Locale::en));
        info!("unique_address_count: {}", self.unique_address_count().to_formatted_string(&Locale::en));

        let tps_stats = self.per_second_stats(&self.transactions_per_second);
        info!("TPS mean: {}", (tps_stats.0));
        info!("TPS median: {}", (tps_stats.1));
        info!("TPS min: {}", tps_stats.2.to_formatted_string(&Locale::en));
        info!("TPS max: {}", tps_stats.3.to_formatted_string(&Locale::en));
    }
}
