use clap::{Args, Parser, Subcommand};
use log::LevelFilter;

#[derive(Parser)]
pub struct Cli {
    #[clap(flatten)]
    pub global_args: GlobalArgs,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Block based analysis (block data, transaction data, etc.)
    BlockPipeline {
        /// Analysis window start time, in unix milliseconds
        start_time: Option<u64>,

        /// Analysis window end time, in unix milliseconds
        end_time: Option<u64>,
    },

    /// Query CoinGecko market_history endpoint
    CoinMarketHistory,

    /// Home page data refresh
    /// temporary until time to write better solution
    HomePageRefresh,

    /// Save current DAA score and timestamp to DB
    SnapshotDaa,

    /// Save current hashrate and timestamp to DB
    SnapshotHashRate,

    /// UTXO based analysis
    UtxoPipeline,
}

#[derive(Args)]
pub struct GlobalArgs {
    /// Log level
    #[clap(long, global = true, default_value_t = LevelFilter::Info)]
    pub log_level: LevelFilter,
}
