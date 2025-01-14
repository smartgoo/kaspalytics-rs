use clap::{Args, Parser, Subcommand};
use log::LevelFilter;

#[derive(Parser)]
pub struct Cli {
    #[clap(flatten)]
    pub global_args: GlobalArgs,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Block based analysis (block data, transaction data, etc.)
    BlockPipeline {
        /// Analysis window start time, in unix milliseconds
        start_time: Option<u64>,

        /// Analysis window end time, in unix milliseconds
        end_time: Option<u64>,
    },

    SnapshotDaa,

    /// Reset database (drop entire database and recreate). Can only be used in dev env.
    ResetDb,

    /// UTXO based analysis
    UtxoPipeline,
}

#[derive(Args)]
pub struct GlobalArgs {
    /// Log level
    #[clap(long, global = true, default_value_t = LevelFilter::Info)]
    pub log_level: LevelFilter,
}