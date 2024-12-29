use clap::{Parser, Subcommand};

#[derive(Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    BlockAnalysis {
        /// Analysis window start time, in unix milliseconds
        start_time: Option<u64>,

        /// Analysis window end time, in unix milliseconds
        end_time: Option<u64>,
    },

    /// Reset database (drop entire database and recreate). Can only be used in dev env.
    ResetDb,
}
