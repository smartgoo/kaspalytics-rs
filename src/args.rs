use clap::{Parser, ValueEnum};

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long)]
    pub db_url: String,

    #[arg(long)]
    pub reset_db: bool,

    #[arg(long)]
    pub kaspad_app_dir: Option<String>,

    #[arg(long)]
    pub kaspad_network: Network,

    #[arg(long)]
    pub kaspad_network_suffix: Option<u32>,

    #[arg(long)]
    pub kaspad_rpc_url: String,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum Network {
    Mainnet,
    Testnet,
    Devnet,
    Simnet
}