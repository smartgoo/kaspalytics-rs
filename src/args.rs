use clap::Parser;
use kaspa_consensus_core::network::NetworkType;
use kaspa_wrpc_client::WrpcEncoding;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long)]
    pub db_url: String,

    #[arg(long)]
    pub reset_db: bool,

    #[arg(long)]
    pub app_dir: Option<String>,

    #[arg(long)]
    pub network: NetworkType,

    #[arg(long)]
    pub netsuffix: Option<u32>,

    #[arg(long)]
    pub rpc_url: Option<String>,

    #[arg(long)]
    pub rpc_encoding: Option<WrpcEncoding>,
}
