use clap::Parser;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long)]
    pub start_time: Option<u64>,

    #[arg(long)]
    pub end_time: Option<u64>,

    #[arg(long)]
    pub reset_db: bool,
}
