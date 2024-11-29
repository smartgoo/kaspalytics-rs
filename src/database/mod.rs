mod database;
pub mod initialize;

pub use database::Database;

use strum_macros::{Display, EnumIter};

#[derive(Debug, Display, EnumIter)]
pub enum Meta {
    CheckpointBlockHash,
    Network,
    NetworkSuffix,
}
