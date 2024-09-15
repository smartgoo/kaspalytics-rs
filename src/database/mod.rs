pub mod conn;
pub mod initialize;

use strum_macros::{Display, EnumIter};

#[derive(Debug, Display, EnumIter)]
pub enum Meta {
    CheckpointBlockHash,
    Network,
    NetworkSuffix,
}
