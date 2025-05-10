pub mod initialize;
mod pg;
pub mod sql;

pub use pg::Database;

use strum_macros::{Display, EnumIter};

#[derive(Debug, Display, EnumIter)]
pub enum Meta {
    Network,
    NetworkSuffix,
}
