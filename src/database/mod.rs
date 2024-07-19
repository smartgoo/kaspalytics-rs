pub mod conn;
pub mod initialize;

use strum_macros::{Display, EnumIter};

#[derive(Debug, Display, EnumIter)]
pub enum Meta {
    CheckpointBlockHash,
    Network,
    NetworkSuffix,
}

#[derive(Debug, Display, EnumIter)]
pub enum Granularity {
    Second,
    Minute,
    Hour,
    Day,
    Month,
    Quarter,
    Year
}

#[derive(Debug, Display, EnumIter)]
pub enum DataPoint {
    TotalSpcBlocks,
    TotalNonSpcBlocks,
    TotalAcceptingBlocks,
    TotalMergedBlues,
    MeanTxPerSpcBlock,
    MedianTxPerSpcBlock,
    MinTxPerSpcBlock,
    MaxTxPerSpcBlock,
    MeanTxPerBlock,
    MedianTxPerBlock,
    MinTxPerBlock,
    MaxTxPerBlock,
}