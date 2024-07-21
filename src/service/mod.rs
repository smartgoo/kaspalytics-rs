pub mod cache;
pub mod blocks;
mod models;
pub mod vspc;

#[derive(Debug)]
pub enum Event {
    GetBlocksBatch,
    InitialSyncReachedTip,
}