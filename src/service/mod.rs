pub mod blocks;
pub mod cache;
mod models;
pub mod vspc;

#[derive(Debug)]
pub enum Event {
    GetBlocksBatch,
    InitialSyncReachedTip,
}
