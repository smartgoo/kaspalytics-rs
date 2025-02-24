use crate::cache::Cache;
use log::info;
use tokio::time::sleep;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct TxCounter {
    cache: Arc<Cache>,
}

impl TxCounter {
    pub fn new(cache: Arc<Cache>) -> Self {
        TxCounter { cache }
    }
}

impl TxCounter {
    pub async fn run(&self) {
        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs();

            let threshold = now - 86400;

            let effective_total: u64 = self.cache
                .per_second
                .iter()
                .filter(|entry| *entry.key() >= threshold)
                .map(|entry| entry.effective_transaction_count)
                .sum();

            let total: u64 = self.cache
                .per_second
                .iter()
                .filter(|entry| *entry.key() >= threshold)
                .map(|entry| entry.transaction_count)
                .sum();

            info!("txs: {} | effective txs: {}", total, effective_total);

            sleep(Duration::from_secs(30)).await;
        }
    }
}
