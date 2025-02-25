use crate::cache::Cache;
use log::info;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

pub struct Saver {
    cache: Arc<Cache>,
}

impl Saver {
    pub fn new(cache: Arc<Cache>) -> Self {
        Saver { cache }
    }
}

impl Saver {
    pub async fn run(&self) {
        loop {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs();

            let threshold = now - 86400;

            let effective_total: u64 = self
                .cache
                .per_second
                .iter()
                .filter(|entry| *entry.key() >= threshold)
                .map(|entry| entry.effective_transaction_count)
                .sum();

            let total: u64 = self
                .cache
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
