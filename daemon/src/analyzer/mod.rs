mod mining;
mod tx_counter;

use crate::cache::Cache;
use log::info;
use sqlx::{self, PgPool};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::time::sleep;

pub struct Analyzer {
    cache: Arc<Cache>,
    pg_pool: PgPool,
}

impl Analyzer {
    pub fn new(cache: Arc<Cache>, pg_pool: PgPool) -> Self {
        Analyzer { cache, pg_pool }
    }
}

impl Analyzer {
    pub async fn run(&self) {
        // TODO error handling for everything in here
        loop {
            // Skip until cache is at DAG tip
            if !self.cache.synced.load(Ordering::SeqCst) {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let _ = tx_counter::run(self.cache.clone(), self.pg_pool.clone()).await;
            let _ = mining::run(self.cache.clone(), self.pg_pool.clone()).await;

            info!("Analyzer completed, sleeping");
            sleep(Duration::from_secs(30)).await;
        }
    }
}
