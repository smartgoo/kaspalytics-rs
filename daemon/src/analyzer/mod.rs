mod mining;
mod tx_counter;

use crate::cache::dag::{DagCache, Reader};
use log::info;
use sqlx::{self, PgPool};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::time::sleep;

pub struct Analyzer {
    cache: Arc<DagCache>,
    pg_pool: PgPool,
    shutdown_indicator: Arc<AtomicBool>,
}

impl Analyzer {
    pub fn new(cache: Arc<DagCache>, pg_pool: PgPool, shutdown_indicator: Arc<AtomicBool>) -> Self {
        Analyzer {
            cache,
            pg_pool,
            shutdown_indicator,
        }
    }
}

impl Analyzer {
    async fn shutdown(&self) {
        // TODO no shutdown logic currently.
        // Though will need it soon, so plumbing is in place here
        info!("Analyzer shutting down...");
    }

    pub async fn run(&self) {
        while !self.shutdown_indicator.load(Ordering::Relaxed) {
            // Skip until cache is at DAG tip
            if !self.cache.synced() {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let _ = tx_counter::run(self.cache.clone(), &self.pg_pool).await;
            let _ = mining::run(self.cache.clone(), &self.pg_pool).await;

            info!("Analyzer completed, sleeping");
            sleep(Duration::from_secs(15)).await;
        }

        self.shutdown().await;
    }
}
