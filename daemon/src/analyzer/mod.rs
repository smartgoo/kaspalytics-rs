// mod mining;
mod tx_counter;

use crate::cache::Cache;
use kaspalytics_utils::config::Config;
use log::info;
use sqlx::{self, PgPool};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::time::sleep;

pub struct Analyzer {
    config: Config,
    cache: Arc<Cache>,
    pg_pool: PgPool,
    shutdown_flag: Arc<AtomicBool>,
}

impl Analyzer {
    pub fn new(
        config: Config,
        cache: Arc<Cache>,
        pg_pool: PgPool,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Self {
        Analyzer {
            config,
            cache,
            pg_pool,
            shutdown_flag,
        }
    }
}

impl Analyzer {
    async fn shutdown(&self) {
        // TODO no shutdown logic currently.
        // Though will need it soon, so plumbing is in place here
        info!("Exited loop, shutting down...");
    }

    pub async fn run(&self) {
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            // Skip until cache is at DAG tip
            if !self.cache.synced() {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let start = std::time::Instant::now();

            let _ = tx_counter::run(&self.config, &self.pg_pool).await;
            // let _ = mining::run(self.cache.clone(), &self.pg_pool).await;

            info!("Ran in {}ms. Sleeping", start.elapsed().as_millis());
            sleep(Duration::from_secs(30)).await;
        }

        self.shutdown().await;
    }
}
