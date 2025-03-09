mod mining;
mod tx_counter;

use crate::cache::Cache;
use log::info;
use sqlx::{self, PgPool};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;

pub struct Analyzer {
    cache: Arc<Cache>,
    pg_pool: PgPool,
    shutdown_flag: Arc<AtomicBool>,
}

impl Analyzer {
    pub fn new(cache: Arc<Cache>, pg_pool: PgPool) -> Self {
        Analyzer {
            cache,
            pg_pool,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Analyzer {
    async fn main_loop(&self) {
        while !self.shutdown_flag.load(Ordering::SeqCst) {
            // Skip until cache is at DAG tip
            if !self.cache.synced() {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let _ = tx_counter::run(self.cache.clone(), self.pg_pool.clone()).await;
            let _ = mining::run(self.cache.clone(), self.pg_pool.clone()).await;

            info!("Analyzer completed, sleeping");
            sleep(Duration::from_secs(30)).await;
        }
    }

    async fn shutdown(&self) {
        // TODO no shutdown logic currently.
        // Though will need it soon, so plumbing is in place here
        info!("Analyzer shutting down...");
    }

    pub async fn run(&self, mut shutdown_rx: Receiver<()>) {
        // TODO error handling for everything in here

        let shutdown_flag = self.shutdown_flag.clone();

        let _ = tokio::join!(
            tokio::spawn(async move {
                shutdown_rx.recv().await.unwrap();
                shutdown_flag.store(true, Ordering::SeqCst);
            }),
            async { self.main_loop().await },
        );

        self.shutdown().await;
    }
}
