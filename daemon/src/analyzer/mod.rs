mod mining;
mod tx_counter;

use crate::ingest::cache::{DagCache, Reader};
use crate::storage::Storage;
use crate::AppContext;
use log::info;
use sqlx::{self, PgPool};
use std::sync::atomic::AtomicBool;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::time::sleep;

pub struct Analyzer {
    dag_cache: Arc<DagCache>,
    storage: Arc<Storage>,
    pg_pool: PgPool,
    shutdown_flag: Arc<AtomicBool>,
}

impl Analyzer {
    pub fn new(context: Arc<AppContext>) -> Self {
        Analyzer {
            dag_cache: context.dag_cache.clone(),
            storage: context.storage.clone(),
            pg_pool: context.pg_pool.clone(),
            shutdown_flag: context.shutdown_flag.clone(),
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
        while !self.shutdown_flag.load(Ordering::Relaxed) {
            // Skip until cache is at DAG tip
            if !self.dag_cache.synced() {
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            let _ = tx_counter::run(self.dag_cache.clone(), &self.pg_pool).await;
            let _ = mining::run(self.dag_cache.clone(), &self.pg_pool).await;

            info!("Analyzer completed, sleeping");
            sleep(Duration::from_secs(15)).await;
        }

        self.shutdown().await;

        info!("Analyzer shut down complete");
    }
}
