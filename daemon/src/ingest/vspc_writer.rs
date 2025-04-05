use crate::cache::Cache;
use kaspa_rpc_core::GetVirtualChainFromBlockResponse;
use kaspalytics_utils::config::Config;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc::Receiver;

pub struct VspcWriter {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    cache: Arc<Cache>,
    rx: Receiver<GetVirtualChainFromBlockResponse>,

    batch_size: u64,
    batch: Vec<GetVirtualChainFromBlockResponse>,
}

impl VspcWriter {
    pub fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        cache: Arc<Cache>,
        rx: Receiver<GetVirtualChainFromBlockResponse>,
    ) -> Self {
        VspcWriter {
            config,
            shutdown_flag,
            cache,
            rx,
            batch_size: 1000, // TODO
            batch: vec![],
        }
    }

    async fn handle_vspc(&mut self, vspc: GetVirtualChainFromBlockResponse) {
        if self.cache.vspc_ingest_synced() {
            self.batch_size = 10
        } else {
            self.batch_size = 1000
        }

        self.batch.push(vspc);

        if self.batch.len() as u64 >= self.batch_size {
            // TODO write to DB
            self.batch.clear();
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.rx.recv().await {
            if self.shutdown_flag.load(Ordering::Relaxed) {
                break;
            }

            self.handle_vspc(msg).await;
        }
    }
}
