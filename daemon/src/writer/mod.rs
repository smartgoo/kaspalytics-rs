mod insert;
mod model;

use crate::ingest::model::PrunedBlock;
use kaspalytics_utils::log::LogTarget;
use log::{debug, info};
use model::*;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

pub struct Writer {
    pg_pool: PgPool,
    rx: Receiver<Vec<PrunedBlock>>,
    stats: Arc<Mutex<WriterStats>>,
}

struct WriterStats {
    batches_processed: u64,
    total_handle_duration_ms: u64,
}

impl Writer {
    pub fn new(pg_pool: PgPool, rx: Receiver<Vec<PrunedBlock>>) -> Self {
        Writer {
            pg_pool,
            rx,
            stats: Arc::new(Mutex::new(WriterStats {
                batches_processed: 0,
                total_handle_duration_ms: 0,
            })),
        }
    }
}

impl Writer {
    pub async fn handle(&self, blocks: Vec<PrunedBlock>) {
        let start = Instant::now();

        let mut block_queue = Vec::new();
        let mut transaction_queue = Vec::new();
        let mut input_queue = Vec::new();
        let mut output_queue = Vec::new();

        for block in blocks {
            for tx in block.transactions.iter() {
                for (index, input) in tx.inputs.iter().enumerate() {
                    input_queue.push(DbTransactionInput::new(
                        block.hash,
                        block.timestamp,
                        tx.id,
                        index as u32,
                        input,
                    ));
                }

                for (index, output) in tx.outputs.iter().enumerate() {
                    output_queue.push(DbTransactionOutput::new(
                        block.hash,
                        block.timestamp,
                        tx.id,
                        index as u32,
                        output,
                    ));
                }

                transaction_queue.push(DbTransaction::new(block.hash, tx));
            }

            block_queue.push(DbBlock::from(block));
        }

        let block_pool = self.pg_pool.clone();
        let transaction_pool = self.pg_pool.clone();
        let input_pool = self.pg_pool.clone();
        let output_pool = self.pg_pool.clone();

        let insert_start = start.elapsed().as_millis();
        tokio::try_join!(
            tokio::spawn(async {
                insert::insert_blocks_unnest(block_queue, block_pool)
                    .await
                    .unwrap();
            }),
            tokio::spawn(async {
                insert::insert_transactions_unnest(transaction_queue, transaction_pool)
                    .await
                    .unwrap();
            }),
            tokio::spawn(async {
                insert::insert_inputs_unnest(input_queue, input_pool)
                    .await
                    .unwrap();
            }),
            tokio::spawn(async {
                insert::insert_outputs_unnest(output_queue, output_pool)
                    .await
                    .unwrap();
            }),
        )
        .unwrap();
        let insert_end = start.elapsed().as_millis() - insert_start;

        let duration_ms = start.elapsed().as_millis() as u64;
        let mut stats = self.stats.lock().await;
        stats.batches_processed += 1;
        stats.total_handle_duration_ms += duration_ms;

        debug!(
            target: LogTarget::Daemon.as_str(),
            "Writer iter finished in {}ms (DB insert {}ms)",
            duration_ms, insert_end,
        );
    }

    fn shutdown(&self) {
        info!(target: LogTarget::Daemon.as_str(), "Writer shutting down...");
    }

    pub async fn run(&mut self) {
        // Monitor task
        let stats = Arc::clone(&self.stats);
        let log_handle = tokio::spawn(async move {
            let interval_duration = Duration::from_secs(10);
            let mut interval = interval(interval_duration);
            interval.tick().await; // Skip first to prevent logging immediately on startup

            loop {
                interval.tick().await;

                let mut stats = stats.lock().await;

                info!(
                    target: LogTarget::Daemon.as_str(),
                    "Writer Monitor (last {}s): Inserted {} batch(s). Avg batch insert time {}ms",
                    interval_duration.as_secs(),
                    stats.batches_processed,
                    if stats.batches_processed > 0 {
                        stats.total_handle_duration_ms / stats.batches_processed
                    } else {
                        0
                    }
                );

                stats.batches_processed = 0;
                stats.total_handle_duration_ms = 0;
            }
        });

        // Main loop
        while let Some(blocks) = self.rx.recv().await {
            // Writer does not check shutdown_flag
            // It must finish processing all messages in channel
            // channel returns None when closed and no remaining messages

            self.handle(blocks).await;
        }

        // Shutdown logger
        log_handle.abort();

        self.shutdown();

        info!(target: LogTarget::Daemon.as_str(), "Writer shut down complete");
    }
}
