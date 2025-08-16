mod insert;
mod model;

use crate::ingest::model::PruningBatch;
use kaspa_consensus_core::subnets::SUBNETWORK_ID_COINBASE;
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
    rx: Receiver<PruningBatch>,
    stats: Arc<Mutex<WriterStats>>,
}

struct WriterStats {
    batches_processed: u64,
    total_handle_duration_ms: u64,
}

impl Writer {
    pub fn new(pg_pool: PgPool, rx: Receiver<PruningBatch>) -> Self {
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
    pub async fn handle(&self, batch: PruningBatch) {
        let start = Instant::now();

        let mut block_queue = Vec::new();
        let mut blocks_parents_queue = Vec::new();
        let mut blocks_transactions_queue = Vec::new();
        let mut transaction_queue = Vec::new();
        let mut input_queue = Vec::new();
        let mut output_queue = Vec::new();
        // let mut address_transaction_queue = Vec::new();

        for block in batch.blocks {
            for parent in block.parent_hashes.iter() {
                blocks_parents_queue.push(DbBlockParent::new(block.hash, *parent, block.timestamp));
            }

            for (index, transaction_id) in block.transactions.iter().enumerate() {
                blocks_transactions_queue.push(DbBlockTransaction::new(
                    block.hash,
                    *transaction_id,
                    index as u16,
                    block.timestamp,
                ))
            }

            block_queue.push(DbBlock::from(block));
        }

        for tx in batch.transactions {
            for (index, input) in tx.inputs.iter().enumerate() {
                input_queue.push(DbTransactionInput::new(
                    tx.id,
                    index as u32,
                    input,
                    tx.block_time,
                ));

                // if input.utxo_entry.is_some() {
                //     address_transaction_queue.push(DbAddressTransaction::new(
                //         input
                //             .utxo_entry
                //             .as_ref()
                //             .unwrap()
                //             .script_public_key_address
                //             .as_ref()
                //             .unwrap()
                //             .clone(),
                //         tx.id,
                //         tx.block_time,
                //         1,
                //         input.utxo_entry.as_ref().unwrap().amount,
                //     ));
                // }
            }

            for (index, output) in tx.outputs.iter().enumerate() {
                output_queue.push(DbTransactionOutput::new(
                    tx.id,
                    index as u32,
                    output,
                    tx.block_time,
                    tx.subnetwork_id == SUBNETWORK_ID_COINBASE,
                ));
            }

            transaction_queue.push(DbTransaction::from(tx));
        }

        let insert_start = start.elapsed().as_millis();
        let mut tx = self.pg_pool.begin().await.unwrap();

        insert::insert_blocks_unnest(block_queue, &mut tx)
            .await
            .unwrap();
        insert::insert_blocks_parents_unnest(blocks_parents_queue, &mut tx)
            .await
            .unwrap();
        insert::insert_blocks_transactions_unnest(blocks_transactions_queue, &mut tx)
            .await
            .unwrap();
        insert::insert_transactions_unnest(transaction_queue, &mut tx)
            .await
            .unwrap();
        insert::insert_inputs_unnest(input_queue, &mut tx)
            .await
            .unwrap();
        insert::insert_outputs_unnest(output_queue, &mut tx)
            .await
            .unwrap();
        // insert::insert_address_transactions_unnest(address_transaction_queue, &mut tx)
        //     .await
        //     .unwrap();

        tx.commit().await.unwrap();
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
