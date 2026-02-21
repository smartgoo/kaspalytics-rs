mod insert;
mod model;

use crate::analysis::address_activity::AddressActivityWindow;
use crate::ingest::model::{CacheTransaction, PruningBatch};
use chrono::{DateTime, Utc};
use insert::*;
use kaspa_consensus_core::subnets::SUBNETWORK_ID_NATIVE;
use kaspalytics_utils::log::LogTarget;
use log::{debug, info};
use model::DbNotableTx;
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

/// Tracks the top-`max_size` transactions by fee and by total output amount.
/// Gates DB writes: a transaction is only persisted if it belongs to either set.
struct NotableTransactionTracker {
    top_by_fee: BTreeSet<(u64, Vec<u8>)>,
    top_by_amount: BTreeSet<(u64, Vec<u8>)>,
    /// Set of transaction_id bytes currently stored in DB.
    db_state: HashSet<Vec<u8>>,
    max_size: usize,
}

impl NotableTransactionTracker {
    fn new(max_size: usize) -> Self {
        Self {
            top_by_fee: BTreeSet::new(),
            top_by_amount: BTreeSet::new(),
            db_state: HashSet::new(),
            max_size,
        }
    }

    /// Load existing notable transactions from the DB on startup.
    async fn load_from_db(pool: &PgPool, max_size: usize) -> Self {
        let mut tracker = Self::new(max_size);

        let rows = sqlx::query(
            "SELECT transaction_id, fee, total_output_amount
             FROM kaspad.notable_transactions",
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        for row in rows {
            use sqlx::Row;
            let tx_id: Vec<u8> = row.get("transaction_id");
            let fee: Option<i64> = row.try_get("fee").ok().flatten();
            let amount: Option<i64> = row.try_get("total_output_amount").ok().flatten();

            let fee_u64 = fee.unwrap_or(0) as u64;
            let amount_u64 = amount.unwrap_or(0) as u64;

            tracker.top_by_fee.insert((fee_u64, tx_id.clone()));
            tracker.top_by_amount.insert((amount_u64, tx_id.clone()));
            tracker.db_state.insert(tx_id);
        }

        // Trim to max_size if DB somehow has more rows than expected.
        while tracker.top_by_fee.len() > max_size {
            tracker.top_by_fee.pop_first();
        }
        while tracker.top_by_amount.len() > max_size {
            tracker.top_by_amount.pop_first();
        }

        info!(
            target: LogTarget::Daemon.as_str(),
            "NotableTransactionTracker: loaded {} transactions from DB",
            tracker.db_state.len()
        );

        tracker
    }

    /// Evaluate a transaction against the current top sets.
    ///
    /// Returns `(to_insert, to_delete)` — DB rows to add and tx_ids to remove.
    fn evaluate(&mut self, tx: &CacheTransaction) -> (Vec<DbNotableTx>, Vec<Vec<u8>>) {
        let fee = tx.fee.unwrap_or(0);
        let amount: u64 = tx.outputs.iter().map(|o| o.value).sum();
        let tx_id = tx.id.as_bytes().to_vec();

        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        let mut newly_notable = false;

        // -- Fee set --
        let min_fee = self.top_by_fee.iter().next().map(|(f, _)| *f).unwrap_or(0);
        let qualifies_fee = self.top_by_fee.len() < self.max_size || fee > min_fee;

        if qualifies_fee {
            let entry_fee = (fee, tx_id.clone());
            if !self.top_by_fee.contains(&entry_fee) {
                if self.top_by_fee.len() >= self.max_size {
                    if let Some(evicted) = self.top_by_fee.iter().next().cloned() {
                        self.top_by_fee.remove(&evicted);
                        // Only delete from DB if also absent from amount set.
                        let still_notable =
                            self.top_by_amount.iter().any(|(_, id)| id == &evicted.1);
                        if !still_notable && self.db_state.contains(&evicted.1) {
                            to_delete.push(evicted.1);
                        }
                    }
                }
                self.top_by_fee.insert(entry_fee);
                newly_notable = true;
            }
        }

        // -- Amount set --
        let min_amount = self
            .top_by_amount
            .iter()
            .next()
            .map(|(a, _)| *a)
            .unwrap_or(0);
        let qualifies_amount = self.top_by_amount.len() < self.max_size || amount > min_amount;

        if qualifies_amount {
            let entry_amount = (amount, tx_id.clone());
            if !self.top_by_amount.contains(&entry_amount) {
                if self.top_by_amount.len() >= self.max_size {
                    if let Some(evicted) = self.top_by_amount.iter().next().cloned() {
                        self.top_by_amount.remove(&evicted);
                        // Only delete from DB if also absent from fee set.
                        let still_notable =
                            self.top_by_fee.iter().any(|(_, id)| id == &evicted.1);
                        if !still_notable && self.db_state.contains(&evicted.1) {
                            to_delete.push(evicted.1);
                        }
                    }
                }
                self.top_by_amount.insert(entry_amount);
                newly_notable = true;
            }
        }

        // -- DB insert --
        let to_insert = if newly_notable && !self.db_state.contains(&tx_id) {
            self.db_state.insert(tx_id.clone());
            vec![DbNotableTx::new(tx, fee, amount)]
        } else {
            vec![]
        };

        // Remove evicted entries from db_state.
        for id in &to_delete {
            self.db_state.remove(id);
        }

        (to_insert, to_delete)
    }
}

pub struct Writer {
    pg_pool: PgPool,
    rx: Receiver<PruningBatch>,
    stats: Arc<Mutex<WriterStats>>,
    notable_tracker: NotableTransactionTracker,
    address_window: AddressActivityWindow,
    /// Minute-aligned cutoff (unix ms). Computed from the first batch on startup.
    /// Minutely rows >= cutoff are deleted, and transactions in the partial minute
    /// before the cutoff are skipped to avoid double-counting on any restart.
    resync_cutoff_ms: Option<u64>,
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
            // Initialised in run() after loading from DB.
            notable_tracker: NotableTransactionTracker::new(1000),
            address_window: AddressActivityWindow::new(1000, 24 * 60 * 60 * 1000),
            resync_cutoff_ms: None,
        }
    }
}

/// Truncate a millisecond timestamp to the start of its minute.
fn minute_bucket(block_time_ms: u64) -> DateTime<Utc> {
    let truncated = (block_time_ms / 60_000) * 60_000;
    DateTime::<Utc>::from_timestamp_millis(truncated as i64).unwrap()
}

/// Round a millisecond timestamp UP to the next minute boundary.
/// If already aligned, returns the same value.
fn ceil_to_next_minute_ms(block_time_ms: u64) -> u64 {
    let remainder = block_time_ms % 60_000;
    if remainder == 0 {
        block_time_ms
    } else {
        block_time_ms + (60_000 - remainder)
    }
}

impl Writer {
    pub async fn handle(&mut self, batch: PruningBatch) {
        let start = Instant::now();

        // Only accepted native (non-coinbase) transactions.
        let accepted_native: Vec<&CacheTransaction> = batch
            .transactions
            .iter()
            .filter(|tx| {
                tx.accepting_block_hash.is_some() && tx.subnetwork_id == SUBNETWORK_ID_NATIVE
            })
            .collect();

        // On the first batch with accepted transactions, compute the resync cutoff
        // and delete stale minutely rows. This prevents double-counting on any
        // restart — whether from a fresh sync, an old cache, or a clean restart.
        if self.resync_cutoff_ms.is_none() {
            if let Some(earliest) = accepted_native.iter().map(|tx| tx.block_time).min() {
                let cutoff_ms = ceil_to_next_minute_ms(earliest);
                let cutoff_dt =
                    DateTime::<Utc>::from_timestamp_millis(cutoff_ms as i64).unwrap();

                delete_minutely_rows_from(cutoff_dt, &self.pg_pool)
                    .await
                    .unwrap();

                self.resync_cutoff_ms = Some(cutoff_ms);

                info!(
                    target: LogTarget::Daemon.as_str(),
                    "Writer: resync cutoff set to {}, deleted minutely rows at or after cutoff",
                    cutoff_dt,
                );
            }
        }

        // ── 1. Notable transactions ─────────────────────────────────────────
        let mut notable_insert: Vec<DbNotableTx> = Vec::new();
        let mut notable_delete: Vec<Vec<u8>> = Vec::new();

        for tx in &accepted_native {
            let (ins, del) = self.notable_tracker.evaluate(tx);
            notable_insert.extend(ins);
            notable_delete.extend(del);
        }

        // ── 2. Protocol activity — aggregate per (minute_bucket, protocol_id) ──
        let mut protocol_map: HashMap<(DateTime<Utc>, i32), (i64, i64)> = HashMap::new();

        for tx in &accepted_native {
            if let Some(protocol) = &tx.protocol {
                // Skip the partial minute at the resync boundary.
                if let Some(cutoff_ms) = self.resync_cutoff_ms {
                    if (tx.block_time / 60_000) * 60_000 < cutoff_ms {
                        continue;
                    }
                }
                let bucket = minute_bucket(tx.block_time);
                let fee = tx.fee.unwrap_or(0) as i64;
                let entry = protocol_map
                    .entry((bucket, protocol.clone() as i32))
                    .or_default();
                entry.0 += 1; // transaction_count
                entry.1 += fee; // fees_generated
            }
        }

        let protocol_rows: Vec<(DateTime<Utc>, i32, i64, i64)> = protocol_map
            .into_iter()
            .map(|((bucket, pid), (cnt, fees))| (bucket, pid, cnt, fees))
            .collect();

        // ── 3. Address activity — group by minute, then feed into window ─────
        // Group address spending by minute bucket (BTreeMap keeps minutes ordered).
        let mut by_minute: BTreeMap<u64, HashMap<String, (u64, u64)>> = BTreeMap::new();

        for tx in &accepted_native {
            let minute_ms = (tx.block_time / 60_000) * 60_000;
            // Skip the partial minute at the resync boundary.
            if let Some(cutoff_ms) = self.resync_cutoff_ms {
                if minute_ms < cutoff_ms {
                    continue;
                }
            }
            let minute_entries = by_minute.entry(minute_ms).or_default();
            let mut seen_in_tx: HashSet<&str> = HashSet::new();

            for input in &tx.inputs {
                if let Some(utxo) = &input.utxo_entry {
                    if let Some(addr) = &utxo.script_public_key_address {
                        let entry = minute_entries.entry(addr.clone()).or_default();
                        entry.1 += utxo.amount; // total_spent
                        if seen_in_tx.insert(addr.as_str()) {
                            entry.0 += 1; // tx_count (once per tx per address)
                        }
                    }
                }
            }
        }

        let mut addr_flushes: Vec<(u64, Vec<(String, u64, u64)>)> = Vec::new();
        for (minute_ms, entries) in by_minute {
            if let Some(flush) = self.address_window.ingest(minute_ms, entries) {
                addr_flushes.push(flush);
            }
        }

        // ── 4. Write to DB in one transaction ────────────────────────────────
        let insert_start = start.elapsed().as_millis();
        let mut db_tx = self.pg_pool.begin().await.unwrap();

        insert_notable_transactions(&notable_insert, &mut db_tx)
            .await
            .unwrap();
        delete_notable_transactions(&notable_delete, &mut db_tx)
            .await
            .unwrap();
        upsert_protocol_activity_minutely(&protocol_rows, &mut db_tx)
            .await
            .unwrap();

        for (minute_ms, addr_rows) in addr_flushes {
            let bucket = DateTime::<Utc>::from_timestamp_millis(minute_ms as i64).unwrap();
            upsert_address_activity_minutely(bucket, &addr_rows, &mut db_tx)
                .await
                .unwrap();
        }

        db_tx.commit().await.unwrap();
        let insert_end = start.elapsed().as_millis() - insert_start;

        let duration_ms = start.elapsed().as_millis() as u64;
        let mut stats = self.stats.lock().await;
        stats.batches_processed += 1;
        stats.total_handle_duration_ms += duration_ms;

        debug!(
            target: LogTarget::Daemon.as_str(),
            "Writer iter finished in {}ms (DB insert {}ms)",
            duration_ms,
            insert_end,
        );
    }

    fn shutdown(&self) {
        info!(target: LogTarget::Daemon.as_str(), "Writer shutting down...");
    }

    pub async fn run(&mut self) {
        // Load notable transaction state from DB before starting.
        self.notable_tracker = NotableTransactionTracker::load_from_db(&self.pg_pool, 1000).await;

        // Retention pruning task — runs every hour, deletes minutely rows older than 10 days.
        let prune_pool = self.pg_pool.clone();
        let prune_handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(3600));
            ticker.tick().await; // Skip first tick so pruning doesn't run at startup.

            loop {
                ticker.tick().await;
                if let Err(e) = prune_old_minutely_rows(10, &prune_pool).await {
                    log::error!(
                        target: LogTarget::Daemon.as_str(),
                        "Writer: pruning old minutely rows failed: {}",
                        e
                    );
                } else {
                    debug!(
                        target: LogTarget::Daemon.as_str(),
                        "Writer: pruned minutely rows older than 10 days"
                    );
                }
            }
        });

        // Monitor task
        let stats = Arc::clone(&self.stats);
        let log_handle = tokio::spawn(async move {
            let interval_duration = Duration::from_secs(10);
            let mut ticker = interval(interval_duration);
            ticker.tick().await; // Skip first tick to avoid logging immediately on startup.

            loop {
                ticker.tick().await;

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

        // Main loop — processes all pending batches before shutting down.
        while let Some(batch) = self.rx.recv().await {
            self.handle(batch).await;
        }

        // Flush the in-progress address minute on clean shutdown.
        if let Some((minute_ms, rows)) = self.address_window.flush() {
            let bucket = DateTime::<Utc>::from_timestamp_millis(minute_ms as i64).unwrap();
            if let Ok(mut db_tx) = self.pg_pool.begin().await {
                let _ = upsert_address_activity_minutely(bucket, &rows, &mut db_tx).await;
                let _ = db_tx.commit().await;
            }
        }

        log_handle.abort();
        prune_handle.abort();
        self.shutdown();

        info!(target: LogTarget::Daemon.as_str(), "Writer shut down complete");
    }
}
