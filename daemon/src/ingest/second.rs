use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SecondMetrics {
    pub block_count: AtomicU64,
    // blue_block_count TODO
    // red_block_count TODO
    pub mining_node_version_block_counts: DashMap<String, u64>, // TODO Atomic?

    // Count of all transactions - coinbase + standard
    pub transaction_count: AtomicU64,

    // Count of coinbase transactions
    pub coinbase_transaction_count: AtomicU64,
    pub coinbase_accepted_transaction_count: AtomicU64,

    // Count of unique standard (non-coinbase) transactions
    pub unique_transaction_count: AtomicU64,
    pub unique_accepted_transaction_count: AtomicU64,

    // Total fees paid this second
    pub total_fees: AtomicU64,

    // Count of various protocol transactions
    pub kasia_transaction_count: AtomicU64,
    pub krc_transaction_count: AtomicU64,
    pub kns_transaction_count: AtomicU64,

    pub updated_at: DateTime<Utc>,
}

impl SecondMetrics {
    pub fn increment_block_count(&mut self) {
        self.block_count.fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_coinbase_transaction_count(&mut self) {
        self.coinbase_transaction_count
            .fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_coinbase_accepted_transaction_count(&mut self) {
        self.coinbase_accepted_transaction_count
            .fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn decrement_coinbase_accepted_transaction_count(&mut self) {
        self.coinbase_accepted_transaction_count
            .fetch_sub(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_transaction_count(&mut self) {
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_unique_transaction_count(&mut self) {
        self.unique_transaction_count
            .fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_unique_accepted_transaction_count(&mut self) {
        self.unique_accepted_transaction_count
            .fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn decrement_unique_accepted_transaction_count(&mut self) {
        self.unique_accepted_transaction_count
            .fetch_sub(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_total_fees(&mut self, fee: u64) {
        self.total_fees.fetch_add(fee, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn decrement_total_fees(&mut self, fee: u64) {
        self.total_fees.fetch_sub(fee, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_kasia_transaction_count(&mut self) {
        self.kasia_transaction_count.fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn decrement_kasia_transaction_count(&mut self) {
        self.kasia_transaction_count.fetch_sub(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_krc_transaction_count(&mut self) {
        self.krc_transaction_count.fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn decrement_krc_transaction_count(&mut self) {
        self.krc_transaction_count.fetch_sub(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn increment_kns_transaction_count(&mut self) {
        self.kns_transaction_count.fetch_add(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }

    pub fn decrement_kns_transaction_count(&mut self) {
        self.kns_transaction_count.fetch_sub(1, Ordering::Relaxed);
        self.updated_at = Utc::now();
    }
}
