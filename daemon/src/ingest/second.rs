use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(thiserror::Error, Debug)]
pub enum PayloadParseError {
    #[error("Payload less than 19 bytes")]
    InsufficientPayloadLength,

    #[error("First byte 0xaa indicates address payload")]
    InvalidFirstByte,

    #[error("Payload split error")]
    SplitError,
}

fn parse_payload_node_version(payload: Vec<u8>) -> Result<(String, String), PayloadParseError> {
    if payload.len() < 19 {
        return Err(PayloadParseError::InsufficientPayloadLength);
    }

    let length = payload[18] as usize;

    if payload.len() < 19 + length {
        return Err(PayloadParseError::InsufficientPayloadLength);
    }

    let script = &payload[19..19 + length];
    if script.is_empty() || script[0] == 0xaa {
        return Err(PayloadParseError::InvalidFirstByte);
    }

    let payload_str = payload[19 + length..]
        .iter()
        .map(|&b| b as char)
        .collect::<String>();

    let parts: Vec<&str> = payload_str.splitn(2, '/').collect();
    match parts.len() {
        1 => Ok((parts[0].to_string(), "".to_string())),
        2 => {
            // TODO miner_prefix needs work....
            let miner_parts: Vec<&str> = parts[1].splitn(2, '/').collect();
            let miner_prefix = miner_parts[0].to_string();
            Ok((parts[0].to_string(), miner_prefix))
        }
        _ => Err(PayloadParseError::SplitError),
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SecondMetrics {
    pub block_count: AtomicU64,
    // blu_block_count TODO
    // red_block_count TODO
    pub mining_node_version_block_counts: DashMap<String, u64>, // TODO Atomic?

    pub coinbase_transaction_count: u64,
    pub coinbase_accepted_transaction_count: u64,
    pub transaction_count: u64,
    pub unique_transaction_count: u64,
    pub unique_transaction_accepted_count: u64,

    pub updated_at: DateTime<Utc>,
}

impl SecondMetrics {
    pub fn add_block(&mut self, coinbase_tx_payload: Vec<u8>) {
        self.block_count.fetch_add(1, Ordering::Relaxed);

        let (node_version, _) = parse_payload_node_version(coinbase_tx_payload).unwrap();
        self.mining_node_version_block_counts
            .entry(node_version)
            .and_modify(|v| *v += 1)
            .or_insert(1);

        self.updated_at = Utc::now();
    }

    pub fn increment_coinbase_transaction_count(&mut self) {
        self.coinbase_transaction_count += 1;

        self.updated_at = Utc::now();
    }

    pub fn increment_coinbase_accepted_transaction_count(&mut self) {
        self.coinbase_accepted_transaction_count += 1;

        self.updated_at = Utc::now();
    }

    pub fn decrement_coinbase_accepted_transaction_count(&mut self) {
        self.coinbase_accepted_transaction_count -= 1;

        self.updated_at = Utc::now();
    }

    pub fn increment_transaction_count(&mut self) {
        self.transaction_count += 1;

        self.updated_at = Utc::now();
    }

    pub fn increment_unique_transaction_count(&mut self) {
        self.unique_transaction_count += 1;

        self.updated_at = Utc::now();
    }

    pub fn increment_unique_transaction_accepted_count(&mut self) {
        self.unique_transaction_accepted_count += 1;

        self.updated_at = Utc::now();
    }

    pub fn decrement_unique_transaction_accepted_count(&mut self) {
        self.unique_transaction_accepted_count -= 1;

        self.updated_at = Utc::now();
    }
}
