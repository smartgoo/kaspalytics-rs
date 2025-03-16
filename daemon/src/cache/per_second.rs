use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

#[derive(thiserror::Error, Debug)]
pub enum PayloadParseError {
    #[error("First byte 0xaa indicates address payload")]
    InvalidFirstByte,

    #[error("Payload split error")]
    SplitError,
}

fn parse_payload_node_version(payload: Vec<u8>) -> Result<String, PayloadParseError> {
    // let mut version = payload[16];
    let length = payload[18];
    let script = &payload[19_usize..(19 + length as usize)];

    if script[0] == 0xaa {
        return Err(PayloadParseError::InvalidFirstByte);
    }
    // if script[0] < 0x76 { ... }

    let payload_str = payload[19_usize + (length as usize)..]
        .iter()
        .map(|&b| b as char)
        .collect::<String>();

    let node_version = &payload_str
        .split("/")
        .next()
        .ok_or(PayloadParseError::SplitError)?;

    Ok(String::from(*node_version))
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SecondMetrics {
    pub block_count: AtomicU64,
    // red_block_count TODO
    pub mining_node_version_block_counts: DashMap<String, u64>, // TODO Atomic?
    // pub miner_block_counts: DashMap<Hash, u64>,
    pub transaction_count: AtomicU64,
    pub effective_transaction_count: AtomicU64,
}

impl SecondMetrics {
    pub fn add_block(&self, coinbase_tx_payload: Vec<u8>) {
        self.block_count.fetch_add(1, Ordering::SeqCst);

        let node_version = parse_payload_node_version(coinbase_tx_payload).unwrap();
        self.mining_node_version_block_counts
            .entry(node_version)
            .and_modify(|v| *v += 1)
            .or_insert(1);
    }

    pub fn add_transaction(&self) {
        self.transaction_count.fetch_add(1, Ordering::SeqCst);
    }

    pub fn add_transaction_acceptance(&self) {
        self.effective_transaction_count
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn remove_transaction_acceptance(&self) {
        self.effective_transaction_count
            .fetch_sub(1, Ordering::SeqCst);
    }
}
