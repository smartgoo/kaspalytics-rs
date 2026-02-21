use crate::ingest::model::CacheTransaction;
use chrono::{DateTime, Utc};

pub struct DbNotableTx {
    pub transaction_id: Vec<u8>,
    pub block_time: DateTime<Utc>,
    pub protocol_id: Option<i32>,
    pub fee: Option<i64>,
    pub total_output_amount: i64,
}

impl DbNotableTx {
    pub fn new(tx: &CacheTransaction, fee: u64, total_output_amount: u64) -> Self {
        DbNotableTx {
            transaction_id: tx.id.as_bytes().to_vec(),
            block_time: DateTime::<Utc>::from_timestamp_millis(tx.block_time as i64).unwrap(),
            protocol_id: tx.protocol.as_ref().map(|p| p.clone() as i32),
            fee: Some(fee as i64),
            total_output_amount: total_output_amount as i64,
        }
    }
}
