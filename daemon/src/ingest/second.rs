use crate::analysis::transactions::protocol::TransactionProtocol;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use kaspalytics_utils::covenant::CovenantTally;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SecondMetrics {
    pub block_count: u64,
    // blue_block_count TODO
    // red_block_count TODO
    pub mining_node_version_block_counts: DashMap<String, u64>, // TODO Atomic?

    // Count of all transactions - coinbase + standard
    pub transaction_count: u64,

    // Count of coinbase transactions
    pub coinbase_transaction_count: u64,
    pub coinbase_accepted_transaction_count: u64,

    // Count of unique standard (non-coinbase) transactions
    pub unique_transaction_count: u64,
    pub unique_accepted_transaction_count: u64,

    // Total fees paid this second
    pub total_fees: u64,

    // Count of various protocol transactions
    pub protocol_transaction_counts: HashMap<TransactionProtocol, u64>,

    // Counts of accepted transaction outputs by script class (per output)
    pub output_count_pubkey: u64,
    pub output_count_pubkey_ecdsa: u64,
    pub output_count_script_hash: u64,
    pub output_count_nonstandard: u64,

    // Count of accepted transactions that use a covenant / introspection opcode
    pub introspection_opcode_tx_count: u64,

    // Count of accepted transactions that use the ZK precompile opcode
    pub zk_precompile_tx_count: u64,

    // Count of P2SH outputs spent by accepted transactions whose revealed redeem
    // script uses the ZK precompile opcode
    pub zk_precompile_outputs_spent: u64,

    // Count of accepted transactions that create at least one covenant-bound output
    pub covenant_creating_tx_count: u64,

    // Count of covenant-bound outputs created by accepted transactions
    pub covenant_outputs_created: u64,

    // Count of covenant-bound outputs spent by accepted transactions
    pub covenant_outputs_spent: u64,

    pub updated_at: DateTime<Utc>,
}

impl SecondMetrics {
    pub fn increment_block_count(&mut self) {
        self.block_count += 1;
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

    pub fn increment_unique_accepted_transaction_count(&mut self) {
        self.unique_accepted_transaction_count += 1;
        self.updated_at = Utc::now();
    }

    pub fn decrement_unique_accepted_transaction_count(&mut self) {
        self.unique_accepted_transaction_count -= 1;
        self.updated_at = Utc::now();
    }

    pub fn increment_total_fees(&mut self, fee: u64) {
        self.total_fees += fee;
        self.updated_at = Utc::now();
    }

    pub fn decrement_total_fees(&mut self, fee: u64) {
        self.total_fees -= fee;
        self.updated_at = Utc::now();
    }

    pub fn increment_protocol_transaction_count(&mut self, protocol: TransactionProtocol) {
        *self
            .protocol_transaction_counts
            .entry(protocol)
            .or_insert(0) += 1;
        self.updated_at = Utc::now();
    }

    pub fn decrement_protocol_transaction_count(&mut self, protocol: TransactionProtocol) {
        if let Some(count) = self.protocol_transaction_counts.get_mut(&protocol) {
            if *count > 0 {
                *count -= 1;
            }
        }
        self.updated_at = Utc::now();
    }

    pub fn get_protocol_transaction_count(&self, protocol: &TransactionProtocol) -> u64 {
        self.protocol_transaction_counts
            .get(protocol)
            .copied()
            .unwrap_or(0)
    }

    pub fn apply_script_covenant_delta(&mut self, delta: &CovenantTally) {
        self.output_count_pubkey += delta.output_count_pubkey;
        self.output_count_pubkey_ecdsa += delta.output_count_pubkey_ecdsa;
        self.output_count_script_hash += delta.output_count_script_hash;
        self.output_count_nonstandard += delta.output_count_nonstandard;
        if delta.uses_introspection_opcode {
            self.introspection_opcode_tx_count += 1;
        }
        if delta.uses_zk_precompile_opcode {
            self.zk_precompile_tx_count += 1;
        }
        self.zk_precompile_outputs_spent += delta.zk_precompile_outputs_spent;
        if delta.covenant_outputs_created > 0 {
            self.covenant_creating_tx_count += 1;
        }
        self.covenant_outputs_created += delta.covenant_outputs_created;
        self.covenant_outputs_spent += delta.covenant_outputs_spent;
        self.updated_at = Utc::now();
    }

    pub fn remove_script_covenant_delta(&mut self, delta: &CovenantTally) {
        self.output_count_pubkey = self
            .output_count_pubkey
            .saturating_sub(delta.output_count_pubkey);
        self.output_count_pubkey_ecdsa = self
            .output_count_pubkey_ecdsa
            .saturating_sub(delta.output_count_pubkey_ecdsa);
        self.output_count_script_hash = self
            .output_count_script_hash
            .saturating_sub(delta.output_count_script_hash);
        self.output_count_nonstandard = self
            .output_count_nonstandard
            .saturating_sub(delta.output_count_nonstandard);
        if delta.uses_introspection_opcode {
            self.introspection_opcode_tx_count =
                self.introspection_opcode_tx_count.saturating_sub(1);
        }
        if delta.uses_zk_precompile_opcode {
            self.zk_precompile_tx_count = self.zk_precompile_tx_count.saturating_sub(1);
        }
        self.zk_precompile_outputs_spent = self
            .zk_precompile_outputs_spent
            .saturating_sub(delta.zk_precompile_outputs_spent);
        if delta.covenant_outputs_created > 0 {
            self.covenant_creating_tx_count = self.covenant_creating_tx_count.saturating_sub(1);
        }
        self.covenant_outputs_created = self
            .covenant_outputs_created
            .saturating_sub(delta.covenant_outputs_created);
        self.covenant_outputs_spent = self
            .covenant_outputs_spent
            .saturating_sub(delta.covenant_outputs_spent);
        self.updated_at = Utc::now();
    }
}
