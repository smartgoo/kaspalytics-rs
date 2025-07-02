mod inscription;

use crate::ingest::cache::DagCache;
use kaspa_rpc_core::RpcTransaction;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TransactionProtocol {
    Krc,
    Kns,
    Kasia,
}

fn payload_to_string(payload: Vec<u8>) -> String {
    payload.iter().map(|&b| b as char).collect::<String>()
}

pub fn detect_transaction_protocol(transaction: &RpcTransaction) -> Option<TransactionProtocol> {
    // Check for Kasia protocol in transactionpayload
    if payload_to_string(transaction.payload.clone()).contains("ciph_msg") {
        return Some(TransactionProtocol::Kasia);
    }

    // Check inputs for Kasplex or KNS inscription
    for input in transaction.inputs.iter() {
        let parsed_script = inscription::parse_signature_script(&input.signature_script);

        for (opcode, data) in parsed_script {
            if opcode.as_str() == "OP_PUSH" && ["kasplex", "kspr"].contains(&data.as_str()) {
                return Some(TransactionProtocol::Krc);
            }
    
            if opcode.as_str() == "OP_PUSH" && ["kns"].contains(&data.as_str()) {
                return Some(TransactionProtocol::Kns);
            }
        };
    }

    None
}
