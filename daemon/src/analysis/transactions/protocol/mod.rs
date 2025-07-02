pub mod inscription;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TransactionProtocol {
    Krc,
    Kns,
    Kasia,
}
