pub mod inscription;

use serde::{Deserialize, Serialize};
use strum_macros::{Display, EnumIter};

#[derive(Clone, Debug, Display, EnumIter, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub enum TransactionProtocol {
    Krc = 0,
    Kns,
    Kasia,
    Kasplex,
    KSocial,
    Igra,
}
