use kaspa_addresses::{Address, Prefix};
use kaspa_consensus::processes::pruning;
use kaspa_consensus_core::tx::ScriptPublicKey;
use kaspa_consensus_core::Hash;
use kaspa_txscript::extract_script_pub_key_address;
use kaspa_utxoindex::stores::store_manager::Store;
use kaspa_utxoindex::stores::indexed_utxos::{
    UtxoEntryFullAccessKey,
    ScriptPublicKeyBucket,
    TRANSACTION_OUTPOINT_KEY_SIZE
};
use std::collections::HashMap;
use std::sync::Arc;
use crate::utils::config::Config;

pub struct UtxoAnalysis {
    snapshot_id: usize,
    addresses: HashMap<Address, u64>,
}

impl UtxoAnalysis {
    pub fn new() -> Self {
        return UtxoAnalysis {
            snapshot_id:  0, // TODO
            addresses: HashMap::<Address, u64>::new() 
        }
    }
}

impl UtxoAnalysis {
    fn create_snapshot_header() {
        // Snapshot DAA and save to DB
        // Insert record to utxo_snapshot_header
            // Need utxo tip block hash
            // Need utxo tip block timestamp
            // Need utxo tip daa score
            // Need current kaspa price
            // Need timestamp of last synced block for UTXO index
            // Need Circulating Supply in KAS
    }
}

impl UtxoAnalysis {
    pub fn main_new(config: Config) {
        let db = kaspa_database::prelude::ConnBuilder::default()
            .with_db_path(config.kaspad_dirs.utxo_index_db_dir.unwrap().to_path_buf())
            .with_files_limit(128) // TODO files limit?
            .build_readonly()
            .unwrap();

        let store = Store::new(db);

    }

    pub fn main(config: Config) {

        let db = kaspa_database::prelude::ConnBuilder::default()
            .with_db_path(config.kaspad_dirs.utxo_index_db_dir.unwrap().to_path_buf())
            .with_files_limit(128) // TODO files limit?
            .build_readonly()
            .unwrap();

        let store = Store::new(db);

        let mut map = HashMap::<Address, u64>::new();
        let mut dust_address_count = 0;
        let mut dust_address_balance = 0;
        for c in store.utxos_by_script_public_key_store.access.iterator() {
            let (key, utxo) = c.unwrap();

            if utxo.amount <= 1000 {
                dust_address_count += 1;
                dust_address_balance += utxo.amount;
                continue
            }

            let utxo_entry_full_access_key = UtxoEntryFullAccessKey(Arc::new(key.to_vec()));
        
            let script_public_key_bucket_end = key.len() - TRANSACTION_OUTPOINT_KEY_SIZE;
            let script_public_key_bucket = ScriptPublicKeyBucket(key[..script_public_key_bucket_end].to_vec());

            let script_public_key = ScriptPublicKey::from(script_public_key_bucket);

            let addr = extract_script_pub_key_address(&script_public_key, Prefix::Mainnet).unwrap();

            *map.entry(addr)
                .or_insert(0) += utxo.amount;
        }

        println!("{}", map.len());

        analyze_cohorts(&map);
        // create_snapshot_header()

        // iterate_utxo_set
            // do I actaully need to export utxo set to csv? why don't i just iterate the utxo set, load map of addresses, count utxos, count dates, etc... 
    }
}

/// Assume this is your map of address -> sompi
/// (Replace `Address` with whatever your address type is)
fn analyze_cohorts(map: &HashMap<Address, u64>) {
    // For clarity, define the conversion and thresholds in sompi:
    const SOMPI_PER_KAS: u64 = 100_000_000;
    
    // The cohorts in KAS (as written):
    //  1) [0           - 0.01)        KAS
    //  2) [0.01        - 1)           KAS
    //  3) [1           - 100)         KAS
    //  4) [100         - 1_000)       KAS
    //  5) [1_000       - 10_000)      KAS
    //  6) [10_000      - 100_000)     KAS
    //  7) [100_000     - 1_000_000)   KAS
    //  8) [1_000_000   - 10_000_000)  KAS
    //  9) [10_000_000  - 100_000_000) KAS
    // 10) [100_000_000 - 1_000_000_000) KAS
    //
    // We translate those boundaries into sompi:
    //  1) [0           - 0.01  ) KAS => [0                - 1_000_000    ) sompi
    //  2) [0.01        - 1     ) KAS => [1_000_000        - 100_000_000  ) sompi
    //  3) [1           - 100   ) KAS => [100_000_000      - 10_000_000_000 ) sompi
    //  4) [100         - 1_000 ) KAS => [10_000_000_000   - 100_000_000_000 )
    //  5) [1_000       - 10_000) KAS => [100_000_000_000  - 1_000_000_000_000 )
    //  6) [10_000      - 100_000) KAS => [1_000_000_000_000 - 10_000_000_000_000 )
    //  7) [100_000     - 1_000_000) KAS => [10_000_000_000_000 - 100_000_000_000_000 )
    //  8) [1_000_000   - 10_000_000) KAS => [100_000_000_000_000 - 1_000_000_000_000_000 )
    //  9) [10_000_000  - 100_000_000) KAS => [1_000_000_000_000_000 - 10_000_000_000_000_000 )
    // 10) [100_000_000 - 1_000_000_000) KAS => [10_000_000_000_000_000 - 100_000_000_000_000_000 )
    
    // We'll store the counts in a simple array, where each index corresponds to one of the above buckets:
    let mut cohorts = [0u64; 10];

    for (address, &sompi_balance) in map.iter() {
        match sompi_balance {
            0..=999_999 => {
                // 0 <= balance < 1,000,000  (i.e. [0 - 0.01) KAS)
                cohorts[0] += 1;
            }
            1_000_000..=99_999_999 => {
                // [0.01 - 1) KAS
                cohorts[1] += 1;
            }
            100_000_000..=9_999_999_999 => {
                // [1 - 100) KAS
                cohorts[2] += 1;
            }
            10_000_000_000..=99_999_999_999 => {
                // [100 - 1,000) KAS
                cohorts[3] += 1;
            }
            100_000_000_000..=999_999_999_999 => {
                // [1,000 - 10,000) KAS
                cohorts[4] += 1;
            }
            1_000_000_000_000..=9_999_999_999_999 => {
                // [10,000 - 100,000) KAS
                cohorts[5] += 1;
            }
            10_000_000_000_000..=99_999_999_999_999 => {
                // [100,000 - 1,000,000) KAS
                cohorts[6] += 1;
            }
            100_000_000_000_000..=999_999_999_999_999 => {
                // [1,000,000 - 10,000,000) KAS
                cohorts[7] += 1;
            }
            1_000_000_000_000_000..=9_999_999_999_999_999 => {
                // [10,000,000 - 100,000,000) KAS
                cohorts[8] += 1;
            }
            10_000_000_000_000_000..=99_999_999_999_999_999 => {
                // [100,000,000 - 1,000,000,000) KAS
                println!("{} {}", address, sompi_balance / 100_000_000);
                cohorts[9] += 1;
            }
            100000000000000000_u64..=u64::MAX => todo!()
            // If you have addresses with >= 1,000,000,000 KAS, you could handle that too.
            // else { ... }
        }
    }

    // Print or return the cohort counts
    println!("Cohort counts: {:?}", cohorts);

    // Optionally, you can print them in a more human-friendly manner:
    // e.g. cohorts[0]: number of addresses holding [0 - 0.01) KAS, etc.
    
    println!("Addresses with [0 - 0.01) KAS: {}", cohorts[0]);
    println!("Addresses with [0.01 - 1) KAS: {}", cohorts[1]);
    println!("Addresses with [1 - 100) KAS: {}", cohorts[2]);
    println!("Addresses with [100 - 1,000) KAS: {}", cohorts[3]);
    println!("Addresses with [1,000 - 10,000) KAS: {}", cohorts[4]);
    println!("Addresses with [10,000 - 100,000) KAS: {}", cohorts[5]);
    println!("Addresses with [100,000 - 1,000,000) KAS: {}", cohorts[6]);
    println!("Addresses with [1,000,000 - 10,000,000) KAS: {}", cohorts[7]);
    println!("Addresses with [10,000,000 - 100,000,000) KAS: {}", cohorts[8]);
    println!("Addresses with [100,000,000 - 1,000,000,000) KAS: {}", cohorts[9]);
}
