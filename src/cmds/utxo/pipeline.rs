use super::address::percentile::AddressPercentileAnalysis;
use crate::utils::config::Config;
use kaspa_addresses::{Address, Prefix};
use kaspa_consensus_core::tx::ScriptPublicKey;
use kaspa_consensus_core::Hash;
use kaspa_database::prelude::DB;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_txscript::extract_script_pub_key_address;
use kaspa_utxoindex::stores::indexed_utxos::{
    ScriptPublicKeyBucket, TRANSACTION_OUTPOINT_KEY_SIZE,
};
use kaspa_utxoindex::stores::store_manager::Store;
use kaspa_wrpc_client::KaspaRpcClient;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

struct AddressBalances {
    balances: HashMap<Address, u64>,

    // Total sompi held by addresses with a dust balance
    dust_address_sompi_total: u64,

    // Count of addresses that hold a dust balance
    dust_address_count: u64,
}

struct UtxoSnapshotHeader {
    id: Option<u64>,
    block: Option<Hash>,
    block_timestamp: Option<u64>,
    daa_score: Option<u64>,
    kas_price_usd: Option<f64>,
    circulating_supply: Option<u64>,
    snapshot_complete: Option<bool>,
    utxo_count: Option<u64>,
    unique_address_count: Option<u64>,
    unique_address_count_meaningful: Option<u64>,
    unique_address_count_non_meaningful: Option<u64>,
    sompi_held_by_non_meaningful_addresses: Option<u64>,
    percentile_analysis_completed: Option<bool>,
    kas_last_moved_by_age_bucket_complete: Option<bool>,
    distribution_by_kas_bucket_complete: Option<bool>,
    distribution_by_usd_bucket_complete: Option<bool>,
}

impl UtxoSnapshotHeader {
    fn new() -> Self {
        UtxoSnapshotHeader {
            id: None,
            block: None,
            block_timestamp: None,
            daa_score: None,
            kas_price_usd: None,
            circulating_supply: None,
            snapshot_complete: Some(false),
            utxo_count: None,
            unique_address_count: None,
            unique_address_count_meaningful: None,
            unique_address_count_non_meaningful: None,
            sompi_held_by_non_meaningful_addresses: None,
            percentile_analysis_completed: Some(false),
            kas_last_moved_by_age_bucket_complete: Some(false),
            distribution_by_kas_bucket_complete: Some(false),
            distribution_by_usd_bucket_complete: Some(false),
        }
    }

    async fn insert_to_db(&mut self, pg_pool: PgPool) -> i32 {
        let dt = chrono::DateTime::from_timestamp((self.block_timestamp.unwrap() / 1000) as i64, 0)
            .unwrap();

        let r = sqlx::query(
            r#"
                INSERT INTO utxo_snapshot_header
                (snapshot_complete, block, block_timestamp, daa_score, kas_price_usd)
                VALUES
                (false, $1, $2, $3, $4)
                RETURNING id
            "#,
        )
        .bind(self.block.unwrap().to_string())
        .bind(dt)
        .bind(self.daa_score.unwrap() as i64)
        .bind(self.kas_price_usd.unwrap())
        .fetch_one(&pg_pool)
        .await
        .unwrap();

        let id = r.try_get("id").unwrap();
        self.id = Some(id as u64);
        id
    }

    fn set_percentile_analysis_complete(&self, pg_pool: PgPool) {
        

    }
}

pub struct UtxoBasedPipeline {
    config: Config,
    rpc_client: Arc<KaspaRpcClient>,
    pg_pool: PgPool,
    utxo_snapshot_header: UtxoSnapshotHeader,
}

impl UtxoBasedPipeline {
    pub fn new(config: Config, rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) -> Self {
        UtxoBasedPipeline {
            config,
            rpc_client,
            pg_pool,
            utxo_snapshot_header: UtxoSnapshotHeader::new(),
        }
    }
}

impl UtxoBasedPipeline {
    fn get_address_balances(&self, db: Arc<DB>) -> AddressBalances {
        let store = Store::new(db);

        let mut balances = HashMap::<Address, u64>::new();
        let mut dust_address_count = 0;
        let mut dust_address_sompi_total = 0;
        for c in store.utxos_by_script_public_key_store.access.iterator() {
            let (key, utxo) = c.unwrap();

            if utxo.amount <= 1000 {
                dust_address_count += 1;
                dust_address_sompi_total += utxo.amount;
                continue;
            }

            let script_public_key_bucket =
                ScriptPublicKeyBucket(key[..key.len() - TRANSACTION_OUTPOINT_KEY_SIZE].to_vec());
            let script_public_key = ScriptPublicKey::from(script_public_key_bucket);

            let address =
                extract_script_pub_key_address(&script_public_key, Prefix::Mainnet).unwrap();

            *balances.entry(address).or_insert(0) += utxo.amount;
        }

        println!(
            "dust_address_sompi_total: {}",
            dust_address_sompi_total / 100_000_000
        );
        println!("dust_address_count: {}", dust_address_count);

        AddressBalances {
            balances,
            dust_address_sompi_total,
            dust_address_count,
        }
    }

    fn get_utxo_tip(&self, db: Arc<DB>) -> Hash {
        let store = Store::new(db);

        // Return a single utxo tip (order doens't really matter)
        *store.get_tips().unwrap().iter().next().unwrap()
    }

    fn get_circulating_supply(&self, db: Arc<DB>) -> u64 {
        let store = Store::new(db);
        store.get_circulating_supply().unwrap()
    }
}

impl UtxoBasedPipeline {
    async fn insert_address_balances(
        &self,
        utxo_snapshot_id: i32,
        address_data: Rc<HashMap<Address, u64>>,
    ) -> Result<(), sqlx::Error> {
        let db_addresses: Vec<(String, i32, u64)> = address_data
            .iter()
            .map(|(address, sompi)| (address.to_string(), utxo_snapshot_id, *sompi))
            .collect();

        let mut conn = self.pg_pool.acquire().await?;

        let mut copy_in = conn
            .copy_in_raw(
                "COPY address_balance_snapshot (address, utxo_snapshot_id, amount_sompi) FROM STDIN WITH (FORMAT csv)",
            )
            .await?;

        for (address, snapshot_id, amount_sompi) in db_addresses {
            let line = format!("{},{},{}\n", address, snapshot_id, amount_sompi);
            copy_in.send(line.as_bytes()).await?;
        }

        copy_in.finish().await?;
        Ok(())
    }
}

impl UtxoBasedPipeline {
    pub async fn run(&mut self) {
        // Get KAS/USD price
        self.utxo_snapshot_header.kas_price_usd =
            Some(crate::cmds::price::get_kas_usd_price().await.unwrap());

        // Get UTXO tips from utxoindex db
        let db = kaspa_database::prelude::ConnBuilder::default()
            .with_db_path(
                self.config
                    .kaspad_dirs
                    .utxo_index_db_dir
                    .as_ref()
                    .unwrap()
                    .to_path_buf(),
            )
            .with_files_limit(128) // TODO files limit?
            .build_readonly()
            .unwrap();
        self.utxo_snapshot_header.block = Some(self.get_utxo_tip(db.clone()));
        self.utxo_snapshot_header.circulating_supply =
            Some(self.get_circulating_supply(db.clone()));

        // Get block timestamp
        let block_data = self
            .rpc_client
            .get_block(self.utxo_snapshot_header.block.unwrap(), false)
            .await
            .unwrap();
        self.utxo_snapshot_header.block_timestamp = Some(block_data.header.timestamp);
        self.utxo_snapshot_header.daa_score = Some(block_data.header.daa_score);

        // Create initial record in utxo_snapshot_header
        let utxo_snapshot_id = self
            .utxo_snapshot_header
            .insert_to_db(self.pg_pool.clone())
            .await;

        // Snapshot DAA score and timestamp
        crate::cmds::daa::insert_daa_timestamp(
            &self.pg_pool,
            block_data.header.daa_score,
            block_data.header.timestamp,
        )
        .await
        .unwrap();

        // Iterate over UTXOs in utxoindex db, loading address balance data into memory
        let AddressBalances {
            balances,
            dust_address_sompi_total,
            dust_address_count,
        } = self.get_address_balances(db.clone());

        let address_balances = Rc::new(balances);

        // Stash address balances in DB
        self.insert_address_balances(utxo_snapshot_id, address_balances.clone())
            .await
            .unwrap();

        // Address percentile analysis
        AddressPercentileAnalysis::new(
            self.pg_pool.clone(),
            utxo_snapshot_id,
            address_balances.clone(),
            self.utxo_snapshot_header.circulating_supply.unwrap(),
        )
        .run()
        .await;

        analyze_cohorts(address_balances.clone());

        // ------
        // I think flow should look like this:
            // [x] Init DB and Store
            // [x] Get KAS price
            // [x] Get UTXO Tips
            // [x] Create utxo snapshot header record
            // [x] Load all addresses into HashMap `addr_map`
                // [x] with exception of dust
                // [x] but need to track:
                    // [x] dust_address_sompi_total 
                    // [x] dust_address_count
            // [x] Create daa snapshot record
            // [x] Address Balance Snapshot using `addr_map`
                // [x] insert into db in chunks
                // [x] make sure I collect info to populate utxo_snapshot_header record per lines 118 - 124
            // [x] Address Percentile Analysis
            // Distribution By KAS Bucket
                // looks like Python scripts loads Dust addresses here though
                // So either `addr_map` needs to load dust addrs, or I need to ensure I track enough info upstream to calc properly here
                // wait I actually think that total_sompi_held_by_dust_addresses and dust_address_count are good enough!
            // Distribution By USD Bucket
            // KAS Last Moved By Age Bucket
                // Can I use UTXO RPC to do this?
            // Update utxo_snapshot_header
                // self.unique_address_count_non_meaningful = Some(address_data.dust_address_count);
                // self.sompi_held_by_non_meaningful_addresses = Some(address_data.dust_address_sompi_total);

    }
}

fn analyze_cohorts(map: Rc<HashMap<Address, u64>>) {
    // Counts stored in array, each index corresponds to a bucket
    let mut cohorts = [0u64; 10];

    for (_, &sompi_balance) in map.iter() {
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
                cohorts[9] += 1;
            }
            100000000000000000_u64..=u64::MAX => unimplemented!(),
        }
    }

    println!("Cohort counts: {:?}", cohorts);
    println!("Addresses with [0 - 0.01) KAS: {}", cohorts[0]);
    println!("Addresses with [0.01 - 1) KAS: {}", cohorts[1]);
    println!("Addresses with [1 - 100) KAS: {}", cohorts[2]);
    println!("Addresses with [100 - 1,000) KAS: {}", cohorts[3]);
    println!("Addresses with [1,000 - 10,000) KAS: {}", cohorts[4]);
    println!("Addresses with [10,000 - 100,000) KAS: {}", cohorts[5]);
    println!("Addresses with [100,000 - 1,000,000) KAS: {}", cohorts[6]);
    println!(
        "Addresses with [1,000,000 - 10,000,000) KAS: {}",
        cohorts[7]
    );
    println!(
        "Addresses with [10,000,000 - 100,000,000) KAS: {}",
        cohorts[8]
    );
    println!(
        "Addresses with [100,000,000 - 1,000,000,000) KAS: {}",
        cohorts[9]
    );
}
