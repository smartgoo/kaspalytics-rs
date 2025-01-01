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
use std::sync::Arc;

struct AddressData {
    balances: HashMap<Address, u64>,

    // Total sompi held by addresses with a dust balance
    dust_address_sompi_total: u64,

    // Count of addresses that hold a dust balance
    dust_address_count: u64,
}

struct AddressPercentileData {}

pub struct UtxoAnalysis {
    config: Config,
    rpc_client: Arc<KaspaRpcClient>,
    pg_pool: PgPool,

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

impl UtxoAnalysis {
    pub fn new(config: Config, rpc_client: Arc<KaspaRpcClient>, pg_pool: PgPool) -> Self {
        return UtxoAnalysis {
            config,
            rpc_client,
            pg_pool,

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
        };
    }
}

impl UtxoAnalysis {
    fn get_address_balances(&self, db: Arc<DB>) -> AddressData {
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

            let addr = extract_script_pub_key_address(&script_public_key, Prefix::Mainnet).unwrap();

            *balances.entry(addr).or_insert(0) += utxo.amount;
        }

        println!(
            "dust_address_sompi_total: {}",
            dust_address_sompi_total / 100_000_000
        );
        println!("dust_address_count: {}", dust_address_count);

        AddressData {
            balances,
            dust_address_sompi_total,
            dust_address_count,
        }
    }

    fn get_utxo_tip(&self, db: Arc<DB>) -> Hash {
        let store = Store::new(db);

        // Return a single utxo tip (order doens't really matter)
        store.get_tips().unwrap().iter().next().unwrap().clone()
    }
}

impl UtxoAnalysis {
    async fn insert_utxo_snapshot_header_record(&self) -> i32 {
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
        .fetch_one(&self.pg_pool)
        .await
        .unwrap();

        r.try_get("id").unwrap()
    }
}

impl UtxoAnalysis {
    async fn insert_address_balances(&self, utxo_snapshot_id: i32, address_data: &AddressData) {
        // let mut db_addresses = Vec::new();

        let db_addresses: Vec<(String, i32, u64)> = address_data
            .balances
            .iter()
            .map(|(address, sompi)| (address.to_string(), utxo_snapshot_id, sompi.clone()))
            .collect();

        self.insert_to_db_with_pgcopyin(&db_addresses)
            .await
            .unwrap();
    }

    async fn insert_to_db_with_pgcopyin(
        &self,
        db_addresses: &Vec<(String, i32, u64)>,
    ) -> Result<(), sqlx::Error> {
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

impl UtxoAnalysis {
    pub async fn run(&mut self) {
        // Get KAS/USD price
        self.kas_price_usd = Some(crate::utils::price::get_kas_usd_price().await.unwrap());

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
        self.block = Some(self.get_utxo_tip(db.clone()));

        // Get block timestamp
        let block_data = self
            .rpc_client
            .get_block(self.block.unwrap(), false)
            .await
            .unwrap();
        self.block_timestamp = Some(block_data.header.timestamp);
        self.daa_score = Some(block_data.header.daa_score);

        // Create initial record in utxo_snapshot_header
        let utxo_snapshot_id = self.insert_utxo_snapshot_header_record().await;

        // Snapshot DAA score and timestamp
        crate::cmds::daa::insert_daa_timestamp(
            &self.pg_pool,
            block_data.header.daa_score,
            block_data.header.timestamp,
        )
        .await
        .unwrap();

        // Iterate over UTXOs in utxoindex db, loading address balance data into memory
        let address_data = self.get_address_balances(db.clone());

        // Stash address balances in DB
        self.insert_address_balances(utxo_snapshot_id, &address_data)
            .await;

        analyze_cohorts(&address_data.balances);

        analyze_top_percentiles(&address_data.balances);
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
            // Address Percentile Analysis using `addr_map`
            // Distribution By KAS Bucket
                // looks like Python scripts loads Dust addresses here though
                // So either `addr_map` needs to load dust addrs, or I need to ensure I track enough info upstream to calc properly here
                // wait I actually think that total_sompi_held_by_dust_addresses and dust_address_count are good enough!
            // Distribution By USD Bucket
                // Do I really really need this... ? YEs bc of home page lol
                // TODO
            // KAS Last Moved By Age Bucket
                // Can I use UTXO RPC to do this?
            // Update utxo_snapshot_header
                // self.unique_address_count_non_meaningful = Some(address_data.dust_address_count);
                // self.sompi_held_by_non_meaningful_addresses = Some(address_data.dust_address_sompi_total);

    }
}

/// Assume this is your map of address -> sompi
/// (Replace `Address` with whatever your address type is)
fn analyze_cohorts(map: &HashMap<Address, u64>) {
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
                cohorts[9] += 1;
            }
            100000000000000000_u64..=u64::MAX => unimplemented!(),
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

const SOMPI_PER_KAS: u64 = 100_000_000;

/// A helper struct for holding aggregated stats.
#[derive(Debug)]
struct Stats {
    address_count: usize,
    total_kas: f64,
    min_kas: f64,
    max_kas: f64,
    avg_kas: f64,
}

/// Compute stats (count, total, min, max, average) over a slice of balances in sompi.
fn compute_stats(balances: &[u64]) -> Stats {
    if balances.is_empty() {
        return Stats {
            address_count: 0,
            total_kas: 0.0,
            min_kas: 0.0,
            max_kas: 0.0,
            avg_kas: 0.0,
        };
    }

    let address_count = balances.len();
    let total_sompi: u64 = balances.iter().sum();

    let min_sompi = *balances.iter().min().unwrap();
    let max_sompi = *balances.iter().max().unwrap();

    let total_kas = total_sompi as f64 / SOMPI_PER_KAS as f64;
    let min_kas = min_sompi as f64 / SOMPI_PER_KAS as f64;
    let max_kas = max_sompi as f64 / SOMPI_PER_KAS as f64;
    let avg_kas = total_kas / address_count as f64;

    Stats {
        address_count,
        total_kas,
        min_kas,
        max_kas,
        avg_kas,
    }
}

/// Analyzes the top X% addresses by balance and prints statistics.
fn analyze_top_percentiles(map: &HashMap<Address, u64>) {
    // 1) Collect and sort balances (descending)
    let mut balances: Vec<u64> = map.values().cloned().collect();
    balances.sort_unstable_by(|a, b| b.cmp(a)); // descending order

    let total_addresses = balances.len();
    if total_addresses == 0 {
        println!("No addresses found.");
        return;
    }

    // Define the desired top percentages
    //  (0.0001 => 0.01%, 0.001 => 0.1%, 0.01 => 1%, etc.)
    let top_fractions = &[
        (0.0001, "Top 0.01%"),
        (0.001, "Top 0.1%"),
        (0.01, "Top 1%"),
        (0.05, "Top 5%"),
        (0.10, "Top 10%"),
    ];

    for (fraction, label) in top_fractions {
        // 2) Calculate how many addresses fit in the "top fraction"
        let top_count = ((total_addresses as f64) * fraction).ceil() as usize;

        // Safeguard: if top_count is 0 but fraction > 0, just skip or handle accordingly
        if top_count == 0 {
            println!("{} => no addresses in this fraction", label);
            continue;
        }

        // 3) Slice the top portion of balances
        let top_slice = &balances[0..top_count.min(total_addresses)];

        // 4) Compute statistics
        let stats = compute_stats(top_slice);

        // 5) Print or return them as you need
        println!("{} stats:", label);
        println!("  Address count: {}", stats.address_count);
        println!("  Total KAS held: {:.4}", stats.total_kas);
        println!("  Min KAS held: {:.8}", stats.min_kas);
        println!("  Max KAS held: {:.8}", stats.max_kas);
        println!("  Average KAS held: {:.8}", stats.avg_kas);
        println!();
    }
}
