use kaspa_consensus::consensus::{
    factory::MultiConsensusManagementStore,
    storage::ConsensusStorage
};
use kaspa_consensus_core::{
    config::ConfigBuilder,
    network::NetworkId,
};
use log::info;
use sqlx::PgPool;
use std::{path::PathBuf, str::FromStr, sync::Arc};
use std::io::Cursor;
use std::io::Write;

pub fn meta_db_dir(meta_db_dir: PathBuf) -> PathBuf {
    let db =
        kaspa_database::prelude::ConnBuilder::default().with_db_path(meta_db_dir).with_files_limit(128).build_readonly().unwrap();
    let store = MultiConsensusManagementStore::new(db);
    let active_consensus_dir = store.active_consensus_dir_name().unwrap().unwrap();
    PathBuf::from_str(active_consensus_dir.as_str()).unwrap()
}

pub async fn pp_utxo_set_to_pg(pool: &PgPool, network: NetworkId, consensus_db_dir: PathBuf) -> () {
    // TODO return result
    info!("Inserting pruning point UTXO set to Postgres...");

    let config = Arc::new(ConfigBuilder::new(network.into()).adjust_perf_params_to_consensus_params().build());
    let db =
        kaspa_database::prelude::ConnBuilder::default().with_db_path(consensus_db_dir).with_files_limit(128).build_readonly().unwrap();
    let storage = ConsensusStorage::new(db, config);
    
    let mut pg_connection = pool.acquire().await.unwrap();
    let mut pg_copy_in = pg_connection
        .copy_in_raw("COPY outpoints (transaction_id, transaction_index, amount, script_public_key) FROM STDIN WITH (FORMAT BINARY)")
        .await
        .unwrap();

    // Buffer to hold the data for copy
    let mut data = Vec::new();

    // Write the binary header per Postgres requiremnts
    // Signature: PGCOPY\n\377\r\n\0
    data.write_all(b"PGCOPY\n\xFF\r\n\0").unwrap();
    // Flags: 32-bit integer, set to 0 since not including OIDs
    data.extend_from_slice(&0u32.to_be_bytes());
    // Header extension area: 32-bit integer, hardcoding to 0 for this
    data.extend_from_slice(&0u32.to_be_bytes());

    info!("Building PG COPY byte array...");
    let mut count = 0;
    for (key, entry) in storage.pruning_utxoset_stores.read().utxo_set.iterator().map(|p| p.unwrap()) {
        // number of fields in tuple (row)
        data.extend_from_slice(&4u16.to_be_bytes());

        let transaction_id_bytes = key.transaction_id.as_bytes();
        data.extend_from_slice(&32u32.to_be_bytes());
        data.extend_from_slice(&transaction_id_bytes[..]);

        data.extend_from_slice(&4u32.to_be_bytes());
        data.extend_from_slice(&(key.index as u32).to_be_bytes());

        data.extend_from_slice(&8u32.to_be_bytes());
        data.extend_from_slice(&(entry.amount as u64).to_be_bytes());

        let script_public_key_bytes = entry.script_public_key.script();
        data.extend_from_slice(&(script_public_key_bytes.len() as u32).to_be_bytes());
        data.extend_from_slice(script_public_key_bytes);

        count += 1;
    }
    info!("Read {} pruning point UTXO records from rusty-kaspa instance", count);

    // PostgreSQL binary trailer
    data.extend_from_slice(&(-1i16).to_be_bytes());

    info!("Sending byte array to postgres");
    let mut reader = Cursor::new(data);
    pg_copy_in.read_from(&mut reader).await.unwrap();

    let rows_inserted = pg_copy_in.finish().await.unwrap();
    info!("Pruning point UTXO set records inserted to Postgres: {}", rows_inserted);
}