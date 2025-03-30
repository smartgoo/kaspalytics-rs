use chrono::{DateTime, Days, TimeZone, Utc};
use duckdb::{params, params_from_iter, Connection, Error};
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcAcceptedTransactionIds, RpcBlock, RpcScriptClass};
use log::info;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
use tokio::task::spawn_blocking;

use crate::ingest::IngestMessage;

pub struct DbWriter {
    conn: Arc<Mutex<Connection>>,
    shutdown_flag: Arc<AtomicBool>,
}

impl DbWriter {
    pub fn try_new(path: &PathBuf, shutdown_flag: Arc<AtomicBool>) -> Result<Self, Error> {
        let config = duckdb::Config::default()
            .with("max_memory", "4GB")?
            .with("threads", "2")?;
        let conn = Connection::open_with_flags(path, config)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            shutdown_flag,
        })
    }

    pub fn scaffold(&self) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS blocks (
                hash BLOB,
                timestamp TIMESTAMP_MS,
                daa_score BIGINT,
            );

            CREATE TABLE transactions_denormalized (
                block_hash BLOB,
                transaction_id BLOB,
                block_time TIMESTAMP_MS,
                accepting_block BLOB,
                lock_time BIGINT,
                payload BLOB,
                mass BIGINT,
                compute_mass BIGINT,
                input_index INT,
                input_previous_outpoint_transaction_id BLOB,
                input_previous_outpoint_index INT,
                input_signature_script BLOB,
                input_spending_address VARCHAR,
                output_index INT,
                output_amount BIGINT,
                output_script_public_key BLOB,
                output_script_public_key_type INT,
                output_script_public_key_address VARCHAR,
            );

            CREATE TABLE IF NOT EXISTS transactions (
                block_hash BLOB,
                transaction_id BLOB,
                lock_time BIGINT,
                -- subnetwork_id BLOB,
                payload BLOB,
                mass BIGINT,
                compute_mass BIGINT,
                block_time TIMESTAMP_MS,
            );

            CREATE TABLE IF NOT EXISTS transaction_inputs (
                block_hash BLOB,
                transaction_id BLOB,
                index BIGINT,
                previous_outpoint_transaction_id BLOB,
                previous_outpoint_index INT,
                sig_op_count INT,
                signature_script BLOB,
            );

            CREATE TABLE IF NOT EXISTS transaction_outputs (
                block_hash BLOB,
                transaction_id BLOB,
                index INT,
                amount BIGINT,
                script_public_key BLOB,
                script_public_key_type INT,
                script_public_key_address VARCHAR,
            );

            CREATE TABLE IF NOT EXISTS accepting_block_transactions (
                block_hash BLOB,
                transaction_id BLOB
            );

            -- TODO chain_blocks
            "#,
        )?;

        Ok(())
    }
}

impl DbWriter {
    fn handle_blocks(&self, blocks: Vec<RpcBlock>) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let db_tx = conn.transaction()?;

        {
            let mut block_appender = db_tx.appender("blocks")?;
            let mut tx_appender = db_tx.appender("transactions")?;
            let mut tx_input_appender = db_tx.appender("transaction_inputs")?;
            let mut tx_output_appender = db_tx.appender("transaction_outputs")?;

            for block in blocks {
                block_appender.append_row(params![
                    block.header.hash.as_bytes().to_vec(),
                    Utc.timestamp_millis_opt(block.header.timestamp as i64)
                        .unwrap(),
                    block.header.daa_score,
                ])?;

                for tx in block.transactions.iter() {
                    let verbose_data = tx.verbose_data.as_ref().unwrap();
                    tx_appender.append_row(params![
                        block.header.hash.as_bytes().to_vec(),
                        verbose_data.transaction_id.as_bytes().to_vec(),
                        tx.lock_time,
                        tx.payload,
                        tx.mass,
                        verbose_data.compute_mass,
                        Utc.timestamp_millis_opt(verbose_data.block_time as i64)
                            .unwrap()
                    ])?;

                    for (index, input) in tx.inputs.iter().enumerate() {
                        tx_input_appender.append_row(params![
                            block.header.hash.as_bytes().to_vec(),
                            verbose_data.transaction_id.as_bytes().to_vec(),
                            index,
                            input.previous_outpoint.transaction_id.as_bytes().to_vec(),
                            input.previous_outpoint.index,
                            input.sig_op_count,
                            input.signature_script,
                        ])?;
                    }

                    for (index, output) in tx.outputs.iter().enumerate() {
                        tx_output_appender.append_row(params![
                            block.header.hash.as_bytes().to_vec(),
                            verbose_data.transaction_id.as_bytes().to_vec(),
                            index,
                            output.value,
                            output.script_public_key.script(),
                            match output.verbose_data.as_ref().unwrap().script_public_key_type {
                                RpcScriptClass::NonStandard => 0,
                                RpcScriptClass::PubKey => 1,
                                RpcScriptClass::PubKeyECDSA => 2,
                                RpcScriptClass::ScriptHash => 3,
                            },
                            output
                                .verbose_data
                                .as_ref()
                                .unwrap()
                                .script_public_key_address
                                .to_string(),
                        ])?;
                    }
                }
            }

            block_appender.flush()?;
            tx_appender.flush()?;
            tx_input_appender.flush()?;
            tx_output_appender.flush()?;
        }

        db_tx.commit()?;

        Ok(())
    }

    fn handle_acceptances(&self, acceptances: Vec<RpcAcceptedTransactionIds>) -> Result<(), Error> {
        let mut conn = self.conn.lock().unwrap();
        let db_tx = conn.transaction()?;

        {
            let mut appender = db_tx.appender("accepting_block_transactions")?;

            for acceptance in acceptances {
                for tx in acceptance.accepted_transaction_ids {
                    appender.append_row(params![
                        acceptance.accepting_block_hash.as_bytes().to_vec(),
                        tx.as_bytes().to_vec()
                    ])?;
                }
            }

            appender.flush()?;
        }

        db_tx.commit()?;

        Ok(())
    }

    fn handle_removed_chain_blocks(&self, blocks: Vec<Hash>) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();

        let placeholders = vec!["?"; blocks.len()].join(",");
        let query = format!(
            "DELETE FROM accepting_block_transactions WHERE block_hash IN ({})",
            placeholders
        );

        // Prepare and execute the statement
        let mut stmt = conn.prepare(&query)?;

        // Convert Vec<Hash> to Vec<&str> or appropriate type for binding
        // Assuming Hash implements AsRef<str> or ToString
        let params = blocks
            .into_iter()
            .map(|h| h.as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();

        stmt.execute(params_from_iter(params))?;

        Ok(())
    }
}

impl DbWriter {
    fn prune_accepting_block_transactions(&self, threshold: DateTime<Utc>) -> Result<usize, Error> {
        let conn = self.conn.lock().unwrap();

        let count = conn
            .prepare(
                r#"
                DELETE FROM accepting_block_transactions a
                USING blocks b
                WHERE a.block_hash = b.hash
                AND b.timestamp < $1
            "#,
            )?
            .execute(params![threshold])?;

        Ok(count)
    }

    fn prune_transaction_outputs(&self, threshold: DateTime<Utc>) -> Result<usize, Error> {
        let conn = self.conn.lock().unwrap();

        let count = conn
            .prepare(
                r#"
                DELETE FROM transaction_outputs t
                USING blocks b
                WHERE t.block_hash = b.hash
                AND b.timestamp < $1
            "#,
            )?
            .execute(params![threshold])?;

        Ok(count)
    }

    fn prune_transaction_inputs(&self, threshold: DateTime<Utc>) -> Result<usize, Error> {
        let conn = self.conn.lock().unwrap();

        let count = conn
            .prepare(
                r#"
                DELETE FROM transaction_inputs t
                USING blocks b
                WHERE t.block_hash = b.hash
                AND b.timestamp < $1
            "#,
            )?
            .execute(params![threshold])?;

        Ok(count)
    }

    fn prune_transactions(&self, threshold: DateTime<Utc>) -> Result<usize, Error> {
        let conn = self.conn.lock().unwrap();

        let count = conn
            .prepare(
                r#"
                DELETE FROM transactions t
                USING blocks b
                WHERE t.block_hash = b.hash
                AND b.timestamp < $1
            "#,
            )?
            .execute(params![threshold])?;

        Ok(count)
    }

    fn prune_blocks(&self, threshold: DateTime<Utc>) -> Result<usize, Error> {
        let conn = self.conn.lock().unwrap();

        let count = conn
            .prepare(
                r#"
                DELETE FROM blocks
                WHERE timestamp < $1
            "#,
            )?
            .execute(params![threshold])?;

        Ok(count)
    }

    fn checkpoint(&self) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("CHECKPOINT;", [])?;
        Ok(())
    }

    fn vacuum(&self) -> Result<(), Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute("VACUUM;", [])?;
        Ok(())
    }

    // Prunes data, CHECKPOINT, and VACUUM every X minutes
    // Sleeps for Y seconds to detect changes to shutdown_flag
    fn run_pruner(&self) -> Result<(), Error> {
        let checkpoint_interval = Duration::from_secs(60);
        let sleep_for = Duration::from_secs(5);
        let mut last_prune = Instant::now();

        while !self.shutdown_flag.load(Ordering::Relaxed) {
            if last_prune.elapsed() >= checkpoint_interval {
                let start = Instant::now();

                // Use threshold instead of in-SQL NOW() to ensure timestamp consistency across queries
                let threshold = Utc::now().checked_sub_days(Days::new(2)).unwrap();

                let abt_deleted_count = self.prune_accepting_block_transactions(threshold)?;
                let tx_output_deleted_count = self.prune_transaction_outputs(threshold)?;
                let tx_input_deleted_count = self.prune_transaction_inputs(threshold)?;
                let tx_deleted_count = self.prune_transactions(threshold)?;
                let block_deleted_count = self.prune_blocks(threshold)?;

                self.checkpoint()?;
                self.vacuum()?;

                info!(
                    "Pruned {} records from DB in {}ms. Sleeping",
                    abt_deleted_count
                        + tx_input_deleted_count
                        + tx_output_deleted_count
                        + tx_deleted_count
                        + block_deleted_count,
                    start.elapsed().as_millis(),
                );

                last_prune = Instant::now();
            }

            std::thread::sleep(sleep_for);
        }

        info!("Pruner exited loop, shutting down...");

        Ok(())
    }
}

pub async fn run(writer: DbWriter, mut rx: Receiver<IngestMessage>) {
    let writer = Arc::new(writer);

    let pruner_writer = writer.clone();
    let pruner_handle = spawn_blocking(move || pruner_writer.run_pruner().unwrap());

    let receiver_handle = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let writer = writer.clone();
            match msg {
                IngestMessage::Blocks(blocks) => {
                    spawn_blocking(move || writer.handle_blocks(blocks).unwrap())
                        .await
                        .unwrap();
                }
                IngestMessage::AddedAcceptances(acceptances) => {
                    spawn_blocking(move || writer.handle_acceptances(acceptances).unwrap())
                        .await
                        .unwrap();
                }
                IngestMessage::RemovedChainBlocks(blocks) => {
                    spawn_blocking(move || writer.handle_removed_chain_blocks(blocks).unwrap())
                        .await
                        .unwrap();
                }
            }
        }

        info!("Receiver exited loop, shutting down...");
    });

    tokio::try_join!(pruner_handle, receiver_handle).unwrap();
}
