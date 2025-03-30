use chrono::{Days, Utc};
use duckdb::{Connection, Error};
use std::{collections::HashMap, path::PathBuf};

#[derive(Clone)]
pub struct DbReader {
    path: PathBuf,
}

impl DbReader {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn get_connection(&self) -> Result<Connection, Error> {
        let config = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)?
            .with("threads", "2")?
            .with("max_memory", "4GB")?;

        Connection::open_with_flags(self.path.clone(), config)
    }
}

#[allow(dead_code)]
impl DbReader {
    pub fn block_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare("SELECT COUNT(1) FROM blocks;")?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn duplicate_block_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"SELECT COUNT(1)
                FROM (
                    SELECT hash
                    FROM blocks
                    GROUP BY hash
                    HAVING COUNT(1) > 1
                ) AS subq;"#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn transaction_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare("SELECT COUNT(1) FROM transactions;")?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn duplicate_transaction_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"SELECT COUNT(1)
                FROM (
                    SELECT block_hash, transaction_id
                    FROM transactions
                    GROUP BY block_hash, transaction_id
                    HAVING COUNT(1) > 1
                ) AS subq;"#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn transaction_count_24h(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"
                SELECT COUNT(1)
                FROM transactions t
                WHERE t.block_time >= CAST(now() AS TIMESTAMP) - INTERVAL '24 hour';
                "#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn transaction_count_per_hour(&self) -> Result<HashMap<i64, i64>, Error> {
        let conn = self.get_connection()?;

        let mut stmt = conn.prepare(
            "
            SELECT
                epoch(date_trunc('hour', t.block_time)) AS hour_epoch,
                COUNT(1) AS record_count
            FROM transactions t
            WHERE t.block_time >= CAST(now() AS TIMESTAMP) - INTERVAL '24 hour'
            GROUP BY hour_epoch
            ORDER BY hour_epoch;
            ",
        )?;

        let mut rows = stmt.query([])?;
        let mut result = HashMap::new();

        while let Some(row) = rows.next().unwrap() {
            let hour_epoch: i64 = row.get(0)?;
            let record_count: i64 = row.get(1)?;

            result.insert(hour_epoch, record_count);
        }

        Ok(result)
    }

    pub fn effective_transaction_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare("SELECT COUNT(1) FROM accepting_block_transactions;")?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn effective_transaction_count_24h(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"
                SELECT COUNT(1)
                FROM transactions t
                JOIN accepting_block_transactions a
                ON t.transaction_id = a.transaction_id
                WHERE t.block_time >= CAST(now() AS TIMESTAMP) - INTERVAL '24 hour';
                "#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn effective_transaction_count_per_hour(&self) -> Result<HashMap<i64, i64>, Error> {
        let conn = self.get_connection()?;

        let mut stmt = conn.prepare(
            "
            SELECT
                epoch(date_trunc('hour', t.block_time)) AS hour_epoch,
                COUNT(1) AS record_count
            FROM transactions t
            JOIN accepting_block_transactions a
            ON t.transaction_id = a.transaction_id
            WHERE t.block_time >= CAST(now() AS TIMESTAMP) - INTERVAL '24 hour'
            GROUP BY hour_epoch
            ORDER BY hour_epoch;
            ",
        )?;

        let mut rows = stmt.query([])?;
        let mut result = HashMap::new();

        while let Some(row) = rows.next().unwrap() {
            let hour_epoch: i64 = row.get(0)?;
            let record_count: i64 = row.get(1)?;

            result.insert(hour_epoch, record_count);
        }

        Ok(result)
    }

    pub fn input_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare("SELECT COUNT(1) FROM transaction_inputs;")?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn duplicate_input_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"
                SELECT COUNT(1)
                FROM (
                    SELECT block_hash, transaction_id, index
                    FROM transaction_inputs
                    GROUP BY block_hash, transaction_id, index
                    HAVING COUNT(1) > 1
                ) AS subq;
                "#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn output_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare("SELECT COUNT(1) FROM transaction_outputs;")?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn duplicate_output_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"SELECT COUNT(1)
                FROM (
                    SELECT block_hash, transaction_id, index
                    FROM transaction_outputs
                    GROUP BY block_hash, transaction_id, index
                    HAVING COUNT(1) > 1
                ) AS subq;"#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn accepting_block_transactions_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare("SELECT COUNT(1) FROM accepting_block_transactions;")?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }

    pub fn duplicate_accepting_block_transactions_count(&self) -> Result<i64, Error> {
        let conn = self.get_connection()?;

        let count = conn
            .prepare(
                r#"
                SELECT COUNT(1)
                FROM (
                    SELECT block_hash, transaction_id
                    FROM accepting_block_transactions
                    GROUP BY block_hash, transaction_id
                    HAVING COUNT(1) > 1
                ) AS subq;
                "#,
            )?
            .query_row([], |row| row.get::<_, i64>(0))?;

        Ok(count)
    }
}
