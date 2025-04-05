use crate::Cache;
use chrono::{DateTime, Utc};
use kaspa_rpc_core::RpcBlock;
use kaspalytics_utils::config::Config;
use log::{debug, info};
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc::Receiver;

pub struct BlockWriter {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    cache: Arc<Cache>,
    pg_pool: PgPool,
    rx: Receiver<Vec<RpcBlock>>,

    batch_size: u64,
    batch: Vec<RpcBlock>,
}

impl BlockWriter {
    pub fn new(
        config: Config,
        shutdown_flag: Arc<AtomicBool>,
        cache: Arc<Cache>,
        pg_pool: PgPool,
        rx: Receiver<Vec<RpcBlock>>,
    ) -> Self {
        BlockWriter {
            config,
            shutdown_flag,
            cache,
            pg_pool,
            rx,
            batch_size: 100, // TODO
            batch: vec![],
        }
    }
}

impl BlockWriter {
    async fn insert_blocks(
        &self,
        chunk: &[RpcBlock],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        let mut qb = QueryBuilder::new("INSERT INTO kaspad.blocks (block_hash, block_time) ");

        qb.push_values(chunk, |mut builder, block| {
            builder.push_bind(block.header.hash.as_bytes()).push_bind(
                DateTime::<Utc>::from_timestamp_millis(block.header.timestamp as i64).unwrap(),
            );
        });

        qb.push(" ON CONFLICT (block_hash) DO NOTHING");

        qb.build().execute(&mut **tx).await?;

        Ok(())
    }

    async fn insert_transactions(
        &self,
        chunk: &[RpcBlock],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        let transactions = chunk.iter().flat_map(|block| {
            let block_time =
                DateTime::<Utc>::from_timestamp_millis(block.header.timestamp as i64).unwrap();

            block.transactions.iter().map(move |transaction| {
                let tx_id = transaction
                    .verbose_data
                    .as_ref()
                    .unwrap()
                    .transaction_id
                    .as_bytes();

                (tx_id, transaction.payload.clone(), block_time)
            })
        });

        let mut qb = QueryBuilder::new(
            "INSERT INTO kaspad.transactions (transaction_id, payload, block_time) ",
        );

        qb.push_values(transactions, |mut builder, (tx_id, payload, block_time)| {
            builder
                .push_bind(tx_id)
                .push_bind(payload)
                .push_bind(block_time);
        });

        qb.push(" ON CONFLICT (transaction_id) DO NOTHING");
        qb.build().execute(&mut **tx).await?;

        Ok(())
    }

    async fn insert_inputs(
        &self,
        chunk: &[RpcBlock],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        let inputs = chunk
            .iter()
            .flat_map(|block| block.transactions.iter())
            .flat_map(|transaction| {
                let tx_id = transaction
                    .verbose_data
                    .as_ref()
                    .unwrap()
                    .transaction_id
                    .as_bytes();

                transaction
                    .inputs
                    .iter()
                    .enumerate()
                    .map(move |(i, input)| {
                        (
                            tx_id,
                            i as i32,
                            input.previous_outpoint.transaction_id.as_bytes(),
                            input.previous_outpoint.index as i32,
                            input.signature_script.clone(),
                            input.sig_op_count as i32,
                        )
                    })
            });

        let mut qb = QueryBuilder::new(
            r#"
                INSERT INTO kaspad.transactions_inputs
                (transaction_id, index, previous_outpoint_transaction_id, previous_outpoint_index,
                    signature_script, sig_op_count)
            "#,
        );

        qb.push_values(
            inputs,
            |mut builder,
             (tx_id, index, prev_tx_id, prev_index, signature_script, sig_op_count)| {
                builder
                    .push_bind(tx_id)
                    .push_bind(index)
                    .push_bind(prev_tx_id)
                    .push_bind(prev_index)
                    .push_bind(signature_script)
                    .push_bind(sig_op_count);
            },
        );

        qb.push(" ON CONFLICT (transaction_id, index) DO NOTHING");
        qb.build().execute(&mut **tx).await?;

        Ok(())
    }

    async fn insert_outputs(
        &self,
        chunk: &[RpcBlock],
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<(), sqlx::Error> {
        let outputs = chunk
            .iter()
            .flat_map(|block| block.transactions.iter())
            .flat_map(|transaction| {
                let tx_id = transaction
                    .verbose_data
                    .as_ref()
                    .unwrap()
                    .transaction_id
                    .as_bytes();

                transaction
                    .outputs
                    .iter()
                    .enumerate()
                    .map(move |(i, output)| {
                        (
                            tx_id,
                            i as i32,
                            output.value as i64,
                            output.script_public_key.script(),
                            output
                                .verbose_data
                                .as_ref()
                                .unwrap()
                                .script_public_key_address
                                .to_string(),
                        )
                    })
            });

        let mut qb = QueryBuilder::new(
            r#"
                INSERT INTO kaspad.transactions_outputs
                (transaction_id, index, amount, script_public_key, script_public_key_address)
            "#,
        );

        qb.push_values(
            outputs,
            |mut builder, (tx_id, index, amount, script_pub_key, address)| {
                builder
                    .push_bind(tx_id)
                    .push_bind(index)
                    .push_bind(amount)
                    .push_bind(script_pub_key)
                    .push_bind(address);
            },
        );

        qb.push(" ON CONFLICT (transaction_id, index) DO NOTHING");
        qb.build().execute(&mut **tx).await?;

        Ok(())
    }

    async fn insert(&self) -> Result<(), sqlx::Error> {
        let s = std::time::Instant::now();

        let mut tx = self.pg_pool.begin().await?;

        for block_chunk in self.batch.chunks(10) {
            self.insert_blocks(block_chunk, &mut tx).await?;
            self.insert_transactions(block_chunk, &mut tx).await?;
            self.insert_inputs(block_chunk, &mut tx).await?;
            self.insert_outputs(block_chunk, &mut tx).await?;
        }

        tx.commit().await?;

        info!("BlockWriter insert took {}ms", s.elapsed().as_millis());
        Ok(())
    }

    async fn receive_blocks(&mut self, blocks: Vec<RpcBlock>) {
        self.batch.extend(blocks);

        if self.batch.len() as u64 >= self.batch_size {
            self.insert().await.unwrap();
            self.batch.clear();
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.rx.recv().await {
            if self.shutdown_flag.load(Ordering::Relaxed) {
                break;
            }

            self.receive_blocks(msg).await;
        }
    }
}
