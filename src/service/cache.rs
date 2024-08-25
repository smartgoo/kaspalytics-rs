use super::models::*;
use kaspa_consensus_core::Hash;
use kaspa_rpc_core::{RpcBlock, RpcHash, RpcTransaction, RpcTransactionId, RpcTransactionOutput};
use log::info;
use sqlx::PgPool;
use std::collections::BTreeMap;

pub struct DAGCache {
    db_pool: PgPool,

    pub daas_blocks: BTreeMap<u64, Vec<RpcHash>>,
    pub blocks: BTreeMap<RpcHash, RpcBlock>,
    pub blocks_transactions: BTreeMap<RpcHash, Vec<RpcTransactionId>>,
    pub transactions: BTreeMap<RpcTransactionId, RpcTransaction>,
    pub transactions_blocks: BTreeMap<RpcTransactionId, Vec<RpcHash>>,
    pub outputs: BTreeMap<CacheTransactionOutpoint, RpcTransactionOutput>,

    pub chain_blocks: BTreeMap<RpcHash, Vec<RpcTransactionId>>,
    pub transaction_accepting_block: BTreeMap<RpcTransactionId, RpcHash>,

    db_output_buffer: BTreeMap<Hash, Vec<DbTransactionOutput>>,
}

impl DAGCache {
    pub fn new(db_pool: PgPool) -> Self {
        Self {
            db_pool,

            daas_blocks: BTreeMap::<u64, Vec<RpcHash>>::new(),
            blocks: BTreeMap::<RpcHash, RpcBlock>::new(),
            blocks_transactions: BTreeMap::<RpcHash, Vec<RpcTransactionId>>::new(),
            transactions: BTreeMap::<RpcTransactionId, RpcTransaction>::new(), // TODO is this really needed if full TX is in block?
            transactions_blocks: BTreeMap::<RpcTransactionId, Vec<RpcHash>>::new(),
            outputs: BTreeMap::<CacheTransactionOutpoint, RpcTransactionOutput>::new(),

            chain_blocks: BTreeMap::<RpcHash, Vec<RpcTransactionId>>::new(),
            transaction_accepting_block: BTreeMap::<RpcTransactionId, RpcHash>::new(),

            // db_checkpoint_hash: Vec<u64>
            db_output_buffer: BTreeMap::<Hash, Vec<DbTransactionOutput>>::new(),
        }
    }

    pub fn print_cache_sizes(&self) {
        info!("daas_blocks size: {}", self.daas_blocks.len());
        info!("blocks size: {}", self.blocks.len());
        info!(
            "blocks_transactions size: {}",
            self.blocks_transactions.len()
        );
        info!("transactions size: {}", self.transactions.len());
        info!(
            "transactions_blocks size: {}",
            self.transactions_blocks.len()
        );
        info!("outputs size {}", self.outputs.len());

        info!("chain_blocks size: {}", self.chain_blocks.len());
        info!(
            "transaction_accepting_block size: {}",
            self.transaction_accepting_block.len()
        );
        info!("db_output_buffer: {}", self.db_output_buffer.len());
    }
}

impl DAGCache {
    // TODO
    fn outputs_processor(&self, outputs: Vec<DbTransactionOutput>) {
        let db_pool = self.db_pool.clone();
        tokio::task::spawn(async move {
            let query = r#"
                INSERT INTO outpoints (transaction_id, transaction_index, value, script_public_key)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (transaction_id, transaction_index) DO NOTHING
            "#;

            let start = std::time::Instant::now();

            let mut tx = db_pool.begin().await.unwrap();
            let mut count = 0;

            for output in outputs {
                sqlx::query(query)
                    .bind(output.transaction_id.as_bytes())
                    .bind(output.index as i32)
                    .bind(output.value as i64)
                    .bind(output.script_public_key.script())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                count += 1;
            }

            tx.commit().await.unwrap();

            let duration = start.elapsed();

            info!("Inserted {} records in {:?}", count, duration);
        });
    }

    fn remove_transaction(&mut self, block_hash: Hash, transaction_id: Hash) {
        let transaction_in_blocks = self.transactions_blocks.get_mut(&transaction_id).unwrap();

        match transaction_in_blocks.len() {
            1 => {
                // Transaction is only in one cached block, remove
                let transaction = self.transactions.remove(&transaction_id).unwrap();
                self.transactions_blocks.remove(&transaction_id);
                self.transaction_accepting_block.remove(&transaction_id);

                // Remove only spent outputs
                // TODO cap how many ouputs are in cache for memory purposes, while keeping as many as possible
                transaction.inputs.iter().for_each(|input| {
                    self.outputs.remove(&input.previous_outpoint.into());
                });
            }
            _ => {
                // Transaction is in other cached blocks, remove block from self.transactions_blocks vec
                transaction_in_blocks.retain(|&inner_hash| block_hash != inner_hash);
            }
        }
    }

    fn remove_block(&mut self, block_hash: &Hash) {
        // TODO return result
        self.blocks.remove(block_hash);
        self.chain_blocks.remove(block_hash);

        let transaction_ids = self.blocks_transactions.remove(block_hash).unwrap();
        for transaction_id in transaction_ids {
            self.remove_transaction(block_hash.clone(), transaction_id);
        }
    }

    pub fn prune(&mut self) {
        // Keep 600 DAAs of blocks in memory
        // TODO make this adjust based on network block speed?

        while self.daas_blocks.len() > 86600 {
            let (daa, block_hashes) = self.daas_blocks.pop_first().unwrap();

            // Collect outputs for DB insertion batch
            let mut outputs = Vec::<DbTransactionOutput>::new();
            for block_hash in &block_hashes {
                let block = self.blocks.get(&block_hash).unwrap();
                for transaction in &block.transactions {
                    for (idx, output) in transaction.outputs.iter().enumerate() {
                        let db_output = DbTransactionOutput {
                            transaction_id: transaction
                                .verbose_data
                                .as_ref()
                                .unwrap()
                                .transaction_id
                                .clone(),
                            index: idx as u32,
                            value: output.value,
                            script_public_key: output.script_public_key.clone(),
                        };
                        outputs.push(db_output);
                    }
                }
            }

            // TODO Testing getting chainblock for DAA
            let mut chain_blocks = Vec::<Hash>::new();
            for block_hash in &block_hashes {
                if let Some(_) = self.chain_blocks.get(&block_hash) {
                    chain_blocks.push(*block_hash)
                }
            }

            match chain_blocks.len() {
                0 => {
                    // DAA does not have a chain block
                    // All blocks for this DAA should be moved to db_outputs_buffer how????
                    println!("daa {} has no chain blocks", daa)
                }
                1 => {
                    println!("daa {} has 1 chain blocks", daa);
                    self.db_output_buffer.insert(chain_blocks[0], outputs);
                    ()
                }
                _ => println!("daa {} has multiple chain blocks", daa),
            }

            for block_hash in &block_hashes {
                self.remove_block(block_hash);
            }
        }
        let (daa, _) = self.daas_blocks.first_key_value().unwrap();
        info!("oldest daa in cache {}", daa);
    }
}
