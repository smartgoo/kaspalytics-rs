use super::{DbBlock, DbTransaction, DbTransactionInput, DbTransactionOutput};
use sqlx::{PgPool, QueryBuilder};

const CHUNK_SIZE: usize = 1000;

pub async fn insert_blocks_unnest(
    blocks: Vec<DbBlock>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    let mut block_times = Vec::new();
    let mut block_hashes = Vec::new();
    let mut daa_scores = Vec::new();
    for block in blocks.into_iter() {
        block_times.push(block.block_time);
        block_hashes.push(block.block_hash);
        daa_scores.push(block.daa_score);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO kaspad.blocks
        (block_time, block_hash, daa_score) 
        SELECT * FROM UNNEST (
            $1::timestamp[],
            $2::bytea[],
            $3::bigint[]
        )",
    );

    // qb.push(" ON CONFLICT DO NOTHING");

    qb.build()
        .bind(block_times)
        .bind(block_hashes)
        .bind(daa_scores)
        .execute(&pg_pool)
        .await?;

    Ok(())
}

pub async fn insert_transactions_unnest(
    transactions: Vec<DbTransaction>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    if transactions.is_empty() {
        return Ok(());
    }

    // TODO take ownership in iter chunks
    for chunk in transactions.chunks(CHUNK_SIZE) {
        let mut block_times = Vec::with_capacity(CHUNK_SIZE);
        let mut block_hashes = Vec::with_capacity(CHUNK_SIZE);
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut subnetwork_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut payloads = Vec::with_capacity(CHUNK_SIZE);
        let mut masses = Vec::with_capacity(CHUNK_SIZE);
        let mut compute_masses = Vec::with_capacity(CHUNK_SIZE);
        let mut accepting_blocks = Vec::with_capacity(CHUNK_SIZE);

        for tx in chunk.iter() {
            block_times.push(tx.block_time);
            block_hashes.push(tx.block_hash.clone());
            transaction_ids.push(tx.transaction_id.clone());
            subnetwork_ids.push(tx.subnetwork_id.clone());
            payloads.push(tx.payload.clone());
            masses.push(tx.mass);
            compute_masses.push(tx.compute_mass);
            accepting_blocks.push(tx.accepting_block_hash.clone());
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO kaspad.transactions
            (
                block_time, block_hash, transaction_id, subnetwork_id, payload,
                mass, compute_mass, accepting_block_hash
            ) 
            SELECT * FROM UNNEST (
                $1::timestamp[],
                $2::bytea[],
                $3::bytea[],
                $4::text[],
                $5::bytea[],
                $6::bigint[],
                $7::bigint[],
                $8::bytea[]
            )",
        );

        qb.build()
            .bind(block_times)
            .bind(block_hashes)
            .bind(transaction_ids)
            .bind(subnetwork_ids)
            .bind(payloads)
            .bind(masses)
            .bind(compute_masses)
            .bind(accepting_blocks)
            .execute(&pg_pool)
            .await?;
    }

    Ok(())
}

pub async fn insert_inputs_unnest(
    inputs: Vec<DbTransactionInput>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    if inputs.is_empty() {
        return Ok(());
    }

    // TODO take ownership in iter chunks
    for chunk in inputs.chunks(1000) {
        let mut block_times = Vec::with_capacity(CHUNK_SIZE);
        let mut block_hashes = Vec::with_capacity(CHUNK_SIZE);
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut prev_outpoint_tx_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut prev_outpoint_indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut signature_scripts = Vec::with_capacity(CHUNK_SIZE);
        let mut sig_op_counts = Vec::with_capacity(CHUNK_SIZE);

        for input in chunk.iter() {
            block_times.push(input.block_time);
            block_hashes.push(input.block_hash.clone());
            transaction_ids.push(input.transaction_id.clone());
            indexes.push(input.index);
            prev_outpoint_tx_ids.push(input.previous_outpoint_transaction_id.clone());
            prev_outpoint_indexes.push(input.previous_outpoint_index);
            signature_scripts.push(input.signature_script.clone());
            sig_op_counts.push(input.sig_op_count);
        }

        let mut qb = QueryBuilder::new(
            r#"
                INSERT INTO kaspad.transactions_inputs
                (
                    block_time, block_hash, transaction_id, index, previous_outpoint_transaction_id,
                    previous_outpoint_index, signature_script, sig_op_count
                )
                SELECT * FROM UNNEST (
                    $1::timestamp[],
                    $2::bytea[],
                    $3::bytea[],
                    $4::integer[],
                    $5::bytea[],
                    $6::integer[],
                    $7::bytea[],
                    $8::integer[]
                )
            "#,
        );

        qb.build()
            .bind(block_times)
            .bind(block_hashes)
            .bind(transaction_ids)
            .bind(indexes)
            .bind(prev_outpoint_tx_ids)
            .bind(prev_outpoint_indexes)
            .bind(signature_scripts)
            .bind(sig_op_counts)
            .execute(&pg_pool)
            .await?;
    }
    Ok(())
}

pub async fn insert_outputs_unnest(
    outputs: Vec<DbTransactionOutput>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    if outputs.is_empty() {
        return Ok(());
    }

    // TODO take ownership in iter chunks
    for chunk in outputs.chunks(1000) {
        let mut block_times = Vec::with_capacity(CHUNK_SIZE);
        let mut block_hashes = Vec::with_capacity(CHUNK_SIZE);
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut amounts = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_keys = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_key_addresses = Vec::with_capacity(CHUNK_SIZE);

        for output in chunk.iter() {
            block_times.push(output.block_time);
            block_hashes.push(output.block_hash.clone());
            transaction_ids.push(output.transaction_id.clone());
            indexes.push(output.index);
            amounts.push(output.amount);
            script_public_keys.push(output.script_public_key.clone());
            script_public_key_addresses.push(output.script_public_key_address.clone());
        }

        let mut qb = QueryBuilder::new(
            r#"
                INSERT INTO kaspad.transactions_outputs
                (
                    block_time, block_hash, transaction_id, index, amount,
                    script_public_key, script_public_key_address
                )
                SELECT * FROM UNNEST (
                    $1::timestamp[],
                    $2::bytea[],
                    $3::bytea[],
                    $4::integer[],
                    $5::bigint[],
                    $6::bytea[],
                    $7::text[]
                )
            "#,
        );

        qb.build()
            .bind(block_times)
            .bind(block_hashes)
            .bind(transaction_ids)
            .bind(indexes)
            .bind(amounts)
            .bind(script_public_keys)
            .bind(script_public_key_addresses)
            .execute(&pg_pool)
            .await?;
    }

    Ok(())
}
