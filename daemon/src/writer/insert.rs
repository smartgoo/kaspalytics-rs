use crate::writer::model::{DbBlockParent, DbBlockTransaction};

use super::{DbBlock, DbTransaction, DbTransactionInput, DbTransactionOutput};
use sqlx::{PgPool, QueryBuilder};

const CHUNK_SIZE: usize = 1000;

pub async fn insert_blocks_unnest(
    blocks: Vec<DbBlock>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    let mut block_hashes = Vec::new();
    let mut versions = Vec::new();
    // let mut parent_hashes = Vec::new();
    let mut hash_merkle_roots = Vec::new();
    let mut accepted_id_merkle_roots = Vec::new();
    let mut utxo_commitments = Vec::new();
    let mut block_times = Vec::new();
    let mut bits = Vec::new();
    let mut nonces = Vec::new();
    let mut daa_scores = Vec::new();
    let mut blue_works = Vec::new();
    let mut blue_scores = Vec::new();
    let mut pruning_points = Vec::new();
    let mut difficulties = Vec::new();
    let mut selected_parent_hashes = Vec::new();
    let mut is_chain_blocks = Vec::new();

    for block in blocks.into_iter() {
        block_hashes.push(block.block_hash);
        versions.push(block.version);
        // parent_hashes.push(block.parent_hashes);
        hash_merkle_roots.push(block.hash_merkle_root);
        accepted_id_merkle_roots.push(block.accepted_id_merkle_root);
        utxo_commitments.push(block.utxo_commitment);
        block_times.push(block.block_time);
        bits.push(block.bits);
        nonces.push(block.nonce);
        daa_scores.push(block.daa_score);
        blue_works.push(block.blue_work);
        blue_scores.push(block.blue_score);
        pruning_points.push(block.pruning_point);
        difficulties.push(block.difficulty);
        selected_parent_hashes.push(block.selected_parent_hash);
        is_chain_blocks.push(block.is_chain_block);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO kaspad.blocks
        (
            block_hash, version, hash_merkle_root, accepted_id_merkle_root, utxo_commitment,
            block_time, bits, nonce, daa_score, blue_work, blue_score, 
            pruning_point, difficulty, selected_parent_hash, is_chain_block
        ) 
        SELECT * FROM UNNEST (
            $1::bytea[],
            $2::smallint[],
            $3::bytea[],
            $4::bytea[],
            $5::bytea[],
            $6::timestamp[],
            $7::integer[],
            $8::bigint[],
            $9::bigint[],
            $10::bytea[],
            $11::bigint[],
            $12::bytea[],
            $13::double precision[],
            $14::bytea[],
            $15::boolean[]
        )",
    );

    // qb.push(" ON CONFLICT DO NOTHING");

    qb.build()
        .bind(block_hashes)
        .bind(versions)
        // .bind(parent_hashes)
        .bind(hash_merkle_roots)
        .bind(accepted_id_merkle_roots)
        .bind(utxo_commitments)
        .bind(block_times)
        .bind(bits)
        .bind(nonces)
        .bind(daa_scores)
        .bind(blue_works)
        .bind(blue_scores)
        .bind(pruning_points)
        .bind(difficulties)
        .bind(selected_parent_hashes)
        .bind(is_chain_blocks)
        .execute(&pg_pool)
        .await?;

    Ok(())
}

pub async fn insert_blocks_parents_unnest(
    blocks_parents: Vec<DbBlockParent>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    let mut block_hashes = Vec::new();
    let mut parent_hashes = Vec::new();
    let mut block_times = Vec::new();

    for relationship in blocks_parents.into_iter() {
        block_hashes.push(relationship.block_hash);
        parent_hashes.push(relationship.parent_hash);
        block_times.push(relationship.block_time);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO kaspad.blocks_parents
        (block_hash, parent_hash, block_time) 
        SELECT * FROM UNNEST (
            $1::bytea[],
            $2::bytea[],
            $3::timestamp[]
        )",
    );

    // qb.push(" ON CONFLICT DO NOTHING");

    qb.build()
        .bind(block_hashes)
        .bind(parent_hashes)
        .bind(block_times)
        .execute(&pg_pool)
        .await?;

    Ok(())
}

pub async fn insert_blocks_transactions_unnest(
    blocks_transactions: Vec<DbBlockTransaction>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    let mut block_hashes = Vec::new();
    let mut transaction_ids = Vec::new();
    let mut block_times = Vec::new();

    for relationship in blocks_transactions.into_iter() {
        block_hashes.push(relationship.block_hash);
        transaction_ids.push(relationship.transaction_id);
        block_times.push(relationship.block_time);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO kaspad.blocks_transactions
        (block_hash, transaction_id, block_time) 
        SELECT * FROM UNNEST (
            $1::bytea[],
            $2::bytea[],
            $3::timestamp[]
        )",
    );

    // qb.push(" ON CONFLICT DO NOTHING");

    qb.build()
        .bind(block_hashes)
        .bind(transaction_ids)
        .bind(block_times)
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
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut versions = Vec::with_capacity(CHUNK_SIZE);
        let mut lock_times = Vec::with_capacity(CHUNK_SIZE);
        let mut subnetwork_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut gases = Vec::with_capacity(CHUNK_SIZE);
        let mut payloads = Vec::with_capacity(CHUNK_SIZE);
        let mut masses = Vec::with_capacity(CHUNK_SIZE);
        let mut compute_masses = Vec::with_capacity(CHUNK_SIZE);
        let mut accepting_blocks = Vec::with_capacity(CHUNK_SIZE);

        for tx in chunk.iter() {
            block_times.push(tx.block_time);
            transaction_ids.push(tx.transaction_id.clone());
            versions.push(tx.version);
            lock_times.push(tx.lock_time);
            subnetwork_ids.push(tx.subnetwork_id.clone());
            gases.push(tx.gas);
            payloads.push(tx.payload.clone());
            masses.push(tx.mass);
            compute_masses.push(tx.compute_mass);
            accepting_blocks.push(tx.accepting_block_hash.clone());
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO kaspad.transactions
            (
                block_time, transaction_id, version, lock_time, subnetwork_id,
                gas, payload, mass, compute_mass, accepting_block_hash
            ) 
            SELECT * FROM UNNEST (
                $1::timestamp[],
                $2::bytea[],
                $3::smallint[],
                $4::bigint[],
                $5::text[],
                $6::bigint[],
                $7::bytea[],
                $8::bigint[],
                $9::bigint[],
                $10::bytea[]
            )",
        );

        qb.build()
            .bind(block_times)
            .bind(transaction_ids)
            .bind(versions)
            .bind(lock_times)
            .bind(subnetwork_ids)
            .bind(gases)
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
        // let mut block_hashes = Vec::with_capacity(CHUNK_SIZE);
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut prev_outpoint_tx_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut prev_outpoint_indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut signature_scripts = Vec::with_capacity(CHUNK_SIZE);
        let mut sig_op_counts = Vec::with_capacity(CHUNK_SIZE);
        let mut utxo_amounts = Vec::with_capacity(CHUNK_SIZE);
        let mut utxo_script_public_keys = Vec::with_capacity(CHUNK_SIZE);
        let mut utxo_is_coinbases = Vec::with_capacity(CHUNK_SIZE);
        let mut utxo_script_public_key_types = Vec::with_capacity(CHUNK_SIZE);
        let mut utxo_script_public_key_addresses = Vec::with_capacity(CHUNK_SIZE);

        for input in chunk.iter() {
            block_times.push(input.block_time);
            // block_hashes.push(input.block_hash.clone());
            transaction_ids.push(input.transaction_id.clone());
            indexes.push(input.index);
            prev_outpoint_tx_ids.push(input.previous_outpoint_transaction_id.clone());
            prev_outpoint_indexes.push(input.previous_outpoint_index);
            signature_scripts.push(input.signature_script.clone());
            sig_op_counts.push(input.sig_op_count);
            utxo_amounts.push(input.utxo_amount);
            utxo_script_public_keys.push(input.utxo_script_public_key.clone());
            utxo_is_coinbases.push(input.utxo_is_coinbase);
            utxo_script_public_key_types.push(input.utxo_script_public_key_type);
            utxo_script_public_key_addresses.push(input.utxo_script_public_key_address.clone());
        }

        let mut qb = QueryBuilder::new(
            r#"
                INSERT INTO kaspad.transactions_inputs
                (
                    block_time, transaction_id, index, previous_outpoint_transaction_id,
                    previous_outpoint_index, signature_script, sig_op_count, utxo_amount, utxo_script_public_key,
                    utxo_is_coinbase, utxo_script_public_key_type, utxo_script_public_key_address
                )
                SELECT * FROM UNNEST (
                    $1::timestamp[],
                    $2::bytea[],
                    $3::integer[],
                    $4::bytea[],
                    $5::integer[],
                    $6::bytea[],
                    $7::integer[],
                    $8::bigint[],
                    $9::bytea[],
                    $10::boolean[],
                    $11::integer[],
                    $12::varchar[]
                )
            "#,
        );

        qb.build()
            .bind(block_times)
            .bind(transaction_ids)
            .bind(indexes)
            .bind(prev_outpoint_tx_ids)
            .bind(prev_outpoint_indexes)
            .bind(signature_scripts)
            .bind(sig_op_counts)
            .bind(utxo_amounts)
            .bind(utxo_script_public_keys)
            .bind(utxo_is_coinbases)
            .bind(utxo_script_public_key_types)
            .bind(utxo_script_public_key_addresses)
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
        // let mut block_hashes = Vec::with_capacity(CHUNK_SIZE);
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut amounts = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_keys = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_key_addresses = Vec::with_capacity(CHUNK_SIZE);

        for output in chunk.iter() {
            block_times.push(output.block_time);
            // block_hashes.push(output.block_hash.clone());
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
                    block_time, transaction_id, index, amount, script_public_key,
                    script_public_key_address
                )
                SELECT * FROM UNNEST (
                    $1::timestamp[],
                    $2::bytea[],
                    $3::integer[],
                    $4::bigint[],
                    $5::bytea[],
                    $6::text[]
                )
            "#,
        );

        qb.build()
            .bind(block_times)
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
