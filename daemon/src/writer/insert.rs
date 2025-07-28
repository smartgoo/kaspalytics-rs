use crate::writer::model::{DbBlockParent, DbBlockTransaction};

use super::{DbBlock, DbTransaction, DbTransactionInput, DbTransactionOutput};
use sqlx::{PgPool, QueryBuilder};

const CHUNK_SIZE: usize = 1000;

pub async fn insert_blocks_unnest(
    blocks: Vec<DbBlock>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    let mut block_hashes = Vec::new();
    let mut block_times = Vec::new();
    let mut versions = Vec::new();
    let mut hash_merkle_roots = Vec::new();
    let mut accepted_id_merkle_roots = Vec::new();
    let mut utxo_commitments = Vec::new();
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
        block_times.push(block.block_time);
        versions.push(block.version);
        hash_merkle_roots.push(block.hash_merkle_root);
        accepted_id_merkle_roots.push(block.accepted_id_merkle_root);
        utxo_commitments.push(block.utxo_commitment);
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
            block_hash, block_time, \"version\", hash_merkle_root, accepted_id_merkle_root,
            utxo_commitment, bits, nonce, daa_score, blue_work,
            blue_score, pruning_point, difficulty, selected_parent_hash, is_chain_block
        ) 
        SELECT * FROM UNNEST (
            $1::bytea[],                -- block_hash
            $2::timestamp[],            -- block_time
            $3::smallint[],             -- version
            $4::bytea[],                -- hash_merkle_root
            $5::bytea[],                -- accepted_id_merkle_root
            $6::bytea[],                -- utxo_committment
            $7::integer[],              -- bits
            $8::bigint[],               -- nonce
            $9::bigint[],               -- daa_score
            $10::bytea[],               -- blue_work
            $11::bigint[],              -- blue_score
            $12::bytea[],               -- pruning_point
            $13::double precision[],    -- difficulty
            $14::bytea[],               -- selected_parent_hash
            $15::boolean[]              -- is_chain_block
        )
        ON CONFLICT DO NOTHING
        ",
    );

    qb.build()
        .bind(block_hashes)
        .bind(block_times)
        .bind(versions)
        .bind(hash_merkle_roots)
        .bind(accepted_id_merkle_roots)
        .bind(utxo_commitments)
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

    for relationship in blocks_parents.into_iter() {
        block_hashes.push(relationship.block_hash);
        parent_hashes.push(relationship.parent_hash);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO kaspad.blocks_parents
        (
            block_hash, parent_hash
        ) 
        SELECT * FROM UNNEST (
            $1::bytea[],    -- block_hash
            $2::bytea[]     -- parent_hash
        )
        ON CONFLICT DO NOTHING
        ",
    );

    qb.build()
        .bind(block_hashes)
        .bind(parent_hashes)
        .execute(&pg_pool)
        .await?;

    Ok(())
}

pub async fn insert_blocks_transactions_unnest(
    blocks_transactions: Vec<DbBlockTransaction>,
    pg_pool: PgPool,
) -> Result<(), sqlx::Error> {
    let mut block_hashes = Vec::with_capacity(CHUNK_SIZE);
    let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
    let mut indexes = Vec::with_capacity(CHUNK_SIZE);

    for relationship in blocks_transactions.into_iter() {
        block_hashes.push(relationship.block_hash);
        transaction_ids.push(relationship.transaction_id);
        indexes.push(relationship.index);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO kaspad.blocks_transactions
        (
            block_hash, transaction_id, index
        ) 
        SELECT * FROM UNNEST (
            $1::bytea[],    -- block_hash
            $2::bytea[],    -- transaction_id
            $3::smallint[]  -- index
        )
        ON CONFLICT DO NOTHING
        ",
    );

    qb.build()
        .bind(block_hashes)
        .bind(transaction_ids)
        .bind(indexes)
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
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut versions = Vec::with_capacity(CHUNK_SIZE);
        let mut lock_times = Vec::with_capacity(CHUNK_SIZE);
        let mut subnetwork_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut gases = Vec::with_capacity(CHUNK_SIZE);
        let mut masses = Vec::with_capacity(CHUNK_SIZE);
        let mut compute_masses = Vec::with_capacity(CHUNK_SIZE);
        let mut accepting_blocks = Vec::with_capacity(CHUNK_SIZE);
        let mut block_times = Vec::with_capacity(CHUNK_SIZE);
        let mut protocol_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut total_input_amounts = Vec::with_capacity(CHUNK_SIZE);
        let mut total_output_amounts = Vec::with_capacity(CHUNK_SIZE);
        let mut payloads = Vec::with_capacity(CHUNK_SIZE);

        for tx in chunk.iter() {
            transaction_ids.push(tx.transaction_id.clone());
            versions.push(tx.version);
            lock_times.push(tx.lock_time);
            subnetwork_ids.push(tx.subnetwork_id);
            gases.push(tx.gas);
            masses.push(tx.mass);
            compute_masses.push(tx.compute_mass);
            accepting_blocks.push(tx.accepting_block_hash.clone());
            block_times.push(tx.block_time);
            protocol_ids.push(tx.protocol_id);
            total_input_amounts.push(tx.total_input_amount);
            total_output_amounts.push(tx.total_output_amount);
            payloads.push(tx.payload.clone());
        }

        let mut qb = QueryBuilder::new(
            "INSERT INTO kaspad.transactions
            (
                transaction_id, version, lock_time, subnetwork_id, gas,
                mass, compute_mass, accepting_block_hash, block_time, protocol_id,
                total_input_amount, total_output_amount, payload
            ) 
            SELECT * FROM UNNEST (
                $1::bytea[],        -- transaction_id
                $2::smallint[],     -- version
                $3::bigint[],       -- lock_time
                $4::integer[],      -- subnetwork_id
                $5::bigint[],       -- gas
                $6::bigint[],       -- mass
                $7::bigint[],       -- compute_mass
                $8::bytea[],        -- accepting_block_hash
                $9::timestamptz[],  -- block_time
                $10::integer[],     -- protocol
                $11::bigint[],      -- total_input_amount
                $12::bigint[],      -- total_output_amount
                $13::bytea[]        --payload
            )
            ON CONFLICT DO NOTHING
            ",
        );

        qb.build()
            .bind(transaction_ids)
            .bind(versions)
            .bind(lock_times)
            .bind(subnetwork_ids)
            .bind(gases)
            .bind(masses)
            .bind(compute_masses)
            .bind(accepting_blocks)
            .bind(block_times)
            .bind(protocol_ids)
            .bind(total_input_amounts)
            .bind(total_output_amounts)
            .bind(payloads)
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
                    transaction_id, index, previous_outpoint_transaction_id,
                    previous_outpoint_index, signature_script, sig_op_count, utxo_amount, utxo_script_public_key,
                    utxo_is_coinbase, utxo_script_public_key_type, utxo_script_public_key_address
                )
                SELECT * FROM UNNEST (
                    $1::bytea[],    -- transaction_id
                    $2::smallint[], -- index
                    $3::bytea[],    -- previous_outpoint_transaction_id
                    $4::smallint[], -- previous_outpoint_index
                    $5::bytea[],    -- signature_script
                    $6::smallint[], -- sig_op_count
                    $7::bigint[],   -- utxo_amount
                    $8::bytea[],    -- utxo_script_public_key
                    $9::boolean[],  -- utxo_is_coinbase
                    $10::smallint[],-- utxo_script_public_key_type
                    $11::varchar[]  -- utxo_script_public_key_address
                )
                ON CONFLICT DO NOTHING
            "#,
        );

        qb.build()
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
        let mut transaction_ids = Vec::with_capacity(CHUNK_SIZE);
        let mut indexes = Vec::with_capacity(CHUNK_SIZE);
        let mut amounts = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_keys = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_key_types = Vec::with_capacity(CHUNK_SIZE);
        let mut script_public_key_addresses = Vec::with_capacity(CHUNK_SIZE);

        for output in chunk.iter() {
            transaction_ids.push(output.transaction_id.clone());
            indexes.push(output.index);
            amounts.push(output.amount);
            script_public_keys.push(output.script_public_key.clone());
            script_public_key_types.push(output.script_public_key_type);
            script_public_key_addresses.push(output.script_public_key_address.clone());
        }

        let mut qb = QueryBuilder::new(
            r#"
                INSERT INTO kaspad.transactions_outputs
                (
                    transaction_id, index, amount, script_public_key, script_public_key_type,
                    script_public_key_address
                )
                SELECT * FROM UNNEST (
                    $1::bytea[],    -- transaction_id
                    $2::smallint[], -- index
                    $3::bigint[],   -- amount
                    $4::bytea[],    -- script_public_key
                    $5::smallint[], -- script_public_key_type
                    $6::varchar[]   -- script_public_key_address
                )
                ON CONFLICT DO NOTHING
            "#,
        );

        qb.build()
            .bind(transaction_ids)
            .bind(indexes)
            .bind(amounts)
            .bind(script_public_keys)
            .bind(script_public_key_types)
            .bind(script_public_key_addresses)
            .execute(&pg_pool)
            .await?;
    }

    Ok(())
}
