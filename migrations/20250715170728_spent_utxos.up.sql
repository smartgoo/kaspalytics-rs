-- Add migration script here
ALTER TABLE kaspad.transactions_inputs 
DROP COLUMN previous_outpoint_script_public_key,
DROP COLUMN previous_outpoint_script_public_key_address,
DROP COLUMN previous_outpoint_amount,
ADD COLUMN utxo_amount BIGINT,
ADD COLUMN utxo_script_public_key BYTEA,
ADD COLUMN utxo_is_coinbase BOOLEAN,
ADD COLUMN utxo_script_public_key_type INT,
ADD COLUMN utxo_script_public_key_address VARCHAR;

ALTER TABLE kaspad.blocks 
ADD COLUMN version SMALLINT,
ADD COLUMN hash_merkle_root BYTEA,
-- ADD COLUMN parent_hashes BYTEA[],
-- ADD COLUMN children_hashes BYTEA[],
ADD COLUMN accepted_id_merkle_root BYTEA,
ADD COLUMN utxo_commitment BYTEA,
ADD COLUMN bits INTEGER,
ADD COLUMN nonce BIGINT,
ADD COLUMN blue_work BYTEA,
ADD COLUMN blue_score BIGINT,
ADD COLUMN pruning_point BYTEA,
ADD COLUMN difficulty NUMERIC,
ADD COLUMN selected_parent_hash BYTEA,
ADD COLUMN is_chain_block BOOLEAN;

CREATE TABLE IF NOT EXISTS kaspad.blocks_parents (
    block_time TIMESTAMPTZ NOT NULL,
    block_hash BYTEA NOT NULL,
    parent_hash BYTEA NOT NULL
);
CREATE INDEX ON kaspad.blocks_parents (block_hash);
CREATE INDEX ON kaspad.blocks_parents (parent_hash);
SELECT create_hypertable('kaspad.blocks_parents', 'block_time', chunk_time_interval => INTERVAL '1 hour');
SELECT add_retention_policy('kaspad.blocks_parents', INTERVAL '48 hours');
