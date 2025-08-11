-- Ensure TimescaleDB and schema exist
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE SCHEMA IF NOT EXISTS kaspad;

-- Drop existing tables in a safe order (if they exist)
DROP TABLE IF EXISTS kaspad.blocks_transactions CASCADE;
DROP TABLE IF EXISTS kaspad.blocks_parents CASCADE;
DROP TABLE IF EXISTS kaspad.address_transactions CASCADE;
DROP TABLE IF EXISTS kaspad.transactions_inputs CASCADE;
DROP TABLE IF EXISTS kaspad.transactions_outputs CASCADE;
DROP TABLE IF EXISTS kaspad.transactions CASCADE;
DROP TABLE IF EXISTS kaspad.blocks CASCADE;

-- Blocks (hypertable on block_time, 1-day chunks)
CREATE TABLE kaspad.blocks (
    block_hash BYTEA NOT NULL,
    block_time TIMESTAMPTZ NOT NULL,
    "version" SMALLINT,
    hash_merkle_root BYTEA,
    accepted_id_merkle_root BYTEA,
    utxo_commitment BYTEA,
    bits INTEGER,
    nonce BIGINT,
    daa_score BIGINT,
    blue_work BYTEA,
    blue_score BIGINT,
    pruning_point BYTEA,
    difficulty DOUBLE PRECISION,
    selected_parent_hash BYTEA,
    is_chain_block BOOLEAN,
    PRIMARY KEY (block_hash, block_time)
);
SELECT create_hypertable('kaspad.blocks', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Block parents (hypertable)
CREATE TABLE kaspad.blocks_parents (
    block_hash BYTEA NOT NULL,
    parent_hash BYTEA NOT NULL,
    block_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (block_hash, parent_hash, block_time)
);
SELECT create_hypertable('kaspad.blocks_parents', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Blocks to transactions mapping (hypertable)
CREATE TABLE kaspad.blocks_transactions (
    block_hash BYTEA NOT NULL,
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    block_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (block_hash, transaction_id, block_time)
);
SELECT create_hypertable('kaspad.blocks_transactions', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Transactions (hypertable)
CREATE TABLE kaspad.transactions (
    transaction_id BYTEA NOT NULL,
    "version" SMALLINT,
    lock_time BIGINT,
    subnetwork_id INTEGER,
    gas BIGINT,
    mass BIGINT,
    compute_mass BIGINT,
    accepting_block_hash BYTEA,
    block_time TIMESTAMPTZ NOT NULL,
    protocol_id INTEGER,
    total_input_amount BIGINT,
    total_output_amount BIGINT,
    payload BYTEA,
    PRIMARY KEY (transaction_id, block_time)
);
SELECT create_hypertable('kaspad.transactions', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Transaction inputs (hypertable)
CREATE TABLE kaspad.transactions_inputs (
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    previous_outpoint_transaction_id BYTEA,
    previous_outpoint_index SMALLINT,
    signature_script BYTEA,
    "sequence" BIGINT,
    sig_op_count SMALLINT,
    utxo_amount BIGINT,
    utxo_script_public_key BYTEA,
    utxo_is_coinbase BOOLEAN,
    utxo_script_public_key_type SMALLINT,
    utxo_script_public_key_address VARCHAR,
    block_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (transaction_id, "index", block_time)
);
SELECT create_hypertable('kaspad.transactions_inputs', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Transaction outputs (hypertable)
CREATE TABLE kaspad.transactions_outputs (
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    amount BIGINT,
    script_public_key BYTEA,
    script_public_key_type SMALLINT,
    script_public_key_address VARCHAR,
    block_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (transaction_id, "index", block_time)
);
SELECT create_hypertable('kaspad.transactions_outputs', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Address transactions (hypertable enforcing uniqueness within partition)
CREATE TABLE kaspad.address_transactions (
    address VARCHAR NOT NULL,
    transaction_id BYTEA NOT NULL,
    block_time TIMESTAMPTZ NOT NULL, -- TODO remove?
    direction SMALLINT, -- TODO remove?
    utxo_amount BIGINT, -- TODO remove?
    PRIMARY KEY (address, transaction_id, block_time)
);
SELECT create_hypertable('kaspad.address_transactions', 'block_time', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
-- Retain non-time-based indexes
CREATE INDEX ON kaspad.address_transactions (transaction_id);

-- Non-time-series supporting tables
CREATE TABLE kaspad.subnetwork_ids (
    id SERIAL PRIMARY KEY,
    subnetwork_id TEXT
);

CREATE TABLE kaspad.transaction_protocols (
    id SERIAL PRIMARY KEY,
    "name" TEXT,
    "description" TEXT
);