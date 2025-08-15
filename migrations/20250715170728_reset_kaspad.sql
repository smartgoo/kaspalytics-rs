CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE SCHEMA IF NOT EXISTS kaspad;

DROP TABLE IF EXISTS kaspad.blocks_transactions CASCADE;
DROP TABLE IF EXISTS kaspad.blocks_parents CASCADE;
DROP TABLE IF EXISTS kaspad.address_transactions CASCADE;
DROP TABLE IF EXISTS kaspad.transactions_inputs CASCADE;
DROP TABLE IF EXISTS kaspad.transactions_outputs CASCADE;
DROP TABLE IF EXISTS kaspad.transactions CASCADE;
DROP TABLE IF EXISTS kaspad.blocks CASCADE;

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
SELECT create_hypertable('kaspad.blocks', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('kaspad.blocks', INTERVAL '10 days', if_not_exists => TRUE);

CREATE TABLE kaspad.blocks_parents (
    block_hash BYTEA NOT NULL,
    parent_hash BYTEA NOT NULL,
    block_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (block_hash, parent_hash, block_time)
);
CREATE INDEX ON kaspad.blocks_parents (parent_hash);
SELECT create_hypertable('kaspad.blocks_parents', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('kaspad.blocks_parents', INTERVAL '10 days', if_not_exists => TRUE);

CREATE TABLE kaspad.blocks_transactions (
    block_hash BYTEA NOT NULL,
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    block_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (block_hash, transaction_id, block_time)
);
CREATE INDEX ON kaspad.blocks_transactions (transaction_id);
SELECT create_hypertable('kaspad.blocks_transactions', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('kaspad.blocks_transactions', INTERVAL '10 days', if_not_exists => TRUE);

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
SELECT create_hypertable('kaspad.transactions', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('kaspad.transactions', INTERVAL '10 days', if_not_exists => TRUE);

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
CREATE INDEX ON kaspad.transactions_inputs (utxo_script_public_key_address);
SELECT create_hypertable('kaspad.transactions_inputs', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('kaspad.transactions_inputs', INTERVAL '10 days', if_not_exists => TRUE);

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
CREATE INDEX ON kaspad.transactions_outputs (script_public_key_address);
SELECT create_hypertable('kaspad.transactions_outputs', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
SELECT add_retention_policy('kaspad.transactions_outputs', INTERVAL '10 days', if_not_exists => TRUE);

-- CREATE TABLE kaspad.address_transactions (
--     address VARCHAR NOT NULL,
--     transaction_id BYTEA NOT NULL,
--     block_time TIMESTAMPTZ NOT NULL, -- TODO remove?
--     direction SMALLINT, -- TODO remove?
--     utxo_amount BIGINT, -- TODO remove?
--     PRIMARY KEY (address, transaction_id, block_time)
-- );
-- CREATE INDEX ON kaspad.address_transactions (transaction_id);
-- SELECT create_hypertable('kaspad.address_transactions', 'block_time', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
-- SELECT add_retention_policy('kaspad.address_transactions', INTERVAL '10 days', if_not_exists => TRUE);

CREATE TABLE kaspad.subnetwork_ids (
    id SERIAL PRIMARY KEY,
    subnetwork_id TEXT
);

CREATE TABLE kaspad.transaction_protocols (
    id SERIAL PRIMARY KEY,
    "name" TEXT,
    "description" TEXT
);

CREATE MATERIALIZED VIEW kaspad.protocol_activity_per_minute
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', block_time) AS minute_bucket,
    protocol_id,
    COUNT(*) AS transaction_count,
    COALESCE(SUM(COALESCE(total_input_amount, 0) - COALESCE(total_output_amount, 0)), 0) AS fees_generated,
    AVG(mass) AS avg_mass,
    MAX(mass) AS max_mass,
    MIN(mass) AS min_mass,
    SUM(total_output_amount) AS total_volume
FROM kaspad.transactions
WHERE subnetwork_id = 0 AND accepting_block_hash IS NOT NULL
GROUP BY minute_bucket, protocol_id
WITH NO DATA;

SELECT add_retention_policy(
    'kaspad.protocol_activity_per_minute', 
    INTERVAL '10 days', 
    if_not_exists => TRUE,
);

SELECT add_continuous_aggregate_policy(
    'kaspad.protocol_activity_per_minute',
    start_offset => INTERVAL '10 days',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE,
);

CREATE MATERIALIZED VIEW kaspad.address_spending_per_minute
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', block_time) AS minute_bucket,
    utxo_script_public_key_address AS address,
    COUNT(*) AS transaction_count,
    SUM(utxo_amount) AS total_spent
FROM kaspad.transactions_inputs
WHERE utxo_script_public_key_address IS NOT NULL
GROUP BY minute_bucket, utxo_script_public_key_address
WITH NO DATA;

SELECT add_continuous_aggregate_policy('kaspad.address_spending_per_minute',
    start_offset => INTERVAL '10 days',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE);

SELECT add_retention_policy('kaspad.address_spending_per_minute', INTERVAL '10 days', if_not_exists => TRUE);


-- Most active addresses by receiving (transaction outputs)
CREATE MATERIALIZED VIEW kaspad.address_receiving_per_minute
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', block_time) AS minute_bucket,
    script_public_key_address AS address,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_received
FROM kaspad.transactions_outputs
WHERE script_public_key_address IS NOT NULL
GROUP BY minute_bucket, script_public_key_address
WITH NO DATA;

SELECT add_continuous_aggregate_policy('kaspad.address_receiving_per_minute',
    start_offset => INTERVAL '10 days',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE);

SELECT add_retention_policy('kaspad.address_receiving_per_minute', INTERVAL '10 days', if_not_exists => TRUE);