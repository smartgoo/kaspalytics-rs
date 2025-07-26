-- -----------------------------------------
-- -- Reset kaspad.blocks
-- -----------------------------------------
-- DROP TABLE kaspad.blocks CASCADE;

-- CREATE TABLE IF NOT EXISTS kaspad.blocks (
--     block_time TIMESTAMPTZ NOT NULL,
--     block_hash BYTEA NOT NULL,
--     version SMALLINT,
--     hash_merkle_root BYTEA,
--     accepted_id_merkle_root BYTEA,
--     utxo_commitment BYTEA,
--     bits INTEGER,
--     nonce BIGINT,
--     daa_score BIGINT,
--     blue_work BYTEA,
--     blue_score BIGINT,
--     pruning_point BYTEA,
--     difficulty NUMERIC,
--     selected_parent_hash BYTEA,
--     is_chain_block BOOLEAN
-- );

-- CREATE INDEX ON kaspad.blocks (block_hash);
-- SELECT create_hypertable('kaspad.blocks', 'block_time', chunk_time_interval => INTERVAL '1 hour');
-- -- SELECT remove_retention_policy('kaspad.blocks');
-- SELECT add_retention_policy('kaspad.blocks', INTERVAL '72 hours');


-- -----------------------------------------
-- -- Create kaspad.blocks_parents
-- -----------------------------------------
-- CREATE TABLE IF NOT EXISTS kaspad.blocks_parents (
--     block_time TIMESTAMPTZ NOT NULL,
--     block_hash BYTEA NOT NULL,
--     parent_hash BYTEA NOT NULL
-- );

-- CREATE INDEX ON kaspad.blocks_parents (block_hash);
-- CREATE INDEX ON kaspad.blocks_parents (parent_hash);

-- SELECT create_hypertable('kaspad.blocks_parents', 'block_time', chunk_time_interval => INTERVAL '1 hour');
-- SELECT add_retention_policy('kaspad.blocks_parents', INTERVAL '72 hours');


-- -----------------------------------------
-- -- Create kaspad.blocks_transactions
-- -----------------------------------------
-- CREATE TABLE IF NOT EXISTS kaspad.blocks_transactions (
--     block_time TIMESTAMPTZ NOT NULL,
--     block_hash BYTEA NOT NULL,
--     transaction_id BYTEA NOT NULL
-- );

-- CREATE INDEX ON kaspad.blocks_transactions (block_hash);
-- CREATE INDEX ON kaspad.blocks_transactions (transaction_id);

-- SELECT create_hypertable('kaspad.blocks_transactions', 'block_time', chunk_time_interval => INTERVAL '1 hour');
-- SELECT add_retention_policy('kaspad.blocks_transactions', INTERVAL '72 hours');


-- -----------------------------------------
-- -- Reset kaspad.transactions
-- -----------------------------------------
-- DROP TABLE kaspad.transactions CASCADE;

-- CREATE TABLE IF NOT EXISTS kaspad.transactions (
--     block_time TIMESTAMPTZ NOT NULL,
--     transaction_id BYTEA NOT NULL,
--     version SMALLINT,
--     lock_time BIGINT,
--     subnetwork_id TEXT,
--     gas BIGINT,
--     payload BYTEA,
--     mass BIGINT,
--     compute_mass BIGINT,
--     accepting_block_hash BYTEA
-- );

-- CREATE INDEX ON kaspad.transactions (transaction_id);

-- SELECT create_hypertable('kaspad.transactions', 'block_time', chunk_time_interval => INTERVAL '1 hour');
-- -- SELECT remove_retention_policy('kaspad.transactions');
-- SELECT add_retention_policy('kaspad.transactions', INTERVAL '72 hours');

-- -----------------------------------------
-- -- Reset kaspad.transactions_inputs
-- -----------------------------------------
-- DROP TABLE kaspad.transactions_inputs CASCADE;

-- CREATE TABLE IF NOT EXISTS kaspad.transactions_inputs (
--     block_time TIMESTAMPTZ NOT NULL,
--     transaction_id BYTEA NOT NULL,
--     index SMALLINT,
--     previous_outpoint_transaction_id BYTEA,
--     previous_outpoint_index SMALLINT,
--     signature_script BYTEA,
--     sequence BIGINT,
--     sig_op_count SMALLINT,
--     utxo_amount BIGINT,
--     utxo_script_public_key BYTEA,
--     utxo_is_coinbase BOOLEAN,
--     utxo_script_public_key_type SMALLINT,
--     utxo_script_public_key_address VARCHAR
-- );

-- CREATE INDEX ON kaspad.transactions_inputs (transaction_id);

-- SELECT create_hypertable('kaspad.transactions_inputs', 'block_time', chunk_time_interval => INTERVAL '1 hour');
-- -- SELECT remove_retention_policy('kaspad.transactions_inputs');
-- SELECT add_retention_policy('kaspad.transactions_inputs', INTERVAL '72 hours');


-- -----------------------------------------
-- -- Reset kaspad.transactions_outputs
-- -----------------------------------------
-- DROP TABLE kaspad.transactions_outputs CASCADE;

-- CREATE TABLE IF NOT EXISTS kaspad.transactions_outputs (
--     block_time TIMESTAMPTZ NOT NULL,
--     transaction_id BYTEA NOT NULL,
--     index SMALLINT,
--     amount BIGINT,
--     script_public_key BYTEA,
--     script_public_key_type SMALLINT,
--     script_public_key_address VARCHAR
-- );

-- CREATE INDEX ON kaspad.transactions_outputs (transaction_id);

-- SELECT create_hypertable('kaspad.transactions_outputs', 'block_time', chunk_time_interval => INTERVAL '1 hour');
-- -- SELECT remove_retention_policy('kaspad.transactions_outputs');
-- SELECT add_retention_policy('kaspad.transactions_outputs', INTERVAL '72 hours');

----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

-----------------------------------------
-- Reset kaspad.blocks
-----------------------------------------
DROP TABLE kaspad.blocks CASCADE;

CREATE TABLE IF NOT EXISTS kaspad.blocks (
    block_hash BYTEA PRIMARY KEY,
    block_time TIMESTAMPTZ,
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
    difficulty NUMERIC,
    selected_parent_hash BYTEA,
    is_chain_block BOOLEAN
);

-----------------------------------------
-- Create kaspad.blocks_parents
-----------------------------------------
CREATE TABLE IF NOT EXISTS kaspad.blocks_parents (
    block_hash BYTEA NOT NULL,
    parent_hash BYTEA NOT NULL,
    PRIMARY KEY (block_hash, parent_hash)
);

-----------------------------------------
-- Create kaspad.blocks_transactions
-----------------------------------------
CREATE TABLE IF NOT EXISTS kaspad.blocks_transactions (
    block_hash BYTEA NOT NULL,
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    PRIMARY KEY (block_hash, transaction_id)
);

-----------------------------------------
-- Reset kaspad.transactions
-----------------------------------------
DROP TABLE kaspad.transactions CASCADE;

CREATE TABLE IF NOT EXISTS kaspad.transactions (
    transaction_id BYTEA PRIMARY KEY,
    "version" SMALLINT,
    lock_time BIGINT,
    subnetwork_id INTEGER,
    gas BIGINT,
    mass BIGINT,
    compute_mass BIGINT,
    accepting_block_hash BYTEA,
    block_time TIMESTAMPTZ,
    protocol_id INTEGER,
    total_input_amount BIGINT,
    total_output_amount BIGINT,
    payload BYTEA
);

-----------------------------------------
-- Reset kaspad.transactions_inputs
-----------------------------------------
DROP TABLE kaspad.transactions_inputs CASCADE;

CREATE TABLE IF NOT EXISTS kaspad.transactions_inputs (
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
    PRIMARY KEY (transaction_id, "index")
);

-----------------------------------------
-- Reset kaspad.transactions_outputs
-----------------------------------------
DROP TABLE kaspad.transactions_outputs CASCADE;

CREATE TABLE IF NOT EXISTS kaspad.transactions_outputs (
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    amount BIGINT,
    script_public_key BYTEA,
    script_public_key_type SMALLINT,
    script_public_key_address VARCHAR,
    PRIMARY KEY (transaction_id, "index")
);

CREATE TABLE kaspad.subnetwork_ids (
    id SERIAL PRIMARY KEY,
    subnetwork_id TEXT
);

CREATE TABLE kaspad.transaction_protocols (
    id SERIAL PRIMARY KEY,
    "name" TEXT,
    "description" TEXT
);