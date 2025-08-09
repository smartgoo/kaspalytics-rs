DROP TABLE kaspad.blocks CASCADE;

CREATE TABLE kaspad.blocks (
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
    difficulty DOUBLE PRECISION,
    selected_parent_hash BYTEA,
    is_chain_block BOOLEAN
);

CREATE TABLE kaspad.blocks_parents (
    block_hash BYTEA NOT NULL,
    parent_hash BYTEA NOT NULL,
    PRIMARY KEY (block_hash, parent_hash)
);

CREATE TABLE kaspad.blocks_transactions (
    block_hash BYTEA NOT NULL,
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    PRIMARY KEY (block_hash, transaction_id)
);

DROP TABLE kaspad.transactions CASCADE;

CREATE TABLE kaspad.transactions (
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
CREATE INDEX ON kaspad.transactions (block_time);

DROP TABLE kaspad.transactions_inputs CASCADE;

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
    PRIMARY KEY (transaction_id, "index")
);

DROP TABLE kaspad.transactions_outputs CASCADE;

CREATE TABLE kaspad.transactions_outputs (
    transaction_id BYTEA NOT NULL,
    "index" SMALLINT,
    amount BIGINT,
    script_public_key BYTEA,
    script_public_key_type SMALLINT,
    script_public_key_address VARCHAR,
    PRIMARY KEY (transaction_id, "index")
);

CREATE TABLE kaspad.address_transactions (
    address VARCHAR,
    transaction_id BYTEA,
    block_time TIMESTAMPTZ, -- TODO remove?
    direction SMALLINT, -- TODO remove?
    utxo_amount BIGINT, -- TODO remove?
    PRIMARY KEY (address, transaction_id)
);
CREATE INDEX ON kaspad.address_transactions (block_time); -- TODO remove?
CREATE INDEX ON kaspad.address_transactions (transaction_id);

CREATE TABLE kaspad.subnetwork_ids (
    id SERIAL PRIMARY KEY,
    subnetwork_id TEXT
);

CREATE TABLE kaspad.transaction_protocols (
    id SERIAL PRIMARY KEY,
    "name" TEXT,
    "description" TEXT
);