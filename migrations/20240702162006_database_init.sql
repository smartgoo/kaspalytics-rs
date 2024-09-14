-- Add migration script here
CREATE TABLE IF NOT EXISTS meta (
    id SERIAL PRIMARY KEY,
    key VARCHAR(50) UNIQUE,
    value VARCHAR(255) UNIQUE,
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS granularity (
    id SERIAL PRIMARY KEY,
    name VARCHAR(30) UNIQUE -- 'second', 'minute', 'hour', 'day', 'week', 'month'
);

CREATE TABLE IF NOT EXISTS data_point (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE -- e.g., 'total_transactions', 'total_dollars_transacted'
);

CREATE TABLE IF NOT EXISTS analysis (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    granularity_id INT,
    data_point_id INT,
    data DECIMAL(18, 2),
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (granularity_id) REFERENCES granularity(id),
    FOREIGN KEY (data_point_id) REFERENCES data_point(id)
);

CREATE TABLE IF NOT EXISTS outpoints (
    transaction_id BYTEA,
    transaction_index INTEGER,
    value BIGINT,
    script_public_key BYTEA,
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    spent TIMESTAMPTZ,
    CONSTRAINT outpoints_primary_key UNIQUE (transaction_id, transaction_index)
);

CREATE TABLE IF NOT EXISTS transaction_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    coinbase_tx_qty integer,
    tx_qty integer,
    unaccepted_tx_qty integer,
    input_amt_total numeric,
    input_amt_per_tx_mean double precision,
    input_amt_per_tx_median double precision,
    input_amt_per_tx_min numeric,
    input_amt_per_tx_max numeric,
    input_qty_total integer,
    output_amt_total_coinbase numeric,
    output_amt_total numeric,
    output_amt_per_tx_mean double precision,
    output_amt_per_tx_median double precision,
    output_amt_per_tx_min numeric,
    output_amt_per_tx_max numeric,
    output_qty_total_coinbase integer,
    output_qty_total integer,
    fees_total numeric,
    fees_mean double precision,
    fees_median double precision,
    fees_min double precision,
    fees_max double precision,
    skipped_tx_missing_inputs integer,
    inputs_missing_previous_outpoint integer,
    unique_senders integer,
    unique_recipients integer,
    unique_addresses integer,
    tx_per_second_mean double precision,
    tx_per_second_median double precision,
    tx_per_second_min double precision,
    tx_per_second_max double precision,
    output_amt_total_change_adjusted numeric,
    output_amt_per_tx_mean_change_adjusted numeric,
    output_amt_per_tx_median_change_adjusted numeric,
    output_amt_per_tx_min_change_adjusted numeric,
    output_amt_per_tx_max_change_adjusted numeric,
    date date
);

CREATE TABLE IF NOT EXISTS block_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spc_blocks_total integer,
    non_spc_blocks_total integer,
    accepting_blocks_total integer,
    merged_blues_total integer,
    merged_reds_total integer,
    daas_total integer,
    blocks_per_daa_mean double precision,
    blocks_per_daa_median double precision,
    blocks_per_daa_min integer,
    blocks_per_daa_max integer,
    block_interval_mean double precision,
    block_interval_median double precision,
    block_interval_min double precision,
    block_interval_max double precision,
    spc_block_interval_mean double precision,
    spc_block_interval_median double precision,
    spc_block_interval_min double precision,
    spc_block_interval_max double precision,
    blocks_per_second_mean double precision,
    blocks_per_second_median double precision,
    blocks_per_second_min double precision,
    blocks_per_second_max double precision,
    spc_blocks_per_second_mean double precision,
    spc_blocks_per_second_median double precision,
    spc_blocks_per_second_min double precision,
    spc_blocks_per_second_max double precision,
    txs_per_accepting_block_mean double precision,
    txs_per_accepting_block_median double precision,
    txs_per_accepting_block_min integer,
    txs_per_accepting_block_max integer,
    txs_per_block_mean double precision,
    txs_per_block_median double precision,
    txs_per_block_min double precision,
    txs_per_block_max double precision,
    input_amt_per_accepting_block_mean double precision,
    input_amt_per_accepting_block_median double precision,
    input_amt_per_accepting_block_min bigint,
    input_amt_per_accepting_block_max bigint,
    output_amt_per_accepting_block_mean double precision,
    output_amt_per_accepting_block_median double precision,
    output_amt_per_accepting_block_min bigint,
    output_amt_per_accepting_block_max bigint,
    unique_miners integer,
    date date
);