-- Add migration script here
CREATE TABLE IF NOT EXISTS meta (
    id SERIAL PRIMARY KEY,
    key VARCHAR(50) UNIQUE,
    value VARCHAR(255) UNIQUE,
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date date UNIQUE,
    coinbase_tx_qty INTEGER,
    tx_qty INTEGER,
    unaccepted_tx_qty INTEGER,
    input_amt_total NUMERIC,
    input_amt_per_tx_mean DOUBLE PRECISION,
    input_amt_per_tx_median DOUBLE PRECISION,
    input_amt_per_tx_min NUMERIC,
    input_amt_per_tx_max NUMERIC,
    input_qty_total INTEGER,
    output_amt_total_coinbase NUMERIC,
    output_amt_total NUMERIC,
    output_amt_per_tx_mean DOUBLE PRECISION,
    output_amt_per_tx_median DOUBLE PRECISION,
    output_amt_per_tx_min NUMERIC,
    output_amt_per_tx_max NUMERIC,
    output_qty_total_coinbase INTEGER,
    output_qty_total INTEGER,
    fees_total NUMERIC,
    fees_mean DOUBLE PRECISION,
    fees_median DOUBLE PRECISION,
    fees_min DOUBLE PRECISION,
    fees_max DOUBLE PRECISION,
    skipped_tx_missing_inputs INTEGER,
    inputs_missing_previous_outpoint INTEGER,
    unique_senders INTEGER,
    unique_recipients INTEGER,
    unique_addresses INTEGER,
    tx_per_second_mean DOUBLE PRECISION,
    tx_per_second_median DOUBLE PRECISION,
    tx_per_second_min DOUBLE PRECISION,
    tx_per_second_max DOUBLE PRECISION,
    output_amt_total_change_adjusted NUMERIC,
    output_amt_per_tx_mean_change_adjusted NUMERIC,
    output_amt_per_tx_median_change_adjusted NUMERIC,
    output_amt_per_tx_min_change_adjusted NUMERIC,
    output_amt_per_tx_max_change_adjusted NUMERIC
);

CREATE TABLE IF NOT EXISTS block_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date date UNIQUE,
    spc_blocks_total INTEGER,
    non_spc_blocks_total INTEGER,
    accepting_blocks_total INTEGER,
    merged_blues_total INTEGER,
    merged_reds_total INTEGER,
    daas_total INTEGER,
    blocks_per_daa_mean DOUBLE PRECISION,
    blocks_per_daa_median DOUBLE PRECISION,
    blocks_per_daa_min INTEGER,
    blocks_per_daa_max INTEGER,
    block_interval_mean DOUBLE PRECISION,
    block_interval_median DOUBLE PRECISION,
    block_interval_min DOUBLE PRECISION,
    block_interval_max DOUBLE PRECISION,
    spc_block_interval_mean DOUBLE PRECISION,
    spc_block_interval_median DOUBLE PRECISION,
    spc_block_interval_min DOUBLE PRECISION,
    spc_block_interval_max DOUBLE PRECISION,
    blocks_per_second_mean DOUBLE PRECISION,
    blocks_per_second_median DOUBLE PRECISION,
    blocks_per_second_min DOUBLE PRECISION,
    blocks_per_second_max DOUBLE PRECISION,
    spc_blocks_per_second_mean DOUBLE PRECISION,
    spc_blocks_per_second_median DOUBLE PRECISION,
    spc_blocks_per_second_min DOUBLE PRECISION,
    spc_blocks_per_second_max DOUBLE PRECISION,
    txs_per_accepting_block_mean DOUBLE PRECISION,
    txs_per_accepting_block_median DOUBLE PRECISION,
    txs_per_accepting_block_min INTEGER,
    txs_per_accepting_block_max INTEGER,
    txs_per_block_mean DOUBLE PRECISION,
    txs_per_block_median DOUBLE PRECISION,
    txs_per_block_min DOUBLE PRECISION,
    txs_per_block_max DOUBLE PRECISION,
    input_amt_per_accepting_block_mean DOUBLE PRECISION,
    input_amt_per_accepting_block_median DOUBLE PRECISION,
    input_amt_per_accepting_block_min BIGINT,
    input_amt_per_accepting_block_max BIGINT,
    output_amt_per_accepting_block_mean DOUBLE PRECISION,
    output_amt_per_accepting_block_median DOUBLE PRECISION,
    output_amt_per_accepting_block_min BIGINT,
    output_amt_per_accepting_block_max BIGINT,
    unique_miners INTEGER
);

CREATE TABLE IF NOT EXISTS utxo_snapshot_header (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_complete BOOLEAN NOT NULL,
    block VARCHAR(64) NOT NULL,
    block_TIMESTAMP TIMESTAMP WITH TIME ZONE,
    daa_score BIGINT NOT NULL,
    utxo_count INTEGER,
    unique_address_count INTEGER,
    kas_price_usd DOUBLE PRECISION,
    percentile_analysis_completed BOOLEAN,
    circulating_supply DOUBLE PRECISION,
    kas_last_moved_by_age_bucket_complete BOOLEAN,
    unique_address_count_meaningful INTEGER,
    unique_address_count_non_meaningful INTEGER,
    sompi_held_by_non_meaningful_addresses BIGINT,
    distribution_by_kas_bucket_complete BOOLEAN,
    distribution_by_usd_bucket_complete BOOLEAN
);

CREATE TABLE IF NOT EXISTS daa_snapshot (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    daa_score BIGINT NOT NULL,
    block_timestamp_milliseconds BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS address_balance_snapshot (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    utxo_snapshot_id INTEGER NOT NULL,
    amount_sompi BIGINT NOT NULL,
    address VARCHAR(100)
)

CREATE TABLE IF NOT EXISTS coin_market_history (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    symbol VARCHAR(25)
    price DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
)
