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
    snapshot_complete BOOLEAN,
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
    utxo_snapshot_id INTEGER REFERENCES utxo_snapshot_header (id),
    amount_sompi BIGINT NOT NULL,
    address VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS coin_market_history (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    symbol VARCHAR(25),
    price DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS percentile_analysis (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    utxo_snapshot_id INTEGER REFERENCES utxo_snapshot_header (id),
    min_kas_top_point01_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_point10_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_1_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_5_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_10_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_25_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_50_percent DOUBLE PRECISION NOT NULL,
    min_kas_top_75_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_point01_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_point10_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_1_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_5_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_10_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_25_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_50_percent DOUBLE PRECISION NOT NULL,
    avg_kas_top_75_percent DOUBLE PRECISION NOT NULL,
    addr_count_top_point01_percent INTEGER NOT NULL,
    addr_count_top_point10_percent INTEGER NOT NULL,
    addr_count_top_1_percent INTEGER NOT NULL,
    addr_count_top_5_percent INTEGER NOT NULL,
    addr_count_top_10_percent INTEGER NOT NULL,
    addr_count_top_25_percent INTEGER NOT NULL,
    addr_count_top_50_percent INTEGER NOT NULL,
    addr_count_top_75_percent INTEGER NOT NULL,
    total_kas_top_point01_percent DOUBLE PRECISION,
    total_kas_top_point10_percent DOUBLE PRECISION,
    total_kas_top_1_percent DOUBLE PRECISION,
    total_kas_top_5_percent DOUBLE PRECISION,
    total_kas_top_10_percent DOUBLE PRECISION,
    total_kas_top_25_percent DOUBLE PRECISION,
    total_kas_top_50_percent DOUBLE PRECISION,
    total_kas_top_75_percent DOUBLE PRECISION,
    cs_percent_top_point01_percent DOUBLE PRECISION,
    cs_percent_top_point10_percent DOUBLE PRECISION,
    cs_percent_top_1_percent DOUBLE PRECISION,
    cs_percent_top_5_percent DOUBLE PRECISION,
    cs_percent_top_10_percent DOUBLE PRECISION,
    cs_percent_top_25_percent DOUBLE PRECISION,
    cs_percent_top_50_percent DOUBLE PRECISION,
    cs_percent_top_75_percent DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS distribution_by_kas_bucket (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    utxo_snapshot_id INTEGER REFERENCES utxo_snapshot_header (id),
    addr_qty_0_to_p01 INTEGER,
    addr_qty_p01_to_1 INTEGER,
    addr_qty_1_to_100 INTEGER,
    addr_qty_100_to_1k INTEGER,
    addr_qty_1k_to_10k INTEGER,
    addr_qty_10k_to_100k INTEGER,
    addr_qty_100k_to_1m INTEGER,
    addr_qty_1m_to_10m INTEGER,
    addr_qty_10m_to_100m INTEGER,
    addr_qty_100m_to_1b INTEGER,
    addr_qty_1b_to_10b INTEGER,
    pct_addr_0_to_p01 DOUBLE PRECISION,
    pct_addr_p01_to_1 DOUBLE PRECISION,
    pct_addr_1_to_100 DOUBLE PRECISION,
    pct_addr_100_to_1k DOUBLE PRECISION,
    pct_addr_1k_to_10k DOUBLE PRECISION,
    pct_addr_10k_to_100k DOUBLE PRECISION,
    pct_addr_100k_to_1m DOUBLE PRECISION,
    pct_addr_1m_to_10m DOUBLE PRECISION,
    pct_addr_10m_to_100m DOUBLE PRECISION,
    pct_addr_100m_to_1b DOUBLE PRECISION,
    pct_addr_1b_to_10b DOUBLE PRECISION,
    tot_kas_0_to_p01 DOUBLE PRECISION,
    tot_kas_p01_to_1 DOUBLE PRECISION,
    tot_kas_1_to_100 DOUBLE PRECISION,
    tot_kas_100_to_1k DOUBLE PRECISION,
    tot_kas_1k_to_10k DOUBLE PRECISION,
    tot_kas_10k_to_100k DOUBLE PRECISION,
    tot_kas_100k_to_1m DOUBLE PRECISION,
    tot_kas_1m_to_10m DOUBLE PRECISION,
    tot_kas_10m_to_100m DOUBLE PRECISION,
    tot_kas_100m_to_1b DOUBLE PRECISION,
    tot_kas_1b_to_10b DOUBLE PRECISION,
    pct_kas_0_to_p01 DOUBLE PRECISION,
    pct_kas_p01_to_1 DOUBLE PRECISION,
    pct_kas_1_to_100 DOUBLE PRECISION,
    pct_kas_100_to_1k DOUBLE PRECISION,
    pct_kas_1k_to_10k DOUBLE PRECISION,
    pct_kas_10k_to_100k DOUBLE PRECISION,
    pct_kas_100k_to_1m DOUBLE PRECISION,
    pct_kas_1m_to_10m DOUBLE PRECISION,
    pct_kas_10m_to_100m DOUBLE PRECISION,
    pct_kas_100m_to_1b DOUBLE PRECISION,
    pct_kas_1b_to_10b DOUBLE PRECISION,
    tot_usd_0_to_p01 DOUBLE PRECISION,
    tot_usd_p01_to_1 DOUBLE PRECISION,
    tot_usd_1_to_100 DOUBLE PRECISION,
    tot_usd_100_to_1k DOUBLE PRECISION,
    tot_usd_1k_to_10k DOUBLE PRECISION,
    tot_usd_10k_to_100k DOUBLE PRECISION,
    tot_usd_100k_to_1m DOUBLE PRECISION,
    tot_usd_1m_to_10m DOUBLE PRECISION,
    tot_usd_10m_to_100m DOUBLE PRECISION,
    tot_usd_100m_to_1b DOUBLE PRECISION,
    tot_usd_1b_to_10b DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS kas_last_moved_by_age_bucket (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    utxo_snapshot_id INTEGER REFERENCES utxo_snapshot_header (id),
    qty_kas_lt_1d DOUBLE PRECISION,
    qty_kas_1d_to_1w DOUBLE PRECISION,
    qty_kas_1w_to_1m DOUBLE PRECISION,
    qty_kas_1m_to_3m DOUBLE PRECISION,
    qty_kas_3m_to_6m DOUBLE PRECISION,
    qty_kas_6m_to_1y DOUBLE PRECISION,
    qty_kas_1y_to_2y DOUBLE PRECISION,
    qty_kas_2y_to_3y DOUBLE PRECISION,
    qty_kas_3y_to_5y DOUBLE PRECISION,
    qty_kas_5y_to_7y DOUBLE PRECISION,
    qty_kas_7y_to_10y DOUBLE PRECISION,
    qty_kas_gt_to_10y DOUBLE PRECISION,
    cs_percent_lt_1d DOUBLE PRECISION,
    cs_percent_1d_to_1w DOUBLE PRECISION,
    cs_percent_1w_to_1m DOUBLE PRECISION,
    cs_percent_1m_to_3m DOUBLE PRECISION,
    cs_percent_3m_to_6m DOUBLE PRECISION,
    cs_percent_6m_to_1y DOUBLE PRECISION,
    cs_percent_1y_to_2y DOUBLE PRECISION,
    cs_percent_2y_to_3y DOUBLE PRECISION,
    cs_percent_3y_to_5y DOUBLE PRECISION,
    cs_percent_5y_to_7y DOUBLE PRECISION,
    cs_percent_7y_to_10y DOUBLE PRECISION,
    cs_percent_gt_10y DOUBLE PRECISION
);