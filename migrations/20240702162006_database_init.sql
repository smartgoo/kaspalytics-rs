CREATE TABLE IF NOT EXISTS meta (
    id SERIAL PRIMARY KEY,
    key VARCHAR(50) UNIQUE,
    value VARCHAR(255) UNIQUE,
    created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date date UNIQUE,
    coinbase_tx_qty INTEGER,
    tx_qty INTEGER,
    input_qty_total INTEGER,
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
    tx_per_second_max DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS block_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date date UNIQUE,
    chain_block_count INTEGER,
    txs_per_block_mean DOUBLE PRECISION,
    txs_per_block_median DOUBLE PRECISION,
    txs_per_block_min DOUBLE PRECISION,
    txs_per_block_max DOUBLE PRECISION,
    unique_miners INTEGER
);

CREATE TABLE IF NOT EXISTS utxo_snapshot_header (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    block VARCHAR(64) NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE,
    daa_score BIGINT NOT NULL,
    utxo_count BIGINT,
    unique_address_count BIGINT,
    kas_price_usd DOUBLE PRECISION,
    percentile_analysis_completed BOOLEAN,
    circulating_supply DOUBLE PRECISION,
    kas_last_moved_by_age_bucket_complete BOOLEAN,
    unique_address_count_dust BIGINT,
    sompi_held_by_dust_addresses BIGINT,
    distribution_by_kas_bucket_complete BOOLEAN
);

CREATE TABLE IF NOT EXISTS daa_snapshot (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    daa_score BIGINT NOT NULL,
    block_timestamp TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS address_balance_snapshot (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    utxo_snapshot_id INTEGER REFERENCES utxo_snapshot_header (id),
    amount_sompi BIGINT NOT NULL,
    address VARCHAR(100)
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
    sompi_0_to_p01 DOUBLE PRECISION,
    sompi_p01_to_1 DOUBLE PRECISION,
    sompi_1_to_100 DOUBLE PRECISION,
    sompi_100_to_1k DOUBLE PRECISION,
    sompi_1k_to_10k DOUBLE PRECISION,
    sompi_10k_to_100k DOUBLE PRECISION,
    sompi_100k_to_1m DOUBLE PRECISION,
    sompi_1m_to_10m DOUBLE PRECISION,
    sompi_10m_to_100m DOUBLE PRECISION,
    sompi_100m_to_1b DOUBLE PRECISION,
    sompi_1b_to_10b DOUBLE PRECISION,
    cs_percent_0_to_p01 DOUBLE PRECISION,
    cs_percent_p01_to_1 DOUBLE PRECISION,
    cs_percent_1_to_100 DOUBLE PRECISION,
    cs_percent_100_to_1k DOUBLE PRECISION,
    cs_percent_1k_to_10k DOUBLE PRECISION,
    cs_percent_10k_to_100k DOUBLE PRECISION,
    cs_percent_100k_to_1m DOUBLE PRECISION,
    cs_percent_1m_to_10m DOUBLE PRECISION,
    cs_percent_10m_to_100m DOUBLE PRECISION,
    cs_percent_100m_to_1b DOUBLE PRECISION,
    cs_percent_1b_to_10b DOUBLE PRECISION,
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
    sompi_lt_1d BIGINT,
    sompi_1d_to_1w BIGINT,
    sompi_1w_to_1m BIGINT,
    sompi_1m_to_3m BIGINT,
    sompi_3m_to_6m BIGINT,
    sompi_6m_to_1y BIGINT,
    sompi_1y_to_2y BIGINT,
    sompi_2y_to_3y BIGINT,
    sompi_3y_to_5y BIGINT,
    sompi_5y_to_7y BIGINT,
    sompi_7y_to_10y BIGINT,
    sompi_gt_10y BIGINT,
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

CREATE TABLE IF NOT EXISTS hash_rate (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "timestamp" TIMESTAMP WITH TIME ZONE,
    hash_rate NUMERIC(40,0) NOT NULL,
    difficulty NUMERIC(40,0) NOT NULL
);

CREATE INDEX idx_timestamp ON hash_rate ("timestamp");

-- TODO add "granularity" field to this table (day, minute, etc.)
-- TODO add constraint on granularity and timestmap
CREATE TABLE IF NOT EXISTS coin_market_history (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL UNIQUE,
    symbol CHARACTER(10) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    market_cap DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS key_value (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    "key" CHARACTER VARYING NOT NULL UNIQUE,
    "value" CHARACTER VARYING NOT NULL,
    updated_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS known_addresses (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    address CHARACTER VARYING NOT NULL UNIQUE,
    label CHARACTER VARYING NOT NULL,
    type CHARACTER VARYING NOT NULL,
    added_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);
