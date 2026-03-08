-- Add realized price aggregate columns to utxo_snapshot_header
ALTER TABLE utxo_snapshot_header
    ADD COLUMN realized_cap_usd DOUBLE PRECISION,
    ADD COLUMN sompi_in_profit BIGINT,
    ADD COLUMN sompi_in_loss BIGINT,
    ADD COLUMN realized_price_analysis_complete BOOLEAN;

-- URPD: one row per price bucket per snapshot
CREATE TABLE IF NOT EXISTS utxo_realized_price_distribution (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    utxo_snapshot_id INT NOT NULL REFERENCES utxo_snapshot_header(id),
    price_bucket_low DOUBLE PRECISION NOT NULL,
    price_bucket_high DOUBLE PRECISION NOT NULL,
    sompi BIGINT NOT NULL,
    utxo_count BIGINT NOT NULL,
    cs_percent DOUBLE PRECISION NOT NULL
);

CREATE INDEX ON utxo_realized_price_distribution (utxo_snapshot_id);
