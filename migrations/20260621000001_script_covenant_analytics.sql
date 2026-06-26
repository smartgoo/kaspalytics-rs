-- Daily per-day counts of transaction output script classes and covenant /
-- introspection opcode activity, written by the block pipeline.
CREATE TABLE IF NOT EXISTS script_covenant_daily_summary (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date date UNIQUE,
    output_count_pubkey BIGINT,
    output_count_pubkey_ecdsa BIGINT,
    output_count_script_hash BIGINT,
    output_count_nonstandard BIGINT,
    introspection_opcode_tx_count BIGINT,
    covenant_creating_tx_count BIGINT,
    covenant_outputs_created BIGINT,
    covenant_outputs_spent BIGINT
);

-- Count of existing UTXOs bound to a covenant id, captured per UTXO snapshot.
ALTER TABLE utxo_snapshot_header
    ADD COLUMN utxo_count_covenant BIGINT;
