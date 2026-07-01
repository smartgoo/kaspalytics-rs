-- Per-ZK-tag breakdown of the aggregate zk_precompile_* metrics, written by the
-- block pipeline. OpZkPrecompile (0xa6) selects its verifier from a 1-byte tag
-- (Groth16 = 0x20, R0Succinct = 0x21) pushed immediately before the opcode. These
-- columns are overlapping subsets of zk_precompile_tx_count /
-- zk_precompile_outputs_spent; unknown_tag counts uses whose tag could not be
-- statically resolved (non-canonical script or a future tag) and is expected ~0.
ALTER TABLE script_covenant_daily_summary
    ADD COLUMN zk_precompile_groth16_tx_count BIGINT,
    ADD COLUMN zk_precompile_groth16_outputs_spent BIGINT,
    ADD COLUMN zk_precompile_r0succinct_tx_count BIGINT,
    ADD COLUMN zk_precompile_r0succinct_outputs_spent BIGINT,
    ADD COLUMN zk_precompile_unknown_tag_tx_count BIGINT,
    ADD COLUMN zk_precompile_unknown_tag_outputs_spent BIGINT;
