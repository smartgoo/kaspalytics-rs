-- Daily per-day counts of ZK precompile opcode (OpZkPrecompile, 0xa6) activity,
-- written by the block pipeline alongside the existing covenant / introspection
-- opcode metrics:
--   - zk_precompile_tx_count: accepted transactions using the opcode
--   - zk_precompile_outputs_spent: P2SH outputs whose revealed redeem script uses it
ALTER TABLE script_covenant_daily_summary
    ADD COLUMN zk_precompile_tx_count BIGINT,
    ADD COLUMN zk_precompile_outputs_spent BIGINT;
