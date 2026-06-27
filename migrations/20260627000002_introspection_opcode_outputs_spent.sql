-- Daily per-day count of covenant / introspection opcode outputs spent, written
-- by the block pipeline alongside the existing introspection_opcode_tx_count.
-- Mirrors zk_precompile_outputs_spent so both opcode families expose symmetric
-- transaction-count and outputs-spent metrics:
--   - introspection_opcode_outputs_spent: P2SH outputs whose revealed redeem
--     script uses a covenant / introspection opcode
ALTER TABLE script_covenant_daily_summary
    ADD COLUMN introspection_opcode_outputs_spent BIGINT;
