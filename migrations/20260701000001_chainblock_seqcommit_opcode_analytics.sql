-- Daily per-day counts of chain-block sequencing-commitment opcode
-- (OpChainblockSeqCommit, 0xd4) activity, written by the block pipeline alongside
-- the existing covenant / introspection opcode metrics. This opcode sits within
-- the introspection opcode range, so these counts are an overlapping subset of
-- introspection_opcode_tx_count / introspection_opcode_outputs_spent:
--   - chainblock_seqcommit_tx_count: accepted transactions using the opcode
--   - chainblock_seqcommit_outputs_spent: P2SH outputs whose revealed redeem script uses it
ALTER TABLE script_covenant_daily_summary
    ADD COLUMN chainblock_seqcommit_tx_count BIGINT,
    ADD COLUMN chainblock_seqcommit_outputs_spent BIGINT;
