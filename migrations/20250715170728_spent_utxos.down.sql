-- Add migration script here
ALTER TABLE kaspad.transactions_inputs 
ADD COLUMN previous_outpoint_script_public_key BYTEA,
ADD COLUMN previous_outpoint_script_public_key_address VARCHAR,
ADD COLUMN previous_outpoint_amount BIGINT,
DROP COLUMN utxo_amount,
DROP COLUMN utxo_script_public_key,
DROP COLUMN utxo_is_coinbase,
DROP COLUMN utxo_script_public_key_type,
DROP COLUMN utxo_script_public_key_address;

ALTER TABLE kaspad.blocks 
DROP COLUMN version,
DROP COLUMN hash_merkle_root,
-- DROP COLUMN parent_hashes,
-- DROP COLUMN children_hashes,
DROP COLUMN accepted_id_merkle_root,
DROP COLUMN utxo_commitment,
DROP COLUMN bits,
DROP COLUMN nonce,
DROP COLUMN blue_work,
DROP COLUMN blue_score,
DROP COLUMN pruning_point,
DROP COLUMN difficulty,
DROP COLUMN selected_parent_hash,
DROP COLUMN is_chain_block;

DROP TABLE kaspad.blocks_parents;