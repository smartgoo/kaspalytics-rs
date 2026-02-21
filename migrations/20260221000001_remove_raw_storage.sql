-- Remove raw block/transaction storage and replace with narrow bounded tables.
-- Moves away from TimescaleDB — all 3 new tables are plain PostgreSQL tables.
-- Retention is handled by the daemon (periodic DELETE in the Writer).
--
-- Order: create new tables → backfill from old data → drop old tables/views.

-- ═══════════════════════════════════════════════════════════════════════════
-- 1. CREATE NEW TABLES
-- ═══════════════════════════════════════════════════════════════════════════

-- Top-~2000 notable transactions (top-1000 by fee + top-1000 by amount).
-- Size bounded by in-memory NotableTransactionTracker in the daemon.
CREATE TABLE IF NOT EXISTS kaspad.notable_transactions (
    transaction_id      BYTEA       PRIMARY KEY,
    block_time          TIMESTAMPTZ NOT NULL,
    protocol_id         INTEGER,
    fee                 BIGINT,
    total_output_amount BIGINT
);
CREATE INDEX IF NOT EXISTS notable_transactions_block_time_fee_idx
    ON kaspad.notable_transactions (block_time, fee);
CREATE INDEX IF NOT EXISTS notable_transactions_block_time_amount_idx
    ON kaspad.notable_transactions (block_time, total_output_amount);

-- Per-minute protocol activity aggregates.
CREATE TABLE IF NOT EXISTS kaspad.protocol_activity_minutely (
    minute_bucket       TIMESTAMPTZ NOT NULL,
    protocol_id         INTEGER     NOT NULL,
    transaction_count   BIGINT      NOT NULL DEFAULT 0,
    fees_generated      BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_bucket, protocol_id)
);
CREATE INDEX IF NOT EXISTS protocol_activity_minutely_bucket_idx
    ON kaspad.protocol_activity_minutely (minute_bucket);

-- Per-minute address activity for top-1000 most active addresses.
CREATE TABLE IF NOT EXISTS kaspad.address_activity_minutely (
    minute_bucket       TIMESTAMPTZ NOT NULL,
    address             VARCHAR     NOT NULL,
    transaction_count   BIGINT      NOT NULL DEFAULT 0,
    total_spent         BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (minute_bucket, address)
);
CREATE INDEX IF NOT EXISTS address_activity_minutely_bucket_idx
    ON kaspad.address_activity_minutely (minute_bucket);

-- ═══════════════════════════════════════════════════════════════════════════
-- 2. BACKFILL FROM OLD DATA
-- ═══════════════════════════════════════════════════════════════════════════

-- 2a. Notable transactions: union of top-1000 by fee + top-1000 by output amount.
--     The old PK is (transaction_id, block_time) so DISTINCT ON deduplicates.
INSERT INTO kaspad.notable_transactions
    (transaction_id, block_time, protocol_id, fee, total_output_amount)
WITH deduped AS (
    SELECT DISTINCT ON (transaction_id)
        transaction_id, block_time, protocol_id, fee, total_output_amount
    FROM kaspad.transactions
    WHERE subnetwork_id = 0 AND accepting_block_hash IS NOT NULL
    ORDER BY transaction_id, block_time DESC
),
top_by_fee AS (
    SELECT * FROM deduped WHERE fee IS NOT NULL
    ORDER BY fee DESC
    LIMIT 1000
),
top_by_amount AS (
    SELECT * FROM deduped WHERE total_output_amount IS NOT NULL
    ORDER BY total_output_amount DESC
    LIMIT 1000
)
SELECT * FROM top_by_fee
UNION
SELECT * FROM top_by_amount
ON CONFLICT (transaction_id) DO NOTHING;

-- 2b. Protocol activity: copy directly from the continuous aggregate.
INSERT INTO kaspad.protocol_activity_minutely
    (minute_bucket, protocol_id, transaction_count, fees_generated)
SELECT minute_bucket, protocol_id, transaction_count, fees_generated
FROM kaspad.protocol_activity_per_minute
WHERE protocol_id IS NOT NULL
ON CONFLICT (minute_bucket, protocol_id) DO NOTHING;

-- 2c. Address activity: only keep per-minute rows for the top-1000 addresses
--     (ranked by total transaction count across the entire window).
INSERT INTO kaspad.address_activity_minutely
    (minute_bucket, address, transaction_count, total_spent)
WITH top_addresses AS (
    SELECT address
    FROM kaspad.address_spending_per_minute
    GROUP BY address
    ORDER BY SUM(transaction_count) DESC
    LIMIT 1000
)
SELECT asp.minute_bucket, asp.address, asp.transaction_count, asp.total_spent
FROM kaspad.address_spending_per_minute asp
INNER JOIN top_addresses ta ON ta.address = asp.address
ON CONFLICT (minute_bucket, address) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════════
-- 3. DROP OLD TABLES AND VIEWS
-- ═══════════════════════════════════════════════════════════════════════════

-- Drop continuous aggregates first (CASCADE removes associated refresh/retention policies)
DROP MATERIALIZED VIEW IF EXISTS kaspad.protocol_activity_per_minute CASCADE;
DROP MATERIALIZED VIEW IF EXISTS kaspad.address_spending_per_minute CASCADE;
DROP MATERIALIZED VIEW IF EXISTS kaspad.address_receiving_per_minute CASCADE;

-- Drop raw tables
DROP TABLE IF EXISTS kaspad.transactions_inputs CASCADE;
DROP TABLE IF EXISTS kaspad.transactions_outputs CASCADE;
DROP TABLE IF EXISTS kaspad.blocks_transactions CASCADE;
DROP TABLE IF EXISTS kaspad.blocks_parents CASCADE;
DROP TABLE IF EXISTS kaspad.transactions CASCADE;
DROP TABLE IF EXISTS kaspad.blocks CASCADE;
DROP TABLE IF EXISTS kaspad.subnetwork_ids CASCADE;
DROP TABLE IF EXISTS kaspad.transaction_protocols CASCADE;
