-- Drop and recreate kaspad.protocol_activity_per_minute materialized view
-- This is required to remove fields
DROP MATERIALIZED VIEW kaspad.protocol_activity_per_minute;

CREATE MATERIALIZED VIEW kaspad.protocol_activity_per_minute
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 minute', block_time) AS minute_bucket,
    protocol_id,
    COUNT(*) AS transaction_count,
    COALESCE(SUM(COALESCE(total_input_amount, 0) - COALESCE(total_output_amount, 0)), 0) AS fees_generated,
    SUM(total_output_amount) AS total_volume
FROM kaspad.transactions
WHERE subnetwork_id = 0 AND accepting_block_hash IS NOT NULL
GROUP BY minute_bucket, protocol_id
WITH NO DATA;

CREATE INDEX ON kaspad.protocol_activity_per_minute (minute_bucket, protocol_id);
