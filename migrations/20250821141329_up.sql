-- Add migration script here

SELECT add_continuous_aggregate_policy('kaspad.protocol_activity_per_minute',
    start_offset => INTERVAL '10 days',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE);