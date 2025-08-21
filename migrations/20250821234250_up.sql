-- Add migration script here

SELECT remove_continuous_aggregate_policy('kaspad.protocol_activity_per_minute');
SELECT add_continuous_aggregate_policy('kaspad.protocol_activity_per_minute',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '60 seconds'
);

SELECT remove_continuous_aggregate_policy('kaspad.address_spending_per_minute');
SELECT add_continuous_aggregate_policy('kaspad.address_spending_per_minute',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '60 seconds'
);

SELECT remove_continuous_aggregate_policy('kaspad.address_receiving_per_minute');
SELECT add_continuous_aggregate_policy('kaspad.address_receiving_per_minute',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '60 seconds'
);
