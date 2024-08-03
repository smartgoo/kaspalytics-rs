-- Add migration script here
CREATE TABLE IF NOT EXISTS meta (
    id SERIAL PRIMARY KEY,
    key VARCHAR(50) UNIQUE,
    value VARCHAR(255) UNIQUE,
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS granularity (
    id SERIAL PRIMARY KEY,
    name VARCHAR(30) UNIQUE -- 'second', 'minute', 'hour', 'day', 'week', 'month'
);

CREATE TABLE IF NOT EXISTS data_point (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE -- e.g., 'total_transactions', 'total_dollars_transacted'
);

CREATE TABLE IF NOT EXISTS analysis (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ,
    granularity_id INT,
    data_point_id INT,
    data DECIMAL(18, 2),
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (granularity_id) REFERENCES granularity(id),
    FOREIGN KEY (data_point_id) REFERENCES data_point(id)
);

CREATE TABLE IF NOT EXISTS outpoints (
    transaction_id BYTEA,
    transaction_index INTEGER,
    value BIGINT,
    script_public_key BYTEA,
    created TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    spent TIMESTAMPTZ,
    CONSTRAINT outpoints_primary_key UNIQUE (transaction_id, transaction_index)
)