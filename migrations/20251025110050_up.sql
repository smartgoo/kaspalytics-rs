-- Add migration script here

ALTER TABLE kas_last_moved_by_age_bucket
ADD COLUMN sompi_3y_to_4y BIGINT,
ADD COLUMN sompi_4y_to_5y BIGINT,
ADD COLUMN sompi_5y_to_6y BIGINT,
ADD COLUMN sompi_6y_to_7y BIGINT,
ADD COLUMN sompi_7y_to_8y BIGINT,
ADD COLUMN sompi_8y_to_9y BIGINT,
ADD COLUMN sompi_9y_to_10y BIGINT,
ADD COLUMN cs_percent_3y_to_4y DOUBLE PRECISION,
ADD COLUMN cs_percent_4y_to_5y DOUBLE PRECISION,
ADD COLUMN cs_percent_5y_to_6y DOUBLE PRECISION,
ADD COLUMN cs_percent_6y_to_7y DOUBLE PRECISION,
ADD COLUMN cs_percent_7y_to_8y DOUBLE PRECISION,
ADD COLUMN cs_percent_8y_to_9y DOUBLE PRECISION,
ADD COLUMN cs_percent_9y_to_10y DOUBLE PRECISION;

UPDATE kas_last_moved_by_age_bucket
SET
    sompi_3y_to_4y = sompi_3y_to_5y, 
    cs_percent_3y_to_4y = cs_percent_3y_to_5y;

ALTER TABLE kas_last_moved_by_age_bucket
DROP COLUMN sompi_3y_to_5y,
DROP COLUMN sompi_5y_to_7y,
DROP COLUMN sompi_7y_to_10y,
DROP COLUMN cs_percent_3y_to_5y,
DROP COLUMN cs_percent_5y_to_7y,
DROP COLUMN cs_percent_7y_to_10y;
