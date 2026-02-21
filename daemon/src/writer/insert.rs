use super::model::DbNotableTx;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};

pub async fn insert_notable_transactions(
    rows: &[DbNotableTx],
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut transaction_ids: Vec<Vec<u8>> = Vec::with_capacity(rows.len());
    let mut block_times: Vec<DateTime<Utc>> = Vec::with_capacity(rows.len());
    let mut protocol_ids: Vec<Option<i32>> = Vec::with_capacity(rows.len());
    let mut fees: Vec<Option<i64>> = Vec::with_capacity(rows.len());
    let mut total_output_amounts: Vec<Option<i64>> = Vec::with_capacity(rows.len());

    for row in rows {
        transaction_ids.push(row.transaction_id.clone());
        block_times.push(row.block_time);
        protocol_ids.push(row.protocol_id);
        fees.push(row.fee);
        total_output_amounts.push(Some(row.total_output_amount));
    }

    sqlx::query(
        "INSERT INTO kaspad.notable_transactions
            (transaction_id, block_time, protocol_id, fee, total_output_amount)
        SELECT * FROM UNNEST(
            $1::bytea[],
            $2::timestamptz[],
            $3::integer[],
            $4::bigint[],
            $5::bigint[]
        )
        ON CONFLICT (transaction_id) DO NOTHING",
    )
    .bind(transaction_ids)
    .bind(block_times)
    .bind(protocol_ids)
    .bind(fees)
    .bind(total_output_amounts)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn delete_notable_transactions(
    ids: &[Vec<u8>],
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }

    sqlx::query("DELETE FROM kaspad.notable_transactions WHERE transaction_id = ANY($1::bytea[])")
        .bind(ids)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

/// `rows`: (minute_bucket, protocol_id, transaction_count, fees_generated)
pub async fn upsert_protocol_activity_minutely(
    rows: &[(DateTime<Utc>, i32, i64, i64)],
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut minute_buckets: Vec<DateTime<Utc>> = Vec::with_capacity(rows.len());
    let mut protocol_ids: Vec<i32> = Vec::with_capacity(rows.len());
    let mut transaction_counts: Vec<i64> = Vec::with_capacity(rows.len());
    let mut fees_generated: Vec<i64> = Vec::with_capacity(rows.len());

    for (bucket, protocol_id, tx_count, fees) in rows {
        minute_buckets.push(*bucket);
        protocol_ids.push(*protocol_id);
        transaction_counts.push(*tx_count);
        fees_generated.push(*fees);
    }

    sqlx::query(
        "INSERT INTO kaspad.protocol_activity_minutely
            (minute_bucket, protocol_id, transaction_count, fees_generated)
        SELECT * FROM UNNEST(
            $1::timestamptz[],
            $2::integer[],
            $3::bigint[],
            $4::bigint[]
        )
        ON CONFLICT (minute_bucket, protocol_id) DO UPDATE SET
            transaction_count = protocol_activity_minutely.transaction_count + EXCLUDED.transaction_count,
            fees_generated    = protocol_activity_minutely.fees_generated + EXCLUDED.fees_generated",
    )
    .bind(minute_buckets)
    .bind(protocol_ids)
    .bind(transaction_counts)
    .bind(fees_generated)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// `rows`: (address, transaction_count, total_spent)
pub async fn upsert_address_activity_minutely(
    minute_bucket: DateTime<Utc>,
    rows: &[(String, u64, u64)],
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut addresses: Vec<String> = Vec::with_capacity(rows.len());
    let mut transaction_counts: Vec<i64> = Vec::with_capacity(rows.len());
    let mut total_spents: Vec<i64> = Vec::with_capacity(rows.len());

    for (addr, tx_count, spent) in rows {
        addresses.push(addr.clone());
        transaction_counts.push(*tx_count as i64);
        total_spents.push(*spent as i64);
    }

    sqlx::query(
        "INSERT INTO kaspad.address_activity_minutely
            (minute_bucket, address, transaction_count, total_spent)
        SELECT $1, * FROM UNNEST(
            $2::varchar[],
            $3::bigint[],
            $4::bigint[]
        )
        ON CONFLICT (minute_bucket, address) DO UPDATE SET
            transaction_count = address_activity_minutely.transaction_count + EXCLUDED.transaction_count,
            total_spent       = address_activity_minutely.total_spent + EXCLUDED.total_spent",
    )
    .bind(minute_bucket)
    .bind(addresses)
    .bind(transaction_counts)
    .bind(total_spents)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

/// Delete rows older than `retention_days` from the two time-bounded minutely tables.
/// Called periodically (e.g. hourly) from the Writer to enforce retention without TimescaleDB.
pub async fn prune_old_minutely_rows(
    retention_days: i64,
    pool: &PgPool,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM kaspad.protocol_activity_minutely
         WHERE minute_bucket < NOW() - $1 * INTERVAL '1 day'",
    )
    .bind(retention_days)
    .execute(pool)
    .await?;

    sqlx::query(
        "DELETE FROM kaspad.address_activity_minutely
         WHERE minute_bucket < NOW() - $1 * INTERVAL '1 day'",
    )
    .bind(retention_days)
    .execute(pool)
    .await?;

    Ok(())
}
