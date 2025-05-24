use chrono::{DateTime, Utc};
use sqlx::PgPool;
use strum_macros::Display;

#[derive(Clone, Display, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KeyRegistry {
    PriceUsd,
    PriceBtc,
    MarketCap,
    Volume,
    DaaScore,
    PruningPoint,
    CsSompi,
    TransactionCount24h,
    CoinbaseTransactionCount24h,
    CoinbaseAcceptedTransactionCount24h,
    UniqueTransactionCount24h,
    UniqueTransactionAcceptedCount24h,
    AcceptedTransactionCountPerHour24h,
    AcceptedTransactionCountPerMinute1h,
    AcceptedTransactionCountPerSecond1m,
    MinerNodeVersionCount1h,
}

pub async fn upsert<V>(
    pg_pool: &PgPool,
    key: KeyRegistry,
    value: V,
    updated_at: DateTime<Utc>,
) -> Result<(), sqlx::Error>
where
    V: ToString,
{
    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES($1, $2, $3)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $2, updated_timestamp = $3
        "#,
    )
    .bind(key.to_string())
    .bind(value.to_string())
    .bind(updated_at)
    .execute(pg_pool)
    .await?;

    Ok(())
}

// TODO move Postgres key_value to a typed field structure
// pub async fn get(
//     pg_pool: &PgPool,
//     key: KeyRegistry,
// ) -> Result<Option<V>, sqlx::Error> {
//     sqlx::query(
//         "SELECT \"value\", updated_timestamp FROM key_value WHERE \"key\" = $1;",
//     )
//     .bind(key.as_str())
//     .execute(pg_pool)
//     .await?;

//     Ok(())
// }
