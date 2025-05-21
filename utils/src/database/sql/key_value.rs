use chrono::{DateTime, Utc};
use sqlx::PgPool;

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KeyRegistry {
    PriceUsd,
    PriceBtc,
    MarketCap,
    Volume,
    DaaScore,
    PruningPoint,
    CsSompi,
    TransactionCount86400s,
    CoinbaseTransactionCount86400s,
    CoinbaseAcceptedTransactionCount86400s,
    UniqueTransactionCount86400s,
    UniqueTransactionAcceptedCount86400s,
    AcceptedTransactionCountPerHour24h,
    AcceptedTransactionCountPerMinute60m,
    AcceptedTransactionCountPerSecond60s,
    MinerNodeVersions1h,
}

impl KeyRegistry {
    fn as_str(&self) -> &str {
        match self {
            KeyRegistry::PriceUsd => "price_usd",
            KeyRegistry::PriceBtc => "price_btc",
            KeyRegistry::MarketCap => "market_cap",
            KeyRegistry::Volume => "volume",
            KeyRegistry::DaaScore => "daa_score",
            KeyRegistry::PruningPoint => "pruning_point",
            KeyRegistry::CsSompi => "cs_sompi",
            KeyRegistry::TransactionCount86400s => "transaction_count_86400s",
            KeyRegistry::CoinbaseTransactionCount86400s => "coinbase_transaction_count_86400s",
            KeyRegistry::CoinbaseAcceptedTransactionCount86400s => {
                "coinbase_accepted_transaction_count_86400s"
            }
            KeyRegistry::UniqueTransactionCount86400s => "unique_transaction_count_86400s",
            KeyRegistry::UniqueTransactionAcceptedCount86400s => {
                "unique_transaction_accepted_count_86400s"
            }
            KeyRegistry::AcceptedTransactionCountPerHour24h => {
                "accepted_transaction_count_per_hour_24h"
            }
            KeyRegistry::AcceptedTransactionCountPerMinute60m => {
                "accepted_transaction_count_per_minute_60m"
            }
            KeyRegistry::AcceptedTransactionCountPerSecond60s => {
                "accpeted_transaction_count_per_second_60s"
            }
            KeyRegistry::MinerNodeVersions1h => "miner_node_versions_1h",
        }
    }
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
    .bind(key.as_str())
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
