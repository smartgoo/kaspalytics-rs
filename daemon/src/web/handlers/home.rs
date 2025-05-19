use super::super::AppState;
use axum::{
    extract::State,
    response::sse::{Event, Sse},
};
use chrono::{DateTime, Utc};
use kaspalytics_utils::database::sql::hash_rate;
use kaspalytics_utils::formatters::hash_rate_with_unit;
use kaspalytics_utils::math::percent_change;
use log::info;
use rust_decimal::{prelude::{FromPrimitive, ToPrimitive}, Decimal};
use serde::Serialize;
use sqlx::{PgPool, Row};
use strum::IntoEnumIterator;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use strum_macros::EnumIter;
use tokio::sync::{watch, Mutex};
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};

#[derive(Clone, Copy, Debug, EnumIter, Eq, Hash, PartialEq, Serialize)]
enum SseKey {
    PriceUsd,
    PriceBtc,
    PruningPoint,
    MarketCap,
    Volume,
    DaaScore,
    CsSompi,
    CoinbaseTxCount86400s,
    CoinbaseAcceptedTxCount86400s,
    TxCount86400s,
    UniqueTxCount86400s,
    UniqueAcceptedTxCount86400s,
    AcceptedTxPerHour24h,
    AcceptedTxPerMinute60m,
    AcceptedTxPerSecond60s,
    MinerNodeVersions1h,
    CsAging,
    AddressByKasBalance,
    HashRate,
    HashRate7dChange,
    HashRate30dChange,
    HashRate90dChange,
    Difficulty,
    PercentIssued,
}

impl SseKey {
    fn as_str(&self) -> &'static str {
        match self {
            Self::PriceUsd => "price_usd",
            Self::PriceBtc => "price_btc",
            Self::PruningPoint => "pruning_point",
            Self::MarketCap => "market_cap",
            Self::Volume => "volume",
            Self::DaaScore => "daa_score",
            Self::CsSompi => "cs_sompi",
            Self::CoinbaseTxCount86400s => "coinbase_transaction_count_86400s",
            Self::CoinbaseAcceptedTxCount86400s => "coinbase_accepted_transaction_count_86400s",
            Self::TxCount86400s => "transaction_count_86400s",
            Self::UniqueTxCount86400s => "unique_transaction_count_86400s",
            Self::UniqueAcceptedTxCount86400s => "unique_transaction_accepted_count_86400s",
            Self::AcceptedTxPerHour24h => "accepted_transaction_count_per_hour_24h",
            Self::AcceptedTxPerMinute60m => "accepted_transaction_count_per_minute_60m",
            Self::AcceptedTxPerSecond60s => "accepted_transaction_count_per_second_60s",
            Self::MinerNodeVersions1h => "miner_node_versions_1h",
            Self::CsAging => "cs_aging",
            Self::AddressByKasBalance => "address_by_kas_balance",
            Self::HashRate => "hash_rate",
            Self::HashRate7dChange => "hash_rate_7d_change",
            Self::HashRate30dChange => "hash_rate_30d_change",
            Self::HashRate90dChange => "hash_rate_90d_change",
            Self::Difficulty => "difficulty",
            Self::PercentIssued => "percent_issued",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        use SseKey::*;
        Some(match s {
            "price_usd" => PriceUsd,
            "price_btc" => PriceBtc,
            "pruning_point" => PruningPoint,
            "market_cap" => MarketCap,
            "volume" => Volume,
            "daa_score" => DaaScore,
            "cs_sompi" => CsSompi,
            "coinbase_transaction_count_86400s" => CoinbaseTxCount86400s,
            "coinbase_accepted_transaction_count_86400s" => CoinbaseAcceptedTxCount86400s,
            "transaction_count_86400s" => TxCount86400s,
            "unique_transaction_count_86400s" => UniqueTxCount86400s,
            "unique_transaction_accepted_count_86400s" => UniqueAcceptedTxCount86400s,
            "accepted_transaction_count_per_hour_24h" => AcceptedTxPerHour24h,
            "accepted_transaction_count_per_minute_60m" => AcceptedTxPerMinute60m,
            "accepted_transaction_count_per_second_60s" => AcceptedTxPerSecond60s,
            "miner_node_versions_1h" => MinerNodeVersions1h,
            "cs_aging" => CsAging,
            "address_by_kas_balance" => AddressByKasBalance,
            "hash_rate" => HashRate,
            "hash_rate_7d_change" => HashRate7dChange,
            "hash_rate_30d_change" => HashRate30dChange,
            "hash_rate_90d_change" => HashRate90dChange,
            "difficulty" => Difficulty,
            "percent_issued" => PercentIssued,
            _ => return None,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
struct SseField {
    data: String,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
struct SseData {
    fields: HashMap<SseKey, SseField>,
}

impl SseData {
    fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    fn set(&mut self, key: SseKey, value: SseField) {
        self.fields.insert(key, value);
    }

    fn get(&self, key: &SseKey) -> Option<&SseField> {
        self.fields.get(key)
    }

    fn diff(&self, other: &SseData) -> HashMap<String, SseField> {
        let mut changes = HashMap::new();
        for key in SseKey::iter() {
            if let (None, Some(b)) = (self.get(&key), other.get(&key)) {
                changes.insert(key.as_str().to_string(), b.clone());
            } else if let (Some(a), Some(b)) = (self.get(&key), other.get(&key)) {
                if a.data != b.data {
                    changes.insert(key.as_str().to_string(), b.clone());
                }
            }
        }

        changes
    }
}

impl SseData {
    pub async fn from_db(pg_pool: &PgPool, cutoff: Option<DateTime<Utc>>) -> Result<Self, sqlx::Error> {
        let mut data = Self::new();

        let sql = r#"
            SELECT key, value, updated_timestamp 
            FROM key_value 
            WHERE key IN (
                'price_usd',
                'price_btc',
                'pruning_point',
                'market_cap',
                'volume',
                'daa_score',
                'cs_sompi',
                'coinbase_transaction_count_86400s',
                'coinbase_accepted_transaction_count_86400s',
                'transaction_count_86400s',
                'unique_transaction_count_86400s',
                'unique_transaction_accepted_count_86400s',
                'accepted_transaction_count_per_hour_24h',
                'accepted_transaction_count_per_minute_60m',
                'accepted_transaction_count_per_second_60s',
                'miner_node_versions_1h'
            )
        "#;

        let rows = match cutoff {
            Some(cutoff) => {
                let query_with_cutoff = format!("{} AND updated_timestamp > $1", sql);
                sqlx::query(&query_with_cutoff)
                    .bind(cutoff)
                    .fetch_all(pg_pool)
                    .await?
            }
            None => sqlx::query(sql).fetch_all(pg_pool).await?,
        };

        for row in rows {
            let key: String = row.get("key");
            if let Some(k) = SseKey::from_str(&key) {
                let value: String = row.get("value");
                let timestamp: DateTime<Utc> = row.get("updated_timestamp");
                data.set(k, SseField { data: value, timestamp });
            }
        }

        let hash_rate = hash_rate::get(pg_pool).await?;
        let hash_rate_c = hash_rate_with_unit(&[hash_rate.hash_rate.to_u64().unwrap()]);
        let hash_rate_str = format!("{} {}",
            Decimal::from_f64(hash_rate_c.0[0])
                .unwrap()
                .round_dp(2)
                .to_string(),
            hash_rate_c.1
        );

        data.set(SseKey::HashRate, SseField {
            data: hash_rate_str,
            timestamp: hash_rate.timestamp,
        });

        data.set(SseKey::Difficulty, SseField {
            data: hash_rate.difficulty.to_string(),
            timestamp: hash_rate.timestamp,
        });

        for days in [7, 30, 90] {
            let past = hash_rate::get_x_days_ago(pg_pool, days).await?;
            if let Some(change) = percent_change(hash_rate.hash_rate, past.hash_rate, 2) {
                let key = match days {
                    7 => SseKey::HashRate7dChange,
                    30 => SseKey::HashRate30dChange,
                    90 => SseKey::HashRate90dChange,
                    _ => continue,
                };

                data.set(key, SseField {
                    data: change.to_string(),
                    timestamp: past.timestamp,
                });
            }
        }

        Ok(data)
    }
}

struct SseState {
    pg_pool: PgPool,
    last_query_time: Mutex<DateTime<Utc>>,
    data: Mutex<SseData>
}

impl SseState {
    fn new(pg_pool: PgPool) -> Self {
        Self {
            pg_pool,
            last_query_time: Mutex::new(Utc::now()),
            data: Mutex::new(SseData::new()),
        }
    }

    async fn create_event(&self) -> Result<Event, Infallible> {
        let last_query_time = *self.last_query_time.lock().await;

        let last_data = self.data.lock().await.clone();
        let new_data = SseData::from_db(&self.pg_pool, Some(last_query_time)).await.unwrap();
        let deltas = last_data.diff(&new_data);

        *self.last_query_time.lock().await = Utc::now();
        *self.data.lock().await = new_data;

        let json = serde_json::to_string(&deltas).unwrap();
        Ok(Event::default().data(json))
    }
}

pub async fn stream(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        loop {
            interval.tick().await;
            let _ = tx.send(());
        }
    });

    let stream_state = Arc::new(SseState::new(state.pg_pool));

    let stream = WatchStream::new(rx).then({
        let stream_state = stream_state.clone();
        move |_| {
            let stream_state = stream_state.clone();
            async move { stream_state.create_event().await }
        }
    });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}
