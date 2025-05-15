use axum::{
    extract::State,
    response::sse::{Event, Sse},
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};

use super::AppState;

#[derive(Clone, Serialize)]
struct SseField {
    data: String,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
struct SseData {
    price_usd: Option<SseField>,
    price_btc: Option<SseField>,
    pruning_point: Option<SseField>,
    market_cap: Option<SseField>,
    volume: Option<SseField>,
    daa_score: Option<SseField>,
    cs_sompi: Option<SseField>,
    coinbase_transaction_count_86400s: Option<SseField>,
    coinbase_accepted_transaction_count_86400s: Option<SseField>,
    transaction_count_86400s: Option<SseField>,
    unique_transaction_count_86400s: Option<SseField>,
    unique_transaction_accepted_count_86400s: Option<SseField>,
    accepted_transaction_count_per_hour_24h: Option<SseField>,
    accepted_transaction_count_per_minute_60m: Option<SseField>,
    accepted_transaction_count_per_second_60s: Option<SseField>,
    miner_node_versions_1h: Option<SseField>,
}

impl SseData {
    fn new() -> Self {
        SseData {
            price_usd: None,
            price_btc: None,
            pruning_point: None,
            market_cap: None,
            volume: None,
            daa_score: None,
            cs_sompi: None,
            coinbase_transaction_count_86400s: None,
            coinbase_accepted_transaction_count_86400s: None,
            transaction_count_86400s: None,
            unique_transaction_count_86400s: None,
            unique_transaction_accepted_count_86400s: None,
            accepted_transaction_count_per_hour_24h: None,
            accepted_transaction_count_per_minute_60m: None,
            accepted_transaction_count_per_second_60s: None,
            miner_node_versions_1h: None,
        }
    }

    async fn from_db(pg_pool: &PgPool, cutoff: Option<DateTime<Utc>>) -> Result<Self, sqlx::Error> {
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

        let mut data = SseData::new();
        for row in rows {
            let key: String = row.get("key");
            let value: String = row.get("value");
            let timestamp: DateTime<Utc> = row.get("updated_timestamp");
            let sse_field = SseField {
                data: value,
                timestamp,
            };
            data.set_field(&key, sse_field);
        }

        Ok(data)
    }

    fn set_field(&mut self, key: &str, value: SseField) {
        match key {
            "price_usd" => self.price_usd = Some(value),
            "price_btc" => self.price_btc = Some(value),
            "pruning_point" => self.pruning_point = Some(value),
            "market_cap" => self.market_cap = Some(value),
            "volume" => self.volume = Some(value),
            "daa_score" => self.daa_score = Some(value),
            "cs_sompi" => self.cs_sompi = Some(value),
            "coinbase_transaction_count_86400s" => {
                self.coinbase_transaction_count_86400s = Some(value)
            }
            "coinbase_accepted_transaction_count_86400s" => {
                self.coinbase_accepted_transaction_count_86400s = Some(value)
            }
            "transaction_count_86400s" => self.transaction_count_86400s = Some(value),
            "unique_transaction_count_86400s" => self.unique_transaction_count_86400s = Some(value),
            "unique_transaction_accepted_count_86400s" => {
                self.unique_transaction_accepted_count_86400s = Some(value)
            }
            "accepted_transaction_count_per_hour_24h" => {
                self.accepted_transaction_count_per_hour_24h = Some(value)
            }
            "accepted_transaction_count_per_minute_60m" => {
                self.accepted_transaction_count_per_minute_60m = Some(value)
            }
            "accepted_transaction_count_per_second_60s" => {
                self.accepted_transaction_count_per_second_60s = Some(value)
            }
            "miner_node_versions_1h" => self.miner_node_versions_1h = Some(value),
            _ => {} // Ignore unknown keys
        }
    }
}

fn diff(old: &SseData, new: &SseData) -> HashMap<String, SseField> {
    let mut differences = HashMap::new();

    macro_rules! compare_field {
        ($field:ident, $key:expr) => {
            if let (Some(old_field), Some(new_field)) = (old.$field.as_ref(), new.$field.as_ref()) {
                if old_field.data != new_field.data {
                    differences.insert($key.to_string(), new_field.clone());
                }
            }
        };
    }

    compare_field!(price_usd, "price_usd");
    compare_field!(price_btc, "price_btc");
    compare_field!(pruning_point, "pruning_point");
    compare_field!(market_cap, "market_cap");
    compare_field!(volume, "volume");
    compare_field!(daa_score, "daa_score");
    compare_field!(cs_sompi, "cs_sompi");
    compare_field!(
        coinbase_transaction_count_86400s,
        "coinbase_transaction_count_86400s"
    );
    compare_field!(
        coinbase_accepted_transaction_count_86400s,
        "coinbase_accepted_transaction_count_86400s"
    );
    compare_field!(transaction_count_86400s, "transaction_count_86400s");
    compare_field!(
        unique_transaction_count_86400s,
        "unique_transaction_count_86400s"
    );
    compare_field!(
        unique_transaction_accepted_count_86400s,
        "unique_transaction_accepted_count_86400s"
    );
    compare_field!(
        accepted_transaction_count_per_hour_24h,
        "accepted_transaction_count_per_hour_24h"
    );
    compare_field!(
        accepted_transaction_count_per_minute_60m,
        "accepted_transaction_count_per_minute_60m"
    );
    compare_field!(
        accepted_transaction_count_per_second_60s,
        "accepted_transaction_count_per_second_60s"
    );
    compare_field!(miner_node_versions_1h, "miner_node_versions_1h");

    differences
}

pub async fn home_page_sse(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let _ = tx.send(());
        }
    });

    let last_query_time_store = Arc::new(Mutex::new(Utc::now()));
    let data_store = Arc::new(Mutex::new(SseData::new()));

    let stream = WatchStream::new(rx).then(move |_| {
        let pg_pool = state.pg_pool.clone();
        let last_query_time_store = last_query_time_store.clone();
        let data_store = data_store.clone();

        async move {
            let last_query_time = *last_query_time_store.lock().await;
            let old_data = data_store.lock().await.clone();

            let new_data = SseData::from_db(&pg_pool, Some(last_query_time))
                .await
                .unwrap();

            let deltas = diff(&old_data, &new_data);

            *last_query_time_store.lock().await = Utc::now();
            *data_store.lock().await = new_data;

            Ok(Event::default().data(serde_json::to_string(&deltas).unwrap()))
        }
    });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}
