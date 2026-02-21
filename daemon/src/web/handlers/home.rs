use super::super::AppState;
use crate::analysis::{
    mining,
    transactions::{counter as tx_counter, fees, protocol::TransactionProtocol},
};
use crate::ingest::cache::DagCache;
use crate::storage::cache::CacheEntry;
use crate::storage::{Reader, Storage};
use crate::AppContext;
use axum::{
    extract::State,
    response::sse::{Event, Sse},
};
use chrono::{DateTime, Utc};
use kaspalytics_utils::formatters::hash_rate_with_unit;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::Serialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use strum::IntoEnumIterator;
use strum_macros::{Display, EnumIter};
use tokio::sync::{watch, Mutex};
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};

#[derive(Clone, Copy, Debug, Display, EnumIter, Eq, Hash, PartialEq, Serialize)]
enum SseKey {
    // Markets
    PriceUsd,
    PriceBtc,
    PruningPoint,
    MarketCap,
    Volume,

    // Network
    DaaScore,
    CsSompi,
    Difficulty,
    PercentIssued,
    HashRate,
    HashRate7dChange,
    HashRate30dChange,
    HashRate90dChange,

    // Tx Counts
    UniqueAcceptedTransactionCount24h,
    UniqueAcceptedTransactionCountPerHour24h,
    ProtocolTransactionCounts24h,

    // Fees
    FeeMean60s,
    // FeeMedian60s,
    FeesTotal60s,
    FeeMean60m,
    // FeeMedian60m,
    FeesTotal60m,
    FeeMean24h,
    // FeeMedian24h,
    FeesTotal24h,
    FeesMeanTimeline,
    FeerateLow,
    FeerateNormal,
    FeeratePriority,

    // Misc
    MinerNodeVersionCount1h,
    CsAging,
    AddressByKasBalance,
    MempoolTransactionCount,
}

#[derive(Clone, Debug, Serialize)]
struct SseField {
    data: String,
    timestamp: DateTime<Utc>,
}

impl<T: ToString> From<T> for SseField {
    fn from(value: T) -> Self {
        SseField {
            data: value.to_string(),
            timestamp: Utc::now(),
        }
    }
}

impl<T: ToString> From<CacheEntry<T>> for SseField {
    fn from(value: CacheEntry<T>) -> Self {
        SseField {
            data: value.value.to_string(),
            timestamp: value.timestamp,
        }
    }
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

    fn data(&self) -> HashMap<String, SseField> {
        let mut data = HashMap::new();
        for key in SseKey::iter() {
            if let Some(v) = self.get(&key) {
                data.insert(key.to_string(), v.clone());
            }
        }

        data
    }

    fn diff(&self, other: &SseData) -> HashMap<String, SseField> {
        let mut changes = HashMap::new();
        for key in SseKey::iter() {
            if let (None, Some(b)) = (self.get(&key), other.get(&key)) {
                changes.insert(key.to_string(), b.clone());
            } else if let (Some(a), Some(b)) = (self.get(&key), other.get(&key)) {
                if a.data != b.data {
                    changes.insert(key.to_string(), b.clone());
                }
            }
        }

        changes
    }
}

impl SseData {
    pub async fn with_data(
        storage: Arc<Storage>,
        dag_cache: Arc<DagCache>,
        cutoff: Option<DateTime<Utc>>,
    ) -> Result<Self, sqlx::Error> {
        let mut data = Self::new();

        Self::collect_hash_rate_data(&mut data, &storage).await?;
        Self::collect_price_data(&mut data, &storage, cutoff).await;
        Self::collect_market_data(&mut data, &storage, cutoff).await;
        Self::collect_chain_data(&mut data, &storage, cutoff).await;
        Self::collect_transaction_data(&mut data, &storage, &dag_cache).await;
        Self::collect_fee_data(&mut data, &dag_cache).await;
        Self::collect_feerate_data(&mut data, &storage, cutoff).await;
        Self::collect_mining_data(&mut data, &dag_cache).await;

        Ok(data)
    }

    async fn collect_hash_rate_data(&mut self, storage: &Arc<Storage>) -> Result<(), sqlx::Error> {
        let hash_rate = storage.get_hash_rate().await;
        let hash_rate_c = hash_rate_with_unit(&[hash_rate.value]);
        let hash_rate_str = format!(
            "{} {}",
            Decimal::from_f64(hash_rate_c.0[0]).unwrap().round_dp(2),
            hash_rate_c.1
        );

        self.set(
            SseKey::HashRate,
            SseField {
                data: hash_rate_str,
                timestamp: hash_rate.timestamp,
            },
        );

        let difficulty = storage.get_difficulty().await;
        self.set(
            SseKey::Difficulty,
            SseField {
                data: difficulty.value.to_string(),
                timestamp: hash_rate.timestamp,
            },
        );

        // Get cached hash rate changes
        let hash_rate_7d = storage.get_hash_rate_7d_change().await;
        self.set(
            SseKey::HashRate7dChange,
            SseField {
                data: hash_rate_7d.value.to_string(),
                timestamp: hash_rate_7d.timestamp,
            },
        );

        let hash_rate_30d = storage.get_hash_rate_30d_change().await;
        self.set(
            SseKey::HashRate30dChange,
            SseField {
                data: hash_rate_30d.value.to_string(),
                timestamp: hash_rate_30d.timestamp,
            },
        );

        let hash_rate_90d = storage.get_hash_rate_90d_change().await;
        self.set(
            SseKey::HashRate90dChange,
            SseField {
                data: hash_rate_90d.value.to_string(),
                timestamp: hash_rate_90d.timestamp,
            },
        );

        Ok(())
    }

    async fn collect_price_data(&mut self, storage: &Storage, cutoff: Option<DateTime<Utc>>) {
        let price_usd = storage.get_price_usd().await;
        match cutoff {
            Some(cutoff) => {
                if price_usd.timestamp > cutoff {
                    self.set(SseKey::PriceUsd, SseField::from(price_usd));
                }
            }
            None => {
                self.set(SseKey::PriceUsd, SseField::from(price_usd));
            }
        }

        let price_btc = storage.get_price_btc().await;
        match cutoff {
            Some(cutoff) => {
                if price_btc.timestamp > cutoff {
                    self.set(SseKey::PriceBtc, SseField::from(price_btc));
                }
            }
            None => {
                self.set(SseKey::PriceBtc, SseField::from(price_btc));
            }
        }
    }

    async fn collect_market_data(&mut self, storage: &Storage, cutoff: Option<DateTime<Utc>>) {
        let market_cap = storage.get_market_cap().await;
        match cutoff {
            Some(cutoff) => {
                if market_cap.timestamp > cutoff {
                    self.set(SseKey::MarketCap, SseField::from(market_cap));
                }
            }
            None => {
                self.set(SseKey::MarketCap, SseField::from(market_cap));
            }
        }

        let volume = storage.get_volume().await;
        match cutoff {
            Some(cutoff) => {
                if volume.timestamp > cutoff {
                    self.set(SseKey::Volume, SseField::from(volume));
                }
            }
            None => {
                self.set(SseKey::Volume, SseField::from(volume));
            }
        }
    }

    async fn collect_chain_data(&mut self, storage: &Storage, cutoff: Option<DateTime<Utc>>) {
        let pruning_point = storage.get_pruning_point().await;
        match cutoff {
            Some(cutoff) => {
                if pruning_point.timestamp > cutoff {
                    self.set(SseKey::PruningPoint, SseField::from(pruning_point));
                }
            }
            None => {
                self.set(SseKey::PruningPoint, SseField::from(pruning_point));
            }
        }

        let daa_score = storage.get_daa_score().await;
        match cutoff {
            Some(cutoff) => {
                if daa_score.timestamp > cutoff {
                    self.set(SseKey::DaaScore, SseField::from(daa_score));
                }
            }
            None => {
                self.set(SseKey::DaaScore, SseField::from(daa_score));
            }
        }

        let circulating_supply = storage.get_circulating_supply().await;
        match cutoff {
            Some(cutoff) => {
                if circulating_supply.timestamp > cutoff {
                    self.set(SseKey::CsSompi, SseField::from(circulating_supply));
                }
            }
            None => {
                self.set(SseKey::CsSompi, SseField::from(circulating_supply));
            }
        }
    }

    async fn collect_transaction_data(&mut self, storage: &Storage, dag_cache: &Arc<DagCache>) {
        let threshold = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 86400;

        self.set(
            SseKey::UniqueAcceptedTransactionCountPerHour24h,
            SseField::from(
                serde_json::to_string(&tx_counter::unique_accepted_count_per_hour_24h(dag_cache))
                    .unwrap(),
            ),
        );

        self.set(
            SseKey::UniqueAcceptedTransactionCount24h,
            SseField::from(tx_counter::unique_accepted_transaction_count(
                dag_cache, threshold,
            )),
        );

        // Collect all protocol transaction counts in a HashMap
        let protocol_counts: HashMap<String, u64> = TransactionProtocol::iter()
            .map(|p| {
                let count = tx_counter::protocol_transaction_count(dag_cache, p.clone(), threshold);
                (p.to_string(), count)
            })
            .collect();

        self.set(
            SseKey::ProtocolTransactionCounts24h,
            SseField::from(serde_json::to_string(&protocol_counts).unwrap()),
        );

        self.set(
            SseKey::MempoolTransactionCount,
            SseField::from(storage.get_mempool_transaction_count().await),
        );
    }

    async fn collect_fee_data(&mut self, dag_cache: &Arc<DagCache>) {
        self.set(
            SseKey::FeeMean60s,
            SseField::from(fees::median_fee(dag_cache, 60)),
        );

        self.set(
            SseKey::FeeMean60m,
            SseField::from(fees::median_fee(dag_cache, 3600)),
        );

        self.set(
            SseKey::FeeMean24h,
            SseField::from(fees::median_fee(dag_cache, 86400)),
        );

        self.set(
            SseKey::FeesTotal60s,
            SseField::from(fees::total_fees(dag_cache, 60)),
        );

        self.set(
            SseKey::FeesTotal60m,
            SseField::from(fees::total_fees(dag_cache, 3600)),
        );

        self.set(
            SseKey::FeesTotal24h,
            SseField::from(fees::total_fees(dag_cache, 86400)),
        );

        // self.set(
        //     SseKey::FeesMeanTimeline,
        //     SseField::from(
        //         serde_json::to_string(&fees::average_fee_by_time_bucket(dag_cache, 300, 86400))
        //             .unwrap(),
        //     ),
        // )
    }

    async fn collect_feerate_data(&mut self, storage: &Storage, cutoff: Option<DateTime<Utc>>) {
        let feerate_low = storage.get_feerate_low().await;
        match cutoff {
            Some(cutoff) => {
                if feerate_low.timestamp > cutoff {
                    self.set(SseKey::FeerateLow, SseField::from(feerate_low));
                }
            }
            None => {
                self.set(SseKey::FeerateLow, SseField::from(feerate_low));
            }
        }

        let feerate_normal = storage.get_feerate_normal().await;
        match cutoff {
            Some(cutoff) => {
                if feerate_normal.timestamp > cutoff {
                    self.set(SseKey::FeerateNormal, SseField::from(feerate_normal));
                }
            }
            None => {
                self.set(SseKey::FeerateNormal, SseField::from(feerate_normal));
            }
        }

        let feerate_priority = storage.get_feerate_priority().await;
        match cutoff {
            Some(cutoff) => {
                if feerate_priority.timestamp > cutoff {
                    self.set(SseKey::FeeratePriority, SseField::from(feerate_priority));
                }
            }
            None => {
                self.set(SseKey::FeeratePriority, SseField::from(feerate_priority));
            }
        }
    }

    async fn collect_mining_data(&mut self, dag_cache: &Arc<DagCache>) {
        self.set(
            SseKey::MinerNodeVersionCount1h,
            SseField::from(
                serde_json::to_string(&mining::mining_node_version_share_60m(dag_cache)).unwrap(),
            ),
        );
    }
}

struct SseState {
    context: Arc<AppContext>,
    deltas_since: Mutex<Option<DateTime<Utc>>>,
    data: Mutex<Option<SseData>>,
}

impl SseState {
    fn new(context: Arc<AppContext>) -> Self {
        Self {
            context,
            deltas_since: Mutex::new(None),
            data: Mutex::new(Some(SseData::new())),
        }
    }

    async fn create_event(&self) -> Result<Event, Infallible> {
        let deltas_since = *self.deltas_since.lock().await;

        let last_data = self.data.lock().await.clone();
        let new_data = SseData::with_data(
            self.context.storage.clone(),
            self.context.dag_cache.clone(),
            deltas_since,
        )
        .await
        .unwrap();

        let event_data = match last_data {
            Some(last) => last.diff(&new_data),
            None => new_data.data(),
        };

        *self.deltas_since.lock().await = Some(Utc::now());
        *self.data.lock().await = Some(new_data);

        let json = serde_json::to_string(&event_data).unwrap();
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

    let stream_state = Arc::new(SseState::new(state.context));

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
