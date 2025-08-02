use super::{Error, Reader, Writer};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;
use kaspa_rpc_core::{RpcFeeEstimate, RpcMempoolEntry};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Default)]
pub struct CacheEntry<T> {
    pub value: T,
    pub timestamp: DateTime<Utc>,
}

#[derive(Default)]
pub struct Cache {
    // Markets data
    price_usd: RwLock<CacheEntry<Decimal>>,
    price_btc: RwLock<CacheEntry<Decimal>>,
    market_cap: RwLock<CacheEntry<Decimal>>,
    volume: RwLock<CacheEntry<Decimal>>,

    // Network Data
    pruning_point: RwLock<CacheEntry<Hash>>,
    sink_blue_score: RwLock<CacheEntry<u64>>,
    daa_score: RwLock<CacheEntry<u64>>,
    circulating_supply: RwLock<CacheEntry<u64>>,
    difficulty: RwLock<CacheEntry<Decimal>>,
    hash_rate: RwLock<CacheEntry<u64>>,
    fee_rate_priority: RwLock<CacheEntry<Decimal>>,
    fee_rate_normal: RwLock<CacheEntry<Decimal>>,
    fee_rate_low: RwLock<CacheEntry<Decimal>>,
    mempool_entries: RwLock<CacheEntry<Vec<RpcMempoolEntry>>>,

    // Hash rate change
    hash_rate_7d_change: RwLock<CacheEntry<Decimal>>,
    hash_rate_30d_change: RwLock<CacheEntry<Decimal>>,
    hash_rate_90d_change: RwLock<CacheEntry<Decimal>>,
}

impl Writer for Cache {
    async fn set_price_usd(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.price_usd.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_price_btc(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.price_btc.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_market_cap(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.market_cap.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_volume(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.volume.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_pruning_point(
        &self,
        value: Hash,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.pruning_point.write().await = CacheEntry::<Hash> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_sink_blue_score(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.sink_blue_score.write().await = CacheEntry::<u64> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_daa_score(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.daa_score.write().await = CacheEntry::<u64> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_circulating_supply(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.circulating_supply.write().await = CacheEntry::<u64> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_hash_rate(
        &self,
        difficulty: Decimal,
        hash_rate: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.difficulty.write().await = CacheEntry::<Decimal> {
            value: difficulty,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        *self.hash_rate.write().await = CacheEntry::<u64> {
            value: hash_rate,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_fee_rates(
        &self,
        value: RpcFeeEstimate,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.fee_rate_priority.write().await = CacheEntry::<Decimal> {
            value: Decimal::from_f64(value.priority_bucket.feerate).unwrap(),
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        *self.fee_rate_normal.write().await = CacheEntry::<Decimal> {
            value: Decimal::from_f64(value.normal_buckets[0].feerate).unwrap(),
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        *self.fee_rate_low.write().await = CacheEntry::<Decimal> {
            value: Decimal::from_f64(value.low_buckets[0].feerate).unwrap(),
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_mempool_entries(
        &self,
        value: Vec<kaspa_rpc_core::RpcMempoolEntry>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.mempool_entries.write().await = CacheEntry::<Vec<RpcMempoolEntry>> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_hash_rate_7d_change(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.hash_rate_7d_change.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_hash_rate_30d_change(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.hash_rate_30d_change.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }

    async fn set_hash_rate_90d_change(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        *self.hash_rate_90d_change.write().await = CacheEntry::<Decimal> {
            value,
            timestamp: timestamp.unwrap_or(Utc::now()),
        };

        Ok(())
    }
}

impl Reader for Cache {
    async fn get_price_usd(&self) -> CacheEntry<Decimal> {
        self.price_usd.read().await.clone()
    }

    async fn get_price_btc(&self) -> CacheEntry<Decimal> {
        self.price_btc.read().await.clone()
    }

    async fn get_market_cap(&self) -> CacheEntry<Decimal> {
        self.market_cap.read().await.clone()
    }

    async fn get_volume(&self) -> CacheEntry<Decimal> {
        self.volume.read().await.clone()
    }

    async fn get_pruning_point(&self) -> CacheEntry<Hash> {
        self.pruning_point.read().await.clone()
    }

    async fn get_daa_score(&self) -> CacheEntry<u64> {
        self.daa_score.read().await.clone()
    }

    async fn get_circulating_supply(&self) -> CacheEntry<u64> {
        self.circulating_supply.read().await.clone()
    }

    async fn get_difficulty(&self) -> CacheEntry<Decimal> {
        self.difficulty.read().await.clone()
    }

    async fn get_hash_rate(&self) -> CacheEntry<u64> {
        self.hash_rate.read().await.clone()
    }

    async fn get_hash_rate_7d_change(&self) -> CacheEntry<Decimal> {
        self.hash_rate_7d_change.read().await.clone()
    }

    async fn get_hash_rate_30d_change(&self) -> CacheEntry<Decimal> {
        self.hash_rate_30d_change.read().await.clone()
    }

    async fn get_hash_rate_90d_change(&self) -> CacheEntry<Decimal> {
        self.hash_rate_90d_change.read().await.clone()
    }
}
