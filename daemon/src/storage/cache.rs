use super::{Error, Reader, Writer};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;
use rust_decimal::Decimal;
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
    daa_score: RwLock<CacheEntry<u64>>,
    circulating_supply: RwLock<CacheEntry<u64>>,
    difficulty: RwLock<CacheEntry<Decimal>>,
    hash_rate: RwLock<CacheEntry<u64>>,
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
}
