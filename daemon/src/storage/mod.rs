pub mod cache;
use cache::{Cache, CacheEntry};
use chrono::{DateTime, Utc};
use kaspa_hashes::Hash;
use kaspalytics_utils::database::sql::{key_value, key_value::KeyRegistry};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    DbError(#[from] sqlx::Error),
}

pub struct Storage {
    cache: Arc<Cache>,
    pg_pool: PgPool,
}

impl Storage {
    pub fn new(cache: Arc<Cache>, pg_pool: PgPool) -> Self {
        Storage { cache, pg_pool }
    }
}

pub trait Writer {
    async fn set_price_usd(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    async fn set_price_btc(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    async fn set_market_cap(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    async fn set_volume(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    async fn set_pruning_point(
        &self,
        value: Hash,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    async fn set_daa_score(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;

    async fn set_circulating_supply(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error>;
}

impl Writer for Storage {
    async fn set_price_usd(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_price_usd(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::PriceUsd,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }

    async fn set_price_btc(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_price_btc(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::PriceBtc,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }

    async fn set_market_cap(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_market_cap(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::MarketCap,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }

    async fn set_volume(
        &self,
        value: Decimal,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_volume(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::Volume,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }

    async fn set_pruning_point(
        &self,
        value: Hash,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_pruning_point(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::PruningPoint,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }

    async fn set_daa_score(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_daa_score(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::DaaScore,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }

    async fn set_circulating_supply(
        &self,
        value: u64,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.cache.set_circulating_supply(value, timestamp).await?;

        key_value::upsert(
            &self.pg_pool,
            KeyRegistry::CsSompi,
            value,
            timestamp.unwrap_or(Utc::now()),
        )
        .await?;

        Ok(())
    }
}

pub trait Reader {
    async fn get_price_usd(&self) -> CacheEntry<Decimal>;
    async fn get_price_btc(&self) -> CacheEntry<Decimal>;
    async fn get_market_cap(&self) -> CacheEntry<Decimal>;
    async fn get_volume(&self) -> CacheEntry<Decimal>;

    async fn get_pruning_point(&self) -> CacheEntry<Hash>;
    async fn get_daa_score(&self) -> CacheEntry<u64>;
    async fn get_circulating_supply(&self) -> CacheEntry<u64>;
}

impl Reader for Storage {
    async fn get_price_usd(&self) -> CacheEntry<Decimal> {
        self.cache.get_price_usd().await
    }

    async fn get_price_btc(&self) -> CacheEntry<Decimal> {
        self.cache.get_price_btc().await
    }

    async fn get_market_cap(&self) -> CacheEntry<Decimal> {
        self.cache.get_market_cap().await
    }

    async fn get_volume(&self) -> CacheEntry<Decimal> {
        self.cache.get_volume().await
    }

    async fn get_pruning_point(&self) -> CacheEntry<Hash> {
        self.cache.get_pruning_point().await
    }

    async fn get_daa_score(&self) -> CacheEntry<u64> {
        self.cache.get_daa_score().await
    }

    async fn get_circulating_supply(&self) -> CacheEntry<u64> {
        self.cache.get_circulating_supply().await
    }
}
