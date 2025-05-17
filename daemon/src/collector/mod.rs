use chrono::Utc;
use futures::future::BoxFuture;
use kaspa_rpc_core::{api::rpc::RpcApi, RpcError};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::database::sql::{hash_rate, key_value, key_value::KeyRegistry};
use log::error;
use sqlx::PgPool;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::time::{interval, Duration};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    Database(#[from] sqlx::Error),

    #[error("{0}")]
    Rpc(#[from] RpcError),

    #[error("{0}")]
    Http(#[from] reqwest::Error),
}

pub struct Collector {
    shutdown_flag: Arc<AtomicBool>,
    pg_pool: PgPool,
    rpc_client: Arc<KaspaRpcClient>,
}

impl Collector {
    pub fn new(
        shutdown_flag: Arc<AtomicBool>,
        pg_pool: PgPool,
        rpc_client: Arc<KaspaRpcClient>,
    ) -> Self {
        Self {
            shutdown_flag,
            pg_pool,
            rpc_client,
        }
    }

    pub async fn run(&self) {
        self.spawn_task(Duration::from_secs(30), "market data update", {
            let pg_pool = self.pg_pool.clone();
            move || {
                let pg_pool = pg_pool.clone();
                Box::pin(async move {
                    update_markets_data(&pg_pool).await
                })
            }
        });

        self.spawn_task(Duration::from_secs(1), "block dag info update", {
            let pg_pool = self.pg_pool.clone();
            let rpc_client = self.rpc_client.clone();
            move || {
                let pg_pool = pg_pool.clone();
                let rpc_client = rpc_client.clone();
                Box::pin(async move {
                    update_block_dag_info(&rpc_client, &pg_pool).await
                })
            }
        });

        self.spawn_task(Duration::from_secs(1), "coin supply info update", {
            let pg_pool = self.pg_pool.clone();
            let rpc_client = self.rpc_client.clone();
            move || {
                let pg_pool = pg_pool.clone();
                let rpc_client = rpc_client.clone();
                Box::pin(async move {
                    update_coin_supply_info(&rpc_client, &pg_pool).await
                })
            }
        });

        self.spawn_task(Duration::from_secs(1), "snapshot hash rate", {
            let pg_pool = self.pg_pool.clone();
            let rpc_client = self.rpc_client.clone();
            move || {
                let pg_pool = pg_pool.clone();
                let rpc_client = rpc_client.clone();
                Box::pin(async move {
                    snapshot_hash_rate(&rpc_client, &pg_pool).await
                })
            }
        });

        while !self.shutdown_flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn spawn_task<F>(&self, interval_duration: Duration, task_name: &'static str, mut task_fn: F)
    where
        F: FnMut() -> BoxFuture<'static, Result<(), Error>> + Send + 'static,
    {
        let shutdown_flag = self.shutdown_flag.clone();

        tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            while !shutdown_flag.load(Ordering::Relaxed) {
                ticker.tick().await;
                if let Err(e) = task_fn().await {
                    error!("Error during {}: {}", task_name, e);
                }
            }
        });
    }
}

async fn update_markets_data(pg_pool: &PgPool) -> Result<(), Error> {
    let date = Utc::now();

    let data = kaspalytics_utils::coingecko::get_coin_data().await?;

    key_value::upsert(
        pg_pool,
        KeyRegistry::PriceUsd,
        data.market_data.current_price.usd,
        date,
    )
    .await?;

    key_value::upsert(
        pg_pool,
        KeyRegistry::PriceBtc,
        data.market_data.current_price.btc,
        date,
    )
    .await?;

    key_value::upsert(
        pg_pool,
        KeyRegistry::MarketCap,
        data.market_data.market_cap.usd,
        date,
    )
    .await?;

    key_value::upsert(
        pg_pool,
        KeyRegistry::Volume,
        data.market_data.total_volume.usd,
        date,
    )
    .await?;

    Ok(())
}

async fn update_block_dag_info(
    rpc_client: &Arc<KaspaRpcClient>,
    pg_pool: &PgPool,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_block_dag_info().await?;

    key_value::upsert(pg_pool, KeyRegistry::DaaScore, data.virtual_daa_score, date).await?;

    key_value::upsert(
        pg_pool,
        KeyRegistry::PruningPoint,
        data.pruning_point_hash,
        date,
    )
    .await?;

    Ok(())
}

async fn update_coin_supply_info(
    rpc_client: &Arc<KaspaRpcClient>,
    pg_pool: &PgPool,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_coin_supply().await?;
    key_value::upsert(pg_pool, KeyRegistry::CsSompi, data.circulating_sompi, date).await?;

    Ok(())
}

async fn snapshot_hash_rate(
    rpc_client: &Arc<KaspaRpcClient>,
    pg_pool: &PgPool,
) -> Result<(), Error> {
    let data = rpc_client.get_block_dag_info().await?;

    let timestamp =
        chrono::DateTime::from_timestamp((data.past_median_time / 1000) as i64, 0).unwrap();

    let hash_rate = (data.difficulty * 2f64) as u64;
    let hash_rate_10bps = hash_rate * 10u64;

    hash_rate::insert(pg_pool, timestamp, hash_rate_10bps, data.difficulty as u64).await?;

    Ok(())
}
