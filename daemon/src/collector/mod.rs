use chrono::Utc;
use futures::future::BoxFuture;
use kaspa_rpc_core::{api::rpc::RpcApi, RpcError};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::{database::sql::hash_rate, log::LogTarget};
use log::error;
use sqlx::PgPool;
use std::sync::{atomic::Ordering, Arc};
use tokio::time::{interval, Duration};

use crate::{
    storage::{self, Storage, Writer},
    AppContext,
};

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("{0}")]
    Database(#[from] sqlx::Error),

    #[error("{0}")]
    Rpc(#[from] RpcError),

    #[error("{0}")]
    Http(#[from] reqwest::Error),

    #[error("{0}")]
    Storage(#[from] storage::Error),
}

pub struct Collector {
    context: Arc<AppContext>,
}

impl Collector {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context }
    }

    pub async fn run(&self) {
        self.spawn_task(Duration::from_secs(30), "market data update", {
            let context = self.context.clone();
            move || {
                let storage = context.storage.clone();
                Box::pin(async move { update_markets_data(storage).await })
            }
        });

        self.spawn_task(Duration::from_secs(1), "block dag info update", {
            let context = self.context.clone();
            move || {
                let rpc_client = context.rpc_client.clone();
                let storage = context.storage.clone();
                Box::pin(async move { update_block_dag_info(&rpc_client, storage).await })
            }
        });

        self.spawn_task(Duration::from_secs(1), "coin supply info update", {
            let context = self.context.clone();
            move || {
                let rpc_client = context.rpc_client.clone();
                let storage = context.storage.clone();
                Box::pin(async move { update_coin_supply_info(&rpc_client, storage).await })
            }
        });

        self.spawn_task(Duration::from_secs(15), "snapshot hash rate", {
            let context = self.context.clone();
            move || {
                let pg_pool = context.pg_pool.clone();
                let rpc_client = context.rpc_client.clone();
                Box::pin(async move { snapshot_hash_rate(&rpc_client, &pg_pool).await })
            }
        });

        while !self.context.shutdown_flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn spawn_task<F>(&self, interval_duration: Duration, task_name: &'static str, mut task_fn: F)
    where
        F: FnMut() -> BoxFuture<'static, Result<(), Error>> + Send + 'static,
    {
        let shutdown_flag = self.context.shutdown_flag.clone();

        tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            while !shutdown_flag.load(Ordering::Relaxed) {
                ticker.tick().await;
                if let Err(e) = task_fn().await {
                    error!(target: LogTarget::Daemon.as_str(), "Error during {}: {}", task_name, e);
                }
            }
        });
    }
}

async fn update_markets_data(storage: Arc<Storage>) -> Result<(), Error> {
    let date = Utc::now();

    let data = kaspalytics_utils::coingecko::get_coin_data().await?;

    storage
        .set_price_usd(data.market_data.current_price.usd, Some(date))
        .await?;
    storage
        .set_price_btc(data.market_data.current_price.btc, Some(date))
        .await?;
    storage
        .set_market_cap(data.market_data.market_cap.usd, Some(date))
        .await?;
    storage
        .set_volume(data.market_data.total_volume.usd, Some(date))
        .await?;

    Ok(())
}

async fn update_block_dag_info(
    rpc_client: &Arc<KaspaRpcClient>,
    storage: Arc<Storage>,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_block_dag_info().await?;

    storage
        .set_daa_score(data.virtual_daa_score, Some(date))
        .await?;
    storage
        .set_pruning_point(data.pruning_point_hash, Some(date))
        .await?;

    Ok(())
}

async fn update_coin_supply_info(
    rpc_client: &Arc<KaspaRpcClient>,
    storage: Arc<Storage>,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_coin_supply().await?;

    storage
        .set_circulating_supply(data.circulating_sompi, Some(date))
        .await?;

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
