use crate::storage::Reader;
use chrono::Utc;
use kaspa_rpc_core::{api::rpc::RpcApi, RpcError};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::database::sql::hash_rate;
use kaspalytics_utils::log::LogTarget;
use log::error;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use sqlx::PgPool;
use std::sync::{atomic::Ordering, Arc};
use tokio::time::Duration;

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
        // Markets data update task
        {
            let context = self.context.clone();
            let shutdown_flag = context.shutdown_flag.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(30));
                while !shutdown_flag.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if let Err(e) = update_markets_data(context.storage.clone()).await {
                        error!(target: LogTarget::Daemon.as_str(), "Error during update_markets_data: {}", e);
                    }
                }
            });
        }

        // Sink Blue Score update task
        {
            let context = self.context.clone();
            let shutdown_flag = context.shutdown_flag.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                while !shutdown_flag.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if let Err(e) =
                        update_sink_blue_score(&context.rpc_client, context.storage.clone()).await
                    {
                        error!(target: LogTarget::Daemon.as_str(), "Error during update_sink_blue_score: {}", e);
                    }
                }
            });
        }

        // Block DAG info update task
        {
            let context = self.context.clone();
            let shutdown_flag = context.shutdown_flag.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                while !shutdown_flag.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if let Err(e) =
                        update_block_dag_info(&context.rpc_client, context.storage.clone()).await
                    {
                        error!(target: LogTarget::Daemon.as_str(), "Error during update_block_dag_info: {}", e);
                    }
                }
            });
        }

        // Coin supply info update task
        {
            let context = self.context.clone();
            let shutdown_flag = context.shutdown_flag.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                while !shutdown_flag.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if let Err(e) =
                        update_coin_supply_info(&context.rpc_client, context.storage.clone()).await
                    {
                        error!(target: LogTarget::Daemon.as_str(), "Error during update_coin_supply_info: {}", e);
                    }
                }
            });
        }

        // Hash rate snapshot task
        {
            let context = self.context.clone();
            let shutdown_flag = context.shutdown_flag.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(15));
                while !shutdown_flag.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if let Err(e) =
                        snapshot_hash_rate(&context.rpc_client, context.storage.clone()).await
                    {
                        error!(target: LogTarget::Daemon.as_str(), "Error during snapshot_hash_rate: {}", e);
                    }
                }
            });
        }

        // Hash rate changes update task
        {
            let context = self.context.clone();
            let shutdown_flag = context.shutdown_flag.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(60));
                while !shutdown_flag.load(Ordering::Relaxed) {
                    interval.tick().await;
                    if let Err(e) =
                        update_hash_rate_changes(&context.pg_pool, context.storage.clone()).await
                    {
                        error!(target: LogTarget::Daemon.as_str(), "Error during update_hash_rate_changes: {}", e);
                    }
                }
            });
        }

        while !self.context.shutdown_flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
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

async fn update_sink_blue_score(
    rpc_client: &Arc<KaspaRpcClient>,
    storage: Arc<Storage>,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_sink_blue_score().await?;

    storage
        .set_sink_blue_score(data, Some(date))
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
    storage: Arc<Storage>,
) -> Result<(), Error> {
    let data = rpc_client.get_block_dag_info().await?;

    let hash_rate = (data.difficulty * 2f64) as u64;
    let hash_rate_10bps = hash_rate * 10u64;

    storage
        .set_hash_rate(
            Decimal::from_f64(data.difficulty).unwrap(),
            hash_rate_10bps,
            None,
        )
        .await?;

    Ok(())
}

async fn update_hash_rate_changes(pg_pool: &PgPool, storage: Arc<Storage>) -> Result<(), Error> {
    let hash_rate = storage.get_hash_rate().await;
    let current_hash_rate = Decimal::from_u64(hash_rate.value).unwrap();

    for days in [7, 30, 90] {
        let past = hash_rate::get_x_days_ago(pg_pool, days).await?;
        if let Some(change) =
            kaspalytics_utils::math::percent_change(current_hash_rate, past.hash_rate, 2)
        {
            match days {
                7 => {
                    storage
                        .set_hash_rate_7d_change(change, Some(past.timestamp))
                        .await?
                }
                30 => {
                    storage
                        .set_hash_rate_30d_change(change, Some(past.timestamp))
                        .await?
                }
                90 => {
                    storage
                        .set_hash_rate_90d_change(change, Some(past.timestamp))
                        .await?
                }
                _ => continue,
            };
        }
    }

    Ok(())
}
