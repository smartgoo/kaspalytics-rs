use chrono::Utc;
use kaspa_rpc_core::{api::rpc::RpcApi, RpcError};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspalytics_utils::database;
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
        Collector {
            shutdown_flag,
            pg_pool,
            rpc_client,
        }
    }

    pub async fn run(&self) {
        // TODO error
        let mut interval = interval(Duration::from_secs(60));

        while !self.shutdown_flag.load(Ordering::Relaxed) {
            interval.tick().await;

            if let Err(e) = update_markets_data(&self.pg_pool).await {
                error!("Error during market data update: {}", e);
            }

            if let Err(e) = update_block_dag_info(&self.rpc_client, &self.pg_pool).await {
                error!("Error during block dag info update: {}", e);
            }

            if let Err(e) = update_coin_supply_info(&self.rpc_client, &self.pg_pool).await {
                error!("Error during coin supply info update: {}", e);
            }

            if let Err(e) = snapshot_hash_rate(&self.rpc_client, &self.pg_pool).await {
                error!("Error during snapshot hash rate: {}", e);
            }
        }
    }
}

async fn update_markets_data(pg_pool: &PgPool) -> Result<(), Error> {
    let date = Utc::now();

    let data = kaspalytics_utils::coingecko::get_coin_data().await?;

    database::sql::key_value::upsert_price_usd(pg_pool, data.market_data.current_price.usd, date)
        .await?;

    database::sql::key_value::upsert_price_btc(pg_pool, data.market_data.current_price.btc, date)
        .await?;

    database::sql::key_value::upsert_market_cap(pg_pool, data.market_data.market_cap.usd, date)
        .await?;

    database::sql::key_value::upsert_volume(pg_pool, data.market_data.total_volume.usd, date)
        .await?;

    Ok(())
}

async fn update_block_dag_info(
    rpc_client: &Arc<KaspaRpcClient>,
    pg_pool: &PgPool,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_block_dag_info().await?;
    database::sql::key_value::upsert_daa_score(pg_pool, data.virtual_daa_score, date).await?;
    database::sql::key_value::upsert_pruning_point(pg_pool, data.pruning_point_hash, date).await?;

    Ok(())
}

async fn update_coin_supply_info(
    rpc_client: &Arc<KaspaRpcClient>,
    pg_pool: &PgPool,
) -> Result<(), Error> {
    let date = Utc::now();
    let data = rpc_client.get_coin_supply().await?;
    database::sql::key_value::upsert_cs_sompi(pg_pool, data.circulating_sompi, date).await?;

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

    database::sql::hash_rate::insert(pg_pool, timestamp, hash_rate_10bps, data.difficulty as u64)
        .await?;

    Ok(())
}
