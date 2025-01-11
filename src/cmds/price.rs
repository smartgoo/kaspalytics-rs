use reqwest;
use reqwest::Error;
use serde::Deserialize;

#[derive(Deserialize)]
struct Price {
    usd: f64,
}

#[derive(Deserialize)]
struct Response {
    kaspa: Price,
}

pub async fn get_kas_usd_price() -> Result<f64, Error> {
    let response =
        reqwest::get("https://api.coingecko.com/api/v3/simple/price?ids=kaspa&vs_currencies=usd")
            .await?
            .error_for_status()?;

    let data: Response = response.json().await?;
    Ok(data.kaspa.usd)
}

// TODO commit to just one CoinGecko price service - /v3/simple/price or /v3/coins/kaspa/market_chart
// /v3/simple/price would be:
// pub async fn insert_price() TODO need db table though, apparently only have a coinmarket_history_table atm
// pub async fn snapshot_price()

// /v3/coins/kaspa/market_chart would be:
// pub async fn insert_coin_market_history()
// pub async fn snapshot_coin_market_history()
