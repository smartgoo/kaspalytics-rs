use reqwest;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Price {
    pub usd: f64,
}

#[derive(Deserialize)]
pub struct PriceResponse {
    pub kaspa: Price,
}

pub async fn get_simple_price() -> Result<PriceResponse, reqwest::Error> {
    let url = "https://api.coingecko.com/api/v3/simple/price?ids=kaspa&vs_currencies=usd";
    let response = reqwest::get(url).await?.error_for_status()?;
    let data: PriceResponse = response.json().await?;

    Ok(data)
}

#[derive(Deserialize)]
pub struct MarketChartResponse {
    pub prices: Vec<[f64; 2]>,
    pub market_caps: Vec<[f64; 2]>,
    pub total_volumes: Vec<[f64; 2]>,
}

pub async fn get_market_chart() -> Result<MarketChartResponse, reqwest::Error> {
    // Public API allows up to 365 day
    let url = "https://api.coingecko.com/api/v3/coins/kaspa/market_chart?vs_currency=USD&days=365";
    let response = reqwest::get(url).await?.error_for_status()?;
    let data: MarketChartResponse = response.json().await?;

    Ok(data)
}