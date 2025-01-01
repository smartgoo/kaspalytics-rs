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
