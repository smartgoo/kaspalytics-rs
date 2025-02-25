use chrono::DateTime;
use kaspalytics_utils::config::Config;
use reqwest;
use reqwest::Error;
use serde::Deserialize;
use sqlx::PgPool;

#[derive(Deserialize)]
struct Price {
    usd: f64,
}

#[derive(Deserialize)]
struct PriceResponse {
    kaspa: Price,
}

pub async fn get_kas_usd_price() -> Result<f64, Error> {
    let response =
        reqwest::get("https://api.coingecko.com/api/v3/simple/price?ids=kaspa&vs_currencies=usd")
            .await?
            .error_for_status()?;

    let data: PriceResponse = response.json().await?;
    Ok(data.kaspa.usd)
}

// TODO commit to just one CoinGecko price service - /v3/simple/price or /v3/coins/kaspa/market_chart

#[derive(Deserialize)]
struct MarketChartResponse {
    prices: Vec<[f64; 2]>,
    market_caps: Vec<[f64; 2]>,
    total_volumes: Vec<[f64; 2]>,
}

pub async fn get_coin_market_history(config: Config, pg_pool: &PgPool) {
    // Public API allows up to 365 day
    let url = "https://api.coingecko.com/api/v3/coins/kaspa/market_chart?vs_currency=USD&days=365";
    let response = reqwest::get(url).await.unwrap();
    let data: MarketChartResponse = response.json().await.unwrap();

    for ((price_info, market_cap_info), volume_info) in data
        .prices
        .iter()
        .zip(data.market_caps.iter())
        .zip(data.total_volumes.iter())
    {
        let timestamp_ms = price_info[0];
        let price = price_info[1];
        let market_cap = market_cap_info[1];
        let volume = volume_info[1];

        // Convert the timestamp (milliseconds since epoch) to a DateTime<Utc>
        let seconds = (timestamp_ms / 1000.0) as i64;
        let date = DateTime::from_timestamp(seconds, 0).expect("Invalid timestamp");
        // let date = TimeZone::from_utc_datetime(&ts, Utc);

        // Insert a new record or update if a duplicate timestamp exists.
        // This uses PostgreSQL's ON CONFLICT to update the record if a duplicate timestamp (the constraint field) is found.
        sqlx::query(
            r#"
            INSERT INTO coin_market_history (timestamp, symbol, price, market_cap, volume)
            VALUES ($1, 'KAS', $2, $3, $4)
            ON CONFLICT ("timestamp") DO UPDATE
              SET price = EXCLUDED.price,
                  market_cap = EXCLUDED.market_cap,
                  volume = EXCLUDED.volume
            "#,
        )
        .bind(date.naive_utc())
        .bind(price)
        .bind(market_cap)
        .bind(volume)
        .execute(pg_pool)
        .await
        .unwrap();
    }

    kaspalytics_utils::email::send_email(
        &config,
        format!("{} | coin-market-history completed", config.env),
        "".to_string(),
    );
}
