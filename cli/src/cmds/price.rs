use chrono::DateTime;
use kaspalytics_utils::config::Config;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use sqlx::PgPool;

pub async fn get_coin_market_history(config: Config, pg_pool: PgPool) {
    let data = kaspalytics_utils::coingecko::get_market_chart()
        .await
        .unwrap();

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

        let seconds = (timestamp_ms / dec!(1000)).to_i64().unwrap();
        let date = DateTime::from_timestamp(seconds, 0).expect("Invalid timestamp");

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
        .execute(&pg_pool)
        .await
        .unwrap();
    }

    let _ = kaspalytics_utils::email::send_email(
        &config,
        "coin-market-history completed".to_string(),
        "".to_string(),
    );
}
