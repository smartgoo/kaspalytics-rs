use crate::database;
use kaspa_consensus_core::network::{NetworkId, NetworkType};
use sqlx::postgres::PgPool;
use std::str::FromStr;
use strum::IntoEnumIterator;

pub async fn apply_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Run sqlx migrations
    sqlx::migrate!().run(pool).await?;

    Ok(())
}

pub async fn insert_enums(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Insert all Granularity keys to granularity table
    for variant in database::Granularity::iter() {
        let name = format!("{:?}", variant);
        sqlx::query("INSERT INTO granularity (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
            .bind(name)
            .execute(pool)
            .await?;
    }

    // Insert all DataPoint keys to data_point table
    for variant in database::DataPoint::iter() {
        let name = format!("{:?}", variant);
        sqlx::query("INSERT INTO data_point (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
            .bind(name)
            .execute(pool)
            .await?;
    }

    // Insert all Meta keys to meta table
    for variant in database::Meta::iter() {
        let name = format!("{:?}", variant);
        sqlx::query("INSERT INTO meta (key) VALUES ($1) ON CONFLICT (key) DO NOTHING")
            .bind(name)
            .execute(pool)
            .await?;
    }

    Ok(())
}

pub async fn get_meta_network(pool: &PgPool) -> Result<Option<String>, sqlx::Error> {
    let network: (Option<String>,) = sqlx::query_as("SELECT value FROM meta WHERE key = $1")
        .bind(database::Meta::Network.to_string())
        .fetch_one(pool)
        .await?;

    Ok(network.0)
}

pub async fn get_meta_network_suffix(pool: &PgPool) -> Result<Option<String>, sqlx::Error> {
    let suffix: (Option<String>,) = sqlx::query_as("SELECT value FROM meta WHERE key = $1")
        .bind(database::Meta::NetworkSuffix.to_string())
        .fetch_one(pool)
        .await?;

    Ok(suffix.0)
}

pub async fn get_meta_network_id(pool: &PgPool) -> Result<Option<NetworkId>, sqlx::Error> {
    let network = match get_meta_network(pool).await? {
        Some(value) => value,
        None => return Ok(None),
    };
    let network_type = NetworkType::from_str(&network).unwrap();

    let netsuffix = get_meta_network_suffix(pool)
        .await?
        .and_then(|value| value.parse::<u32>().ok());

    let network_id = NetworkId::try_new(network_type)
        .unwrap_or_else(|_| NetworkId::with_suffix(network_type, netsuffix.unwrap()));

    Ok(Some(network_id))
}

pub async fn insert_network_meta(pool: &PgPool, network_id: NetworkId) -> Result<(), sqlx::Error> {
    // Set network in PG meta table
    sqlx::query("UPDATE meta SET value = $1 WHERE key = $2")
        .bind(format!("{:?}", network_id.network_type))
        .bind(database::Meta::Network.to_string())
        .execute(pool)
        .await?;

    let suffix: Option<String> = network_id.suffix.map(|suffix| suffix.to_string());

    if let Some(suffix_str) = suffix {
        sqlx::query("UPDATE meta SET value = $1 WHERE key = $2")
        .bind(suffix_str)
        .bind(database::Meta::NetworkSuffix.to_string())
        .execute(pool)
        .await?;
    }

    Ok(())
}
