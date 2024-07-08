use crate::database;
use kaspa_consensus_core::network::NetworkId;
use sqlx::postgres::PgPool;
use strum::IntoEnumIterator;

pub async fn apply_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Run sqlx migrations
    sqlx::migrate!()
        .run(pool)
        .await?;

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
    let db_network: (Option<String>,) = sqlx::query_as("SELECT value FROM meta WHERE key = $1")
        .bind(database::Meta::Network.to_string())
        .fetch_one(pool)
        .await?;

    Ok(db_network.0)
}

pub async fn get_meta_network_suffix(pool: &PgPool) -> Result<Option<String>, sqlx::Error> {
    let db_network: (Option<String>,) = sqlx::query_as("SELECT value FROM meta WHERE key = $1")
        .bind(database::Meta::NetworkSuffix.to_string())
        .fetch_one(pool)
        .await?;

    Ok(db_network.0)
}

pub async fn store_network_meta(pool: &PgPool, network_id: NetworkId) -> Result<(), sqlx::Error> {
    // Set network in PG meta table
    sqlx::query("UPDATE meta SET value = $1 WHERE key = $2")
        .bind(format!("{:?}", network_id.network_type))
        .bind(database::Meta::Network.to_string())
        .execute(pool)
        .await?;

    // Set network in PG meta table
    sqlx::query("UPDATE meta SET value = $1 WHERE key = $2")
        .bind(format!("{:?}", network_id.suffix))
        .bind(database::Meta::NetworkSuffix.to_string())
        .execute(pool)
        .await?;

    Ok(())
}