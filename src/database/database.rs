use log::info;
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    Connection, Executor, PgConnection,
};

pub struct Database {
    pub url: String,
    pub database_name: String,
}

impl Database {
    pub fn new(url: String) -> Self {
        let database_name = &url
            .split('/')
            .last()
            .expect("Invalid connection string");

        Self { url: url.to_string(), database_name: database_name.to_string() }
    }
}

impl Database {
    pub async fn open_connection_pool(&self, max: u32) -> Result<PgPool, sqlx::Error> {
        Ok(PgPoolOptions::new().max_connections(max).connect(&self.url).await?)
    }

    pub async fn open_connection(&self) -> Result<PgConnection, sqlx::Error> {
        Ok(PgConnection::connect(&self.url).await?)
    }
}

impl Database {
    fn base_url(&self) -> String {
        let mut conn_parts: Vec<&str> = self.url.split('/').collect();
        conn_parts.pop();
        conn_parts.join("/")
    }

    pub async fn drop_and_create_database(&self) -> Result<(), sqlx::Error> {
        // Connect without specifying database name in order to drop and recreate
        let base_url = self.base_url();
        let mut conn = PgConnection::connect(&base_url).await?;
    
        info!("Dropping PG database {}...", &self.database_name);
        conn.execute(format!("DROP DATABASE {}", &self.database_name).as_str()).await?;
    
        info!("Creating PG database {}...", &self.database_name);
        conn.execute(format!("CREATE DATABASE {}", &self.database_name).as_str()).await?;
    
        conn.close().await?;
    
        Ok(())
    }
}