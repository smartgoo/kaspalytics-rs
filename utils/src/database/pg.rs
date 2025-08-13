use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    Connection, PgConnection,
};

pub struct Database {
    pub url: String,
    pub database_name: String,
}

impl Database {
    pub fn new(url: String) -> Self {
        let database_name = &url.split('/').next_back().expect("Invalid connection string");

        Self {
            url: url.to_string(),
            database_name: database_name.to_string(),
        }
    }
}

impl Database {
    pub async fn open_connection_pool(&self, max: u32) -> Result<PgPool, sqlx::Error> {
        PgPoolOptions::new()
            .max_connections(max)
            .connect(&self.url)
            .await
    }

    #[allow(dead_code)]
    pub async fn open_connection(&self) -> Result<PgConnection, sqlx::Error> {
        PgConnection::connect(&self.url).await
    }
}
