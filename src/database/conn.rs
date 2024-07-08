use sqlx::{postgres::{PgPool, PgPoolOptions}, Connection, Executor, PgConnection};

pub fn parse_base_url(url: &str) -> String {
    let mut conn_parts: Vec<&str> = url.split('/').collect();
    conn_parts.pop();
    let base_url = conn_parts.join("/");
    base_url
}

pub async fn open_connection_pool(url: &str) -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
    
    Ok(pool)
}

pub async fn open_connection(url: &str) -> Result<PgConnection, sqlx::Error> {
    let conn = PgConnection::connect(url).await?;
    Ok(conn)
}

pub async fn close_connection(conn: PgConnection) -> Result<(), sqlx::Error> {
    conn.close().await?;
    Ok(())
}

pub async fn drop_db(conn: &mut PgConnection, database: &str) -> Result<(), sqlx::Error> {
    conn.execute(format!("DROP DATABASE {}", database).as_str()).await?;
    Ok(())
}

pub async fn create_db(conn: &mut PgConnection, database: &str) -> Result<(), sqlx::Error> {
    conn.execute(format!("CREATE DATABASE {}", database).as_str()).await?;
    Ok(())
}