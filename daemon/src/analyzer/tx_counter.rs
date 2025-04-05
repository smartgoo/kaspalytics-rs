use chrono::Utc;
use kaspalytics_utils::config::Config;
use log::debug;
use sqlx::PgPool;

async fn run_transaction_count_24h(db: &DbReader, pg_pool: &PgPool) {
    let data = db.transaction_count_24h().unwrap(); // TODO handle error
    debug!("transaction_count_24h: {}", data);

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('transaction_count_24h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(data)
    .bind(Utc::now())
    .execute(pg_pool)
    .await
    .unwrap();
}

async fn run_effective_transaction_count_24h(db: &DbReader, pg_pool: &PgPool) {
    let data = db.effective_transaction_count_24h().unwrap(); // TODO handle error

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('effective_transaction_count_24h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(data as i64)
    .bind(Utc::now())
    .execute(pg_pool)
    .await
    .unwrap();
}

async fn run_effective_transaction_count_per_hour(db: &DbReader, pg_pool: &PgPool) {
    let data = db.effective_transaction_count_per_hour().unwrap(); // TODO handle error
    debug!("effective_count_per_hour: {:?}", data);

    sqlx::query(
        r#"
        INSERT INTO key_value ("key", "value", updated_timestamp)
        VALUES('effective_transaction_count_per_hour_24h', $1, $2)
        ON CONFLICT ("key") DO UPDATE
            SET "value" = $1, updated_timestamp = $2
        "#,
    )
    .bind(serde_json::to_string(&data).unwrap())
    .bind(Utc::now())
    .execute(pg_pool)
    .await
    .unwrap();
}

pub async fn run(config: &Config, pg_pool: &PgPool) {
    // TODO return errors for all functions

    let db = DbReader::new(config.kaspalytics_dirs.db_dir.clone());

    run_transaction_count_24h(&db, pg_pool).await;
    run_effective_transaction_count_24h(&db, pg_pool).await;
    run_effective_transaction_count_per_hour(&db, pg_pool).await;
}
