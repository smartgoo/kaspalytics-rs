use axum::response::sse::{Event, Sse};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex};
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};

#[derive(Clone, Serialize)]
struct SseField {
    data: String,
    timestamp: DateTime<Utc>,
}

async fn fetch_sse_data(
    pg_pool: &PgPool,
    poll_time: DateTime<Utc>,
) -> Result<HashMap<String, SseField>, sqlx::Error> {
    let rows = sqlx::query(
        r#"
        SELECT key, value, updated_timestamp 
        FROM key_value 
        WHERE updated_timestamp > $1
        "#,
    )
    .bind(poll_time)
    .fetch_all(pg_pool)
    .await?;

    let mut map: HashMap<String, SseField> = HashMap::new();
    for row in rows {
        let key: String = row.get("key");
        let value: String = row.get("value");
        let timestamp: DateTime<Utc> = row.get("updated_timestamp");
        map.insert(
            key,
            SseField {
                data: value,
                timestamp,
            },
        );
    }

    Ok(map)
}

pub async fn home_page_sse(pg_pool: PgPool) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let _ = tx.send(());
        }
    });

    let poll_time = Arc::new(Mutex::new(Utc::now()));

    let stream = WatchStream::new(rx).then(move |_| {
        let pg_pool = pg_pool.clone();
        let poll_time = poll_time.clone();

        async move {
            let current_poll_time = *poll_time.lock().await;
            let data = fetch_sse_data(&pg_pool, current_poll_time).await.unwrap();
            *poll_time.lock().await = Utc::now();

            Ok(Event::default().data(serde_json::to_string(&data).unwrap()))
        }
    });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(5))
            .text("ping"),
    )
}
