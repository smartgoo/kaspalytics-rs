use crate::ingest::{cache::Reader, model::CacheBlock};
use crate::web::{AppContext, AppState};
use axum::{
    extract::State,
    response::{sse::Event, Sse},
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use kaspa_hashes::Hash;
use serde::Serialize;
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::{watch, Mutex};
use tokio_stream::wrappers::WatchStream;

#[derive(Serialize)]
struct SseBlock {
    hash: Hash,
    daa_score: u64,
    timestamp: u64,
    parents: Vec<Hash>,
}

impl From<CacheBlock> for SseBlock {
    fn from(value: CacheBlock) -> Self {
        SseBlock {
            hash: value.hash,
            daa_score: value.daa_score,
            timestamp: value.timestamp,
            parents: value.parent_hashes,
        }
    }
}

struct SseState {
    context: Arc<AppContext>,
    deltas_since: Mutex<Option<DateTime<Utc>>>,
}

impl SseState {
    fn new(context: Arc<AppContext>) -> Self {
        Self {
            context,
            deltas_since: Mutex::new(None),
        }
    }

    async fn create_event(&self) -> Result<Event, Infallible> {
        let deltas_since = *self.deltas_since.lock().await;

        if !self.context.dag_cache.synced() {
            return Ok(Event::default().data("server not synced to dag tip"));
        }

        let data: Vec<SseBlock> = match deltas_since {
            Some(since) => self
                .context
                .dag_cache
                .blocks_iter()
                .filter(|block| block.seen_at >= since)
                .map(|block| SseBlock::from(block.value().clone()))
                .collect(),
            None => {
                let since = Utc::now() - chrono::Duration::seconds(30);
                self.context
                    .dag_cache
                    .blocks_iter()
                    .filter(|block| block.seen_at >= since)
                    .map(|block| SseBlock::from(block.value().clone()))
                    .collect()
            }
        };

        *self.deltas_since.lock().await = Some(Utc::now());
        let json = serde_json::to_string(&data).unwrap();
        Ok(Event::default().data(json))
    }
}

pub async fn stream(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let _ = tx.send(());
        }
    });

    let stream_state = Arc::new(SseState::new(state.context));

    let stream = WatchStream::new(rx).then({
        let stream_state = stream_state.clone();
        move |_| {
            let stream_state = stream_state.clone();
            async move { stream_state.create_event().await }
        }
    });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}
