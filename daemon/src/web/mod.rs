use crate::cache::{Cache, CacheReader};
use axum::{
    response::sse::{Event, Sse},
    routing::get,
    Router,
};
use kaspalytics_utils::config::Config;
use log::{error, info};
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_stream::{wrappers::WatchStream, Stream, StreamExt};

pub struct WebServer {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    cache: Arc<Cache>,
}

impl WebServer {
    pub fn new(config: Config, shutdown_flag: Arc<AtomicBool>, cache: Arc<Cache>) -> Self {
        WebServer {
            config,
            shutdown_flag,
            cache,
        }
    }

    pub async fn run(self) {
        let app = Router::new().route("/sse", get(move || sse_handler(self.cache.clone())));

        let addr = format!("0.0.0.0:{}", self.config.web_port);

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to {}: {}", addr, e);
                return;
            }
        };

        info!("Web server running on http://{}", addr);

        let shutdown_flag = self.shutdown_flag.clone();
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                while !shutdown_flag.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                info!("Web server shutting down");
            })
            .await
            .unwrap_or_else(|e| error!("Web server error: {}", e));
    }
}

async fn sse_handler(cache: Arc<Cache>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = watch::channel(());

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let _ = tx.send(());
        }
    });

    let stream = WatchStream::new(rx).then(move |_| {
        let cache = cache.clone();

        async move {
            let data = cache.tip_timestamp();
            Ok(Event::default().data(data.to_string()))
        }
    });

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(5))
            .text("ping"),
    )
}
