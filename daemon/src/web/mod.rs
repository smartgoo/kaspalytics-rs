mod handlers;

use axum::{routing::get, Router};
use kaspalytics_utils::config::Config;
use log::{error, info};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct WebServer {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    pg_pool: PgPool,
}

impl WebServer {
    pub fn new(config: Config, shutdown_flag: Arc<AtomicBool>, pg_pool: PgPool) -> Self {
        WebServer {
            config,
            shutdown_flag,
            pg_pool,
        }
    }

    pub async fn run(self) {
        let app = Router::new().route(
            "/sse/home/stream",
            get(move || handlers::home_page_sse(self.pg_pool.clone())),
        );

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
