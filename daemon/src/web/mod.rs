mod handlers;

use crate::storage::cache::Cache;
use crate::storage::Storage;
use crate::AppContext;
use axum::http::HeaderValue;
use axum::{routing::get, Router};
use http::Method;
use kaspalytics_utils::config::{Config, Env};
use log::{error, info};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
struct AppState {
    pg_pool: PgPool,
    storage: Arc<Storage>,
}

pub struct WebServer {
    config: Config,
    shutdown_flag: Arc<AtomicBool>,
    state: AppState,
    storage: Arc<Storage>,
}

impl WebServer {
    pub fn new(context: Arc<AppContext>) -> Self {
        WebServer {
            config: context.config.clone(),
            shutdown_flag: context.shutdown_flag.clone(),
            state: AppState {
                pg_pool: context.pg_pool.clone(),
                storage: context.storage.clone(),
            },
            storage: context.storage.clone(),
        }
    }

    fn configure_cors(&self) -> CorsLayer {
        match self.config.env {
            Env::Prod => {
                let origins: Vec<HeaderValue> = self
                    .config
                    .allowed_origins
                    .iter()
                    .map(|origin| origin.parse().unwrap())
                    .collect();

                CorsLayer::new()
                    .allow_methods([Method::GET])
                    .allow_origin(origins)
            }
            _ => CorsLayer::new()
                .allow_methods([Method::GET])
                .allow_origin(Any)
                .allow_headers(Any),
        }
    }
}

impl WebServer {
    pub async fn run(&self) {
        let app = Router::new()
            .route("/home/stream", get(handlers::home::stream))
            .with_state(self.state.clone())
            .layer(self.configure_cors());

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
                info!("Web server shutting down...");
            })
            .await
            .unwrap_or_else(|e| error!("Web server error: {}", e));
        info!("Web server shut down complete")
    }
}
