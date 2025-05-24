mod handlers;

use crate::AppContext;
use axum::http::HeaderValue;
use axum::{routing::get, Router};
use http::Method;
use kaspalytics_utils::config::Env;
use log::{error, info};
use tower_governor::GovernorLayer;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
    limit::RequestBodyLimitLayer,
};
use tower_governor::{
    governor::{Governor, GovernorConfigBuilder},
    key_extractor::SmartIpKeyExtractor,
};

#[derive(Clone)]
struct AppState {
    context: Arc<AppContext>,
}

pub struct WebServer {
    context: Arc<AppContext>,
    state: AppState,
}

impl WebServer {
    pub fn new(context: Arc<AppContext>) -> Self {
        WebServer {
            context: context.clone(),
            state: AppState {
                context: context.clone(),
            },
        }
    }

    fn configure_cors(&self) -> CorsLayer {
        match self.context.config.env {
            Env::Prod => {
                let origins: Vec<HeaderValue> = self
                    .context
                    .config
                    .allowed_origins
                    .iter()
                    .map(|origin| origin.parse().unwrap())
                    .collect();

                CorsLayer::new()
                    .allow_methods([Method::GET])
                    .allow_origin(origins)
                    .max_age(Duration::from_secs(3600))
            }
            _ => CorsLayer::new()
                .allow_methods([Method::GET])
                .allow_origin(Any)
                .allow_headers(Any)
                .max_age(Duration::from_secs(3600)),
        }
    }
}

impl WebServer {
    pub async fn run(&self) {
        // Rate limit: 5 new SSE connections per minute per IP
        let governor_conf = GovernorConfigBuilder::default()
            .per_second(60)
            .burst_size(5)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap();

        let app = Router::new()
            .route("/home/stream", get(handlers::home::stream))
            .with_state(self.state.clone())
            .layer(self.configure_cors())
            .layer(GovernorLayer { config: Arc::new(governor_conf) })
            .layer(RequestBodyLimitLayer::new(1024 * 1024)) // 1MB limit
            .layer(TraceLayer::new_for_http());

        let addr = format!("0.0.0.0:{}", self.context.config.web_port);

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind to {}: {}", addr, e);
                return;
            }
        };

        info!("Web server running on http://{}", addr);

        let shutdown_flag = self.context.shutdown_flag.clone();
        
        let server = axum::serve(listener, app);
        let shutdown = async {
            while !shutdown_flag.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!("Web server shutting down...");
        };

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    error!("Web server error: {}", e);
                }
            }
            _ = shutdown => {
                info!("Shutdown flag detected, terminating server");
            }
        }
        info!("Web server shut down complete")
    }
}
