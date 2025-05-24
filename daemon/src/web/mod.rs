mod handlers;

use crate::AppContext;
use axum::http::{HeaderValue, Request, Response};
use axum::{routing::get, Router};
use http::Method;
use kaspalytics_utils::{config::Env, log::LogTarget};
use log::{error, info, warn};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tower_governor::{
    governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor, GovernorLayer,
};
use tower_http::{
    classify::{ServerErrorsAsFailures, SharedClassifier},
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::{Level, Span};

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

    fn cors_layer(&self) -> CorsLayer {
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

    #[allow(clippy::type_complexity)]
    fn trace_layer<B>(
        &self,
    ) -> TraceLayer<
        SharedClassifier<ServerErrorsAsFailures>,
        DefaultMakeSpan,
        impl FnMut(&Request<B>, &Span) + Clone,
        impl FnMut(&Response<B>, Duration, &Span) + Clone,
    > {
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
            .on_response(DefaultOnResponse::new().level(Level::INFO))
            .on_request(|request: &Request<B>, span: &Span| {
                let path = request.uri().path().to_string();
                span.record("path", &path);
                info!(
                    target: LogTarget::Web.as_str(),
                    "HTTP Request: {} {} from {}",
                    request.method(),
                    path,
                    request
                        .headers()
                        .get("x-forwarded-for")
                        .and_then(|v| v.to_str().ok())
                        .or_else(|| request
                            .headers()
                            .get("x-real-ip")
                            .and_then(|v| v.to_str().ok()))
                        .unwrap_or("unknown")
                );
            })
            .on_response(|response: &Response<B>, latency: Duration, span: &Span| {
                let path = span
                    .field("path")
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let status = response.status();
                if status.is_server_error() {
                    error!(
                        target: LogTarget::WebErr.as_str(),
                        "HTTP Response: {} {} - {} ({}ms)",
                        response.status(),
                        response.status().as_str(),
                        path,
                        latency.as_millis()
                    );
                } else if status.is_client_error() {
                    warn!(
                        target: LogTarget::Web.as_str(),
                        "HTTP Response: {} {} - {} ({}ms)",
                        response.status(),
                        response.status().as_str(),
                        path,
                        latency.as_millis()
                    );
                } else {
                    info!(
                        target: LogTarget::Web.as_str(),
                        "HTTP Response: {} {} - {} ({}ms)",
                        response.status(),
                        response.status().as_str(),
                        path,
                        latency.as_millis()
                    );
                }
            })
    }
}

impl WebServer {
    pub async fn run(&self) {
        info!(target: "web", "Starting WebServer configuration");

        let governor_conf = GovernorConfigBuilder::default()
            .per_second(60)
            .burst_size(10)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap_or_else(|| {
                error!(target: "web_err", "Failed to configure rate limiting");
                panic!("Rate limiting configuration failed");
            });

        info!(target: "web", "Rate limiting configured: 60 req/sec with burst of 10");

        let app = Router::new()
            .route("/home/stream", get(handlers::home::stream))
            .with_state(self.state.clone())
            .layer(self.cors_layer())
            .layer(GovernorLayer {
                config: Arc::new(governor_conf),
            })
            .layer(RequestBodyLimitLayer::new(64 * 1024))
            .layer(self.trace_layer());

        let addr = format!("0.0.0.0:{}", self.context.config.web_port);

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!(target: "web_err", "Failed to bind to {}: {}", addr, e);
                return;
            }
        };

        info!(target: "web", "Web server running on http://{}", addr);

        let shutdown_flag = self.context.shutdown_flag.clone();

        let server = axum::serve(listener, app);
        let shutdown = async {
            while !shutdown_flag.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(target: "web", "Web server shutting down...");
        };

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    error!(target: "web_err", "Web server error: {}", e);
                }
            }
            _ = shutdown => {
                info!(target: "web", "Shutdown flag detected, terminating server");
            }
        }
        info!(target: "web", "Web server shut down complete")
    }
}
