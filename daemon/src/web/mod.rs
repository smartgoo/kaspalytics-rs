mod handlers;

use crate::AppContext;
use axum::http::{HeaderValue, Request, Response};
use axum::{middleware, routing::get, Router};
use http::Method;
use kaspalytics_utils::{config::Env, log::LogTarget};
use log::{error, info, warn};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tower_http::{
    classify::{ServerErrorsAsFailures, SharedClassifier},
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::{Level, Span};

async fn cache_control_middleware(
    request: Request<axum::body::Body>,
    next: middleware::Next,
) -> Response<axum::body::Body> {
    let uri_path = request.uri().path().to_string();
    let response = next.run(request).await;

    // Check if this is a 404 response for block/transaction/explorer endpoints
    if response.status() == http::StatusCode::NOT_FOUND
        && (uri_path.contains("/block/")
            || uri_path.contains("/transaction/")
            || uri_path.contains("/explorer/"))
    {
        let (mut parts, body) = response.into_parts();

        // Override cache-control for not found responses
        parts.headers.insert(
            "cache-control",
            HeaderValue::from_static("public, max-age=5, s-maxage=5"),
        );

        return Response::from_parts(parts, body);
    }

    response
}

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
        info!(target: "web", "Starting WebServer...");

        let app = Router::new()
            .route("/sse/v1/home/stream", get(handlers::home::stream))
            .route(
                "/sse/v1/visualizer/stream",
                get(handlers::visualizer::stream),
            )
            .route(
                "/api/v1/lists/protocol-activity",
                get(handlers::lists::protocol_activity::get_protocol_activity),
            )
            .route(
                "/api/v1/lists/largest-fees",
                get(handlers::lists::largest_fees::get_largest_fees),
            )
            .route(
                "/api/v1/lists/largest-transactions",
                get(handlers::lists::largest_transactions::get_largest_transactions),
            )
            .route(
                "/api/v1/lists/most-active-addresses",
                get(handlers::lists::active_addresses::get_most_active_addresses),
            )
            .with_state(self.state.clone())
            .layer(middleware::from_fn(cache_control_middleware))
            .layer(self.cors_layer())
            .layer(RequestBodyLimitLayer::new(64 * 1024))
            .layer(self.trace_layer());

        let addr = format!(
            "{}:{}",
            self.context.config.web_listen_ip, self.context.config.web_port,
        );

        let listener = match tokio::net::TcpListener::bind(&addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!(target: "web_err", "Failed to bind to {}: {}", addr, e);
                return;
            }
        };

        let shutdown_flag = self.context.shutdown_flag.clone();

        let server = axum::serve(listener, app);
        let shutdown = async {
            while !shutdown_flag.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(target: "web", "Web server shutting down...");
        };

        info!(target: "web", "WebServer started, listening on http://{}", addr);

        tokio::select! {
            result = server => {
                if let Err(e) = result {
                    error!(target: "web_err", "Web server error: {}", e);
                }
            }
            _ = shutdown => {
                info!(target: "web", "WebServer shutting down...");
            }
        }
        info!(target: "web", "WebServer shut down complete")
    }
}
