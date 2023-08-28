use std::fmt::Display;

use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing(service_name: &str) {
    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name(service_name)
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("unable to install jaeger tracer");
    tracing_subscriber::registry()
        // .with(tracing_subscriber::fmt::layer().json())
        .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
}

pub async fn shutdown_tracing() {
    tokio::task::spawn_blocking(opentelemetry::global::shutdown_tracer_provider)
        .await
        .unwrap()
}

pub type LocalResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
pub struct HandlerError(anyhow::Error);

pub type HandlerResult<T> = Result<T, HandlerError>;

impl<T> From<T> for HandlerError
where
    anyhow::Error: From<T>,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl ResponseError for HandlerError {
    fn status_code(&self) -> actix_web::http::StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).body(format!("{self}"))
    }
}

impl Display for HandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub fn hex_string(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .reduce(|s1, s2| s1 + &s2)
        .unwrap()
}
