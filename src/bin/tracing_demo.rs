use std::error::Error;

use actix_web::web::{Bytes, Path};
use actix_web::{get, post, App, HttpServer};
use actix_web_opentelemetry::ClientExt;
use opentelemetry::global;
use opentelemetry::trace::FutureExt;
use tokio::task::spawn_local;
use tracing::instrument;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[post("/client-request/{path}")]
#[instrument]
async fn client_request(path: Path<String>) -> Bytes {
    spawn_local(
        async move {
            awc::Client::new()
                .get(format!("http://127.0.0.1:8080/{path}"))
                .trace_request()
                .send()
                .await
                .unwrap()
                .body()
                .await
                .unwrap()
        }
        .with_current_context(),
    )
    .await
    .unwrap()
}

#[get("/{path}")]
#[instrument]
async fn echo(path: Path<String>) -> String {
    tracing::info!(?path, "echo");
    format!("OK {path}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("tracing_demo")
        .install_batch(opentelemetry::runtime::Tokio)?;

    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    HttpServer::new(move || {
        App::new()
            .wrap(actix_web_opentelemetry::RequestTracing::new())
            .service(client_request)
            .service(echo)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await?;

    global::shutdown_tracer_provider(); // sending remaining spans

    Ok(())
}
