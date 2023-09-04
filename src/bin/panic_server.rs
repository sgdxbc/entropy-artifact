use std::panic;

use actix_web::{get, post, App, HttpResponse, HttpServer};
use opentelemetry::{
    global::{set_text_map_propagator, shutdown_tracer_provider},
    trace::{get_active_span, Status},
};
use tokio::task::spawn_blocking;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[get("/status")]
async fn status() -> String {
    "ok".into()
}

#[post("/crash")]
async fn crash() -> HttpResponse {
    panic!("crashed")
}

#[actix_web::main]
async fn main() {
    set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        // .install_simple()?;
        .with_service_name("panic_server")
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();

    let hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        get_active_span(|span| {
            // span.add_event("panic", vec![KeyValue::new("display", info.to_string())]);
            span.set_status(Status::error(info.to_string()));
            span.end();
        });
        println!("shutdown tracer provider");
        shutdown_tracer_provider();
        hook(info)
    }));

    tracing_subscriber::registry()
        // .with(tracing_subscriber::fmt::layer().json())
        .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    HttpServer::new(move || {
        App::new()
            .wrap(actix_web_opentelemetry::RequestTracing::new())
            .service(status)
            .service(crash)
    })
    .workers(1)
    .bind(("127.0.0.1", 8080))
    .unwrap()
    .run()
    .await
    .unwrap();

    spawn_blocking(shutdown_tracer_provider).await.unwrap(); // sending remaining spans
}
