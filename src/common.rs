use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn setup_tracing(service_name: &str) {
    opentelemetry::global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());
    let tracer = opentelemetry_zipkin::new_pipeline()
        .with_service_name(service_name)
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("unable to install zipkin tracer");
    tracing_subscriber::registry()
        // .with(tracing_subscriber::fmt::layer().json())
        // .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
}

pub async fn shutdown_tracing() {
    tokio::task::spawn_blocking(|| opentelemetry::global::shutdown_tracer_provider())
        .await
        .unwrap()
}
