use std::time::Duration;

use opentelemetry::global;
use opentelemetry::trace::TraceError;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), TraceError> {
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        // .install_simple()?;
        .with_service_name("tracing_demo")
        .install_batch(opentelemetry::runtime::Tokio)?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().json())
        .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    let span = tracing::info_span!("do some work");
    let enter = span.enter();
    println!("in span #1");
    drop(enter);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let enter = span.enter();
    println!("in span #2");
    drop(enter);
    drop(span);

    // tracer.in_span("doing_work", |cx| {
    //     // Traced app logic here...
    // });

    global::shutdown_tracer_provider(); // sending remaining spans

    Ok(())
}
