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
        .install_batch(opentelemetry::runtime::Tokio)?;

    // let provider = TracerProvider::builder()
    //     .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
    //     .build();
    // let tracer = provider.tracer("test".to_string());

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().json())
        .with(EnvFilter::from_default_env())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();

    let span = tracing::info_span!("do some work");
    let entered = span.entered();
    println!("in span");
    drop(entered);

    // tracer.in_span("doing_work", |cx| {
    //     // Traced app logic here...
    // });

    global::shutdown_tracer_provider(); // sending remaining spans

    Ok(())
}
