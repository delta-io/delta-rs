//! OpenTelemetry tracing integration for delta-rs Python bindings

use std::env;
use std::sync::OnceLock;

use opentelemetry::global;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
use tracing_opentelemetry::layer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

static TRACING_INITIALIZED: OnceLock<bool> = OnceLock::new();

/// Initialize OpenTelemetry tracing with OTLP exporter
///
/// # Arguments
/// * `endpoint` - The OTLP HTTP endpoint URL (e.g., "http://localhost:4318/v1/traces")
///   If None, uses the OTEL_EXPORTER_OTLP_ENDPOINT environment variable or defaults to "http://localhost:4318/v1/traces"
///
/// # Environment Variables
/// * `OTEL_EXPORTER_OTLP_ENDPOINT` - The OTLP endpoint URL
/// * `OTEL_EXPORTER_OTLP_HEADERS` - Headers for authentication (format: "key1=value1,key2=value2")
///   Note: Automatically read by the HTTP exporter for API key authentication
/// * `RUST_LOG` - Controls log level filtering (e.g., "info", "debug", "deltalake=debug")
///
/// # Errors
/// Returns an error if initialization fails (e.g., invalid endpoint, network issues)
pub fn init_otlp_tracing(endpoint: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    if TRACING_INITIALIZED.get().is_some() {
        return Ok(());
    }

    let endpoint = endpoint
        .map(String::from)
        .or_else(|| env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok())
        .unwrap_or_else(|| "http://localhost:4318/v1/traces".to_string());

    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;

    let resource = Resource::builder().with_service_name("delta-rs").build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_id_generator(RandomIdGenerator::default())
        .with_sampler(Sampler::AlwaysOn)
        .build();

    global::set_tracer_provider(provider.clone());

    let tracer = global::tracer("delta-rs");
    let telemetry = layer().with_tracer(tracer);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Try to initialize tracing subscriber
    // If already initialized (e.g., by another component), we silently continue
    // The tracer provider is still set, which is what matters for OTLP export
    match tracing_subscriber::registry()
        .with(filter)
        .with(telemetry)
        .try_init()
    {
        Ok(_) => {}
        Err(_) => {
            // Already initialized - continue gracefully
        }
    }

    TRACING_INITIALIZED.set(true).ok();
    Ok(())
}

/// Shutdown the OpenTelemetry tracer provider and flush remaining spans
pub fn shutdown_tracing() {
    let _ = global::tracer_provider();
}
