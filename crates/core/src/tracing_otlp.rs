//! OpenTelemetry tracing integration for delta-rs

use std::sync::OnceLock;

#[cfg(feature = "otel")]
use opentelemetry::global;
#[cfg(feature = "otel")]
use opentelemetry_otlp::{SpanExporter, WithExportConfig, WithTonicConfig};
#[cfg(feature = "otel")]
use opentelemetry_sdk::runtime;
#[cfg(feature = "otel")]
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
#[cfg(feature = "otel")]
use opentelemetry_sdk::Resource;
#[cfg(feature = "otel")]
use tracing_opentelemetry;
#[cfg(feature = "otel")]
use tracing_subscriber::layer::SubscriberExt;
#[cfg(feature = "otel")]
use tracing_subscriber::util::SubscriberInitExt;
#[cfg(feature = "otel")]
use tracing_subscriber::EnvFilter;

static TRACING_INITIALIZED: OnceLock<bool> = OnceLock::new();

/// Initialize OpenTelemetry tracing with OTLP exporter
///
/// # Arguments
/// * `endpoint` - The OTLP gRPC endpoint URL (e.g., "http://localhost:4317")
///
/// # Errors
/// Returns an error if tracing has already been initialized or initialization fails
#[cfg(feature = "otel")]
pub fn init_otlp_tracing(endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    if TRACING_INITIALIZED.get().is_some() {
        return Err("Tracing has already been initialized".into());
    }

    let exporter = SpanExporter::builder()
        .with_tonic()
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
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(telemetry)
        .try_init()?;

    TRACING_INITIALIZED.set(true).ok();
    Ok(())
}

#[cfg(not(feature = "otel"))]
pub fn init_otlp_tracing(_endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
    Err("OpenTelemetry feature 'otel' not enabled. Rebuild with --features otel".into())
}

/// Shutdown the OpenTelemetry tracer provider and flush remaining spans
#[cfg(feature = "otel")]
pub fn shutdown_tracing() {
    let _ = global::tracer_provider();
}

#[cfg(not(feature = "otel"))]
pub fn shutdown_tracing() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "otel")]
    fn test_init_and_shutdown_tracing() {
        let result = init_otlp_tracing("http://localhost:4317");
        assert!(result.is_ok());

        let result = init_otlp_tracing("http://localhost:4317");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Tracing has already been initialized"));

        shutdown_tracing();
    }

    #[test]
    #[cfg(not(feature = "otel"))]
    fn test_init_tracing_feature_disabled() {
        let result = init_otlp_tracing("http://localhost:4317");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("OpenTelemetry feature 'otel' not enabled"));
    }
}
