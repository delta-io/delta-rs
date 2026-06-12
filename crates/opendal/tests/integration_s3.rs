//! End-to-end test of the generic OpenDAL backend over the `s3` service.
//!
//! Exercises a real cloud-style service through the generic path against the
//! S3-compatible endpoint delta-rs already runs in CI (LocalStack on
//! `localhost:4566`, see `docker-compose.yml`). Marked `#[ignore]` and skipped
//! unless `AWS_ENDPOINT_URL` is set, so it never runs without the service.
//!
//! Run with the service up:
//!   docker compose up -d localstack
//!   AWS_ENDPOINT_URL=http://localhost:4566 \
//!   OPENDAL_S3_BUCKET=<existing-bucket> \
//!   cargo test -p deltalake-opendal --features opendal-s3 -- --ignored
#![cfg(feature = "opendal-s3")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use deltalake_opendal::{GenericAdapter, register_opendal_handlers};

#[tokio::test]
#[ignore = "requires an S3-compatible endpoint (LocalStack/MinIO)"]
async fn s3_roundtrip() {
    let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") else {
        eprintln!("AWS_ENDPOINT_URL not set; skipping");
        return;
    };
    let bucket = std::env::var("OPENDAL_S3_BUCKET").unwrap_or_else(|_| "deltalake".to_string());
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "deltalake".to_string());
    let secret_key =
        std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "weloverust".to_string());

    // `s3://` is owned by deltalake-aws, so the generic service is reachable
    // only via the unambiguous `opendal+s3://` scheme.
    register_opendal_handlers("opendal+s3", Arc::new(GenericAdapter::new("s3")));

    let storage_options = HashMap::from([
        ("opendal.endpoint".to_string(), endpoint),
        ("opendal.region".to_string(), region),
        ("opendal.access_key_id".to_string(), access_key),
        ("opendal.secret_access_key".to_string(), secret_key),
        // Path-style addressing is required for non-AWS endpoints.
        (
            "opendal.enable_virtual_host_style".to_string(),
            "false".to_string(),
        ),
    ]);

    let uri = format!("opendal+s3://{bucket}/delta_opendal_test");
    common::roundtrip(&uri, storage_options).await;
}
