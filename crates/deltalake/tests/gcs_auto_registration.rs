//! Smoke tests ensuring the `gcs` feature registers the `gs://` scheme automatically.

#![cfg(feature = "gcs")]

use deltalake::{table::builder::DeltaTableBuilder, DeltaResult};
use url::Url;

fn builder_for(uri: &str) -> DeltaResult<DeltaTableBuilder> {
    let url = Url::parse(uri).expect("static test URI must parse");
    DeltaTableBuilder::from_uri(url)
}

#[test]
fn recognizes_basic_gs_uri() {
    builder_for("gs://test-bucket/delta-table")
        .expect("gs:// scheme should be recognized when the gcs feature is enabled");
}

#[test]
fn accepts_nested_paths() {
    builder_for("gs://some-bucket/nested/path/in/table")
        .expect("nested gs:// paths should also be recognized");
}

#[test]
fn builder_chain_stays_valid() {
    builder_for("gs://test-bucket/table")
        .expect("builder should be constructed successfully")
        .with_version(0)
        .without_files();
}
