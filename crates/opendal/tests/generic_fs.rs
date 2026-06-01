//! End-to-end test of the generic OpenDAL backend over the local `fs` service.
//! Runs entirely on local disk — no network, no external account.
#![cfg(feature = "opendal-fs")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use deltalake_opendal::{GenericAdapter, register_opendal_handlers};

#[tokio::test]
async fn fs_roundtrip() {
    // Register the generic adapter for the OpenDAL `fs` service under a
    // non-colliding scheme so it never shadows core's `file://`.
    register_opendal_handlers("opendalfs", Arc::new(GenericAdapter::new("fs")));

    let tmp = tempfile::tempdir().unwrap();
    let storage_options = HashMap::from([(
        "opendal.root".to_string(),
        tmp.path().to_string_lossy().to_string(),
    )]);

    common::roundtrip("opendalfs://localhost/my_table", storage_options).await;
}
