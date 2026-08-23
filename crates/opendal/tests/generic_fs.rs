//! End-to-end test of the generic OpenDAL backend over the local `fs` service.
//! Runs entirely on local disk — no network, no external account.
#![cfg(feature = "opendal-fs")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use deltalake_opendal::{GenericAdapter, register_opendal_handlers};

#[tokio::test]
async fn fs_roundtrip() {
    // The OpenDAL `fs` service has no native counterpart, so it registers
    // under its bare `fs://` scheme (core owns `file://`, not `fs://`).
    register_opendal_handlers("fs", Arc::new(GenericAdapter::new("fs")));

    let tmp = tempfile::tempdir().unwrap();
    let storage_options = HashMap::from([(
        "opendal.root".to_string(),
        tmp.path().to_string_lossy().to_string(),
    )]);

    common::roundtrip("fs://localhost/my_table", storage_options).await;
}
