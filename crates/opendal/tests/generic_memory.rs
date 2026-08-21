//! End-to-end test of the generic OpenDAL backend over the in-memory service.
//! Proves the generic plumbing without touching the filesystem or network.
#![cfg(feature = "opendal-memory")]

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use deltalake_opendal::{GenericAdapter, register_opendal_handlers};

#[tokio::test]
async fn memory_roundtrip() {
    // `memory://` is owned by core, so the generic service is reachable only
    // via the unambiguous `opendal+memory://` scheme.
    register_opendal_handlers("opendal+memory", Arc::new(GenericAdapter::new("memory")));

    // OpenDAL's memory service is not shared across operator instances, so a
    // fresh reopen can't see prior writes; reload the same handle instead.
    common::roundtrip_same_handle("opendal+memory://localhost/my_table", HashMap::new()).await;
}
