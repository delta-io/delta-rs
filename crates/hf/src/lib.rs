//! HuggingFace Hub storage backend for delta-rs.
//!
//! Built on top of the generic [`deltalake_opendal`] crate: it supplies an
//! [`HfAdapter`] that handles HF-specific URL parsing (`hf://<repo_type>/<owner>/
//! <repo>[@<revision>]/<path>`) and the repo prefix-stripping the generic path
//! can't express, then registers it for the `hf://` scheme. Conditional-create
//! emulation (the HF backend lacks `write_with_if_not_exists`) is applied
//! generically by `deltalake-opendal` based on the operator's capabilities.

mod adapter;
mod config;
mod url;

use std::sync::Arc;

use deltalake_opendal::register_opendal_handlers;

pub use adapter::HfAdapter;

/// Register the HuggingFace Hub storage and log-store factories for the `hf://`
/// scheme. Called automatically at program start via the `hf` feature flag in
/// the top-level `deltalake` crate.
pub fn register_handlers(_additional_prefixes: Option<::url::Url>) {
    register_opendal_handlers("hf", Arc::new(HfAdapter));
}
