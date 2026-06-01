//! Generic [OpenDAL](https://opendal.apache.org/) storage backend for delta-rs.
//!
//! This crate plugs any OpenDAL service into delta's storage registry through a
//! single pair of generic factories ([`OpendalObjectStoreFactory`] and
//! [`OpendalLogStoreFactory`]) parameterized by an [`OpendalAdapter`]. Simple
//! services (operator scoped at the bucket root, URL path == table prefix) use
//! [`GenericAdapter`]; HuggingFace Hub is provided as a built-in specialization
//! behind the `hf` feature.
//!
//! Each backend is registered for a URL scheme via [`register_opendal_handlers`].

mod adapter;
mod config;
mod factory;
mod shim;
mod sorted;

#[cfg(feature = "hf")]
pub mod hf;

use std::sync::Arc;

use deltalake_core::logstore::{logstore_factories, object_store_factories};
use url::Url;

pub use adapter::{GenericAdapter, OpendalAdapter, OperatorSpec};
pub use config::OPENDAL_PREFIX;
pub use factory::{OpendalLogStoreFactory, OpendalObjectStoreFactory};
pub use shim::ConditionalPutShim;

/// Register an [`OpendalAdapter`] as the object-store and log-store factory for
/// `scheme`. This overwrites any existing factory registered for the scheme.
pub fn register_opendal_handlers<A: OpendalAdapter + 'static>(scheme: &str, adapter: Arc<A>) {
    let url = Url::parse(&format!("{scheme}://")).unwrap();
    object_store_factories().insert(
        url.clone(),
        Arc::new(OpendalObjectStoreFactory(adapter.clone())),
    );
    logstore_factories().insert(url, Arc::new(OpendalLogStoreFactory(adapter)));
}

/// Register the OpenDAL-backed storage handlers enabled by feature flags.
///
/// Called automatically at program start via the `hf` feature flag in the
/// top-level `deltalake` crate. Only schemes with no native delta backend are
/// registered, so this never clobbers `s3://`, `gs://`, `az://`, `file://`, or
/// `memory://`.
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    #[cfg(feature = "hf")]
    register_opendal_handlers("hf", Arc::new(hf::HfAdapter));
}
