//! Generic [OpenDAL](https://opendal.apache.org/) storage backend for delta-rs.
//!
//! This crate plugs any OpenDAL service into delta's storage registry through a
//! single pair of generic factories ([`OpendalObjectStoreFactory`] and
//! [`OpendalLogStoreFactory`]) parameterized by an [`OpendalAdapter`]. Simple
//! services (operator scoped at the bucket root, URL path == table prefix) use
//! the built-in [`GenericAdapter`]; services with bespoke URL or path semantics
//! supply their own [`OpendalAdapter`] in a downstream crate.
//!
//! Each backend is registered for a URL scheme via [`register_opendal_handlers`].

mod adapter;
mod config;
mod factory;
mod shim;
mod sorted;

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

/// Generic OpenDAL services to auto-register, as `(delta url scheme, opendal
/// service)` pairs gated by their cargo feature. Each is enabled only when its
/// `opendal-<service>` feature is on.
///
/// Schemes owned by a native delta backend (`s3`/`s3a`, `gs`, `az`/`abfs…`,
/// `hdfs`/`viewfs`, `lakefs`) are deliberately exposed under an `opendal`-
/// prefixed scheme (e.g. `opendals3://`) so enabling them never clobbers the
/// native backend's scheme — users opt in by choosing the prefixed scheme.
/// Services with no native counterpart keep their natural scheme.
const GENERIC_SERVICES: &[(&str, &str)] = &[
    #[cfg(feature = "opendal-fs")]
    ("opendalfs", "fs"),
    #[cfg(feature = "opendal-memory")]
    ("opendalmem", "memory"),
    #[cfg(feature = "opendal-s3")]
    ("opendals3", "s3"),
    #[cfg(feature = "opendal-gcs")]
    ("gcs", "gcs"),
    #[cfg(feature = "opendal-azblob")]
    ("azblob", "azblob"),
    #[cfg(feature = "opendal-azdls")]
    ("azdls", "azdls"),
    #[cfg(feature = "opendal-oss")]
    ("oss", "oss"),
    #[cfg(feature = "opendal-obs")]
    ("obs", "obs"),
    #[cfg(feature = "opendal-cos")]
    ("cos", "cos"),
    #[cfg(feature = "opendal-tos")]
    ("tos", "tos"),
    #[cfg(feature = "opendal-b2")]
    ("b2", "b2"),
    #[cfg(feature = "opendal-swift")]
    ("swift", "swift"),
    #[cfg(feature = "opendal-webhdfs")]
    ("webhdfs", "webhdfs"),
    #[cfg(feature = "opendal-webdav")]
    ("webdav", "webdav"),
    #[cfg(feature = "opendal-ftp")]
    ("ftp", "ftp"),
    #[cfg(feature = "opendal-sftp")]
    ("sftp", "sftp"),
];

/// Register the OpenDAL-backed storage handlers enabled by feature flags.
///
/// Called automatically at program start via the OpenDAL feature flags in the
/// top-level `deltalake` crate. Native delta schemes (`s3://`, `gs://`,
/// `az://`, `file://`, `memory://`, …) are never registered here — see
/// [`GENERIC_SERVICES`].
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    for (scheme, service) in GENERIC_SERVICES {
        register_opendal_handlers(scheme, Arc::new(GenericAdapter::new(*service)));
    }
}
