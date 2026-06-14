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

/// Generic OpenDAL services to auto-register, gated by their cargo feature.
/// Each is enabled only when its `opendal-<service>` feature is on.
///
/// Every service is registered under the unambiguous `opendal+<service>://`
/// scheme (e.g. `opendal+s3://`). When the service's natural scheme does not
/// collide with a native delta backend (see [`NATIVE_SCHEMES`]) it is *also*
/// registered under that bare scheme, so `hf://`, `fs://`, `oss://`, … work
/// directly. Services whose natural scheme is owned by a native backend
/// (`s3`, `memory`, …) are reachable only via the `opendal+` form, so enabling
/// them never clobbers the native backend.
const GENERIC_SERVICES: &[&str] = &[
    #[cfg(feature = "opendal-fs")]
    "fs",
    #[cfg(feature = "opendal-memory")]
    "memory",
    #[cfg(feature = "opendal-s3")]
    "s3",
    #[cfg(feature = "opendal-gcs")]
    "gcs",
    #[cfg(feature = "opendal-azblob")]
    "azblob",
    #[cfg(feature = "opendal-azdls")]
    "azdls",
    #[cfg(feature = "opendal-oss")]
    "oss",
    #[cfg(feature = "opendal-obs")]
    "obs",
    #[cfg(feature = "opendal-cos")]
    "cos",
    #[cfg(feature = "opendal-tos")]
    "tos",
    #[cfg(feature = "opendal-b2")]
    "b2",
    #[cfg(feature = "opendal-swift")]
    "swift",
    #[cfg(feature = "opendal-webhdfs")]
    "webhdfs",
    #[cfg(feature = "opendal-webdav")]
    "webdav",
    #[cfg(feature = "opendal-ftp")]
    "ftp",
    #[cfg(feature = "opendal-sftp")]
    "sftp",
    #[cfg(feature = "opendal-hf")]
    "hf",
];

/// URL schemes owned by native (non-OpenDAL) delta backends. A generic service
/// whose natural scheme appears here is NOT registered under that bare scheme —
/// only under `opendal+<service>://` — so it never clobbers the native backend.
const NATIVE_SCHEMES: &[&str] = &[
    "file", "memory", "s3", "s3a", "gs", "az", "azure", "adl", "abfs", "abfss", "hdfs", "viewfs",
    "lakefs", "dbfs", "uc",
];

/// The scheme prefix that makes any OpenDAL service reachable unambiguously,
/// e.g. `opendal+s3://`. `+` is a valid URL scheme character (RFC 3986).
pub const OPENDAL_SCHEME_PREFIX: &str = "opendal+";

/// Register the OpenDAL-backed storage handlers enabled by feature flags.
///
/// Called automatically at program start via the OpenDAL feature flags in the
/// top-level `deltalake` crate. Each enabled service is registered under
/// `opendal+<service>://`, plus its bare `<service>://` scheme when that neither
/// collides with a native delta backend nor is a WHATWG special scheme that
/// can't form a bare `scheme://` URL (e.g. `ftp`) — see [`GENERIC_SERVICES`].
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    for service in GENERIC_SERVICES {
        let adapter = Arc::new(GenericAdapter::new(*service));
        register_opendal_handlers(
            &format!("{OPENDAL_SCHEME_PREFIX}{service}"),
            adapter.clone(),
        );
        // Also expose the bare `<service>://` scheme, but only when it neither
        // collides with a native delta backend nor is a WHATWG "special" scheme.
        // Special schemes (e.g. `ftp`) require a host, so `Url::parse("ftp://")`
        // fails — those stay reachable only via the `opendal+<service>://` form.
        if !NATIVE_SCHEMES.contains(service) && Url::parse(&format!("{service}://")).is_ok() {
            register_opendal_handlers(service, adapter);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// OpenDAL services whose natural scheme is owned by a native delta backend
    /// must stay listed in [`NATIVE_SCHEMES`] so [`register_handlers`] registers
    /// them ONLY under `opendal+<service>://`, never their bare `<service>://`
    /// scheme. Otherwise the bare registration (a last-writer-wins `.insert`,
    /// run from an unspecified-order `ctor`) would silently shadow the native
    /// backend. This pins the known collisions so dropping one from
    /// `NATIVE_SCHEMES` fails the build rather than clobbering a backend at
    /// startup.
    #[test]
    fn native_scheme_collisions_are_suppressed() {
        for colliding in ["s3", "memory"] {
            assert!(
                NATIVE_SCHEMES.contains(&colliding),
                "`{colliding}` shares a scheme with a native delta backend but is \
                 missing from NATIVE_SCHEMES; register_handlers would shadow it \
                 under bare `{colliding}://`"
            );
        }
    }

    /// `ftp` is a WHATWG "special" scheme: `Url::parse("ftp://")` fails with
    /// `EmptyHost`, so it must never be bare-registered (doing so panicked the
    /// whole process at startup, since `register_handlers` runs from a `ctor`).
    /// It must remain reachable via the `opendal+ftp://` form. Runs only when the
    /// `opendal-ftp` service is compiled in.
    #[cfg(feature = "opendal-ftp")]
    #[test]
    fn special_scheme_registers_without_panic() {
        assert!(Url::parse("ftp://").is_err());
        register_handlers(None);
        assert!(
            object_store_factories()
                .get(&Url::parse("opendal+ftp://").unwrap())
                .is_some(),
            "ftp must be reachable via the opendal+ftp:// scheme"
        );
    }
}
