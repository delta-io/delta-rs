//! The [`OpendalAdapter`] abstraction: the small set of per-service decisions the
//! generic factories delegate to.

use deltalake_core::DeltaResult;
use deltalake_core::logstore::{ObjectStoreRef, StorageConfig};
use object_store::path::Path;
use url::Url;

use crate::config::{OPENDAL_PREFIX, collect_prefixed};

/// Everything the generic factories need to build an OpenDAL-backed store for a
/// delta table: which OpenDAL service to use, its config, and where the table
/// lives within the resulting operator.
#[derive(Debug, Clone)]
pub struct OperatorSpec {
    /// OpenDAL service scheme, e.g. `"fs"`, `"memory"`, `"s3"`.
    pub scheme: String,
    /// Config key/value pairs consumed by [`opendal::Operator::via_iter`].
    pub config: Vec<(String, String)>,
    /// The table prefix relative to the operator root.
    pub table_prefix: Path,
}

/// Per-service specialization for the generic OpenDAL factories.
///
/// Only three things vary between OpenDAL services: how a delta URL plus storage
/// options map onto an [`OperatorSpec`], whether the resulting store needs
/// wrapping, and what prefix the log store should use. Everything else is shared
/// by [`crate::factory`].
pub trait OpendalAdapter: Send + Sync + std::fmt::Debug {
    /// Map a delta table URL and storage options to an [`OperatorSpec`].
    fn resolve(&self, url: &Url, config: &StorageConfig) -> DeltaResult<OperatorSpec>;

    /// Optionally wrap the raw `OpendalStore`. The default is the identity; an
    /// adapter may override this to rewrite paths or otherwise decorate the store.
    fn wrap_store(&self, store: ObjectStoreRef, _spec: &OperatorSpec) -> ObjectStoreRef {
        store
    }

    /// The prefix the log store's `PrefixStore` should apply on top of the root
    /// store. Defaults to [`OperatorSpec::table_prefix`].
    fn logstore_prefix(&self, spec: &OperatorSpec) -> Path {
        spec.table_prefix.clone()
    }
}

/// Adapter for "simple" OpenDAL services whose operator is scoped at the bucket
/// root: the URL host is the bucket and the URL path is the table prefix.
#[derive(Debug, Clone)]
pub struct GenericAdapter {
    /// OpenDAL service scheme passed to `Operator::via_iter`.
    pub service: String,
    /// Storage-option prefix whose entries are forwarded to OpenDAL.
    pub option_prefix: String,
}

impl GenericAdapter {
    /// A generic adapter for `service`, reading `opendal.<key>` storage options.
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            option_prefix: OPENDAL_PREFIX.to_string(),
        }
    }
}

impl OpendalAdapter for GenericAdapter {
    fn resolve(&self, url: &Url, config: &StorageConfig) -> DeltaResult<OperatorSpec> {
        let mut opendal_config = collect_prefixed(&config.raw, &self.option_prefix);

        // Derive the bucket from the URL host unless the user set it explicitly.
        // The operator stays scoped at the bucket root (`root = /`); delta's own
        // `decorate_store` applies the URL path as a PrefixStore on top, so we
        // must not also fold the path into `root` (that would double-prefix).
        let has = |cfg: &[(String, String)], k: &str| cfg.iter().any(|(key, _)| key == k);
        if let Some(host) = url.host_str()
            && !host.is_empty()
            && !has(&opendal_config, "bucket")
        {
            opendal_config.push(("bucket".to_string(), host.to_string()));
        }
        if !has(&opendal_config, "root") {
            opendal_config.push(("root".to_string(), "/".to_string()));
        }

        Ok(OperatorSpec {
            scheme: self.service.clone(),
            config: opendal_config,
            table_prefix: Path::from(url.path().trim_matches('/')),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(adapter: &GenericAdapter, url: &str, opts: &[(&str, &str)]) -> OperatorSpec {
        let raw = opts
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let config = StorageConfig {
            raw,
            ..Default::default()
        };
        adapter.resolve(&Url::parse(url).unwrap(), &config).unwrap()
    }

    #[test]
    fn derives_bucket_and_root_and_prefix() {
        let adapter = GenericAdapter::new("s3");
        let spec = resolve(&adapter, "opendals3://my-bucket/tables/foo", &[]);
        assert_eq!(spec.scheme, "s3");
        assert_eq!(spec.table_prefix.as_ref(), "tables/foo");
        assert!(
            spec.config
                .contains(&("bucket".to_string(), "my-bucket".to_string()))
        );
        assert!(spec.config.contains(&("root".to_string(), "/".to_string())));
    }

    #[test]
    fn explicit_opendal_options_win() {
        let adapter = GenericAdapter::new("s3");
        let spec = resolve(
            &adapter,
            "opendals3://my-bucket/t",
            &[
                ("opendal.bucket", "override"),
                ("opendal.endpoint", "http://x"),
            ],
        );
        assert!(
            spec.config
                .contains(&("bucket".to_string(), "override".to_string()))
        );
        assert!(
            spec.config
                .contains(&("endpoint".to_string(), "http://x".to_string()))
        );
        // bucket appears only once (no duplicate from the host)
        assert_eq!(spec.config.iter().filter(|(k, _)| k == "bucket").count(), 1);
    }
}
