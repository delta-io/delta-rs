//! Generic object-store and log-store factories backed by OpenDAL.

use std::sync::Arc;

use deltalake_core::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
};
use deltalake_core::{DeltaResult, DeltaTableError};
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use url::Url;

use crate::adapter::OpendalAdapter;
use crate::shim::ConditionalPutShim;
use crate::sorted::SortedListStore;

/// [`ObjectStoreFactory`] that builds an OpenDAL operator via an [`OpendalAdapter`].
#[derive(Debug)]
pub struct OpendalObjectStoreFactory<A: OpendalAdapter>(pub Arc<A>);

impl<A: OpendalAdapter + 'static> ObjectStoreFactory for OpendalObjectStoreFactory<A> {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let spec = self.0.resolve(url, config)?;
        // Ensure feature-enabled OpenDAL services are registered. The crate's
        // `#[ctor]` initializer can be dropped by the linker when `opendal` is
        // pulled in as an rlib, so we initialize explicitly (idempotent).
        opendal::init_default_registry();
        let operator = Operator::via_iter(&spec.scheme, spec.config.clone())
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        // Whether the service can perform an atomic create-if-absent, which
        // delta needs to commit `_delta_log/N.json`. Services that can't get
        // the conditional-put shim applied below.
        let needs_conditional_put_shim =
            !operator.info().full_capability().write_with_if_not_exists;
        // Restore the lexicographic listing order that `object_store` (and
        // delta-kernel) require but `OpendalStore` does not guarantee.
        let sorted: ObjectStoreRef =
            Arc::new(SortedListStore::new(Arc::new(OpendalStore::new(operator))));
        let store = self.0.wrap_store(sorted, &spec);
        // Emulate conditional creates for services that lack native support
        // (e.g. HuggingFace). Outermost so its HEAD/PUT see adapter-rewritten
        // paths.
        let store = if needs_conditional_put_shim {
            Arc::new(ConditionalPutShim::new(store)) as ObjectStoreRef
        } else {
            store
        };
        Ok((store, spec.table_prefix))
    }
}

/// [`LogStoreFactory`] that pairs with [`OpendalObjectStoreFactory`].
///
/// The PrefixStore is always recomputed from the adapter's
/// [`OpendalAdapter::logstore_prefix`] rather than trusting the `prefixed_store`
/// passed in by delta's `decorate_store` (which derives its prefix from
/// `url.path()`). For bucket-root services the two agree; for HF, whose operator
/// is scoped deeper than the bucket root, the adapter is authoritative.
#[derive(Debug)]
pub struct OpendalLogStoreFactory<A: OpendalAdapter>(pub Arc<A>);

impl<A: OpendalAdapter + 'static> LogStoreFactory for OpendalLogStoreFactory<A> {
    fn with_options(
        &self,
        _prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let spec = self.0.resolve(location, options)?;
        let prefix = self.0.logstore_prefix(&spec);

        let prefixed_store: ObjectStoreRef = if prefix.as_ref().is_empty() {
            root_store.clone()
        } else {
            Arc::new(PrefixStore::new(root_store.clone(), prefix))
        };

        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}
