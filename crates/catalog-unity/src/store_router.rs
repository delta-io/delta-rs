//! Per-table credential routing for tables that share one object store URL.
//!
//! DataFusion registers an object store per `scheme://container@account`, and delta-rs
//! never replaces a store that is already registered. Unity Catalog vends credentials
//! scoped to a single table directory, so the first table's credentials would otherwise
//! be used for every other table in the same container.

use std::fmt;
use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use datafusion::execution::runtime_env::RuntimeEnv;
use deltalake_core::delta_datafusion::engine::AsObjectStoreUrl;
use futures::stream::{self, BoxStream};
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt as _, ObjectStoreScheme, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result,
};
use url::Url;

type Routes = Vec<(Path, Arc<dyn ObjectStore>)>;

/// An [`ObjectStore`] that sends each request to the store registered for the longest matching path prefix.
#[derive(Default)]
pub struct PrefixRoutingStore {
    routes: RwLock<Routes>,
}

impl fmt::Debug for PrefixRoutingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefixes: Vec<String> = self
            .routes
            .read()
            .unwrap()
            .iter()
            .map(|(p, _)| p.to_string())
            .collect();
        f.debug_struct("PrefixRoutingStore")
            .field("prefixes", &prefixes)
            .finish()
    }
}

impl fmt::Display for PrefixRoutingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PrefixRoutingStore")
    }
}

impl PrefixRoutingStore {
    /// Route everything under `prefix` to `store`, replacing any store already routed there.
    pub fn set(&self, prefix: Path, store: Arc<dyn ObjectStore>) {
        let mut routes = self.routes.write().unwrap();
        match routes.iter_mut().find(|(p, _)| *p == prefix) {
            Some(route) => route.1 = store,
            None => routes.push((prefix, store)),
        }
    }

    fn snapshot(&self) -> Routes {
        self.routes.read().unwrap().clone()
    }

    fn route(&self, location: &Path) -> Result<Arc<dyn ObjectStore>> {
        route_in(&self.snapshot(), location)
    }

    fn route_prefix(&self, prefix: Option<&Path>) -> Result<Arc<dyn ObjectStore>> {
        match prefix {
            Some(prefix) => self.route(prefix),
            None => Err(unrouted("<root>")),
        }
    }
}

fn unrouted(location: impl fmt::Display) -> Error {
    Error::Generic {
        store: "PrefixRoutingStore",
        source: format!("no store registered for path '{location}'").into(),
    }
}

fn route_in(routes: &Routes, location: &Path) -> Result<Arc<dyn ObjectStore>> {
    routes
        .iter()
        .filter(|(prefix, _)| location.prefix_matches(prefix) || location == prefix)
        .max_by_key(|(prefix, _)| prefix.parts().count())
        .map(|(_, store)| Arc::clone(store))
        .ok_or_else(|| unrouted(location))
}

#[async_trait::async_trait]
impl ObjectStore for PrefixRoutingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.route(location)?.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.route(location)?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.route(location)?.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let routes = self.snapshot();
        locations
            .and_then(move |location| {
                let store = route_in(&routes, &location);
                async move {
                    store?.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let routes = self.snapshot();
        // A prefix inside one table goes to that table's store; a wider prefix fans out to every table below it.
        if let Some(prefix) = prefix {
            if let Ok(store) = route_in(&routes, prefix) {
                return store.list(Some(prefix));
            }
        }
        let prefix = prefix.cloned();
        stream::iter(routes)
            .filter(move |(route_prefix, _)| {
                let keep = prefix
                    .as_ref()
                    .is_none_or(|p| route_prefix.prefix_matches(p));
                async move { keep }
            })
            .flat_map(|(route_prefix, store)| store.list(Some(&route_prefix)))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.route_prefix(prefix)?.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.route(from)?.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.route(from)?.rename_opts(from, to, options).await
    }
}

/// Registers a [`PrefixRoutingStore`] per object store URL in a DataFusion runtime and keeps each table's store routed.
#[derive(Debug)]
pub struct UnityStoreRegistry {
    runtime: Arc<RuntimeEnv>,
    routers: DashMap<String, Arc<PrefixRoutingStore>>,
}

impl UnityStoreRegistry {
    /// Create a registry that installs routers into `runtime`.
    pub fn new(runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            runtime,
            routers: DashMap::new(),
        }
    }

    /// Route the table rooted at `table_root` to `store`, replacing credentials from an earlier load.
    pub fn register(&self, table_root: &Url, store: Arc<dyn ObjectStore>) {
        let store_url = table_root.as_object_store_url();
        let router = Arc::clone(
            &self
                .routers
                .entry(store_url.as_str().to_string())
                .or_insert_with(|| {
                    let router = Arc::new(PrefixRoutingStore::default());
                    self.runtime
                        .register_object_store(store_url.as_ref(), router.clone());
                    router
                }),
        );
        let prefix = ObjectStoreScheme::parse(table_root)
            .map(|(_, path)| path)
            .unwrap_or_else(|_| Path::from(table_root.path()));
        router.set(prefix, store);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn routes_by_longest_prefix_and_replaces_routes() {
        let (t1, t2) = (Arc::new(InMemory::new()), Arc::new(InMemory::new()));
        let router = PrefixRoutingStore::default();
        router.set(Path::from("schemas/t1"), t1.clone());
        router.set(Path::from("schemas/t2"), t2.clone());

        let p1 = Path::from("schemas/t1/part-0.parquet");
        let p2 = Path::from("schemas/t2/sP/part-0.parquet");
        router.put(&p1, "one".into()).await.unwrap();
        router.put(&p2, "two".into()).await.unwrap();

        assert!(t1.head(&p1).await.is_ok() && t1.head(&p2).await.is_err());
        assert!(t2.head(&p2).await.is_ok() && t2.head(&p1).await.is_err());
        assert!(router.head(&Path::from("other/x")).await.is_err());

        let fresh = Arc::new(InMemory::new());
        fresh.put(&p1, "new".into()).await.unwrap();
        router.set(Path::from("schemas/t1"), fresh);
        let body = router.get(&p1).await.unwrap().bytes().await.unwrap();
        assert_eq!(body.as_ref(), b"new");
    }
}
