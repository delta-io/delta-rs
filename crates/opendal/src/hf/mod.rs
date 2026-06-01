//! HuggingFace Hub specialization of the generic OpenDAL backend.

mod config;
mod url;

use std::ops::Range;
use std::sync::Arc;

use ::url::Url as ParsedUrl;
use bytes::Bytes;
use deltalake_core::DeltaResult;
use deltalake_core::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use deltalake_core::logstore::{ObjectStoreRef, StorageConfig};
use futures::stream::BoxStream;
use object_store::path::Path;

use crate::adapter::{OpendalAdapter, OperatorSpec, spec_value};
use crate::shim::ConditionalPutShim;

use self::config::HfStorageConfig;
use self::url::parse_hf_url;

/// Adapter for the HuggingFace Hub OpenDAL service.
///
/// HF is special in two ways the generic path can't express:
/// 1. The operator is scoped to `<owner>/<repo>`, deeper than the bucket root,
///    so the in-repo table path (not `url.path()`) is the log-store prefix and
///    every operation must have the repo prefix stripped.
/// 2. The HF backend lacks `write_with_if_not_exists`, so conditional creates
///    are emulated via [`ConditionalPutShim`].
#[derive(Clone, Debug, Default)]
pub struct HfAdapter;

impl OpendalAdapter for HfAdapter {
    fn resolve(&self, url: &ParsedUrl, config: &StorageConfig) -> DeltaResult<OperatorSpec> {
        let hf = HfStorageConfig::from(&config.raw);
        let (repo_type, repo_id, revision, table_path) = parse_hf_url(url, hf.revision.as_deref())?;

        let mut cfg = vec![
            ("repo_type".to_string(), repo_type),
            ("repo_id".to_string(), repo_id),
            ("revision".to_string(), revision),
            ("root".to_string(), "/".to_string()),
        ];
        if let Some(token) = hf.token {
            cfg.push(("token".to_string(), token));
        }
        if let Some(endpoint) = hf.endpoint {
            cfg.push(("endpoint".to_string(), endpoint));
        }

        Ok(OperatorSpec {
            scheme: "hf".to_string(),
            config: cfg,
            table_prefix: Path::from(table_path.trim_end_matches('/')),
        })
    }

    fn wrap_store(&self, store: ObjectStoreRef, spec: &OperatorSpec) -> ObjectStoreRef {
        let strip_prefix = spec_value(spec, "repo_id").unwrap_or_default().to_string();
        let stripped: ObjectStoreRef = Arc::new(HfPrefixStore {
            inner: store,
            strip_prefix,
        });
        // The shim emulates conditional creates and delegates (unchanged paths)
        // to the prefix-stripping store underneath.
        Arc::new(ConditionalPutShim::new(stripped))
    }
}

/// Strips the `<owner>/<repo>` prefix from paths before delegating, because the
/// HF operator is already scoped to the repo while delta computes paths from the
/// URL host onward.
struct HfPrefixStore {
    inner: Arc<dyn ObjectStore>,
    strip_prefix: String,
}

impl HfPrefixStore {
    /// Strip the repo prefix from `p` if present. Returns the original path
    /// unchanged if it doesn't start with the prefix on a directory boundary
    /// (e.g. when callers pass already-relative paths from `PrefixStore`, or a
    /// path that merely shares a string prefix with the repo name).
    fn rewrite(&self, p: &Path) -> Path {
        match p.as_ref().strip_prefix(&self.strip_prefix) {
            Some("") => Path::from(""),
            Some(rest) => match rest.strip_prefix('/') {
                Some(rest) => Path::from(rest),
                None => p.clone(),
            },
            None => p.clone(),
        }
    }
}

impl std::fmt::Debug for HfPrefixStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfPrefixStore").finish()
    }
}

impl std::fmt::Display for HfPrefixStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HfPrefixStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HfPrefixStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.inner
            .put_opts(&self.rewrite(location), bytes, options)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(&self.rewrite(location), options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.inner.get_ranges(&self.rewrite(location), ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let rewritten = prefix.map(|p| self.rewrite(p));
        self.inner.list(rewritten.as_ref())
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let rewritten_prefix = prefix.map(|p| self.rewrite(p));
        let rewritten_offset = self.rewrite(offset);
        self.inner
            .list_with_offset(rewritten_prefix.as_ref(), &rewritten_offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let rewritten = prefix.map(|p| self.rewrite(p));
        self.inner.list_with_delimiter(rewritten.as_ref()).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner
            .copy_opts(&self.rewrite(from), &self.rewrite(to), options)
            .await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.inner
            .rename_opts(&self.rewrite(from), &self.rewrite(to), options)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner
            .put_multipart_opts(&self.rewrite(location), options)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewriter(prefix: &str) -> HfPrefixStore {
        HfPrefixStore {
            inner: Arc::new(object_store::memory::InMemory::new()),
            strip_prefix: prefix.to_string(),
        }
    }

    #[test]
    fn rewrite_strips_repo_prefix() {
        let store = rewriter("owner/repo");
        let out = store.rewrite(&Path::from("owner/repo/table/_delta_log/file.json"));
        assert_eq!(out.as_ref(), "table/_delta_log/file.json");
    }

    #[test]
    fn rewrite_strips_exact_match() {
        let store = rewriter("owner/repo");
        assert_eq!(store.rewrite(&Path::from("owner/repo")).as_ref(), "");
    }

    #[test]
    fn rewrite_leaves_unrelated_path_alone() {
        let store = rewriter("owner/repo");
        // PrefixStore strips its own prefix before delegating, so paths arrive
        // already-relative — must pass through unchanged.
        let out = store.rewrite(&Path::from("table/_delta_log/file.json"));
        assert_eq!(out.as_ref(), "table/_delta_log/file.json");
    }

    #[test]
    fn rewrite_rejects_substring_prefix() {
        // "owner/repo-suffix" must not match "owner/repo" — the prefix has to
        // end on a directory boundary.
        let store = rewriter("owner/repo");
        let out = store.rewrite(&Path::from("owner/repo-suffix/file.json"));
        assert_eq!(out.as_ref(), "owner/repo-suffix/file.json");
    }
}
