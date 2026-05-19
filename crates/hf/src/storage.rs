use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use deltalake_core::DeltaResult;
use deltalake_core::DeltaTableError;
use deltalake_core::logstore::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result as ObjectStoreResult,
};
use deltalake_core::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
};
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use opendal::services::Hf;
use url::Url;

use crate::config::HfStorageConfig;

#[derive(Clone, Debug, Default)]
pub struct HfObjectStoreFactory;

impl ObjectStoreFactory for HfObjectStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let hf_config = HfStorageConfig::from(&config.raw);
        let (repo_type, repo_id, revision, table_path) =
            parse_hf_url(url, hf_config.revision.as_deref())?;

        let mut builder = Hf::default()
            .repo_type(&repo_type)
            .repo_id(&repo_id)
            .revision(&revision)
            .root("/");

        if let Some(token) = &hf_config.token {
            builder = builder.token(token);
        }
        if let Some(endpoint) = &hf_config.endpoint {
            builder = builder.endpoint(endpoint);
        }

        let operator = Operator::new(builder)
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?
            .finish();

        let inner: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(operator));
        let store = Arc::new(HfObjectStore {
            inner,
            strip_prefix: repo_id.clone(),
        }) as ObjectStoreRef;
        Ok((store, Path::from(table_path.trim_end_matches('/'))))
    }
}

/// Wraps `OpendalStore` to make Delta-on-HF possible.
///
/// Two adaptations:
/// 1. The OpenDAL HF backend doesn't support `PutMode::Create` (conditional
///    `if_not_exists` writes), which Delta uses for log commits. HF Hub is a
///    Git-based, fundamentally single-writer store; we approximate the contract
///    with a HEAD-then-PUT — racy across concurrent writers, but correct for the
///    single-writer scenario HF supports.
/// 2. Delta's kernel computes paths from the URL host onward (e.g. for
///    `hf://datasets/owner/repo/path/`, the path is `owner/repo/path/_delta_log`),
///    but the OpenDAL HF operator is already scoped to `owner/repo`. We strip the
///    repo prefix before delegating so the operator sees just `path/_delta_log`.
struct HfObjectStore {
    inner: Arc<dyn ObjectStore>,
    strip_prefix: String,
}

impl HfObjectStore {
    /// Strip the repo prefix from `p` if present. Returns the original path
    /// unchanged if it doesn't start with the prefix on a directory boundary
    /// (e.g. when callers pass already-relative paths from `PrefixStore`, or
    /// a path that merely shares a string prefix with the repo name).
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

impl std::fmt::Debug for HfObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfObjectStore").finish()
    }
}

impl std::fmt::Display for HfObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HfObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for HfObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let location = self.rewrite(location);
        if matches!(options.mode, PutMode::Create) {
            match self.inner.head(&location).await {
                Ok(_) => {
                    return Err(object_store::Error::AlreadyExists {
                        path: location.to_string(),
                        source: "object already exists on HuggingFace Hub".into(),
                    });
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e),
            }
            let mut opts = options;
            opts.mode = PutMode::Overwrite;
            return self.inner.put_opts(&location, bytes, opts).await;
        }
        self.inner.put_opts(&location, bytes, options).await
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

#[derive(Clone, Debug, Default)]
pub struct HfLogStoreFactory;

impl LogStoreFactory for HfLogStoreFactory {
    fn with_options(
        &self,
        // Ignore: built with wrong path prefix by the generic decorate_store logic,
        // which uses url.path() instead of the HF-specific table path within the repo.
        _prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let hf_config = HfStorageConfig::from(&options.raw);
        let (_, _, _, table_path) = parse_hf_url(location, hf_config.revision.as_deref())?;

        let prefixed_store: ObjectStoreRef = if table_path.is_empty() {
            root_store.clone()
        } else {
            Arc::new(PrefixStore::new(
                root_store.clone(),
                table_path.trim_end_matches('/'),
            ))
        };

        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Parse a `hf://` URL into its HuggingFace components.
///
/// URL format: `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path>`
/// or for single-name repos: `hf://<repo_type>/<repo>@<revision>/<path>`
///
/// Returns `(repo_type, repo_id, revision, table_path)`.
fn parse_hf_url(
    url: &Url,
    fallback_revision: Option<&str>,
) -> DeltaResult<(String, String, String, String)> {
    let repo_type = url
        .host_str()
        .ok_or_else(|| {
            DeltaTableError::Generic(
                "HF URL is missing the repo type in the host position (e.g. hf://datasets/…)"
                    .into(),
            )
        })?
        .to_string();

    let raw_path = url.path().trim_start_matches('/');
    let segments: Vec<&str> = raw_path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return Err(DeltaTableError::Generic(
            "HF URL must include at least a repository path (e.g. hf://datasets/org/repo/)".into(),
        ));
    }

    let (repo_id, revision, table_start) = if segments[0].contains('@') {
        // Single-component repo with explicit revision: hf://models/gpt2@main/path
        let (name, rev) = segments[0].split_once('@').unwrap();
        (name.to_string(), Some(rev.to_string()), 1)
    } else if segments.len() >= 2 {
        if let Some((repo_name, rev)) = segments[1].split_once('@') {
            // Two-component repo with revision: hf://datasets/org/repo@main/path
            (
                format!("{}/{}", segments[0], repo_name),
                Some(rev.to_string()),
                2,
            )
        } else {
            // Two-component repo, revision from options: hf://datasets/org/repo/path
            (format!("{}/{}", segments[0], segments[1]), None, 2)
        }
    } else {
        // Single-component repo, no revision in URL: hf://models/gpt2
        (segments[0].to_string(), None, 1)
    };

    let revision = revision
        .or_else(|| fallback_revision.map(|s| s.to_string()))
        .unwrap_or_else(|| "main".to_string());

    let table_path = segments[table_start..].join("/");

    Ok((repo_type, repo_id, revision, table_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(url_str: &str) -> (String, String, String, String) {
        let url = Url::parse(url_str).unwrap();
        parse_hf_url(&url, None).unwrap()
    }

    #[test]
    fn test_two_component_repo_with_path() {
        let (rt, id, rev, path) = parse("hf://datasets/my-org/my-dataset/delta/");
        assert_eq!(rt, "datasets");
        assert_eq!(id, "my-org/my-dataset");
        assert_eq!(rev, "main");
        assert_eq!(path, "delta");
    }

    #[test]
    fn test_two_component_repo_with_revision_and_path() {
        let (rt, id, rev, path) = parse("hf://datasets/my-org/my-dataset@dev/delta/");
        assert_eq!(rt, "datasets");
        assert_eq!(id, "my-org/my-dataset");
        assert_eq!(rev, "dev");
        assert_eq!(path, "delta");
    }

    #[test]
    fn test_bucket_repo() {
        let (rt, id, rev, path) = parse("hf://buckets/my-org/my-bucket/delta/");
        assert_eq!(rt, "buckets");
        assert_eq!(id, "my-org/my-bucket");
        assert_eq!(rev, "main");
        assert_eq!(path, "delta");
    }

    #[test]
    fn test_single_component_repo_with_revision() {
        let (rt, id, rev, path) = parse("hf://models/gpt2@main/checkpoints/");
        assert_eq!(rt, "models");
        assert_eq!(id, "gpt2");
        assert_eq!(rev, "main");
        assert_eq!(path, "checkpoints");
    }

    #[test]
    fn test_repo_at_root() {
        let (rt, id, rev, path) = parse("hf://datasets/my-org/my-dataset/");
        assert_eq!(rt, "datasets");
        assert_eq!(id, "my-org/my-dataset");
        assert_eq!(rev, "main");
        assert_eq!(path, "");
    }

    #[test]
    fn test_fallback_revision() {
        let url = Url::parse("hf://datasets/my-org/my-dataset/delta/").unwrap();
        let (_, _, rev, _) = parse_hf_url(&url, Some("v2")).unwrap();
        assert_eq!(rev, "v2");
    }

    #[test]
    fn test_revision_in_url_takes_priority() {
        let url = Url::parse("hf://datasets/my-org/my-dataset@prod/delta/").unwrap();
        let (_, _, rev, _) = parse_hf_url(&url, Some("fallback")).unwrap();
        assert_eq!(rev, "prod");
    }

    fn rewriter(prefix: &str) -> HfObjectStore {
        HfObjectStore {
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
