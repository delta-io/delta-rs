//! Utility functions for working across Delta tables

use chrono::DateTime;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::path::Path as StdPath;
use url::Url;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

/// A handler for path operations that encapsulates the table root URL.
///
/// This struct provides path handling operations without requiring repeated
/// root parameter passing, improving code organization and API ergonomics.
#[derive(Debug, Clone)]
pub struct DeltaPathHandler {
    root: Url,
}

impl DeltaPathHandler {
    /// Create a new DeltaPathHandler with the given table root URL.
    pub fn new(root: Url) -> Self {
        Self { root }
    }

    /// Get a reference to the root URL.
    pub fn root(&self) -> &Url {
        &self.root
    }

    /// Normalize a data file path for tables using the file scheme.
    ///
    /// Rules:
    /// - If input is a file:// URI, convert to a platform filesystem path.
    /// - If input is another fully-qualified URI or an absolute filesystem path, keep as-is.
    /// - Otherwise, treat input as relative and join under the filesystem path of root.
    pub fn normalize_path_for_file_scheme(&self, input: &str) -> String {
        normalize_path_for_file_scheme(&self.root, input)
    }

    /// For file scheme tables, attempt to relativize an absolute filesystem path
    /// to the table root. If the path is under the table root, return the relative
    /// portion; otherwise return the original path unchanged.
    pub fn relativize_path_for_file_scheme(&self, input: &str) -> String {
        relativize_path_for_file_scheme(&self.root, input)
    }

    /// Convert a path to an object_store::Path for use with a root-scoped filesystem store.
    ///
    /// For file scheme tables using a root-scoped store, paths need to be absolute filesystem
    /// paths with leading slashes stripped (since object_store paths are relative to store root).
    ///
    /// Note: This function intentionally removes ALL leading slashes and backslashes from the path
    /// (e.g., `///path` becomes `path`). This is correct behavior because object_store paths are
    /// always relative to the store root, and any leading separators would be invalid.
    pub fn object_store_path_for_file_root(&self, path: &str) -> Path {
        object_store_path_for_file_root(&self.root, path)
    }

    /// Normalize a path from an Add action for use in a scan.
    ///
    /// Returns a tuple of (normalized_path, needs_bucket_root_store).
    /// - For file scheme: relativizes absolute paths to table root when possible.
    /// - For non-file schemes: strips table root prefix or relativizes to bucket root.
    /// - `needs_bucket_root_store` is true if the path required bucket-level relativization.
    pub fn normalize_add_path_for_scan(&self, path: &str) -> (String, bool) {
        normalize_add_path_for_scan(&self.root, path)
    }

    /// For non-file schemes, if `input` is a fully-qualified URI beginning with root,
    /// strip the table root prefix so the result is relative to the table root.
    pub fn strip_table_root_from_full_uri(&self, input: &str) -> String {
        strip_table_root_from_full_uri(&self.root, input)
    }

    /// For object store paths (e.g., S3 and S3-compatible HTTP endpoints), if `input`
    /// is a fully-qualified URI that points to the same bucket as root, return the
    /// object key relative to the bucket root. Otherwise, return `input` unchanged.
    ///
    /// This function is scheme-tolerant: it can match `s3://bucket/key` against
    /// `http(s)://endpoint/bucket/` (path-style) or `http(s)://bucket.endpoint/`
    /// (virtual-hosted-style) as long as the bucket names are the same.
    ///
    /// **Limitation:** This function compares bucket names only, not full endpoint identity.
    /// If root is `s3://bucket/prefix` and `input` is `http://endpoint/bucket/key`, they
    /// will be recognized as the same bucket even though they use different schemes and
    /// endpoints. This is intentional for S3-compatible endpoint scenarios, but callers
    /// should be aware that scheme differences are not validated.
    pub fn relativize_uri_to_bucket_root(&self, input: &str) -> String {
        relativize_uri_to_bucket_root(&self.root, input)
    }
}

/// Return the uri of commit version.
///
/// ```rust
/// # use deltalake_core::logstore::*;
/// use object_store::path::Path;
/// let uri = commit_uri_from_version(1);
/// assert_eq!(uri, Path::from("_delta_log/00000000000000000001.json"));
/// ```
pub fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    super::DELTA_LOG_PATH.child(version.as_str())
}

/// Returns true if the provided string is either a fully-qualified URI or
/// an absolute filesystem path for the current platform.
pub fn is_absolute_uri_or_path(s: &str) -> bool {
    if Url::parse(s).is_ok() {
        true
    } else {
        StdPath::new(s).is_absolute()
    }
}

/// Normalize a data file path for tables using the file scheme.
///
/// Rules:
/// - If input is a file:// URI, convert to a platform filesystem path.
/// - If input is another fully-qualified URI or an absolute filesystem path, keep as-is.
/// - Otherwise, treat input as relative and join under the filesystem path of `root`.
pub fn normalize_path_for_file_scheme(root: &Url, input: &str) -> String {
    match Url::parse(input) {
        Ok(url) if url.scheme() == "file" => {
            // Use to_file_path() for proper platform-specific conversion.
            // On Windows, url.path() returns "/C:/Users/..." which is incorrect;
            // to_file_path() correctly handles this by removing the leading slash.
            match url.to_file_path() {
                Ok(pathbuf) => pathbuf
                    .to_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| pathbuf.to_string_lossy().into_owned()),
                Err(_) => url.path().to_string(), // fallback for invalid file:// URLs
            }
        }
        Ok(_) => input.to_string(),
        Err(_) => {
            if StdPath::new(input).is_absolute() {
                input.to_string()
            } else {
                let root_fs_path = root
                    .to_file_path()
                    .unwrap_or_else(|_| StdPath::new(root.path()).to_path_buf());
                let rel = input.trim_start_matches(['/', '\\']);
                let full_path = root_fs_path.join(rel);
                full_path
                    .to_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| full_path.to_string_lossy().into_owned())
            }
        }
    }
}

/// For file scheme tables, attempt to relativize an absolute filesystem path
/// to the table root. If the path is under the table root, return the relative
/// portion; otherwise return the original path unchanged.
pub fn relativize_path_for_file_scheme(root: &Url, input: &str) -> String {
    let input_path = StdPath::new(input);
    if !input_path.is_absolute() {
        // Already relative, return as-is
        return input.to_string();
    }

    match root.to_file_path() {
        Ok(root_fs) => {
            if let Ok(rel) = input_path.strip_prefix(&root_fs) {
                rel.to_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| rel.to_string_lossy().into_owned())
            } else {
                // Not under table root, return as-is
                input.to_string()
            }
        }
        Err(_) => input.to_string(),
    }
}

/// Convert a path to an object_store::Path for use with a root-scoped filesystem store.
///
/// For file scheme tables using a root-scoped store, paths need to be absolute filesystem
/// paths with leading slashes stripped (since object_store paths are relative to store root).
///
/// Note: This function intentionally removes ALL leading slashes and backslashes from the path
/// (e.g., `///path` becomes `path`). This is correct behavior because object_store paths are
/// always relative to the store root, and any leading separators would be invalid.
pub fn object_store_path_for_file_root(root: &Url, path: &str) -> Path {
    let abs = match root.to_file_path() {
        Ok(root_fs) => {
            let input_fs = StdPath::new(path);
            if input_fs.is_absolute() {
                input_fs.to_path_buf()
            } else {
                root_fs.join(input_fs)
            }
        }
        Err(_) => StdPath::new(path).to_path_buf(),
    };
    // Remove all leading path separators - object_store paths are relative to store root
    let rel_from_root = abs
        .to_str()
        .map(|s| s.trim_start_matches(['/', '\\']).to_string())
        .unwrap_or_else(|| abs.to_string_lossy().trim_start_matches('/').to_string());
    Path::from(rel_from_root.as_str())
}

/// Normalize a path from an Add action for use in a scan.
///
/// Returns a tuple of (normalized_path, needs_bucket_root_store).
/// - For file scheme: relativizes absolute paths to table root when possible.
/// - For non-file schemes: strips table root prefix or relativizes to bucket root.
/// - `needs_bucket_root_store` is true if the path required bucket-level relativization.
pub fn normalize_add_path_for_scan(root: &Url, path: &str) -> (String, bool) {
    if root.scheme() == "file" {
        // For local filesystem tables, first normalize any input (including file:// URIs)
        // into a platform filesystem path, then relativize absolute paths under the table dir.
        let normalized = normalize_path_for_file_scheme(root, path);
        (relativize_path_for_file_scheme(root, &normalized), false)
    } else {
        // For non-file schemes, try to strip table root first
        match Url::parse(path) {
            Ok(_) => {
                let stripped = strip_table_root_from_full_uri(root, path);
                if stripped != path {
                    (stripped, false)
                } else {
                    // Not under table root; try bucket-level relativization
                    let bucket_rel = relativize_uri_to_bucket_root(root, path);
                    let needs_bucket_root = bucket_rel != path;
                    (bucket_rel, needs_bucket_root)
                }
            }
            Err(_) => {
                // Not a URI; keep as-is (typically already relative)
                (path.to_string(), false)
            }
        }
    }
}

/// For non-file schemes, if `input` is a fully-qualified URI beginning with `root`,
/// strip the table root prefix so the result is relative to the table root.
pub fn strip_table_root_from_full_uri(root: &Url, input: &str) -> String {
    let root_str = root.as_str().trim_end_matches('/');
    let root_with_sep = format!("{}/", root_str);
    if input.starts_with(&root_with_sep) {
        input[root_with_sep.len()..].to_string()
    } else if input == root_str {
        String::new()
    } else {
        input.to_string()
    }
}

/// For object store paths (e.g., S3 and S3-compatible HTTP endpoints), if `input`
/// is a fully-qualified URI that points to the same bucket as `root`, return the
/// object key relative to the bucket root. Otherwise, return `input` unchanged.
///
/// This function is scheme-tolerant: it can match `s3://bucket/key` against
/// `http(s)://endpoint/bucket/` (path-style) or `http(s)://bucket.endpoint/`
/// (virtual-hosted-style) as long as the bucket names are the same.
///
/// **Limitation:** This function compares bucket names only, not full endpoint identity.
/// If `root` is `s3://bucket/prefix` and `input` is `http://endpoint/bucket/key`, they
/// will be recognized as the same bucket even though they use different schemes and
/// endpoints. This is intentional for S3-compatible endpoint scenarios, but callers
/// should be aware that scheme differences are not validated.
pub fn relativize_uri_to_bucket_root(root: &Url, input: &str) -> String {
    // If it's not a URI, return as-is
    let Ok(input_url) = Url::parse(input) else {
        return input.to_string();
    };

    // Try to extract bucket names from both URLs. If either is not a recognizable
    // object-store style URL, leave input unchanged.
    let Some(root_bucket) = extract_bucket_name(root) else {
        return input.to_string();
    };
    let Some(input_bucket) = extract_bucket_name(&input_url) else {
        return input.to_string();
    };

    if root_bucket != input_bucket {
        return input.to_string();
    }

    // Same bucket: return the key (path relative to the bucket root) without leading '/'
    bucket_key_from_url(&input_url).unwrap_or_else(|| input.to_string())
}

/// Attempt to extract an object key relative to bucket root from a URL.
/// Returns `None` if the URL doesn't look like an object-store style URL.
fn bucket_key_from_url(url: &Url) -> Option<String> {
    let path = url.path().trim_start_matches('/');
    let scheme = url.scheme();

    // For s3://, gs://, abfs://, abfss://, az:// URLs, the host is the bucket/container
    // and the path is the key
    if scheme == "s3" || scheme == "gs" || scheme == "abfs" || scheme == "abfss" || scheme == "az" {
        return Some(path.to_string());
    }

    // HTTP(S) S3-compatible: support both virtual-hosted-style and path-style
    if scheme == "http" || scheme == "https" {
        let host = url.host_str().unwrap_or("");
        // virtual-hosted-style: bucket.s3.amazonaws.com or bucket.localstack
        // In this style, entire path is the key
        if looks_like_virtual_hosted_style(host) {
            return Some(path.to_string());
        }
        // path-style: http://endpoint/bucket/key...
        if let Some((_, rest)) = path.split_once('/') {
            return Some(rest.to_string());
        } else {
            // Only bucket with no key - return None to indicate there's no object key
            return None;
        }
    }

    None
}

/// Extract bucket/container name from cloud storage URLs.
/// Supports s3://, gs://, abfs://, abfss://, az://, and s3-compatible http(s) URLs.
fn extract_bucket_name(url: &Url) -> Option<String> {
    let scheme = url.scheme();

    // For s3://, gs://, abfs://, abfss://, az:// URLs, the host is the bucket/container
    if scheme == "s3" || scheme == "gs" || scheme == "abfs" || scheme == "abfss" || scheme == "az" {
        return url.host_str().map(|s| s.to_string());
    }
    if scheme == "http" || scheme == "https" {
        let host = url.host_str().unwrap_or("");
        if let Some(bucket) = bucket_from_virtual_hosted_style(host) {
            return Some(bucket.to_string());
        }
        // path-style: first segment of path is bucket
        let path = url.path().trim_start_matches('/');
        if let Some((bucket, _)) = path.split_once('/') {
            if !bucket.is_empty() {
                return Some(bucket.to_string());
            }
        } else if !path.is_empty() {
            // Only bucket without key
            return Some(path.to_string());
        }
    }
    None
}

/// Checks if a host looks like an S3 virtual-hosted-style endpoint.
///
/// **Note:** This function is S3-specific and only detects AWS S3, S3 website endpoints,
/// and common LocalStack patterns. It does NOT cover other cloud providers:
/// - GCS virtual-hosted-style URLs use `.storage.googleapis.com`
/// - Azure Blob Storage uses different URL patterns
///
/// For other providers, bucket extraction falls back to path-style parsing.
fn looks_like_virtual_hosted_style(host: &str) -> bool {
    // Check for known S3 virtual-hosted-style host suffixes.
    // This includes AWS S3, S3 website endpoints, and common localstack patterns.
    const S3_SUFFIXES: &[&str] = &[
        ".s3.amazonaws.com",
        ".s3.amazonaws.com.cn",
        ".localstack.cloud",
        ".localhost.localstack.cloud",
    ];
    // Match .s3-website-<region>.amazonaws.com and .s3.dualstack.<region>.amazonaws.com
    host.contains(".s3-website-")
        || host.contains(".s3.dualstack.")
        || S3_SUFFIXES.iter().any(|suffix| host.ends_with(suffix))
}

fn bucket_from_virtual_hosted_style(host: &str) -> Option<&str> {
    // bucket is up to first '.'
    host.split_once('.')
        .map(|(b, _)| b)
        .filter(|b| !b.is_empty())
}

impl TryFrom<Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: Add) -> DeltaResult<Self> {
        (&value).try_into()
    }
}

impl TryFrom<&Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: &Add) -> DeltaResult<Self> {
        let last_modified = DateTime::from_timestamp_millis(value.modification_time).ok_or(
            DeltaTableError::MetadataError(format!(
                "invalid modification_time: {:?}",
                value.modification_time
            )),
        )?;

        Ok(Self {
            // IMPORTANT: `ObjectMeta` is only constructed at the boundary where
            // a concrete object store is used. At that point, `value.path` must
            // already be normalized (either relative to table root for remote
            // schemes or an absolute filesystem path for file scheme).
            // The path from the Delta log is already percent-encoded per the Delta
            // protocol. Using `Path::from` instead of `Path::parse` avoids
            // double-encoding (e.g., `%20` becoming `%2520`).
            location: Path::from(value.path.as_str()),
            last_modified,
            size: value.size as u64,
            e_tag: None,
            version: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_meta_from_add_action() {
        let add = Add {
            path: "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
                .to_string(),
            size: 123,
            modification_time: 123456789,
            data_change: true,
            stats: None,
            partition_values: Default::default(),
            tags: Default::default(),
            base_row_id: None,
            default_row_commit_version: None,
            deletion_vector: None,
            clustering_provider: None,
        };

        let meta: ObjectMeta = (&add).try_into().unwrap();
        assert_eq!(
            meta.location,
            Path::from(
                "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
            )
        );
        assert_eq!(meta.size, 123);
        assert_eq!(meta.last_modified.timestamp_millis(), 123456789);
    }
}
