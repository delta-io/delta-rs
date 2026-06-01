//! Parsing of `hf://` URLs into HuggingFace repository components.

use deltalake_core::{DeltaResult, DeltaTableError};
use url::Url;

/// Parse a `hf://` URL into its HuggingFace components.
///
/// URL format: `hf://<repo_type>/<owner>/<repo>[@<revision>]/<path>`
/// or for single-name repos: `hf://<repo_type>/<repo>@<revision>/<path>`
///
/// Returns `(repo_type, repo_id, revision, table_path)`.
pub(crate) fn parse_hf_url(
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
}
