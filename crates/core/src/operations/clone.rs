//! Clone a Delta table from a source location into a target location by
//! creating a new table at the target and registering the source table's files.
use futures::TryStreamExt;
use std::path::Path;
use url::Url;

use crate::kernel::transaction::CommitBuilder;
use crate::kernel::{resolve_snapshot, Action, EagerSnapshot};
use crate::logstore::LogStoreRef;
use crate::operations::create::CreateBuilder;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Builder for performing a shallow clone of a Delta table.
///
/// Construct via `DeltaOps::clone_table()` and configure using the builder methods.
pub struct CloneBuilder {
    /// Source table log store
    log_store: LogStoreRef,
    /// Optional snapshot representing the active state/version of the source table
    snapshot: Option<EagerSnapshot>,
    /// Target table location
    target: Option<Url>,
}

impl CloneBuilder {
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            log_store,
            snapshot,
            target: None,
        }
    }

    /// Set the target table location (must be a `file://` URL).
    pub fn with_target(mut self, target: Url) -> Self {
        self.target = Some(target);
        self
    }
}

impl std::future::IntoFuture for CloneBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let target = self
                .target
                .ok_or_else(|| DeltaTableError::Generic("CloneBuilder missing target".into()))?;
            shallow_clone(self.log_store, self.snapshot, target).await
        })
    }
}

/// Shallow clone a Delta table from a source location to a target location.
/// This function creates a new Delta table at the target location that
/// references the same data files as the source table,
/// without copying the actual data files.
///
/// # Arguments
/// * `target` - The URL where the cloned Delta table will be created. Must be a `file://` URL.
///
/// The source table is the active table referenced by the `DeltaOps` that constructed this builder.
///
/// # Returns
/// Returns a [`DeltaResult<DeltaTable>`]. On success, contains the cloned [`DeltaTable`] instance.
/// On error, returns a [`DeltaTableError`] describing the failure.
/// In the event of an error, the target directory may contain partial data and files.
///
/// # Errors
/// This function returns an error if:
/// - `target` URL is not a `file://` URL.
/// - The source table cannot be loaded.
/// - The target table cannot be created.
/// - File path conversion fails.
/// - Symlink creation fails.
///
/// # Example
/// ```
/// use url::Url;
/// use deltalake_core::DeltaOps;
/// # async fn shallow_clone_example() -> Result<(), deltalake_core::DeltaTableError> {
/// let source = Url::parse("file:///path/to/source_table")?;
/// let target = Url::parse("file:///path/to/target_table")?;
/// let ops = DeltaOps::try_from_uri(source).await?;
/// let table = ops
///     .clone_table()
///     .with_target(target)
///     .await?;
/// # Ok(())
/// # }
/// ```

async fn shallow_clone(
    log_store: LogStoreRef,
    snapshot: Option<EagerSnapshot>,
    target: Url,
) -> DeltaResult<DeltaTable> {
    // Validate that source and target are both filesystem Urls. If not, return an error.
    // We need this because we use symlinks to create the target files.
    // We hope to replace this once delta-rs supports absolute paths.
    let src_url = log_store.config().location.clone();
    if src_url.scheme() != "file" || target.scheme() != "file" {
        return Err(DeltaTableError::InvalidTableLocation(format!(
            "shallow_clone() requires file:// URLs for both source and target; got source='{}' (scheme='{}'), target='{}' (scheme='{}')",
            src_url,
            src_url.scheme(),
            target,
            target.scheme()
        )));
    }

    // 1) Resolve source snapshot from provided state or active version in DeltaOps
    let src_snapshot: EagerSnapshot = resolve_snapshot(log_store.as_ref(), snapshot, true).await?;
    let src_metadata = src_snapshot.metadata().clone();
    let src_schema = src_metadata.parse_schema()?;
    let partition_columns = src_metadata.partition_columns().to_vec();
    let configuration = src_metadata.configuration().clone();
    let src_protocol = src_snapshot.protocol().clone();
    let src_log = log_store;

    // 2) Create a target table mirroring source metadata and protocol
    let mut create = CreateBuilder::new()
        .with_location(target.as_ref().to_string())
        .with_columns(src_schema.fields().cloned())
        .with_partition_columns(partition_columns.clone())
        .with_configuration(
            configuration
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone()))),
        );

    if let Some(n) = src_metadata.name() {
        create = create.with_table_name(n.to_string());
    }
    if let Some(d) = src_metadata.description() {
        create = create.with_comment(d.to_string());
    }

    let mut target_table = create
        .with_actions([Action::Protocol(src_protocol.clone())])
        .await?;

    // 3) Gather source files from src_snapshot
    let file_views: Vec<_> = src_snapshot
        .file_views(&src_log, None)
        .try_collect()
        .await?;

    let mut actions = Vec::with_capacity(file_views.len() + 1);
    actions.push(Action::Metadata(src_metadata));

    // 4) Add files to the target table
    let target_root_path = target_table
        .log_store()
        .config()
        .location
        .to_file_path()
        .map_err(|_| {
            DeltaTableError::InvalidTableLocation(format!(
                "Failed to convert target URL to file path: {}",
                target.as_ref()
            ))
        })?;

    let source_root_path = src_log.config().location.to_file_path().map_err(|_| {
        DeltaTableError::InvalidTableLocation(format!(
            "Failed to convert source URL to file path: {}",
            src_url.as_ref()
        ))
    })?;

    for view in file_views {
        let mut add = view.add_action();
        add.data_change = true;
        // Absolute paths are not supported for now, create symlinks instead.
        add_symlink(
            source_root_path.as_path(),
            target_root_path.as_path(),
            &add.path,
        )?;
        actions.push(Action::Add(add));
    }

    // 5) Commit ADD operations
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: if partition_columns.is_empty() {
            None
        } else {
            Some(partition_columns)
        },
        predicate: None,
    };

    let target_snapshot = target_table.snapshot()?.snapshot().clone();
    let prepared_commit = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(&target_snapshot), target_table.log_store(), operation)
        .into_prepared_commit_future()
        .await?;

    let log_store = target_table.log_store();
    let commit_version = target_snapshot.version() + 1;
    let commit_bytes = prepared_commit.commit_or_bytes();
    let operation_id = uuid::Uuid::new_v4();

    log_store
        .write_commit_entry(commit_version, commit_bytes.clone(), operation_id)
        .await?;

    target_table.update().await?;
    Ok(target_table)
}

fn add_symlink(source_root: &Path, target_root: &Path, add_filename: &str) -> std::io::Result<()> {
    let file_name = Path::new(add_filename);
    let src_path = source_root.join(file_name);
    let link_path = target_root.join(file_name);

    // Create parent directories if needed
    if let Some(parent) = link_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Best-effort symlink creation: only when both source and target are local filesystems.
    #[cfg(target_family = "windows")]
    {
        use std::os::windows::fs::symlink_file;
        symlink_file(&src_path, &link_path)?;
    }
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::fs::symlink;
        symlink(&src_path, &link_path)?;
    }
    Ok(())
}

#[cfg(all(test, feature = "datafusion"))]
mod tests {
    use super::*;
    use crate::operations::collect_sendable_stream;
    use crate::DeltaOps;
    use crate::DeltaTableBuilder;
    use arrow::array::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::common::test_util::format_batches;
    use tracing::debug;

    #[tokio::test]
    async fn test_non_file_url_rejected() {
        let target = Url::parse("file:///tmp/target").unwrap();
        // Using an in-memory ops means the source is memory:// which is not file:// and should be rejected
        let ops = DeltaOps::new_in_memory();
        let result = ops.clone_table().with_target(target).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            DeltaTableError::InvalidTableLocation(_)
        ));
    }
    #[tokio::test]
    async fn test_simple_table_clone_with_version() -> DeltaResult<()> {
        let source_path = Path::new("../test/tests/data/simple_table");
        let version = Some(2);
        test_shallow_clone(source_path, version).await
    }

    #[tokio::test]
    async fn test_simple_table_clone_no_version() -> DeltaResult<()> {
        let source_path = Path::new("../test/tests/data/simple_table");
        let version = None;
        test_shallow_clone(source_path, version).await
    }

    #[tokio::test]
    async fn test_table_with_simple_partition() -> DeltaResult<()> {
        // This partition test ensures that directories are created as needed by symlink creation.
        let source_path = Path::new("../test/tests/data/delta-0.8.0-partitioned");
        let version = None;
        test_shallow_clone(source_path, version).await
    }

    // For now, deletion vectors are not supported in shallow clones.
    // This gives the error
    // Error: Transaction { source: UnsupportedReaderFeatures([DeletionVectors]) }
    #[ignore]
    #[tokio::test]
    async fn test_deletion_vector() -> DeltaResult<()> {
        let source_path = Path::new("../test/tests/data/table-with-dv-small");
        let version = None;
        test_shallow_clone(source_path, version).await
    }

    async fn test_shallow_clone(source_path: &Path, maybe_version: Option<i64>) -> DeltaResult<()> {
        let source_uri = Url::from_directory_path(std::fs::canonicalize(source_path)?).unwrap();
        let clone_path = tempfile::TempDir::new()?.path().to_owned();
        let clone_uri = Url::from_directory_path(clone_path).unwrap();

        let mut ops = DeltaOps::try_from_uri(source_uri.clone()).await?;
        if let Some(v) = maybe_version {
            ops.0.load_version(v).await?;
        }
        let cloned_table = ops.clone_table().with_target(clone_uri.clone()).await?;

        let mut source_table = DeltaTableBuilder::from_uri(source_uri.clone())?
            .load()
            .await?;
        if let Some(version) = maybe_version {
            source_table.load_version(version).await?;
        }

        let src_uris: Vec<_> = source_table.get_file_uris()?.collect();
        let cloned_uris: Vec<_> = cloned_table.get_file_uris()?.collect();

        let mut src_files: Vec<String> = src_uris
            .into_iter()
            .filter_map(|url| Some(String::from(Path::new(&url).file_name()?.to_str()?)))
            .collect();
        let mut cloned_files: Vec<String> = cloned_uris
            .into_iter()
            .filter_map(|url| Some(String::from(Path::new(&url).file_name()?.to_str()?)))
            .collect();

        src_files.sort();
        cloned_files.sort();
        debug!("Source files: {:#?}", src_files);
        debug!("Cloned files: {:#?}", cloned_files);
        assert_eq!(
            src_files, cloned_files,
            "Cloned table should reference the same files as the source"
        );

        let cloned_ops = DeltaOps::try_from_uri(clone_uri).await?;
        let (_table, stream) = cloned_ops.load().await?;
        let cloned_data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

        let pretty_cloned_data = format_batches(&*cloned_data)?.to_string();
        debug!("");
        debug!("Cloned data:");
        debug!("{pretty_cloned_data}");

        let mut src_ops = DeltaOps::try_from_uri(source_uri).await?;
        if let Some(version) = maybe_version {
            src_ops.0.load_version(version).await?;
        }
        let (_table, stream) = src_ops.load().await?;
        let source_data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

        let expected_lines = format_batches(&*source_data)?.to_string();
        let expected_lines_vec: Vec<&str> = expected_lines.trim().lines().collect();

        assert_batches_sorted_eq!(&expected_lines_vec, &cloned_data);

        debug!("");
        debug!("Source data:");
        debug!("{expected_lines}");
        Ok(())
    }
}
