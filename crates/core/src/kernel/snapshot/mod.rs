//! Delta table snapshots
//!
//! A snapshot represents the state of a Delta Table at a given version.
//!
//! There are two types of snapshots:
//!
//! - [`Snapshot`] is a snapshot where most data is loaded on demand and only the
//!   bare minimum - [`Protocol`] and [`Metadata`] - is cached in memory.
//! - [`EagerSnapshot`] is a snapshot where much more log data is eagerly loaded into memory.
//!
//! The submodules provide structures and methods that aid in generating
//! and consuming snapshots.
//!
//! ## Reading the log
//!
//!

use std::sync::{Arc, LazyLock};

use arrow::array::RecordBatch;
use arrow::compute::{filter_record_batch, is_not_null};
use arrow::datatypes::SchemaRef;
use delta_kernel::actions::{Remove, Sidecar};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::derive_macro_utils::ToDataType;
use delta_kernel::schema::{SchemaRef as KernelSchemaRef, StructField, ToSchema};
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::{
    Engine, EvaluationHandler, Expression, ExpressionEvaluator, PredicateRef, Version,
};
use futures::future::ready;
use futures::stream::{once, BoxStream};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use serde_json::Deserializer;
use url::Url;

use super::{Action, CommitInfo, Metadata, Protocol};
use crate::kernel::arrow::engine_ext::{kernel_to_arrow, ExpressionEvaluatorExt};
use crate::kernel::{spawn_blocking_with_span, StructType, ARROW_HANDLER};
use crate::logstore::{LogStore, LogStoreExt};
use crate::{to_kernel_predicate, DeltaResult, DeltaTableConfig, DeltaTableError, PartitionFilter};

pub use self::log_data::*;
pub use iterators::*;
pub use scan::*;
pub use stream::*;

mod iterators;
mod log_data;
mod scan;
mod serde;
mod stream;

pub(crate) static SCAN_ROW_ARROW_SCHEMA: LazyLock<arrow_schema::SchemaRef> =
    LazyLock::new(|| Arc::new(scan_row_schema().as_ref().try_into_arrow().unwrap()));

/// A snapshot of a Delta table
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    /// Log segment containing all log files in the snapshot
    pub(crate) inner: Arc<KernelSnapshot>,
    /// Configuration for the current session
    config: DeltaTableConfig,
    /// Logical table schema
    schema: SchemaRef,
}

impl Snapshot {
    pub async fn try_new_with_engine(
        engine: Arc<dyn Engine>,
        table_root: Url,
        config: DeltaTableConfig,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let snapshot = match spawn_blocking_with_span(move || {
            let mut builder = KernelSnapshot::builder_for(table_root);
            if let Some(version) = version {
                builder = builder.at_version(version);
            }
            builder.build(engine.as_ref())
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))?
        {
            Ok(snapshot) => snapshot,
            Err(e) => {
                // TODO: we should have more handling-friendly errors upstream in kernel.
                if e.to_string().contains("No files in log segment") {
                    return Err(DeltaTableError::NotATable(e.to_string()));
                } else {
                    return Err(e.into());
                }
            }
        };

        let schema = Arc::new(
            snapshot
                .table_configuration()
                .schema()
                .as_ref()
                .try_into_arrow()?,
        );

        Ok(Self {
            inner: snapshot,
            config,
            schema,
        })
    }

    /// Create a new [`Snapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        // TODO: bundle operation_id with logstore ...
        let engine = log_store.engine(None);

        // NB: kernel engine uses Url::join to construct paths,
        // if the path does not end with a slash, the would override the entire path.
        // So we need to be extra sure its ends with a slash.
        let mut table_root = log_store.table_root_url();
        if !table_root.path().ends_with('/') {
            table_root.set_path(&format!("{}/", table_root.path()));
        }

        Self::try_new_with_engine(engine, table_root, config, version.map(|v| v as u64)).await
    }

    pub fn scan_builder(&self) -> ScanBuilder {
        ScanBuilder::new(self.inner.clone())
    }

    pub fn into_scan_builder(self) -> ScanBuilder {
        ScanBuilder::new(self.inner)
    }

    /// Update the snapshot to the given version
    pub async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<u64>,
    ) -> DeltaResult<()> {
        if let Some(version) = target_version {
            if version == self.version() as u64 {
                return Ok(());
            }
            if version < self.version() as u64 {
                return Err(DeltaTableError::Generic("Cannot downgrade snapshot".into()));
            }
        }

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let current = self.inner.clone();
        let snapshot = spawn_blocking_with_span(move || {
            let mut builder = KernelSnapshot::builder_from(current);
            if let Some(version) = target_version {
                builder = builder.at_version(version);
            }
            builder.build(engine.as_ref())
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        self.inner = snapshot;
        self.schema = Arc::new(
            self.inner
                .table_configuration()
                .schema()
                .as_ref()
                .try_into_arrow()?,
        );

        Ok(())
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.inner.version() as i64
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> KernelSchemaRef {
        self.inner.table_configuration().schema()
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.inner.table_configuration().metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.inner.table_configuration().protocol()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        &self.config
    }

    /// Get the table root of the snapshot
    pub(crate) fn table_root_path(&self) -> DeltaResult<Path> {
        Ok(Path::from_url_path(self.inner.table_root().path())?)
    }

    /// Well known properties of the table
    pub fn table_properties(&self) -> &TableProperties {
        self.inner.table_properties()
    }

    pub fn table_configuration(&self) -> &TableConfiguration {
        self.inner.table_configuration()
    }

    /// Get the active files for the current snapshot.
    ///
    /// This method returns a stream of record batches where each row
    /// represents an active file for the current snapshot.
    ///
    /// The files can be filtered using the provided predicate. This is a
    /// best effort to skip files that are excluded by the predicate. Individual
    /// files may still contain data that is not relevant to the predicate.
    ///
    /// ## Arguments
    ///
    /// * `log_store` - The log store to use for reading the snapshot.
    /// * `predicate` - An optional predicate to filter the files.
    ///
    /// ## Returns
    ///
    /// A stream of active files for the current snapshot.
    pub fn files(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        let scan = match self.scan_builder().with_predicate(predicate).build() {
            Ok(scan) => scan,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let stream = scan
            .scan_metadata(engine)
            .map(|d| Ok(kernel_to_arrow(d?)?.scan_files));

        ScanRowOutStream::new(self.inner.clone(), stream).boxed()
    }

    pub(crate) fn files_from<T: Iterator<Item = RecordBatch> + Send + 'static>(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
        existing_version: Version,
        existing_data: Box<T>,
        existing_predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        let scan = match self.scan_builder().with_predicate(predicate).build() {
            Ok(scan) => scan,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };

        let engine = log_store.engine(None);
        let stream = scan
            .scan_metadata_from(engine, existing_version, existing_data, existing_predicate)
            .map(|d| Ok(kernel_to_arrow(d?)?.scan_files));

        ScanRowOutStream::new(self.inner.clone(), stream).boxed()
    }

    /// Get the commit infos in the snapshot
    ///
    /// ## Parameters
    ///
    /// * `log_store`: The log store to use.
    /// * `limit`: The maximum number of commit infos to return (optional).
    ///
    /// ## Returns
    ///
    /// A stream of commit infos.
    // TODO: move outer error into stream.
    pub(crate) async fn commit_infos(
        &self,
        log_store: &dyn LogStore,
        limit: Option<usize>,
    ) -> DeltaResult<BoxStream<'_, DeltaResult<Option<CommitInfo>>>> {
        let store = log_store.root_object_store(None);

        let log_root = self.table_root_path()?.child("_delta_log");
        let start_from = log_root.child(
            format!(
                "{:020}",
                limit
                    .map(|l| (self.version() - l as i64 + 1).max(0))
                    .unwrap_or(0)
            )
            .as_str(),
        );

        let dummy_url = url::Url::parse("memory:///")
            .map_err(|e| DeltaTableError::InvalidTableLocation(format!("memory:///: {}", e)))?;
        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            // safety: object store path are always valid urls paths.
            let dummy_path = dummy_url.join(meta.location.as_ref()).unwrap();
            if let Some(parsed_path) = ParsedLogPath::try_from(dummy_path)? {
                if matches!(parsed_path.file_type, LogPathFileType::Commit) {
                    commit_files.push(meta);
                }
            }
        }
        commit_files.sort_unstable_by(|a, b| b.location.cmp(&a.location));
        Ok(futures::stream::iter(commit_files)
            .map(move |meta| {
                let store = store.clone();
                async move {
                    let commit_log_bytes = store.get(&meta.location).await?.bytes().await?;

                    for result in Deserializer::from_slice(&commit_log_bytes).into_iter::<Action>()
                    {
                        let action = result?;
                        if let Action::CommitInfo(commit_info) = action {
                            return Ok::<_, DeltaTableError>(Some(commit_info));
                        }
                    }
                    Ok(None)
                }
            })
            .buffered(self.config.log_buffer_size)
            .boxed())
    }

    pub(crate) fn tombstones(
        &self,
        log_store: &dyn LogStore,
    ) -> BoxStream<'_, DeltaResult<TombstoneView>> {
        static TOMBSTONE_SCHEMA: LazyLock<Arc<StructType>> = LazyLock::new(|| {
            Arc::new(
                StructType::try_new(vec![
                    StructField::nullable("remove", Remove::to_data_type()),
                    StructField::nullable("sidecar", Sidecar::to_data_type()),
                ])
                .expect("Failed to create a StructType somehow"),
            )
        });
        static TOMBSTONE_EVALUATOR: LazyLock<Arc<dyn ExpressionEvaluator>> = LazyLock::new(|| {
            let expression = Expression::struct_from(
                Remove::to_schema()
                    .fields()
                    .map(|field| Expression::column(["remove", field.name()])),
            )
            .into();
            ARROW_HANDLER
                .new_expression_evaluator(
                    TOMBSTONE_SCHEMA.clone(),
                    expression,
                    Remove::to_data_type(),
                )
                .expect("Failed to create tombstone evaluator")
        });

        // TODO: which capacity to choose
        let mut builder = RecordBatchReceiverStreamBuilder::new(100);
        let tx = builder.tx();

        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);

        let remove_data = match self.inner.log_segment().read_actions(
            engine.as_ref(),
            TOMBSTONE_SCHEMA.clone(),
            None,
        ) {
            Ok(data) => data,
            Err(err) => {
                return Box::pin(once(ready(Err(DeltaTableError::KernelError(err)))));
            }
        };

        builder.spawn_blocking(move || {
            for res in remove_data {
                let batch = ArrowEngineData::try_from_engine_data(res?.actions)?.into();
                if tx.blocking_send(Ok(batch)).is_err() {
                    break;
                }
            }
            Ok(())
        });

        builder
            .build()
            .map(|maybe_batch| {
                maybe_batch.and_then(|batch| {
                    let filtered = filter_record_batch(&batch, &is_not_null(batch.column(0))?)?;
                    let tombstones = TOMBSTONE_EVALUATOR.evaluate_arrow(filtered)?;
                    Ok((0..tombstones.num_rows())
                        .map(move |idx| TombstoneView::new(tombstones.clone(), idx)))
                })
            })
            .map_ok(|removes| futures::stream::iter(removes.map(Ok::<_, DeltaTableError>)))
            .try_flatten()
            .boxed()
    }

    /// Fetch the latest version of the provided application_id for this snapshot.
    ///
    /// Filters the txn based on the SetTransactionRetentionDuration property and lastUpdated
    async fn application_transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: String,
    ) -> DeltaResult<Option<i64>> {
        // TODO: bundle operation id with log store ...
        let engine = log_store.engine(None);
        let inner = self.inner.clone();
        let version =
            spawn_blocking_with_span(move || inner.get_app_id_version(&app_id, engine.as_ref()))
                .await
                .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;
        Ok(version)
    }

    /// Fetch the [domainMetadata] for a specific domain in this snapshot.
    ///
    /// This returns the latest configuration for the domain, or None if the domain does not exist.
    ///
    /// [domainMetadata]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata
    pub async fn domain_metadata(
        &self,
        log_store: &dyn LogStore,
        domain: impl ToString,
    ) -> DeltaResult<Option<String>> {
        let engine = log_store.engine(None);
        let inner = self.inner.clone();
        let domain = domain.to_string();
        let metadata =
            spawn_blocking_with_span(move || inner.get_domain_metadata(&domain, engine.as_ref()))
                .await
                .map_err(|e| DeltaTableError::GenericError { source: e.into() })??;
        Ok(metadata)
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Snapshot,
    // logical files in the snapshot
    files: Vec<RecordBatch>,
}

pub(crate) async fn resolve_snapshot(
    log_store: &dyn LogStore,
    maybe_snapshot: Option<EagerSnapshot>,
    require_files: bool,
) -> DeltaResult<EagerSnapshot> {
    if let Some(snapshot) = maybe_snapshot {
        if require_files {
            snapshot.with_files(log_store).await
        } else {
            Ok(snapshot)
        }
    } else {
        let mut config = DeltaTableConfig::default();
        config.require_files = require_files;
        EagerSnapshot::try_new(log_store, config, None).await
    }
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<i64>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store, config.clone(), version).await?;
        Self::try_new_with_snapshot(log_store, snapshot).await
    }

    pub(crate) async fn try_new_with_snapshot(
        log_store: &dyn LogStore,
        snapshot: Snapshot,
    ) -> DeltaResult<Self> {
        let files = match snapshot.load_config().require_files {
            true => snapshot.files(log_store, None).try_collect().await?,
            false => vec![],
        };
        Ok(Self { snapshot, files })
    }

    pub(crate) async fn with_files(mut self, log_store: &dyn LogStore) -> DeltaResult<Self> {
        if self.snapshot.config.require_files {
            return Ok(self);
        }
        self.snapshot.config.require_files = true;
        Self::try_new_with_snapshot(log_store, self.snapshot).await
    }

    pub(crate) fn files(&self) -> DeltaResult<&[RecordBatch]> {
        if self.snapshot.config.require_files {
            Ok(&self.files)
        } else {
            Err(DeltaTableError::NotInitializedWithFiles("files".into()))
        }
    }

    /// Update the snapshot to the given version
    pub(crate) async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<Version>,
    ) -> DeltaResult<()> {
        let current_version = self.version() as u64;
        if Some(current_version) == target_version {
            return Ok(());
        }

        self.snapshot.update(log_store, target_version).await?;

        self.files = self
            .snapshot
            .files_from(
                log_store,
                None,
                current_version,
                Box::new(std::mem::take(&mut self.files).into_iter()),
                None,
            )
            .try_collect()
            .await?;

        Ok(())
    }

    /// Get the underlying snapshot
    pub(crate) fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> i64 {
        self.snapshot.version()
    }

    /// Get the timestamp of the given version
    pub fn version_timestamp(&self, version: i64) -> Option<i64> {
        for path in &self.snapshot.inner.log_segment().ascending_commit_files {
            if path.version as i64 == version {
                return Some(path.location.last_modified);
            }
        }
        None
    }

    /// Get the table schema of the snapshot
    pub fn schema(&self) -> KernelSchemaRef {
        self.snapshot.schema()
    }

    /// Get the table arrow schema of the snapshot
    pub fn arrow_schema(&self) -> SchemaRef {
        self.snapshot.arrow_schema()
    }

    /// Get the table metadata of the snapshot
    pub fn metadata(&self) -> &Metadata {
        self.snapshot.metadata()
    }

    /// Get the table protocol of the snapshot
    pub fn protocol(&self) -> &Protocol {
        self.snapshot.protocol()
    }

    /// Get the table config which is loaded with of the snapshot
    pub fn load_config(&self) -> &DeltaTableConfig {
        self.snapshot.load_config()
    }

    /// Well known table configuration
    pub fn table_properties(&self) -> &TableProperties {
        self.snapshot.table_properties()
    }

    pub fn table_configuration(&self) -> &TableConfiguration {
        self.snapshot.table_configuration()
    }

    /// Get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of the log.
    pub fn log_data(&self) -> LogDataHandler<'_> {
        LogDataHandler::new(&self.files, self.snapshot.table_configuration())
    }

    /// Stream the active files in the snapshot
    ///
    /// This function returns a stream of [`LogicalFileView`] objects,
    /// which represent the active files in the snapshot.
    ///
    /// ## Parameters
    ///
    /// * `log_store` - A reference to a [`LogStore`] implementation.
    /// * `predicate` - An optional predicate to filter the files.
    ///
    /// ## Returns
    ///
    /// A stream of [`LogicalFileView`] objects.
    pub fn file_views(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        if !self.snapshot.load_config().require_files {
            return Box::pin(once(ready(Err(DeltaTableError::NotInitializedWithFiles(
                "file_views".into(),
            )))));
        }

        self.snapshot
            .files_from(
                log_store,
                predicate,
                self.version() as u64,
                Box::new(self.files.clone().into_iter()),
                None,
            )
            .map_ok(|batch| {
                futures::stream::iter(0..batch.num_rows()).map(move |idx| {
                    Ok::<_, DeltaTableError>(LogicalFileView::new(batch.clone(), idx))
                })
            })
            .try_flatten()
            .boxed()
    }

    #[deprecated(since = "0.29.0", note = "Use `files` with kernel predicate instead.")]
    pub fn file_views_by_partitions(
        &self,
        log_store: &dyn LogStore,
        filters: &[PartitionFilter],
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        if filters.is_empty() {
            return self.file_views(log_store, None);
        }
        let predicate = match to_kernel_predicate(filters, self.snapshot.schema().as_ref()) {
            Ok(predicate) => Arc::new(predicate),
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        self.file_views(log_store, Some(predicate))
    }

    /// Iterate over all latest app transactions
    pub async fn transaction_version(
        &self,
        log_store: &dyn LogStore,
        app_id: impl ToString,
    ) -> DeltaResult<Option<i64>> {
        self.snapshot
            .application_transaction_version(log_store, app_id.to_string())
            .await
    }

    pub async fn domain_metadata(
        &self,
        log_store: &dyn LogStore,
        domain: impl ToString,
    ) -> DeltaResult<Option<String>> {
        self.snapshot.domain_metadata(log_store, domain).await
    }
}

#[cfg(any(test, feature = "integration_test"))]
pub(crate) fn partitions_schema(
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResult<Option<StructType>> {
    if partition_columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(StructType::try_new(
        partition_columns
            .iter()
            .map(|col| {
                schema.field(col).cloned().ok_or_else(|| {
                    DeltaTableError::Generic(format!("Partition column {col} not found in schema"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )?))
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;

    // use super::log_segment::tests::{concurrent_checkpoint};
    // use super::replay::tests::test_log_replay;
    use super::*;
    use crate::{
        kernel::transaction::CommitData,
        test_utils::{assert_batches_sorted_eq, TestResult, TestTables},
    };

    impl Snapshot {
        pub async fn new_test<'a>(
            commits: impl IntoIterator<Item = &'a CommitData>,
        ) -> DeltaResult<(Self, Arc<dyn LogStore>)> {
            use crate::logstore::{commit_uri_from_version, default_logstore};
            use object_store::memory::InMemory;
            let store = Arc::new(InMemory::new());

            for (idx, commit) in commits.into_iter().enumerate() {
                let uri = commit_uri_from_version(idx as i64);
                let data = commit.get_bytes()?;
                store.put(&uri, data.into()).await?;
            }

            let table_url = url::Url::parse("memory:///").unwrap();

            let log_store = default_logstore(
                store.clone(),
                store.clone(),
                &table_url,
                &Default::default(),
            );

            let engine = log_store.engine(None);
            let snapshot = KernelSnapshot::builder_for(table_url.clone()).build(engine.as_ref())?;
            let schema = snapshot
                .table_configuration()
                .schema()
                .as_ref()
                .try_into_arrow()?;

            Ok((
                Self {
                    inner: snapshot,
                    config: Default::default(),
                    schema: Arc::new(schema),
                },
                log_store,
            ))
        }
    }

    impl EagerSnapshot {
        pub async fn new_test<'a>(
            commits: impl IntoIterator<Item = &'a CommitData>,
        ) -> DeltaResult<Self> {
            let (snapshot, log_store) = Snapshot::new_test(commits).await?;
            let files: Vec<_> = snapshot
                .files(log_store.as_ref(), None)
                .try_collect()
                .await?;
            Ok(Self { snapshot, files })
        }
    }

    #[tokio::test]
    async fn test_snapshots() -> TestResult {
        test_snapshot().await?;
        test_eager_snapshot().await?;

        Ok(())
    }

    // #[ignore]
    // #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    // async fn test_concurrent_checkpoint() -> TestResult {
    //     concurrent_checkpoint().await?;
    //     Ok(())
    // }

    async fn test_snapshot() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;

        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let actual = serde_json::from_slice::<'_, Snapshot>(&bytes);
        assert!(actual.is_ok());

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema().as_ref(), &expected);

        let infos = snapshot
            .commit_infos(&log_store, None)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let infos = infos.into_iter().flatten().collect_vec();
        assert_eq!(infos.len(), 5);

        let tombstones = snapshot
            .tombstones(&log_store)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(tombstones.len(), 31);

        let batches = snapshot
            .files(&log_store, None)
            .try_collect::<Vec<_>>()
            .await?;
        let expected = [
            "+---------------------------------------------------------------------+------+------------------+-------------------------------------------------------+----------------+-----------------------------------------------------------------------+",
            "| path                                                                | size | modificationTime | stats_parsed                                          | deletionVector | fileConstantValues                                                    |",
            "+---------------------------------------------------------------------+------+------------------+-------------------------------------------------------+----------------+-----------------------------------------------------------------------+",
            "| part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet | 262  | 1587968626000    | {numRecords: , nullCount: , minValues: , maxValues: } |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: } |",
            "| part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet | 262  | 1587968602000    | {numRecords: , nullCount: , minValues: , maxValues: } |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: } |",
            "| part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet | 429  | 1587968602000    | {numRecords: , nullCount: , minValues: , maxValues: } |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: } |",
            "| part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet | 429  | 1587968602000    | {numRecords: , nullCount: , minValues: , maxValues: } |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: } |",
            "| part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet | 429  | 1587968602000    | {numRecords: , nullCount: , minValues: , maxValues: } |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: } |",
            "+---------------------------------------------------------------------+------+------------------+-------------------------------------------------------+----------------+-----------------------------------------------------------------------+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        let log_store = TestTables::Checkpoints.table_builder()?.build_storage()?;

        for version in 0..=12 {
            let snapshot = Snapshot::try_new(&log_store, Default::default(), Some(version)).await?;
            let batches = snapshot
                .files(&log_store, None)
                .try_collect::<Vec<_>>()
                .await?;
            let num_files = batches.iter().map(|b| b.num_rows() as i64).sum::<i64>();
            assert_eq!(num_files, version);
        }

        Ok(())
    }

    async fn test_eager_snapshot() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;

        let snapshot = EagerSnapshot::try_new(&log_store, Default::default(), None).await?;

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let actual = serde_json::from_slice::<'_, EagerSnapshot>(&bytes);
        assert!(actual.is_ok());

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"#;
        let expected: StructType = serde_json::from_str(schema_string)?;
        assert_eq!(snapshot.schema().as_ref(), &expected);

        let log_store = TestTables::Checkpoints.table_builder()?.build_storage()?;

        for version in 0..=12 {
            let snapshot =
                EagerSnapshot::try_new(&log_store, Default::default(), Some(version)).await?;
            let batches: Vec<_> = snapshot.file_views(&log_store, None).try_collect().await?;
            assert_eq!(batches.len(), version as usize);
        }

        Ok(())
    }
}
