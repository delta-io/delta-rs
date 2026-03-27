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
use futures::stream::{BoxStream, once};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store::path::Path;
use serde_json::Deserializer;
use url::Url;

use super::{Action, CommitInfo, Metadata, Protocol};
use crate::kernel::arrow::engine_ext::{ExpressionEvaluatorExt, rb_from_scan_meta};
use crate::kernel::{ARROW_HANDLER, StructType, spawn_blocking_with_span};
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError, PartitionFilter, to_kernel_predicate};

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
        version: Option<Version>,
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

        Self::try_new_with_engine(engine, table_root, config, version).await
    }

    pub fn scan_builder(&self) -> ScanBuilder {
        ScanBuilder::new(self.inner.clone())
    }

    pub fn into_scan_builder(self) -> ScanBuilder {
        ScanBuilder::new(self.inner)
    }

    /// Update the snapshot to the given version
    pub async fn update(
        self: Arc<Self>,
        engine: Arc<dyn Engine>,
        target_version: Option<Version>,
    ) -> DeltaResult<Arc<Self>> {
        let current_version = self.version();
        if let Some(version) = target_version {
            if version < current_version {
                return Err(DeltaTableError::VersionDowngrade {
                    current_version,
                    requested_version: version,
                });
            }
            if version == current_version {
                return self
                    .refresh_same_version_checkpoint_if_needed(engine, version)
                    .await;
            }
            if version < self.version() {
                return Err(DeltaTableError::Generic("Cannot downgrade snapshot".into()));
            }
        }

        let current = self.inner.clone();
        let task_engine = engine.clone();
        let snapshot = spawn_blocking_with_span(move || {
            let mut builder = KernelSnapshot::builder_from(current);
            if let Some(version) = target_version {
                builder = builder.at_version(version);
            }
            builder.build(task_engine.as_ref())
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        let schema = Arc::new(
            snapshot
                .table_configuration()
                .schema()
                .as_ref()
                .try_into_arrow()?,
        );

        let snapshot = Arc::new(Self {
            inner: snapshot,
            schema,
            config: self.config.clone(),
        });
        if snapshot.version() as u64 == current_version {
            // `target_version` was `None`, but there were no new commits.
            // We may still need to pick up a checkpoint written for the current version.
            snapshot
                .refresh_same_version_checkpoint_if_needed(engine, current_version)
                .await
        } else {
            Ok(snapshot)
        }
    }

    /// Rebuild a same-version snapshot if a checkpoint showed up after it was loaded.
    ///
    /// Version bumps still go through the normal snapshot update path.
    async fn refresh_same_version_checkpoint_if_needed(
        self: Arc<Self>,
        engine: Arc<dyn Engine>,
        current_version: Version,
    ) -> DeltaResult<Arc<Self>> {
        if self.checkpoint_version() == Some(current_version) {
            return Ok(self);
        }

        let mut table_root = self.inner.table_root().clone();
        if !table_root.path().ends_with('/') {
            table_root.set_path(&format!("{}/", table_root.path()));
        }
        let log_root = table_root
            .join("_delta_log/")
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        let checkpoint_version = read_last_checkpoint_version(engine.clone(), log_root).await?;
        if checkpoint_version != Some(current_version) {
            return Ok(self);
        }

        tracing::trace!(
            version = current_version,
            "rebuilding snapshot to adopt a newly available checkpoint at the current version"
        );
        Snapshot::try_new_with_engine(
            engine,
            table_root,
            self.config.clone(),
            Some(current_version),
        )
        .await
        .map(Arc::new)
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> Version {
        self.inner.version()
    }

    /// Get the checkpoint version currently backing this snapshot, if any.
    pub(crate) fn checkpoint_version(&self) -> Option<Version> {
        self.inner.log_segment().checkpoint_version
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
            .map(|d| Ok(rb_from_scan_meta(d?)?));

        match ScanRowOutStream::try_new(self.inner.clone(), stream) {
            Ok(s) => s.boxed(),
            Err(err) => Box::pin(once(ready(Err(err)))),
        }
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
            .map(|d| Ok(rb_from_scan_meta(d?)?));

        match ScanRowOutStream::try_new(self.inner.clone(), stream) {
            Ok(s) => s.boxed(),
            Err(err) => Box::pin(once(ready(Err(err)))),
        }
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
    /// A stream of [`LogicalFileView`] objects, newest first.
    pub fn file_views(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        self.files(log_store, predicate)
            .map_ok(|batch| {
                futures::stream::iter(0..batch.num_rows()).map(move |idx| {
                    Ok::<_, DeltaTableError>(LogicalFileView::new(batch.clone(), idx))
                })
            })
            .try_flatten()
            .boxed()
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
        let start_from = if let Some(limit) = limit {
            let v = self.version() as i64;
            std::cmp::max(v - limit as i64 + 1, 0)
        } else {
            0
        };
        let start_from = log_root.child(format!("{:020}", start_from).as_str());

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
            if let Some(parsed_path) = ParsedLogPath::try_from(dummy_path)?
                && matches!(parsed_path.file_type, LogPathFileType::Commit)
            {
                commit_files.push(meta);
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
        if let Some(version) = version {
            return Ok(Some(version.try_into().map_err(|e| {
                DeltaTableError::GenericError {
                    source: Box::new(e),
                }
            })?));
        }
        Ok(None)
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
    snapshot: Arc<Snapshot>,
    // logical files in the snapshot
    files: Vec<RecordBatch>,
}

#[derive(::serde::Deserialize)]
struct LastCheckpointVersionHint {
    version: Version,
}

/// Read `_last_checkpoint` and return the hinted version when present.
///
/// Missing, empty, or invalid hint files return `Ok(None)` so callers can fall back to listing.
async fn read_last_checkpoint_version(
    engine: Arc<dyn Engine>,
    log_root: Url,
) -> DeltaResult<Option<Version>> {
    spawn_blocking_with_span(move || {
        let storage = engine.storage_handler();
        let checkpoint_path = log_root
            .join("_last_checkpoint")
            .map_err(|e| DeltaTableError::Generic(e.to_string()))?;
        match storage.read_files(vec![(checkpoint_path, None)])?.next() {
            Some(Ok(data)) => Ok(serde_json::from_slice::<LastCheckpointVersionHint>(&data)
                .inspect_err(|e| tracing::warn!("invalid _last_checkpoint JSON: {e}"))
                .ok()
                .map(|hint| hint.version)),
            Some(Err(delta_kernel::Error::FileNotFound(_))) => Ok(None),
            Some(Err(err)) => Err(err.into()),
            None => {
                tracing::warn!("empty _last_checkpoint file");
                Ok(None)
            }
        }
    })
    .await
    .map_err(|e| DeltaTableError::Generic(e.to_string()))?
}

pub(crate) async fn resolve_snapshot(
    log_store: &dyn LogStore,
    maybe_snapshot: Option<EagerSnapshot>,
    require_files: bool,
    version: Option<Version>,
) -> DeltaResult<EagerSnapshot> {
    if let Some(snapshot) = maybe_snapshot {
        if let Some(version) = version
            && snapshot.version() as Version != version
        {
            return Err(DeltaTableError::Generic(
                "Provided snapshot version does not match the requested version".to_string(),
            ));
        }
        if require_files {
            snapshot.with_files(log_store).await
        } else {
            Ok(snapshot)
        }
    } else {
        let mut config = DeltaTableConfig::default();
        config.require_files = require_files;
        EagerSnapshot::try_new(log_store, config, version).await
    }
}

impl EagerSnapshot {
    /// Create a new [`EagerSnapshot`] instance
    pub async fn try_new(
        log_store: &dyn LogStore,
        config: DeltaTableConfig,
        version: Option<Version>,
    ) -> DeltaResult<Self> {
        let snapshot = Snapshot::try_new(log_store, config.clone(), version).await?;
        Self::try_new_with_snapshot(log_store, snapshot.into()).await
    }

    pub(crate) async fn try_new_with_snapshot(
        log_store: &dyn LogStore,
        snapshot: Arc<Snapshot>,
    ) -> DeltaResult<Self> {
        let files = match snapshot.load_config().require_files {
            true => snapshot.files(log_store, None).try_collect().await?,
            false => vec![],
        };
        Ok(Self { snapshot, files })
    }

    pub(crate) async fn with_files(self, log_store: &dyn LogStore) -> DeltaResult<Self> {
        if self.snapshot.config.require_files {
            return Ok(self);
        }
        let mut config = self.snapshot.config.clone();
        config.require_files = true;
        Self::try_new_with_snapshot(
            log_store,
            Snapshot {
                config,
                ..(*self.snapshot).clone()
            }
            .into(),
        )
        .await
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
        let current_version = self.version();
        let previous_snapshot = self.snapshot.clone();
        let updated_snapshot = previous_snapshot
            .clone()
            .update(log_store.engine(None), target_version)
            .await?;
        if Arc::ptr_eq(&updated_snapshot, &previous_snapshot) {
            return Ok(());
        }

        self.snapshot = updated_snapshot;

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
    pub fn version(&self) -> Version {
        self.snapshot.version()
    }

    /// Get the checkpoint version currently backing this snapshot, if any.
    pub(crate) fn checkpoint_version(&self) -> Option<Version> {
        self.snapshot.checkpoint_version()
    }

    /// Get the timestamp of the given version
    pub fn version_timestamp(&self, version: Version) -> Option<i64> {
        for path in &self.snapshot.inner.log_segment().ascending_commit_files {
            if path.version == version {
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
    /// A stream of [`LogicalFileView`] objects, newest first.
    pub fn file_views(
        &self,
        log_store: &dyn LogStore,
        predicate: Option<PredicateRef>,
    ) -> BoxStream<'_, DeltaResult<LogicalFileView>> {
        self.snapshot.file_views(log_store, predicate)
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
    use std::sync::Arc;

    use futures::TryStreamExt;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    // use super::log_segment::tests::{concurrent_checkpoint};
    // use super::replay::tests::test_log_replay;
    use super::*;
    use crate::{
        DeltaTable, checkpoints,
        kernel::transaction::CommitData,
        kernel::transaction::{CommitBuilder, TableReference},
        kernel::{Action, DataType, PrimitiveType, StructField, StructType},
        protocol::{DeltaOperation, SaveMode},
        test_utils::{TestResult, TestTables, assert_batches_sorted_eq, make_test_add},
    };

    impl Snapshot {
        pub async fn new_test<'a>(
            commits: impl IntoIterator<Item = &'a CommitData>,
        ) -> DeltaResult<(Self, Arc<dyn LogStore>)> {
            use crate::logstore::{commit_uri_from_version, default_logstore};
            use object_store::memory::InMemory;
            let store = Arc::new(InMemory::new());

            for (idx, commit) in commits.into_iter().enumerate() {
                let uri = commit_uri_from_version(Some(idx as u64));
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
            Ok(Self {
                snapshot: snapshot.into(),
                files,
            })
        }
    }

    async fn checkpoint_rebase_table() -> DeltaResult<(TempDir, DeltaTable)> {
        let table_dir = tempfile::tempdir().unwrap();
        let mut table = DeltaTable::new_in_memory()
            .create()
            .with_location(table_dir.path().to_str().unwrap())
            .with_columns(
                [StructField::new(
                    "id",
                    DataType::Primitive(PrimitiveType::Integer),
                    true,
                )]
                .into_iter(),
            )
            .await?;

        append_test_add(&mut table, "part-00000.snappy.parquet").await?;
        append_test_add(&mut table, "part-00001.snappy.parquet").await?;
        Ok((table_dir, table))
    }

    async fn append_test_add(table: &mut DeltaTable, path: &str) -> DeltaResult<Version> {
        let version = CommitBuilder::default()
            .with_actions(vec![Action::Add(make_test_add(path, &[], 0))])
            .build(
                table
                    .state
                    .as_ref()
                    .map(|state| state as &dyn TableReference),
                table.log_store(),
                DeltaOperation::Write {
                    mode: SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?
            .version();
        table.update_state().await?;
        Ok(version)
    }

    async fn eager_file_paths(
        snapshot: &EagerSnapshot,
        log_store: &dyn LogStore,
    ) -> DeltaResult<Vec<String>> {
        Ok(snapshot
            .file_views(log_store, None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|file| file.path().to_string())
            .collect())
    }

    async fn checkpoint_file_paths(
        log_store: &dyn LogStore,
        version: Version,
    ) -> DeltaResult<Vec<object_store::path::Path>> {
        let checkpoint_prefix = format!("{version:020}.checkpoint", version = version);
        Ok(log_store
            .object_store(None)
            .list(Some(log_store.log_path()))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|meta| meta.location)
            .filter(|path| {
                path.filename()
                    .is_some_and(|file_name| file_name.starts_with(&checkpoint_prefix))
            })
            .collect())
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
            let num_files = batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
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

    #[tokio::test]
    async fn test_eager_file_views_return_newest_files_first() -> TestResult {
        let (_dir, mut table) = checkpoint_rebase_table().await?;

        append_test_add(&mut table, "part-00002.snappy.parquet").await?;
        let version = table.version().unwrap();
        checkpoints::create_checkpoint(&table, None).await?;

        let snapshot =
            EagerSnapshot::try_new(table.log_store().as_ref(), Default::default(), None).await?;
        assert_eq!(snapshot.version(), version);

        let paths = eager_file_paths(&snapshot, table.log_store().as_ref()).await?;
        assert_eq!(
            paths,
            vec![
                "part-00002.snappy.parquet",
                "part-00001.snappy.parquet",
                "part-00000.snappy.parquet",
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_update_explicit_same_version_adopts_late_checkpoint() -> TestResult {
        let (_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let snapshot =
            Snapshot::try_new(log_store.as_ref(), Default::default(), Some(version)).await?;
        assert_eq!(snapshot.version(), version);
        assert_eq!(snapshot.checkpoint_version(), None);

        checkpoints::create_checkpoint(&table, None).await?;

        let updated = Arc::new(snapshot)
            .update(log_store.engine(None), Some(version as u64))
            .await?;
        assert_eq!(updated.version(), version);
        assert_eq!(updated.checkpoint_version(), Some(version));
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_update_latest_same_version_adopts_late_checkpoint() -> TestResult {
        let (_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let snapshot = Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?;
        assert_eq!(snapshot.version(), version);
        assert_eq!(snapshot.checkpoint_version(), None);

        checkpoints::create_checkpoint(&table, None).await?;

        let updated = Arc::new(snapshot)
            .update(log_store.engine(None), None)
            .await?;
        assert_eq!(updated.version(), version);
        assert_eq!(updated.checkpoint_version(), Some(version));
        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_update_explicit_same_version_adopts_late_checkpoint() -> TestResult
    {
        let (_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let mut snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), Some(version)).await?;
        let expected_paths = eager_file_paths(&snapshot, log_store.as_ref()).await?;
        assert_eq!(snapshot.snapshot.checkpoint_version(), None);

        checkpoints::create_checkpoint(&table, None).await?;

        snapshot
            .update(log_store.as_ref(), Some(version as u64))
            .await?;
        assert_eq!(snapshot.version(), version);
        assert_eq!(snapshot.snapshot.checkpoint_version(), Some(version));
        assert_eq!(
            eager_file_paths(&snapshot, log_store.as_ref()).await?,
            expected_paths
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_update_latest_same_version_adopts_late_checkpoint() -> TestResult {
        let (_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let mut snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), None).await?;
        let expected_paths = eager_file_paths(&snapshot, log_store.as_ref()).await?;
        assert_eq!(snapshot.snapshot.checkpoint_version(), None);

        checkpoints::create_checkpoint(&table, None).await?;

        snapshot.update(log_store.as_ref(), None).await?;
        assert_eq!(snapshot.version(), version);
        assert_eq!(snapshot.snapshot.checkpoint_version(), Some(version));
        assert_eq!(
            eager_file_paths(&snapshot, log_store.as_ref()).await?,
            expected_paths
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_same_version_checkpoint_refresh_is_idempotent() -> TestResult {
        let (_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let mut snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), Some(version)).await?;

        checkpoints::create_checkpoint(&table, None).await?;
        snapshot
            .update(log_store.as_ref(), Some(version as u64))
            .await?;
        assert_eq!(snapshot.snapshot.checkpoint_version(), Some(version));

        let prior_snapshot = snapshot.snapshot.clone();
        let prior_files_ptr = snapshot.files.as_ptr();
        let prior_paths = eager_file_paths(&snapshot, log_store.as_ref()).await?;

        snapshot
            .update(log_store.as_ref(), Some(version as u64))
            .await?;

        assert!(Arc::ptr_eq(&snapshot.snapshot, &prior_snapshot));
        assert_eq!(snapshot.files.as_ptr(), prior_files_ptr);
        assert_eq!(
            eager_file_paths(&snapshot, log_store.as_ref()).await?,
            prior_paths
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_update_same_version_surfaces_invalid_current_checkpoint_hint()
    -> TestResult {
        let (_table_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let snapshot =
            Snapshot::try_new(log_store.as_ref(), Default::default(), Some(version)).await?;

        checkpoints::create_checkpoint(&table, None).await?;
        let checkpoint_paths = checkpoint_file_paths(log_store.as_ref(), version).await?;
        assert!(!checkpoint_paths.is_empty());
        for checkpoint_path in checkpoint_paths {
            log_store
                .object_store(None)
                .delete(&checkpoint_path)
                .await?;
        }

        let err = Arc::new(snapshot)
            .update(log_store.engine(None), Some(version as u64))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Had a _last_checkpoint hint but didn't find any checkpoints"),
            "expected same-version checkpoint refresh to surface the kernel checkpoint error: {err}"
        );
        Ok(())
    }
}
