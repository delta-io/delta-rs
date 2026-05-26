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
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt as _};
use serde_json::Deserializer;
use url::Url;

use super::{Action, CommitInfo, Metadata, Protocol};
use crate::checkpoints::parse_last_checkpoint_hint;
use crate::kernel::arrow::engine_ext::{ExpressionEvaluatorExt, rb_from_scan_meta};
use crate::kernel::{ARROW_HANDLER, StructType, spawn_blocking_with_span};
use crate::logstore::{LogStore, LogStoreExt};
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError, PartitionFilter, to_kernel_predicate};

pub use self::log_data::*;
pub(crate) use self::stats_projection::{
    FIELD_STATS_PARSED, FileStatsMaterialization, StatsProjection,
};
pub use iterators::*;
pub use scan::*;
pub use stream::*;

mod iterators;
mod log_data;
mod scan;
mod serde;
mod stats_projection;
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
    /// Optional materialized replay state owned by this snapshot.
    materialized_files: Option<Arc<MaterializedFiles>>,
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

        Ok(Self {
            inner: snapshot,
            config,
            materialized_files: None,
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

        let snapshot = Arc::new(Self {
            inner: snapshot,
            config: self.config.clone(),
            materialized_files: None,
        });
        if snapshot.version() as u64 == current_version {
            // `target_version` was `None`, but there were no new commits.
            // We may still need to pick up a checkpoint written for the current version.
            self.refresh_same_version_checkpoint_if_needed(engine, current_version)
                .await
        } else {
            match self.materialized_files() {
                Some(materialized_files) => {
                    snapshot
                        .materialize_files_with_engine(engine, Some(materialized_files))
                        .await
                }
                None => Ok(snapshot),
            }
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
        let snapshot = Snapshot::try_new_with_engine(
            engine.clone(),
            table_root,
            self.config.clone(),
            Some(current_version),
        )
        .await
        .map(Arc::new)?;

        match self.materialized_files() {
            Some(materialized_files) => {
                snapshot
                    .materialize_files_with_engine(engine, Some(materialized_files))
                    .await
            }
            None => Ok(snapshot),
        }
    }

    /// Get the table version of the snapshot
    pub fn version(&self) -> Version {
        self.inner.version()
    }

    /// Get the checkpoint version currently backing this snapshot, if any.
    pub(crate) fn checkpoint_version(&self) -> Option<Version> {
        self.inner.log_segment().checkpoint_version
    }

    /// Get the logical table schema of the snapshot
    pub fn schema(&self) -> KernelSchemaRef {
        self.inner.table_configuration().logical_schema()
    }

    /// Convert the lgoical schema into an Arrow [SchemaRef]
    ///
    /// NOTE: This can panic if the table's logical schema is not compatible with Arrow!
    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::new(
            self.schema()
                .as_ref()
                .try_into_arrow()
                .expect("Failed to coerce the logical schema into Arrow"),
        )
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

    pub(crate) fn materialized_files(&self) -> Option<&Arc<MaterializedFiles>> {
        self.materialized_files.as_ref()
    }

    pub(crate) async fn ensure_materialized_files(
        self: Arc<Self>,
        log_store: &dyn LogStore,
    ) -> DeltaResult<Arc<Self>> {
        if self.materialized_files.is_some() {
            return Ok(self);
        }

        self.materialize_files_with_engine(log_store.engine(None), None)
            .await
    }

    pub(crate) fn try_log_data(&self) -> DeltaResult<LogDataHandler<'_>> {
        let materialized_files = self
            .materialized_files
            .as_ref()
            .ok_or_else(|| DeltaTableError::NotInitializedWithFiles("log_data".to_string()))?;
        Ok(LogDataHandler::new(
            &materialized_files.batches,
            self.table_configuration(),
        ))
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
        // Use cached parsed batches when no predicate is present. This avoids
        // replaying stats through JSON.
        if predicate.is_none()
            && let Some(cached) = self.cached_parsed_batches()
        {
            return cached;
        }
        if predicate.is_some() && self.config.skip_stats {
            return self.files_with_engine(log_store.engine(None), predicate);
        }

        match self
            .materialized_files()
            .and_then(|materialized_files| materialized_files.full_table_seed())
        {
            Some(materialized_seed) => {
                let (existing_version, existing_data, existing_predicate) =
                    materialized_seed.into_parts();
                self.files_from(
                    log_store.engine(None),
                    predicate,
                    existing_version,
                    Box::new(existing_data),
                    existing_predicate,
                )
            }
            None => self.files_with_engine(log_store.engine(None), predicate),
        }
    }

    fn cached_parsed_batches(&self) -> Option<SendableRBStream> {
        let materialized = self.materialized_files()?;
        if materialized.existing_predicate.is_some() {
            return None;
        }
        match materialized.scope {
            MaterializedFilesScope::FullTable => {
                let batches = Arc::clone(&materialized.batches);
                Some(
                    futures::stream::iter((0..batches.len()).map(move |i| Ok(batches[i].clone())))
                        .boxed(),
                )
            }
        }
    }

    fn files_with_engine(
        &self,
        engine: Arc<dyn Engine>,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        self.files_with_engine_materialized(engine, predicate, FileStatsMode::PreserveRaw)
    }

    fn files_with_engine_preserving_raw(
        &self,
        engine: Arc<dyn Engine>,
        predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        self.files_with_engine_materialized(engine, predicate, FileStatsMode::FullPreserveRaw)
    }

    fn files_with_engine_materialized(
        &self,
        engine: Arc<dyn Engine>,
        predicate: Option<PredicateRef>,
        stats_mode: FileStatsMode,
    ) -> SendableRBStream {
        self.warn_if_skip_stats_with_predicate(&predicate);
        let skip_stats = self.config.skip_stats;
        let scan = match self.scan_for_files(predicate, skip_stats, stats_mode) {
            Ok(scan) => scan,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        let stats_materialization = scan.stats_materialization().clone();

        let stream = scan
            .scan_metadata(engine)
            .map(|d| Ok(rb_from_scan_meta(d?)?));

        match ScanRowOutStream::try_new_with_materialization(
            self.inner.clone(),
            stream,
            stats_materialization,
        ) {
            Ok(s) => s.boxed(),
            Err(err) => Box::pin(once(ready(Err(err)))),
        }
    }

    fn scan_for_files(
        &self,
        predicate: Option<PredicateRef>,
        skip_stats: bool,
        stats_mode: FileStatsMode,
    ) -> DeltaResult<Scan> {
        let stats_materialization =
            self.file_stats_materialization(predicate.as_ref(), skip_stats, stats_mode)?;

        self.scan_builder()
            .with_predicate(predicate)
            .with_stats_materialization(stats_materialization)
            .build()
    }

    fn file_stats_materialization(
        &self,
        predicate: Option<&PredicateRef>,
        skip_stats: bool,
        stats_mode: FileStatsMode,
    ) -> DeltaResult<FileStatsMaterialization> {
        if skip_stats {
            return Ok(FileStatsMaterialization::without_stats());
        }

        let stats_projection = match stats_mode {
            FileStatsMode::PreserveRaw => {
                StatsProjection::for_scan_inputs(self.inner.as_ref(), None, predicate)?
            }
            FileStatsMode::FullPreserveRaw => StatsProjection::full(),
        };

        Ok(FileStatsMaterialization::compatibility(stats_projection))
    }

    fn warn_if_skip_stats_with_predicate(&self, predicate: &Option<PredicateRef>) {
        if self.config.skip_stats && predicate.is_some() {
            tracing::warn!(
                "`DeltaTable` was opened with `skip_stats=true`, but this query has \
                 a predicate. Every file in the table will be scanned. To avoid \
                 this, open a separate `DeltaTable` without `skip_stats=true` for \
                 query workloads."
            );
        }
    }

    pub(crate) fn files_from<T: Iterator<Item = RecordBatch> + Send + 'static>(
        &self,
        engine: Arc<dyn Engine>,
        predicate: Option<PredicateRef>,
        existing_version: Version,
        existing_data: Box<T>,
        existing_predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        self.files_from_materialized(
            engine,
            predicate,
            existing_version,
            existing_data,
            existing_predicate,
            FileStatsMode::PreserveRaw,
        )
    }

    fn files_from_preserving_raw<T: Iterator<Item = RecordBatch> + Send + 'static>(
        &self,
        engine: Arc<dyn Engine>,
        predicate: Option<PredicateRef>,
        existing_version: Version,
        existing_data: Box<T>,
        existing_predicate: Option<PredicateRef>,
    ) -> SendableRBStream {
        self.files_from_materialized(
            engine,
            predicate,
            existing_version,
            existing_data,
            existing_predicate,
            FileStatsMode::FullPreserveRaw,
        )
    }

    fn files_from_materialized<T: Iterator<Item = RecordBatch> + Send + 'static>(
        &self,
        engine: Arc<dyn Engine>,
        predicate: Option<PredicateRef>,
        existing_version: Version,
        existing_data: Box<T>,
        existing_predicate: Option<PredicateRef>,
        stats_mode: FileStatsMode,
    ) -> SendableRBStream {
        self.warn_if_skip_stats_with_predicate(&predicate);
        let skip_stats = self.config.skip_stats;
        let scan = match self.scan_for_files(predicate, skip_stats, stats_mode) {
            Ok(scan) => scan,
            Err(err) => return Box::pin(once(ready(Err(err)))),
        };
        let stats_materialization = scan.stats_materialization().clone();

        let stream = scan
            .scan_metadata_from(engine, existing_version, existing_data, existing_predicate)
            .map(|d| Ok(rb_from_scan_meta(d?)?));

        match ScanRowOutStream::try_new_with_materialization(
            self.inner.clone(),
            stream,
            stats_materialization,
        ) {
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

    fn with_materialized_files(&self, materialized_files: Option<Arc<MaterializedFiles>>) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            materialized_files,
        }
    }

    async fn materialize_files_with_engine(
        self: Arc<Self>,
        engine: Arc<dyn Engine>,
        materialized_seed: Option<&Arc<MaterializedFiles>>,
    ) -> DeltaResult<Arc<Self>> {
        if materialized_seed.is_none() && self.materialized_files.is_some() {
            return Ok(self);
        }

        let batches = match materialized_seed.and_then(|seed| seed.full_table_seed()) {
            Some(materialized_seed) => {
                let (existing_version, existing_data, existing_predicate) =
                    materialized_seed.into_parts();
                self.files_from_preserving_raw(
                    engine,
                    None,
                    existing_version,
                    Box::new(existing_data),
                    existing_predicate,
                )
                .try_collect()
                .await?
            }
            None => {
                self.files_with_engine_preserving_raw(engine, None)
                    .try_collect()
                    .await?
            }
        };
        let materialized_files = Arc::new(MaterializedFiles::full(self.version(), batches));
        Ok(Arc::new(
            self.with_materialized_files(Some(materialized_files)),
        ))
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

        let log_root = self.table_root_path()?.join("_delta_log");
        let start_from = if let Some(limit) = limit {
            let v = self.version() as i64;
            std::cmp::max(v - limit as i64 + 1, 0)
        } else {
            0
        };
        let start_from = log_root
            .clone()
            .join(format!("{:020}", start_from).as_str());

        let dummy_url = url::Url::parse("memory:///")
            .map_err(|e| DeltaTableError::InvalidTableLocation(format!("memory:///: {}", e)))?;
        let mut commit_files = Vec::new();
        for meta in store
            .list_with_offset(Some(&log_root), &start_from)
            .try_collect::<Vec<_>>()
            .await?
        {
            let dummy_path = dummy_url
                .join(meta.location.as_ref())
                .map_err(|err| DeltaTableError::InvalidTableLocation(err.to_string()))?;
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

        let remove_data = match self
            .inner
            .log_segment()
            .read_actions(engine.as_ref(), TOMBSTONE_SCHEMA.clone())
        {
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
            return Ok(Some(version));
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

/// Stats materialization mode for file replay APIs that preserve compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileStatsMode {
    /// Use raw JSON stats and the requested parsed stats shape.
    PreserveRaw,
    /// Use raw JSON stats and the full parsed stats schema.
    FullPreserveRaw,
}

/// Scope for materialized file replay data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
pub(crate) enum MaterializedFilesScope {
    FullTable,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MaterializedFiles {
    pub(crate) version: Version,
    pub(crate) scope: MaterializedFilesScope,
    pub(crate) existing_predicate: Option<PredicateRef>,
    pub(crate) batches: Arc<[RecordBatch]>,
}

#[derive(Debug, Clone)]
struct MaterializedFilesSeed {
    version: Version,
    existing_predicate: Option<PredicateRef>,
    batches: Arc<[RecordBatch]>,
}

impl MaterializedFilesSeed {
    fn into_parts(self) -> (Version, SharedRecordBatchIter, Option<PredicateRef>) {
        (
            self.version,
            SharedRecordBatchIter::new(self.batches),
            self.existing_predicate,
        )
    }
}

impl IntoIterator for MaterializedFilesSeed {
    type Item = RecordBatch;
    type IntoIter = SharedRecordBatchIter;

    fn into_iter(self) -> Self::IntoIter {
        SharedRecordBatchIter::new(self.batches)
    }
}

#[derive(Debug)]
struct SharedRecordBatchIter {
    batches: Arc<[RecordBatch]>,
    next_idx: usize,
}

impl SharedRecordBatchIter {
    fn new(batches: Arc<[RecordBatch]>) -> Self {
        Self {
            batches,
            next_idx: 0,
        }
    }
}

impl Iterator for SharedRecordBatchIter {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.batches.get(self.next_idx).cloned()?;
        self.next_idx += 1;
        Some(batch)
    }
}

impl MaterializedFiles {
    fn full(version: Version, batches: Vec<RecordBatch>) -> Self {
        Self {
            version,
            scope: MaterializedFilesScope::FullTable,
            existing_predicate: None,
            batches: batches.into(),
        }
    }

    fn full_table_seed(&self) -> Option<MaterializedFilesSeed> {
        match self.scope {
            MaterializedFilesScope::FullTable => Some(MaterializedFilesSeed {
                version: self.version,
                existing_predicate: self.existing_predicate.clone(),
                batches: self.batches.clone(),
            }),
        }
    }
}

/// A snapshot of a Delta table that has been eagerly loaded into memory.
#[derive(Debug, Clone, PartialEq)]
pub struct EagerSnapshot {
    snapshot: Arc<Snapshot>,
}

/// Read `_last_checkpoint` and return the hinted version when present.
///
/// Missing, empty, or invalid hint files return `Ok(None)`. Callers can fall back to listing.
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
            Some(Ok(data)) => Ok(parse_last_checkpoint_hint(&data).map(|hint| hint.version)),
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
        let config = DeltaTableConfig {
            require_files,
            ..Default::default()
        };
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
        let snapshot = match snapshot.load_config().require_files {
            true => snapshot.ensure_materialized_files(log_store).await?,
            false => snapshot,
        };
        Ok(Self { snapshot })
    }

    pub(crate) async fn with_files(self, log_store: &dyn LogStore) -> DeltaResult<Self> {
        if self.snapshot.materialized_files().is_some() && self.snapshot.config.require_files {
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

    /// Update the snapshot to the given version
    pub(crate) async fn update(
        &mut self,
        log_store: &dyn LogStore,
        target_version: Option<Version>,
    ) -> DeltaResult<()> {
        let previous_snapshot = self.snapshot.clone();
        let updated_snapshot = previous_snapshot
            .clone()
            .update(log_store.engine(None), target_version)
            .await?;
        if Arc::ptr_eq(&updated_snapshot, &previous_snapshot) {
            return Ok(());
        }

        self.snapshot = updated_snapshot;
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

    /// Get the timestamp of the given version
    pub fn version_timestamp(&self, version: Version) -> Option<i64> {
        for path in &self
            .snapshot
            .inner
            .log_segment()
            .listed
            .ascending_commit_files
        {
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

    /// Try to get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of
    /// the log.
    pub fn try_log_data(&self) -> DeltaResult<LogDataHandler<'_>> {
        match self.snapshot.try_log_data() {
            Ok(log_data) => Ok(log_data),
            Err(DeltaTableError::NotInitializedWithFiles(_)) => Ok(LogDataHandler::new(
                &[],
                self.snapshot.table_configuration(),
            )),
            Err(err) => Err(err),
        }
    }

    /// Get a [`LogDataHandler`] for the snapshot to inspect the currently loaded state of the log.
    pub fn log_data(&self) -> LogDataHandler<'_> {
        match self.try_log_data() {
            Ok(log_data) => log_data,
            Err(err) => {
                tracing::error!("unexpected error loading EagerSnapshot log data: {err}");
                LogDataHandler::new(&[], self.snapshot.table_configuration())
            }
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

    use arrow_ipc::writer::FileWriter;
    use arrow_schema::DataType as ArrowDataType;
    use delta_kernel::expressions::Scalar;
    use futures::TryStreamExt;
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tempfile::TempDir;

    // use super::log_segment::tests::{concurrent_checkpoint};
    // use super::replay::tests::test_log_replay;
    use super::*;
    use crate::{
        DeltaTable, DeltaTableConfig, TableProperty, checkpoints,
        kernel::transaction::CommitData,
        kernel::transaction::{CommitBuilder, TableReference},
        kernel::{Action, DataType, PrimitiveType, StructField, StructType},
        protocol::{DeltaOperation, SaveMode},
        test_utils::{
            TestResult, TestTables, assert_batches_sorted_eq, make_test_add,
            object_store::{
                drain_recorded_object_store_operations as drain_recorded_ops, recording_log_store,
            },
        },
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

            Ok((
                Self {
                    inner: snapshot,
                    config: Default::default(),
                    materialized_files: None,
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
            Self::try_new_with_snapshot(log_store.as_ref(), snapshot.into()).await
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

    async fn append_test_add_with_stats(
        table: &mut DeltaTable,
        path: &str,
        stats: &str,
    ) -> DeltaResult<Version> {
        let mut add = make_test_add(path, &[], 0);
        add.size = 1;
        add.stats = Some(stats.to_string());

        let version = CommitBuilder::default()
            .with_actions(vec![Action::Add(add)])
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

    async fn selective_stats_table() -> DeltaResult<(TempDir, DeltaTable)> {
        let table_dir = tempfile::tempdir().unwrap();
        let table_url = url::Url::from_directory_path(table_dir.path()).unwrap();
        let mut table = DeltaTable::try_from_url(table_url)
            .await?
            .create()
            .with_columns([
                StructField::new("value", DataType::Primitive(PrimitiveType::Long), true),
                StructField::new("other", DataType::Primitive(PrimitiveType::Long), true),
            ])
            .with_configuration_property(TableProperty::DataSkippingNumIndexedCols, Some("2"))
            .await?;

        let custom_stats = r#"{"numRecords":3,"minValues":{"value":1,"other":10},"maxValues":{"value":3,"other":30},"nullCount":{"value":0,"other":0}}"#;
        append_test_add_with_stats(&mut table, "part-00000.snappy.parquet", custom_stats).await?;

        Ok((table_dir, table))
    }

    fn field_count(batch: &RecordBatch, name: &str) -> usize {
        batch
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name() == name)
            .count()
    }

    fn stats_parsed_field_names(batch: &RecordBatch) -> Vec<String> {
        let schema = batch.schema();
        let stats_field = schema
            .field_with_name("stats_parsed")
            .expect("stats_parsed field should exist");
        let ArrowDataType::Struct(fields) = stats_field.data_type() else {
            panic!("stats_parsed should be a struct");
        };
        fields.iter().map(|field| field.name().clone()).collect()
    }

    fn nested_stats_field_names(batch: &RecordBatch, field_name: &str) -> Vec<String> {
        let schema = batch.schema();
        let stats_field = schema
            .field_with_name("stats_parsed")
            .expect("stats_parsed field should exist");
        let ArrowDataType::Struct(stats_fields) = stats_field.data_type() else {
            panic!("stats_parsed should be a struct");
        };
        let (_, field) = stats_fields
            .find(field_name)
            .unwrap_or_else(|| panic!("{field_name} should exist"));
        let ArrowDataType::Struct(fields) = field.data_type() else {
            panic!("{field_name} should be a struct");
        };
        fields.iter().map(|field| field.name().clone()).collect()
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

    fn legacy_eager_snapshot_payload(snapshot: &EagerSnapshot) -> serde_json::Value {
        let mut snapshot_value = serde_json::to_value(snapshot.snapshot()).unwrap();
        let snapshot_fields = snapshot_value
            .as_array_mut()
            .expect("snapshot serde should use a sequence");
        snapshot_fields.pop();

        let materialized_files = snapshot
            .snapshot
            .materialized_files()
            .expect("expected materialized files for legacy eager snapshot payload");
        let bytes = if materialized_files.batches.is_empty() {
            Vec::new()
        } else {
            let mut buffer = vec![];
            let mut writer =
                FileWriter::try_new(&mut buffer, materialized_files.batches[0].schema().as_ref())
                    .unwrap();
            for batch in materialized_files.batches.iter() {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
            drop(writer);
            buffer
        };

        json!([snapshot_value, bytes])
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
            "+---------------------------------------------------------------------+------+------------------+-------+----------------+---------------------------------------------------------------------------------------------+----------------+",
            "| path                                                                | size | modificationTime | stats | deletionVector | fileConstantValues                                                                          | stats_parsed   |",
            "+---------------------------------------------------------------------+------+------------------+-------+----------------+---------------------------------------------------------------------------------------------+----------------+",
            "| part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet | 262  | 1587968626000    |       |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: , clusteringProvider: } | {numRecords: } |",
            "| part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet | 262  | 1587968602000    |       |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: , clusteringProvider: } | {numRecords: } |",
            "| part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet | 429  | 1587968602000    |       |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: , clusteringProvider: } | {numRecords: } |",
            "| part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet | 429  | 1587968602000    |       |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: , clusteringProvider: } | {numRecords: } |",
            "| part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet | 429  | 1587968602000    |       |                | {partitionValues: {}, baseRowId: , defaultRowCommitVersion: , tags: , clusteringProvider: } | {numRecords: } |",
            "+---------------------------------------------------------------------+------+------------------+-------+----------------+---------------------------------------------------------------------------------------------+----------------+",
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

    #[tokio::test]
    async fn test_snapshot_try_log_data_requires_materialized_files() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;

        assert!(matches!(
            snapshot.try_log_data(),
            Err(DeltaTableError::NotInitializedWithFiles(_))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_log_data_is_non_panicking_without_files() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };

        let snapshot = EagerSnapshot::try_new(&log_store, config, None).await?;

        assert_eq!(snapshot.log_data().num_files(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_try_log_data_is_non_panicking_without_files() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };

        let snapshot = EagerSnapshot::try_new(&log_store, config, None).await?;

        assert_eq!(snapshot.try_log_data()?.num_files(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_roundtrip_preserves_materialized_state() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?)
            .ensure_materialized_files(&log_store)
            .await?;

        let bytes = serde_json::to_vec(snapshot.as_ref())?;
        let actual: Snapshot = serde_json::from_slice(&bytes)?;

        assert!(actual.materialized_files().is_some());
        assert_eq!(
            actual.try_log_data()?.num_files(),
            snapshot.try_log_data()?.num_files()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_roundtrip_preserves_empty_materialized_state() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns([StructField::new(
                "id",
                DataType::Primitive(PrimitiveType::Integer),
                true,
            )])
            .await?;
        let log_store = table.log_store();
        let snapshot =
            Arc::new(Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?)
                .ensure_materialized_files(log_store.as_ref())
                .await?;

        assert!(snapshot.materialized_files().is_some());
        assert_eq!(snapshot.try_log_data()?.num_files(), 0);

        let bytes = serde_json::to_vec(snapshot.as_ref())?;
        let actual: Snapshot = serde_json::from_slice(&bytes)?;

        assert!(actual.materialized_files().is_some());
        assert_eq!(actual.try_log_data()?.num_files(), 0);

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
    async fn test_eager_snapshot_try_new_with_snapshot_is_zero_io_when_materialized() -> TestResult
    {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let snapshot =
            Arc::new(Snapshot::try_new(log_store.as_ref(), Default::default(), Some(12)).await?)
                .ensure_materialized_files(log_store.as_ref())
                .await?;
        drain_recorded_ops(&mut operations).await;

        let eager =
            EagerSnapshot::try_new_with_snapshot(log_store.as_ref(), snapshot.clone()).await?;
        let constructor_ops = drain_recorded_ops(&mut operations).await;

        assert!(
            constructor_ops.is_empty(),
            "expected zero IO, got {constructor_ops:?}"
        );
        assert!(Arc::ptr_eq(&snapshot, &eager.snapshot));

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_serde_is_wrapper_only() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = EagerSnapshot::try_new(&log_store, Default::default(), None).await?;

        let value = serde_json::to_value(&snapshot)?;
        let elements = value.as_array().expect("expected eager snapshot sequence");
        assert_eq!(elements.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_snapshot_legacy_serde_preserves_materialized_state() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = EagerSnapshot::try_new(&log_store, Default::default(), None).await?;
        let legacy = legacy_eager_snapshot_payload(&snapshot);

        let actual: EagerSnapshot = serde_json::from_value(legacy)?;

        assert_eq!(
            actual.log_data().num_files(),
            snapshot.log_data().num_files()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_file_views_skip_stats_same_paths() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let skip_cfg = DeltaTableConfig {
            skip_stats: true,
            ..Default::default()
        };
        let with_skip = EagerSnapshot::try_new(base.as_ref(), skip_cfg, Some(12)).await?;
        let full = EagerSnapshot::try_new(base.as_ref(), Default::default(), Some(12)).await?;
        let mut paths_skip: Vec<String> = with_skip
            .file_views(base.as_ref(), None)
            .map_ok(|v| v.path().to_string())
            .try_collect()
            .await?;
        let mut paths_full: Vec<String> = full
            .file_views(base.as_ref(), None)
            .map_ok(|v| v.path().to_string())
            .try_collect()
            .await?;
        paths_skip.sort();
        paths_full.sort();
        assert_eq!(paths_skip, paths_full);
        Ok(())
    }

    #[tokio::test]
    async fn test_skip_stats_leaves_stats_parsed_null() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;

        let default_eager =
            EagerSnapshot::try_new(base.as_ref(), Default::default(), Some(12)).await?;
        let default_stats: Vec<bool> = default_eager
            .file_views(base.as_ref(), None)
            .map_ok(|view| view.stats().is_some())
            .try_collect()
            .await?;
        assert!(!default_stats.is_empty());
        assert!(default_stats.iter().any(|b| *b));

        let skip_cfg = DeltaTableConfig {
            skip_stats: true,
            ..Default::default()
        };
        let skip_eager = EagerSnapshot::try_new(base.as_ref(), skip_cfg, Some(12)).await?;
        let skip_stats: Vec<Option<String>> = skip_eager
            .file_views(base.as_ref(), None)
            .map_ok(|view| view.stats())
            .try_collect()
            .await?;
        assert!(!skip_stats.is_empty());
        assert!(skip_stats.iter().all(|s| s.is_none()));

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_stats_with_predicate_keeps_stats_omitted() -> TestResult {
        use delta_kernel::expressions::Scalar;

        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;

        let skip_cfg = DeltaTableConfig {
            skip_stats: true,
            ..Default::default()
        };
        let snapshot = Snapshot::try_new(base.as_ref(), skip_cfg, Some(12)).await?;

        let predicate: PredicateRef =
            Arc::new(Expression::column(["value"]).gt(Scalar::String("".to_string())));

        let stats: Vec<Option<String>> = snapshot
            .file_views(base.as_ref(), Some(predicate))
            .map_ok(|view| view.stats())
            .try_collect()
            .await?;

        assert!(!stats.is_empty());
        assert!(stats.iter().all(|stats| stats.is_none()));

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_skip_stats_with_predicate_keeps_stats_omitted() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;

        let skip_cfg = DeltaTableConfig {
            skip_stats: true,
            ..Default::default()
        };
        let snapshot = EagerSnapshot::try_new(base.as_ref(), skip_cfg, Some(12)).await?;

        let predicate: PredicateRef =
            Arc::new(Expression::column(["value"]).gt(Scalar::String("".to_string())));

        let stats: Vec<Option<String>> = snapshot
            .file_views(base.as_ref(), Some(predicate))
            .map_ok(|view| view.stats())
            .try_collect()
            .await?;

        assert!(!stats.is_empty());
        assert!(stats.iter().all(|stats| stats.is_none()));

        Ok(())
    }

    #[tokio::test]
    async fn snapshot_no_predicate_scan_preserves_raw_stats_but_keeps_narrow_parsed_stats()
    -> TestResult {
        let (_table_dir, table) = selective_stats_table().await?;
        let log_store = table.log_store();
        let snapshot = Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?;

        let batches: Vec<_> = snapshot
            .files(log_store.as_ref(), None)
            .try_collect()
            .await?;

        assert!(!batches.is_empty());
        assert!(
            batches
                .iter()
                .all(|batch| batch.column_by_name("stats").is_some())
        );
        assert!(
            batches
                .iter()
                .all(|batch| field_count(batch, "stats_parsed") == 1)
        );
        assert!(
            batches
                .iter()
                .all(|batch| stats_parsed_field_names(batch) == vec!["numRecords"])
        );

        let file = LogicalFileView::new(batches[0].clone(), 0);
        assert_eq!(file.num_records(), Some(3));

        let add = file.to_add();
        let stats = add
            .stats
            .expect("raw stats should be preserved for Add compatibility");
        let stats: serde_json::Value = serde_json::from_str(&stats)?;
        assert_eq!(stats["minValues"]["value"], json!(1));
        assert_eq!(stats["maxValues"]["other"], json!(30));
        assert_eq!(stats["nullCount"]["value"], json!(0));

        Ok(())
    }

    #[tokio::test]
    async fn eager_snapshot_materialized_files_keep_full_parsed_stats() -> TestResult {
        let (_table_dir, table) = selective_stats_table().await?;
        let eager =
            EagerSnapshot::try_new(table.log_store().as_ref(), Default::default(), None).await?;

        let log_data = eager.log_data();
        let files = log_data.iter().collect::<Vec<_>>();
        assert_eq!(files.len(), 1);

        let file = &files[0];
        assert_eq!(file.num_records(), Some(3));
        assert!(
            file.min_values().is_some(),
            "materialized compatibility data must retain parsed min stats"
        );
        assert!(
            file.max_values().is_some(),
            "materialized compatibility data must retain parsed max stats"
        );
        assert!(
            file.null_counts().is_some(),
            "materialized compatibility data must retain parsed null-count stats"
        );

        Ok(())
    }

    #[tokio::test]
    async fn snapshot_predicate_scan_projects_only_required_stats_fields() -> TestResult {
        let (_table_dir, table) = selective_stats_table().await?;
        let snapshot =
            Snapshot::try_new(table.log_store().as_ref(), Default::default(), None).await?;
        let predicate: PredicateRef = Arc::new(Expression::column(["value"]).eq(Scalar::Long(1)));

        let batches: Vec<_> = snapshot
            .files(table.log_store().as_ref(), Some(predicate))
            .try_collect()
            .await?;

        assert_eq!(batches.len(), 1);
        assert!(batches[0].column_by_name("stats").is_some());
        assert_eq!(field_count(&batches[0], "stats_parsed"), 1);
        assert_eq!(
            stats_parsed_field_names(&batches[0]),
            vec!["numRecords", "nullCount", "minValues", "maxValues"]
        );
        assert_eq!(
            nested_stats_field_names(&batches[0], "minValues"),
            vec!["value"]
        );
        assert_eq!(
            nested_stats_field_names(&batches[0], "maxValues"),
            vec!["value"]
        );
        assert_eq!(
            nested_stats_field_names(&batches[0], "nullCount"),
            vec!["value"]
        );

        let add = LogicalFileView::new(batches[0].clone(), 0).to_add();
        let stats = add
            .stats
            .expect("raw stats should be preserved for Add compatibility");
        let stats: serde_json::Value = serde_json::from_str(&stats)?;
        assert_eq!(stats["minValues"]["other"], json!(10));

        Ok(())
    }

    #[test]
    fn test_materialized_files_full_table_seed_shares_batches() {
        let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
        let materialized_files = MaterializedFiles::full(7, vec![batch]);

        let seed = materialized_files
            .full_table_seed()
            .expect("full-table materialized state should be reusable");

        assert_eq!(seed.version, materialized_files.version);
        assert_eq!(
            seed.existing_predicate,
            materialized_files.existing_predicate
        );
        assert!(Arc::ptr_eq(&seed.batches, &materialized_files.batches));
        assert_eq!(
            seed.into_iter().collect::<Vec<_>>(),
            materialized_files.batches.as_ref()
        );
    }

    #[tokio::test]
    async fn test_eager_file_views_reuses_materialized_files_same_version() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), Some(12)).await?;

        drain_recorded_ops(&mut operations).await;

        let _: Vec<_> = snapshot
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected file_views() to reuse materialized state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_file_views_reuses_materialized_files_after_update() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let mut snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), Some(11)).await?;
        snapshot.update(log_store.as_ref(), Some(12)).await?;

        drain_recorded_ops(&mut operations).await;

        let _: Vec<_> = snapshot
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected updated file_views() to reuse materialized state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_without_materialized_files_replays_log() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let snapshot = Snapshot::try_new(log_store.as_ref(), Default::default(), Some(12)).await?;

        drain_recorded_ops(&mut operations).await;

        let _: Vec<_> = snapshot
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert!(
            replay_ops.iter().any(|op| op.is_log_replay_read()),
            "expected plain Snapshot::file_views() to replay log data, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_eager_update_preserves_materialized_files() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let mut snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), Some(12)).await?;

        drain_recorded_ops(&mut operations).await;

        assert!(snapshot.update(log_store.as_ref(), Some(11)).await.is_err());
        assert_eq!(snapshot.version(), 12);

        drain_recorded_ops(&mut operations).await;

        let file_views: Vec<_> = snapshot
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert_eq!(file_views.len(), 12);
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected failed update to preserve materialized state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_update_without_initial_files_does_not_create_partial_cache() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let config = DeltaTableConfig {
            require_files: false,
            ..Default::default()
        };
        let mut snapshot = EagerSnapshot::try_new(log_store.as_ref(), config, Some(11)).await?;

        assert!(snapshot.snapshot().materialized_files().is_none());

        snapshot.update(log_store.as_ref(), Some(12)).await?;

        assert!(snapshot.snapshot().materialized_files().is_none());

        drain_recorded_ops(&mut operations).await;

        let file_views: Vec<_> = snapshot
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert_eq!(file_views.len(), 12);
        assert!(
            replay_ops.iter().any(|op| op.is_log_replay_read()),
            "expected file_views() without cached files to replay log state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_file_views_reuses_materialized_files_with_partition_predicate() -> TestResult
    {
        let base = TestTables::Delta0_8_0Partitioned
            .table_builder()?
            .build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let snapshot = Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?;
        let eager = EagerSnapshot::try_new(log_store.as_ref(), Default::default(), None).await?;

        let predicate = Arc::new(to_kernel_predicate(
            &[
                PartitionFilter::try_from(("month", "=", "2"))?,
                PartitionFilter::try_from(("year", "=", "2020"))?,
            ],
            eager.schema().as_ref(),
        )?);

        let expected_paths: Vec<_> = snapshot
            .file_views(log_store.as_ref(), Some(predicate.clone()))
            .map_ok(|view| view.path_raw().to_string())
            .try_collect()
            .await?;
        assert_eq!(
            expected_paths,
            vec![
                "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet".to_string(),
                "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet".to_string(),
            ]
        );

        drain_recorded_ops(&mut operations).await;

        let eager_paths: Vec<_> = eager
            .file_views(log_store.as_ref(), Some(predicate))
            .map_ok(|view| view.path_raw().to_string())
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert_eq!(eager_paths, expected_paths);
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected predicate file_views() to reuse materialized state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_eager_file_views_reuses_materialized_files_with_data_predicate() -> TestResult {
        let (_table_dir, table) = selective_stats_table().await?;
        let (log_store, mut operations) = recording_log_store(table.log_store());

        let eager = EagerSnapshot::try_new(log_store.as_ref(), Default::default(), None).await?;
        drain_recorded_ops(&mut operations).await;

        let predicate: PredicateRef = Arc::new(Expression::column(["value"]).eq(Scalar::Long(1)));
        let files: Vec<_> = eager
            .file_views(log_store.as_ref(), Some(predicate))
            .try_collect()
            .await?;
        let replay_ops = drain_recorded_ops(&mut operations).await;

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].num_records(), Some(3));
        assert!(files[0].min_values().is_some());
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected data-predicate file_views() to reuse materialized state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
            .update(log_store.engine(None), Some(version))
            .await?;
        assert_eq!(updated.version(), version);
        assert_eq!(updated.checkpoint_version(), Some(version));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

        snapshot.update(log_store.as_ref(), Some(version)).await?;
        assert_eq!(snapshot.version(), version);
        assert_eq!(snapshot.snapshot.checkpoint_version(), Some(version));
        assert_eq!(
            eager_file_paths(&snapshot, log_store.as_ref()).await?,
            expected_paths
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
    async fn test_snapshot_update_latest_same_version_without_changes_reuses_arc() -> TestResult {
        let log_store = TestTables::Checkpoints.table_builder()?.build_storage()?;
        let snapshot =
            Arc::new(Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?)
                .ensure_materialized_files(log_store.as_ref())
                .await?;
        let prior_snapshot = snapshot.clone();
        let prior_materialized = snapshot
            .materialized_files()
            .cloned()
            .expect("expected materialized files");

        let updated = snapshot.update(log_store.engine(None), None).await?;

        assert!(Arc::ptr_eq(&updated, &prior_snapshot));
        assert!(Arc::ptr_eq(
            updated
                .materialized_files()
                .expect("expected materialized files"),
            &prior_materialized
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_eager_snapshot_same_version_checkpoint_refresh_is_idempotent() -> TestResult {
        let (_dir, table) = checkpoint_rebase_table().await?;
        let version = table.version().unwrap();
        let log_store = table.log_store();
        let mut snapshot =
            EagerSnapshot::try_new(log_store.as_ref(), Default::default(), Some(version)).await?;

        checkpoints::create_checkpoint(&table, None).await?;
        snapshot.update(log_store.as_ref(), Some(version)).await?;
        assert_eq!(snapshot.snapshot.checkpoint_version(), Some(version));

        let prior_snapshot = snapshot.snapshot.clone();
        let prior_materialized = snapshot
            .snapshot
            .materialized_files()
            .cloned()
            .expect("expected materialized files");
        let prior_paths = eager_file_paths(&snapshot, log_store.as_ref()).await?;

        snapshot.update(log_store.as_ref(), Some(version)).await?;

        assert!(Arc::ptr_eq(&snapshot.snapshot, &prior_snapshot));
        assert!(Arc::ptr_eq(
            snapshot
                .snapshot
                .materialized_files()
                .expect("expected materialized files"),
            &prior_materialized
        ));
        assert_eq!(
            eager_file_paths(&snapshot, log_store.as_ref()).await?,
            prior_paths
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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
            .update(log_store.engine(None), Some(version))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Had a _last_checkpoint hint but didn't find any checkpoints"),
            "expected same-version checkpoint refresh to surface the kernel checkpoint error: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_cached_parsed_batches_short_circuit_guards() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;

        let plain = Snapshot::try_new(base.as_ref(), Default::default(), Some(12)).await?;
        assert!(plain.cached_parsed_batches().is_none());

        let eager = EagerSnapshot::try_new(base.as_ref(), Default::default(), Some(12)).await?;
        let cached_stream = eager
            .snapshot()
            .cached_parsed_batches()
            .expect("materialized full-table cache -> Some");
        let cached_batches: Vec<_> = cached_stream.try_collect().await?;
        let direct_batches = eager
            .snapshot()
            .materialized_files()
            .expect("materialized cache present")
            .batches
            .clone();
        assert_eq!(cached_batches.len(), direct_batches.len());
        for (a, b) in cached_batches.iter().zip(direct_batches.iter()) {
            assert_eq!(a.num_rows(), b.num_rows());
            assert_eq!(a.schema(), b.schema());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_file_views_no_predicate_matches_fresh_replay() -> TestResult {
        let base = TestTables::Checkpoints.table_builder()?.build_storage()?;

        let eager = EagerSnapshot::try_new(base.as_ref(), Default::default(), Some(12)).await?;
        let eager_paths: Vec<String> = eager
            .file_views(base.as_ref(), None)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect()
            .await?;

        let plain = Snapshot::try_new(base.as_ref(), Default::default(), Some(12)).await?;
        let plain_paths: Vec<String> = plain
            .file_views(base.as_ref(), None)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect()
            .await?;

        assert_eq!(
            eager_paths, plain_paths,
            "short-circuit cache replay must yield the same files in the same \
             order as a fresh kernel replay with predicate = None",
        );

        Ok(())
    }
}
