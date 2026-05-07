//! Datafusion TableProvider implementation for Delta Lake tables.
//!
//! <div class="warning">
//!
//! The table provider is based on Snapshots of a Delta Table. Therefore, it represents
//! a static view of the table at a specific point in time. Changes to the underlying
//! Delta Table after the snapshot was taken will not be reflected in queries executed.
//!
//! To work with a dynamic view of the table that reflects ongoing changes, consider using
//! the catalog abstractions in this crate, which provide a higher-level interface for managing
//! Delta Tables within DataFusion sessions.
//!
//! </div>
//!
//! # Overview
//!
//! The [`DeltaScan`] struct integrates Delta Tables with DataFusion by implementing the
//! `TableProvider` trait. It encapsulates all delta table-specific logic required to translate
//! DataFusion's logical and physical plans into operations on Delta Lake data. This includes
//! - table scans
//!
//! ## Table Scans
//!
//! Scanning a Delta Table involves two major steps:
//! - planning the physical data file reads based on Datafusion's abstractions
//! - applying Delta features by transforming the physical data into the table's logical schema
//!
use std::any::Any;
use std::collections::HashSet;
use std::{borrow::Cow, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{TableType, sink::DataSinkExec};
use datafusion::logical_expr::{TableProviderFilterPushDown, dml::InsertOp};
use datafusion::prelude::Expr;
use datafusion::{
    catalog::{Session, TableProvider},
    logical_expr::LogicalPlan,
    physical_plan::ExecutionPlan,
};
use delta_kernel::{Engine, table_configuration::TableConfiguration, table_features::TableFeature};
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

pub use self::scan::DeltaScanExec;
pub(crate) use self::scan::KernelScanPlan;
use self::scan::ProjectedScanContract;
use super::data_sink::DeltaDataSink;
use crate::DeltaTableError;
use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::engine::DataFusionEngine;
use crate::delta_datafusion::table_provider::TableProviderBuilder;
use crate::kernel::transaction::{PROTOCOL, TransactionError};
use crate::kernel::{EagerSnapshot, SendableScanMetadataStream, Snapshot};
use crate::logstore::LogStoreRef;
use crate::protocol::SaveMode;
use crate::table::normalize_table_url;

mod scan;

mod serde_file_id_set {
    use std::collections::HashSet;

    use serde::{Deserialize as _, Deserializer, Serializer};

    pub(crate) fn serialize<S>(set: &HashSet<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut values: Vec<_> = set.iter().collect();
        values.sort();
        serializer.collect_seq(values)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<HashSet<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<String>::deserialize(deserializer)?;
        Ok(values.into_iter().collect())
    }
}

/// Default column name for the file id column we add to files read from disk.
pub(crate) use crate::delta_datafusion::file_id::FILE_ID_COLUMN_DEFAULT;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum MissingFilePolicy {
    /// Return an error if any selected file IDs are not found in the scan.
    #[default]
    Error,
    /// Silently skip file IDs that are not found in the scan.
    /// Useful for operations like compaction where files may have been concurrently removed.
    #[allow(dead_code)]
    Ignore,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileSelection {
    #[serde(with = "serde_file_id_set")]
    file_ids: HashSet<String>,
    missing_file_policy: MissingFilePolicy,
}

impl FileSelection {
    /// Create a selection from pre-normalized file IDs.
    ///
    /// This constructor does not normalize or validate the input IDs.
    /// Callers with relative file paths or Add actions should prefer:
    /// - [`FileSelection::from_file_paths`]
    /// - [`FileSelection::from_adds`]
    /// - [`normalize_path_as_file_id`]
    #[cfg(test)]
    pub(crate) fn new(file_ids: impl IntoIterator<Item = String>) -> Self {
        Self {
            file_ids: file_ids.into_iter().collect(),
            missing_file_policy: MissingFilePolicy::Error,
        }
    }

    pub(crate) fn with_missing_file_policy(mut self, policy: MissingFilePolicy) -> Self {
        self.missing_file_policy = policy;
        self
    }

    pub(crate) fn from_file_paths(
        file_paths: impl IntoIterator<Item = impl AsRef<str>>,
        table_root_url: &Url,
    ) -> crate::DeltaResult<Self> {
        let table_root_url = ensure_table_root_url(table_root_url);
        let mut file_ids = HashSet::new();
        for path in file_paths {
            let file_id = normalize_path_as_file_id_with_table_root(
                path.as_ref(),
                &table_root_url,
                "file path",
            )?;
            file_ids.insert(file_id);
        }

        Ok(Self {
            file_ids,
            missing_file_policy: MissingFilePolicy::Error,
        })
    }

    pub(crate) fn from_adds(
        adds: impl IntoIterator<Item = crate::kernel::Add>,
        table_root_url: &Url,
    ) -> crate::DeltaResult<Self> {
        Self::from_file_paths(
            adds.into_iter()
                // Add.path is URI-decoded once, while scan file IDs are keyed by the
                // object-store/raw path form. Route through Path so literal '%' bytes in
                // partition directories are escaped back to the canonical file-id form.
                .map(|add| String::from(Path::from(add.path))),
            table_root_url,
        )
    }
}

pub(crate) fn ensure_table_root_url(table_root_url: &Url) -> Url {
    if table_root_url.path().ends_with('/') {
        table_root_url.clone()
    } else {
        let mut table_root_url = table_root_url.clone();
        table_root_url.set_path(&format!("{}/", table_root_url.path()));
        table_root_url
    }
}

pub(crate) fn canonical_table_root_identity(root: &url::Url) -> url::Url {
    let mut root = ensure_table_root_url(&normalize_table_url(root));
    let _ = root.set_username("");
    let _ = root.set_password(None);
    root.set_query(None);
    root.set_fragment(None);

    if root.scheme() == "file" {
        return canonical_local_table_root_url(&root).unwrap_or(root);
    }

    root
}

fn canonical_local_table_root_url(root: &url::Url) -> Option<url::Url> {
    let path = root.to_file_path().ok()?;
    let canonical_path = std::fs::canonicalize(path).ok()?;
    let canonical_root = Url::from_directory_path(canonical_path).ok()?;

    Some(ensure_table_root_url(&normalize_table_url(&canonical_root)))
}

pub(crate) fn redact_url_for_error(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_password(None);
    let _ = redacted.set_username("");
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

pub(crate) fn redact_url_str_for_error(urlish: &str) -> String {
    Url::parse(urlish).map_or_else(
        |_| {
            urlish
                .split(['?', '#'])
                .next()
                .unwrap_or(urlish)
                .to_string()
        },
        |url| redact_url_for_error(&url),
    )
}

fn normalize_path_as_file_id_with_table_root(
    path: &str,
    table_root_url: &Url,
    context: &'static str,
) -> crate::DeltaResult<String> {
    debug_assert!(
        table_root_url.path().ends_with('/'),
        "table_root_url must end with '/' before normalization"
    );

    let file_url = Url::parse(path)
        .or_else(|_| table_root_url.join(path))
        .map_err(|e| {
            DeltaTableError::InvalidTableLocation(format!(
                "Failed to normalize {context} '{}' against '{}': {e}",
                redact_url_str_for_error(path),
                redact_url_for_error(table_root_url),
            ))
        })?;

    Ok(file_url.to_string())
}

pub(crate) fn normalize_path_as_file_id(
    path: &str,
    table_root_url: &Url,
    context: &'static str,
) -> crate::DeltaResult<String> {
    let table_root_url = ensure_table_root_url(table_root_url);
    normalize_path_as_file_id_with_table_root(path, &table_root_url, context)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SnapshotWrapper {
    Snapshot(Arc<Snapshot>),
    EagerSnapshot(Arc<EagerSnapshot>),
}

impl From<Arc<Snapshot>> for SnapshotWrapper {
    fn from(snap: Arc<Snapshot>) -> Self {
        SnapshotWrapper::Snapshot(snap)
    }
}

impl From<Snapshot> for SnapshotWrapper {
    fn from(snap: Snapshot) -> Self {
        SnapshotWrapper::Snapshot(snap.into())
    }
}

impl From<Arc<EagerSnapshot>> for SnapshotWrapper {
    fn from(esnap: Arc<EagerSnapshot>) -> Self {
        SnapshotWrapper::EagerSnapshot(esnap)
    }
}

impl From<EagerSnapshot> for SnapshotWrapper {
    fn from(esnap: EagerSnapshot) -> Self {
        SnapshotWrapper::EagerSnapshot(esnap.into())
    }
}

impl SnapshotWrapper {
    pub(crate) fn table_configuration(&self) -> &TableConfiguration {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.table_configuration(),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.snapshot().table_configuration(),
        }
    }

    pub(crate) fn snapshot(&self) -> &Snapshot {
        match self {
            SnapshotWrapper::Snapshot(snap) => snap.as_ref(),
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.snapshot(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaScan {
    snapshot: SnapshotWrapper,
    config: DeltaScanConfig,
    scan_schema: SchemaRef,
    /// Provider/public schema, including configured file id capability when enabled.
    full_schema: SchemaRef,
    row_index_column: Option<String>,
    #[serde(skip)]
    file_skipping_predicate: Option<Vec<Expr>>,
    #[serde(skip)]
    log_store: Option<LogStoreRef>,
    #[serde(skip)]
    read_operation_id: Option<Uuid>,
    file_selection: Option<FileSelection>,
}

/// Deletion vector selection for one data file.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletionVectorSelection {
    /// Fully-qualified file URI.
    pub filepath: String,
    /// Row-level keep mask where `true` means keep and `false` means deleted.
    pub keep_mask: Vec<bool>,
}

impl DeltaScan {
    // create new delta scan
    pub fn new(snapshot: impl Into<SnapshotWrapper>, config: DeltaScanConfig) -> Result<Self> {
        let snapshot = snapshot.into();
        let scan_schema = config.table_schema(snapshot.table_configuration())?;
        let full_schema = if let Some(file_id_column) =
            config.provider_file_id_column(None, scan_schema.as_ref())
        {
            let mut fields = scan_schema.fields().to_vec();
            fields.push(crate::delta_datafusion::file_id::file_id_field(Some(
                file_id_column,
            )));
            Arc::new(Schema::new(fields))
        } else {
            scan_schema.clone()
        };
        Ok(Self {
            snapshot,
            config,
            scan_schema,
            full_schema,
            row_index_column: None,
            file_skipping_predicate: None,
            log_store: None,
            read_operation_id: None,
            file_selection: None,
        })
    }

    pub(crate) fn with_file_skipping_predicate(
        mut self,
        predicate: impl IntoIterator<Item = Expr>,
    ) -> Self {
        self.file_skipping_predicate = Some(predicate.into_iter().collect());
        self
    }

    pub(crate) fn with_file_selection(mut self, selection: FileSelection) -> Self {
        self.file_selection = Some(selection);
        self
    }

    pub(crate) fn with_row_index_column(mut self, column: impl ToString) -> Result<Self> {
        let column = column.to_string();
        if self.full_schema.field_with_name(&column).is_ok() {
            return Err(DataFusionError::Plan(format!(
                "DeltaScan row index column '{column}' conflicts with an existing scan column"
            )));
        }

        let mut fields = self.full_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            column.clone(),
            DataType::UInt64,
            false,
        )));
        self.full_schema = Arc::new(Schema::new(fields));
        self.row_index_column = Some(column);
        Ok(self)
    }

    /// Restrict reads to the provided add actions by normalizing them into a
    /// [`FileSelection`], so callers can pass kernel `Add`s directly.
    pub(crate) fn with_selected_adds(
        mut self,
        adds: impl IntoIterator<Item = crate::kernel::Add>,
    ) -> crate::DeltaResult<Self> {
        let table_root = self.snapshot.table_configuration().table_root().clone();
        self.file_selection = Some(FileSelection::from_adds(adds, &table_root)?);
        Ok(self)
    }

    /// Attach the runtime log store handle required for session setup on read paths and writes.
    pub(crate) fn with_log_store(mut self, log_store: impl Into<LogStoreRef>) -> Self {
        self.log_store = Some(log_store.into());
        self
    }

    /// Scope runtime object store registration to a specific operation's temporary copy when
    /// the caller needs operation local reads.
    pub(crate) fn with_operation_id(mut self, operation_id: Uuid) -> Self {
        self.read_operation_id = Some(operation_id);
        self
    }

    fn ensure_supported_reader_features(&self) -> std::result::Result<(), TransactionError> {
        match PROTOCOL.can_read_from_protocol(self.snapshot.snapshot().protocol()) {
            Ok(()) => Ok(()),
            Err(TransactionError::UnsupportedTableFeatures(features))
                if features.as_slice() == [TableFeature::ColumnMapping]
                    && self
                        .snapshot
                        .table_configuration()
                        .is_feature_enabled(&TableFeature::ColumnMapping) =>
            {
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn ensure_read_ready(&self, session: &dyn Session) -> Result<()> {
        self.ensure_supported_reader_features()
            .map_err(crate::DeltaTableError::from)?;
        if let Some(log_store) = &self.log_store {
            super::update_datafusion_session(session, log_store.as_ref(), self.read_operation_id)?;
        }
        Ok(())
    }

    /// Materialize deletion vector keep masks for files in this scan.
    ///
    /// The result is sorted lexicographically by filepath for deterministic ordering and includes
    /// only files that have deletion vectors.
    ///
    /// This API materializes all deletion vectors in memory.
    pub async fn deletion_vectors(
        &self,
        session: &dyn Session,
    ) -> Result<Vec<DeletionVectorSelection>> {
        self.ensure_read_ready(session)?;
        let engine = DataFusionEngine::new_from_session(session);

        let scan_plan = KernelScanPlan::try_new(
            self.snapshot.snapshot(),
            None,
            &[],
            &self.config,
            self.file_skipping_predicate.clone(),
        )?;

        let stream = self.scan_metadata_stream(&scan_plan, engine.clone());

        scan::replay_deletion_vectors(engine, &scan_plan, &self.config, stream).await
    }

    fn scan_metadata_stream(
        &self,
        scan_plan: &KernelScanPlan,
        engine: Arc<dyn Engine>,
    ) -> SendableScanMetadataStream {
        scan_plan
            .scan
            .scan_metadata_seeded(engine, self.snapshot.snapshot().materialized_files())
    }

    pub fn builder() -> TableProviderBuilder {
        TableProviderBuilder::new()
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.ensure_read_ready(session)?;
        let engine = DataFusionEngine::new_from_session(session);
        let contract = ProjectedScanContract::try_new(
            self.scan_schema.clone(),
            self.full_schema.clone(),
            &self.config,
            self.row_index_column.as_deref(),
            projection,
            filters,
        )?;
        let scan_plan = KernelScanPlan::try_new_with_contract(
            self.snapshot.snapshot(),
            contract,
            filters,
            &self.config,
            self.file_skipping_predicate.clone(),
        )?;

        let stream = self.scan_metadata_stream(&scan_plan, engine.clone());

        scan::execution_plan(
            &self.config,
            session,
            scan_plan,
            stream,
            engine,
            limit,
            self.file_selection.as_ref(),
        )
        .await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let log_store = self.log_store.clone().ok_or_else(|| {
            DataFusionError::Plan(
                "DeltaScan insert_into requires a runtime log_store handle".to_string(),
            )
        })?;

        super::update_datafusion_session(state, log_store.as_ref(), self.read_operation_id)?;

        let snapshot = match &self.snapshot {
            SnapshotWrapper::EagerSnapshot(esnap) => esnap.as_ref().clone(),
            SnapshotWrapper::Snapshot(snap) => {
                EagerSnapshot::try_new_with_snapshot(log_store.as_ref(), snap.clone()).await?
            }
        };

        let save_mode = match insert_op {
            InsertOp::Append => SaveMode::Append,
            InsertOp::Overwrite => SaveMode::Overwrite,
            InsertOp::Replace => {
                return Err(DataFusionError::Plan(
                    "Replace operation is not supported for DeltaScan".to_string(),
                ));
            }
        };

        let data_sink = DeltaDataSink::new(log_store, snapshot, save_mode);

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(data_sink),
            None,
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(scan::supports_filters_pushdown(
            filter,
            self.snapshot.table_configuration(),
            &self.config,
        ))
    }
}

#[cfg(test)]
pub(crate) fn test_multi_partitioned_override_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Arc::new(arrow::datatypes::Field::new(
            "letter",
            arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::UInt16),
                Box::new(arrow::datatypes::DataType::Utf8),
            ),
            true,
        )),
        Arc::new(arrow::datatypes::Field::new(
            "date",
            arrow::datatypes::DataType::Date32,
            true,
        )),
        Arc::new(arrow::datatypes::Field::new(
            "data",
            arrow::datatypes::DataType::Dictionary(
                Box::new(arrow::datatypes::DataType::UInt16),
                Box::new(arrow::datatypes::DataType::Binary),
            ),
            true,
        )),
        Arc::new(arrow::datatypes::Field::new(
            "number",
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        )),
    ]))
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Date32Array, Int32Array, Int64Array, StringArray, TimestampMillisecondArray},
        datatypes::{
            DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
        },
        record_batch::RecordBatch,
    };
    use arrow_array::{
        DictionaryArray, UInt16Array,
        builder::{BinaryDictionaryBuilder, StringDictionaryBuilder},
        types::UInt16Type,
    };
    use datafusion::{
        catalog::Session,
        datasource::MemTable,
        datasource::{
            physical_plan::{FileScanConfig, ParquetSource},
            source::DataSource,
        },
        error::DataFusionError,
        logical_expr::dml::InsertOp,
        physical_optimizer::pruning::PruningPredicate,
        physical_plan::{ExecutionPlanVisitor, collect_partitioned, visit_execution_plan},
        prelude::{col, lit},
    };
    use datafusion_datasource::file::FileSource as _;
    use datafusion_datasource::source::DataSourceExec;
    use futures::{StreamExt as _, TryStreamExt as _};
    use parquet::file::reader::FileReader as _;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::{
        fs::File,
        sync::{Arc, Mutex},
    };
    use url::Url;

    use super::*;
    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{DeltaScanConfig, session::create_session},
        kernel::{
            Action, DataType, EagerSnapshot, PrimitiveType, ProtocolInner, Snapshot, StructField,
            StructType,
        },
        logstore::get_actions,
        operations::create::CreateBuilder,
        test_utils::{
            TestResult, TestTables,
            object_store::{
                drain_recorded_object_store_operations as drain_recorded_ops, recording_log_store,
            },
            open_fs_path,
        },
    };

    #[test]
    fn test_canonical_table_root_identity_strips_username_query_and_fragment() {
        let url =
            Url::parse("https://urluser:urlpassword@example.com/path?token=abc#frag").unwrap();

        let canonical = canonical_table_root_identity(&url);

        assert_eq!(canonical.username(), "");
        assert!(canonical.password().is_none());
        assert!(canonical.query().is_none());
        assert!(canonical.fragment().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn test_canonical_table_root_identity_canonicalizes_symlink_equivalent_file_roots() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let real_table = tmp_dir.path().join("real").join("table");
        std::fs::create_dir_all(&real_table).unwrap();

        let link_table = tmp_dir.path().join("link").join("table");
        std::fs::create_dir_all(link_table.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&real_table, &link_table).unwrap();

        let real_url = Url::from_directory_path(&real_table).unwrap();
        let link_url = Url::from_directory_path(&link_table).unwrap();

        assert_ne!(
            normalize_table_url(&real_url),
            normalize_table_url(&link_url)
        );
        assert_eq!(
            canonical_table_root_identity(&real_url),
            canonical_table_root_identity(&link_url)
        );
    }

    /// Extracts fields from the parquet scan
    #[derive(Default)]
    struct DeltaScanVisitor {
        num_scanned: Option<usize>,
        total_bytes_scanned: Option<usize>,
    }

    impl DeltaScanVisitor {
        fn pre_visit_delta_scan(
            &mut self,
            delta_scan_exec: &scan::DeltaScanExec,
        ) -> Result<bool, DataFusionError> {
            let Some(metrics) = delta_scan_exec.metrics() else {
                return Ok(true);
            };

            self.num_scanned = metrics
                .sum_by_name("count_files_scanned")
                .map(|v| v.as_usize());

            Ok(true)
        }

        fn pre_visit_data_source(
            &mut self,
            datasource_exec: &DataSourceExec,
        ) -> Result<bool, DataFusionError> {
            let Some(scan_config) = datasource_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(true);
            };

            let pq_metrics = scan_config
                .metrics()
                .clone_inner()
                .sum_by_name("bytes_scanned");
            self.total_bytes_scanned = pq_metrics.map(|v| v.as_usize());

            Ok(true)
        }
    }

    impl ExecutionPlanVisitor for DeltaScanVisitor {
        type Error = DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if let Some(delta_scan_exec) = plan.as_any().downcast_ref::<scan::DeltaScanExec>() {
                return self.pre_visit_delta_scan(delta_scan_exec);
            };

            if let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
                return self.pre_visit_data_source(datasource_exec);
            }

            Ok(true)
        }
    }

    #[derive(Default)]
    struct ParquetPredicateVisitor {
        predicate: Option<String>,
        pruning_predicate: Option<String>,
    }

    impl ExecutionPlanVisitor for ParquetPredicateVisitor {
        type Error = DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(true);
            };
            let Some(scan_config) = datasource_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(true);
            };
            let Some(parquet_source) = scan_config
                .file_source
                .as_any()
                .downcast_ref::<ParquetSource>()
            else {
                return Ok(true);
            };
            let Some(predicate) = parquet_source.filter() else {
                return Ok(true);
            };

            let pruning_predicate = PruningPredicate::try_new(
                predicate.clone(),
                parquet_source.table_schema().table_schema().clone(),
            )?;
            self.predicate = Some(predicate.to_string());
            self.pruning_predicate = Some(pruning_predicate.predicate_expr().to_string());
            Ok(false)
        }
    }

    async fn create_in_memory_id_table() -> crate::DeltaResult<crate::DeltaTable> {
        let schema = StructType::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        )])?;
        crate::DeltaTable::new_in_memory()
            .create()
            .with_columns(schema.fields().cloned())
            .await
    }

    async fn create_in_memory_id_table_with_rows(
        values: Vec<i64>,
    ) -> crate::DeltaResult<crate::DeltaTable> {
        let table = create_in_memory_id_table().await?;
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "id",
                ArrowDataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(values))],
        )?;
        table.write(vec![batch]).await
    }

    async fn create_in_memory_id_table_with_reader_protocol(
        min_reader_version: i32,
    ) -> crate::DeltaResult<crate::DeltaTable> {
        let schema = StructType::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        )])?;
        crate::DeltaTable::new_in_memory()
            .create()
            .with_columns(schema.fields().cloned())
            .with_actions(vec![Action::Protocol(
                ProtocolInner::new(min_reader_version, 2).as_kernel(),
            )])
            .await
    }

    async fn build_insert_input(
        state: &dyn Session,
        schema: SchemaRef,
        values: Vec<i64>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])?;
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
        mem_table.scan(state, None, &[], None).await
    }

    #[derive(Debug)]
    struct RootRegistrationTrackingLogStore {
        inner: LogStoreRef,
        root_calls: Arc<Mutex<Vec<Option<Uuid>>>>,
    }

    #[async_trait::async_trait]
    impl crate::logstore::LogStore for RootRegistrationTrackingLogStore {
        fn name(&self) -> String {
            self.inner.name()
        }

        async fn refresh(&self) -> crate::DeltaResult<()> {
            self.inner.refresh().await
        }

        async fn read_commit_entry(
            &self,
            version: crate::kernel::Version,
        ) -> crate::DeltaResult<Option<bytes::Bytes>> {
            self.inner.read_commit_entry(version).await
        }

        async fn write_commit_entry(
            &self,
            version: crate::kernel::Version,
            commit_or_bytes: crate::logstore::CommitOrBytes,
            operation_id: Uuid,
        ) -> std::result::Result<(), TransactionError> {
            self.inner
                .write_commit_entry(version, commit_or_bytes, operation_id)
                .await
        }

        async fn abort_commit_entry(
            &self,
            version: crate::kernel::Version,
            commit_or_bytes: crate::logstore::CommitOrBytes,
            operation_id: Uuid,
        ) -> std::result::Result<(), TransactionError> {
            self.inner
                .abort_commit_entry(version, commit_or_bytes, operation_id)
                .await
        }

        async fn get_latest_version(
            &self,
            start_version: crate::kernel::Version,
        ) -> crate::DeltaResult<crate::kernel::Version> {
            self.inner.get_latest_version(start_version).await
        }

        fn object_store(&self, operation_id: Option<Uuid>) -> Arc<dyn object_store::ObjectStore> {
            self.inner.object_store(operation_id)
        }

        fn root_object_store(
            &self,
            operation_id: Option<Uuid>,
        ) -> Arc<dyn object_store::ObjectStore> {
            self.root_calls.lock().unwrap().push(operation_id);
            self.inner.root_object_store(operation_id)
        }

        fn config(&self) -> &crate::logstore::LogStoreConfig {
            self.inner.config()
        }
    }

    #[tokio::test]
    async fn test_insert_into_append_works() -> TestResult {
        let table = create_in_memory_id_table().await?;
        let log_store = table.log_store();
        let provider = DeltaScan::builder()
            .with_log_store(log_store.clone())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let input = build_insert_input(&state, provider.schema(), vec![11, 13]).await?;

        let write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await?;
        let _write_batches: Vec<_> = collect_partitioned(write_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let read_provider = DeltaScan::builder().with_log_store(log_store).await?;
        session.register_table("delta_table", read_provider)?;
        let batches = session
            .sql("SELECT id FROM delta_table ORDER BY id")
            .await?
            .collect()
            .await?;
        let expected = vec!["+----+", "| id |", "+----+", "| 11 |", "| 13 |", "+----+"];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_overwrite_works() -> TestResult {
        let table = create_in_memory_id_table().await?;
        let log_store = table.log_store();

        let append_provider = DeltaScan::builder()
            .with_log_store(log_store.clone())
            .build()
            .await?;
        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let append_input = build_insert_input(&state, append_provider.schema(), vec![1, 2]).await?;
        let append_plan = append_provider
            .insert_into(&state, append_input, InsertOp::Append)
            .await?;
        let _append_batches: Vec<_> = collect_partitioned(append_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let overwrite_provider = DeltaScan::builder()
            .with_log_store(log_store.clone())
            .build()
            .await?;
        let overwrite_input =
            build_insert_input(&state, overwrite_provider.schema(), vec![11, 13]).await?;
        let overwrite_plan = overwrite_provider
            .insert_into(&state, overwrite_input, InsertOp::Overwrite)
            .await?;
        let _overwrite_batches: Vec<_> = collect_partitioned(overwrite_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let commit_bytes = log_store
            .read_commit_entry(2)
            .await?
            .expect("expected overwrite commit bytes at version 2");
        let overwrite_actions = get_actions(2, &commit_bytes)?;
        assert!(
            overwrite_actions
                .iter()
                .any(|action| matches!(action, Action::Remove(_))),
            "expected overwrite commit to contain at least one Remove action"
        );
        assert!(
            overwrite_actions
                .iter()
                .any(|action| matches!(action, Action::Add(_))),
            "expected overwrite commit to contain at least one Add action"
        );

        let read_provider = DeltaScan::builder().with_log_store(log_store).await?;
        session.register_table("delta_table", read_provider)?;
        let batches = session
            .sql("SELECT id FROM delta_table ORDER BY id")
            .await?
            .collect()
            .await?;
        let expected = vec!["+----+", "| id |", "+----+", "| 11 |", "| 13 |", "+----+"];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_dictionary_source_is_cast_to_table_schema() -> TestResult {
        let table = create_in_memory_id_table().await?;
        let log_store = table.log_store();
        let provider = DeltaScan::builder()
            .with_log_store(log_store.clone())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let dictionary_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Int64),
            ),
            true,
        )]));

        let dictionary_values = Int64Array::from(vec![11, 13]);
        let dictionary_keys = UInt16Array::from(vec![0, 1]);
        let dictionary_array =
            DictionaryArray::<UInt16Type>::new(dictionary_keys, Arc::new(dictionary_values));
        let dictionary_batch =
            RecordBatch::try_new(dictionary_schema.clone(), vec![Arc::new(dictionary_array)])?;

        let mem_table = MemTable::try_new(dictionary_schema, vec![vec![dictionary_batch]])?;
        let input = mem_table.scan(&state, None, &[], None).await?;
        let write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await?;
        let _write_batches: Vec<_> = collect_partitioned(write_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let read_provider = DeltaScan::builder().with_log_store(log_store).await?;
        session.register_table("delta_table", read_provider)?;
        let batches = session
            .sql("SELECT id FROM delta_table ORDER BY id")
            .await?
            .collect()
            .await?;
        let expected = vec!["+----+", "| id |", "+----+", "| 11 |", "| 13 |", "+----+"];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_rejects_replace() -> TestResult {
        let table = create_in_memory_id_table().await?;
        let provider = DeltaScan::builder()
            .with_log_store(table.log_store())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let input = build_insert_input(&state, provider.schema(), vec![1]).await?;

        let err = provider
            .insert_into(&state, input, InsertOp::Replace)
            .await
            .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("Replace"), "unexpected error: {err_str}");

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_requires_log_store() -> TestResult {
        let table = create_in_memory_id_table().await?;
        let snapshot = table.snapshot()?.snapshot().snapshot().clone();
        let provider = DeltaScan::builder().with_snapshot(snapshot).build().await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let input = build_insert_input(&state, provider.schema(), vec![1]).await?;

        let err = provider
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("log_store"), "unexpected error: {err_str}");

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_registers_operation_scoped_root_object_store() -> TestResult {
        let table = create_in_memory_id_table().await?;
        let root_calls = Arc::new(Mutex::new(Vec::new()));
        let operation_id = Uuid::new_v4();
        let tracked_log_store: LogStoreRef = Arc::new(RootRegistrationTrackingLogStore {
            inner: table.log_store(),
            root_calls: root_calls.clone(),
        });
        let provider = DeltaScan::builder()
            .with_log_store(tracked_log_store)
            .build()
            .await?
            .with_operation_id(operation_id);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let input = build_insert_input(&state, provider.schema(), vec![1]).await?;

        let _write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await?;

        assert!(
            root_calls.lock().unwrap().contains(&Some(operation_id)),
            "expected insert path to register the root object store with operation id {operation_id}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_serde_roundtrip_is_read_only() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store.clone())
            .build()
            .await?;

        let serialized = serde_json::to_vec(&provider)?;
        let decoded: DeltaScan = serde_json::from_slice(&serialized)?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let read_plan = decoded.scan(&state, None, &[], None).await?;
        let _read_batches: Vec<_> = collect_partitioned(read_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let input = build_insert_input(&state, decoded.schema(), vec![1]).await?;
        let err = decoded
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("log_store"), "unexpected error: {err_str}");

        Ok(())
    }

    #[tokio::test]
    async fn test_query_simple_table() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).await?;

        let session = Arc::new(create_session().into_inner());
        session.register_table("delta_table", provider).unwrap();

        let df = session.sql("SELECT * FROM delta_table").await.unwrap();
        let batches = df.collect().await?;

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_query_materialized_snapshot_avoids_log_replay() -> TestResult {
        let base = TestTables::Simple.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);
        let snapshot =
            Arc::new(Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?)
                .ensure_materialized_files(log_store.as_ref())
                .await?;

        drain_recorded_ops(&mut operations).await;

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store.clone())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let read_plan = provider.scan(&state, None, &[], None).await?;
        let _read_batches: Vec<_> = collect_partitioned(read_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let replay_ops = drain_recorded_ops(&mut operations).await;
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected materialized snapshot scan to avoid log replay, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_materialized_snapshot_serde_update_provider_preserves_reuse() -> TestResult {
        let base = TestTables::SimpleWithCheckpoint
            .table_builder()?
            .build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);

        let snapshot =
            Arc::new(Snapshot::try_new(log_store.as_ref(), Default::default(), Some(9)).await?)
                .ensure_materialized_files(log_store.as_ref())
                .await?;

        let bytes = serde_json::to_vec(snapshot.as_ref())?;
        let snapshot: Snapshot = serde_json::from_slice(&bytes)?;
        let snapshot = Arc::new(snapshot)
            .update(log_store.engine(None), Some(10))
            .await?;

        drain_recorded_ops(&mut operations).await;

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store.clone())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let read_plan = provider.scan(&state, None, &[], None).await?;
        let _read_batches: Vec<_> = collect_partitioned(read_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let replay_ops = drain_recorded_ops(&mut operations).await;
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "expected serde+update+provider scan to reuse materialized state, got {replay_ops:?}",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_projected_scan_preserves_filter_columns_for_parquet_pruning() -> TestResult {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("v1", ArrowDataType::Int64, true),
            ArrowField::new("v2", ArrowDataType::Int64, true),
            ArrowField::new("v3", ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![2, 2, 4])),
                Arc::new(Int64Array::from(vec![3, 5, 3])),
            ],
        )?;
        let table = crate::DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await?;
        let provider = DeltaScan::new(
            table.snapshot()?.snapshot().snapshot().clone(),
            DeltaScanConfig::default(),
        )?
        .with_log_store(table.log_store());

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let projection = vec![0];
        let filter = col("v2").eq(lit(2i64)).and(col("v3").eq(lit(3i64)));
        let plan = provider
            .scan(&state, Some(&projection), &[filter], None)
            .await?;

        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "v1");

        let mut visitor = ParquetPredicateVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor)?;

        let predicate = visitor
            .predicate
            .expect("projected scan should push a parquet predicate");
        assert!(
            predicate.contains("v2@1") && predicate.contains("v3@2"),
            "expected filter-only columns to retain stable predicate indices, got {predicate}",
        );

        let pruning_predicate = visitor
            .pruning_predicate
            .expect("projected scan should build a pruning predicate");
        for expected in ["v2_min", "v2_max", "v3_min", "v3_max"] {
            assert!(
                pruning_predicate.contains(expected),
                "expected pruning predicate to reference {expected}, got {pruning_predicate}",
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_simple_table() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let plan = provider.scan(&state, None, &[], None).await?;

        let batches: Vec<_> = collect_partitioned(plan.clone(), session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();

        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();

        assert_eq!(visitor.num_scanned, Some(5));
        assert_eq!(visitor.total_bytes_scanned, Some(231));

        let expected = vec![
            "+----+", "| id |", "+----+", "| 5  |", "| 7  |", "| 9  |", "+----+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_registers_log_store_for_fresh_session() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![11, 13]).await?;
        let provider = DeltaScan::builder()
            .with_log_store(table.log_store())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        session.register_table("delta_table", Arc::new(provider))?;

        let batches = session
            .sql("SELECT id FROM delta_table ORDER BY id")
            .await?
            .collect()
            .await?;
        let expected = vec!["+----+", "| id |", "+----+", "| 11 |", "| 13 |", "+----+"];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_direct_scan_with_log_store_registers_fresh_session() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![11, 13]).await?;
        let provider = DeltaScan::new(
            table.snapshot()?.snapshot().snapshot().clone(),
            DeltaScanConfig::default(),
        )?
        .with_log_store(table.log_store());

        let session = Arc::new(create_session().into_inner());
        session.register_table("delta_table", Arc::new(provider))?;

        let batches = session
            .sql("SELECT id FROM delta_table ORDER BY id")
            .await?
            .collect()
            .await?;
        let expected = vec!["+----+", "| id |", "+----+", "| 11 |", "| 13 |", "+----+"];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_direct_scan_rejects_unsupported_reader_protocol() -> TestResult {
        let table = create_in_memory_id_table_with_reader_protocol(2).await?;
        let provider = DeltaScan::new(
            table.snapshot()?.snapshot().snapshot().clone(),
            DeltaScanConfig::default(),
        )?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let err = provider
            .scan(&state, None, &[], None)
            .await
            .expect_err("unsupported reader protocol should fail before scan planning");
        assert!(
            err.to_string().contains("Unsupported"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_reads_only_selected_files() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.scan_builder().build()?.table_root().clone();
        let total_file_count = snapshot
            .file_views(log_store.as_ref(), None)
            .try_fold(0usize, |count, _| async move { Ok(count + 1) })
            .await?;
        assert_eq!(total_file_count, 5);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let baseline_provider = DeltaScan::builder()
            .with_snapshot(snapshot.clone())
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?;
        let baseline_plan = baseline_provider.scan(&state, None, &[], None).await?;

        let mut baseline_visitor = DeltaScanVisitor::default();
        visit_execution_plan(baseline_plan.as_ref(), &mut baseline_visitor).unwrap();
        let baseline_num_scanned = baseline_visitor.num_scanned.unwrap_or_default();
        assert_eq!(baseline_num_scanned, total_file_count);

        let selected_file_ids: Vec<String> = snapshot
            .file_views(log_store.as_ref(), None)
            .take(2)
            .map_ok(|view| table_root.join(view.path_raw()).unwrap().to_string())
            .try_collect()
            .await?;
        assert_eq!(selected_file_ids.len(), 2);

        let expected_selected_rows: usize = selected_file_ids
            .iter()
            .map(|file_id| {
                let file_url = Url::parse(file_id).expect("expected selected file id to be a URL");
                let parquet_path = file_url
                    .to_file_path()
                    .expect("expected selected file id to be a local file URL");
                let reader = SerializedFileReader::new(
                    File::open(parquet_path).expect("failed to open selected parquet file"),
                )
                .expect("failed to read selected parquet metadata");
                reader.metadata().file_metadata().num_rows() as usize
            })
            .sum();

        let selection = FileSelection::new(selected_file_ids.clone());
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_selection(selection);

        let plan = provider.scan(&state, None, &[], None).await?;

        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();

        assert_eq!(visitor.num_scanned, Some(selected_file_ids.len()));

        let batches: Vec<_> = collect_partitioned(plan.clone(), session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let returned_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(returned_rows, expected_selected_rows);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_from_file_paths_reads_selected_file() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.scan_builder().build()?.table_root().clone();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let selected_path = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .next()
            .unwrap();
        let selection = FileSelection::from_file_paths([selected_path.as_str()], &table_root)?;

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_selection(selection);

        let plan = provider.scan(&state, None, &[], None).await?;
        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();
        assert_eq!(visitor.num_scanned, Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_selected_adds_reads_file_with_encoded_partition_path() -> TestResult {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("modified", ArrowDataType::Utf8, true),
            ArrowField::new("country", ArrowDataType::Utf8, true),
            ArrowField::new("value", ArrowDataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-02",
                ])),
                Arc::new(StringArray::from(vec![
                    "Germany",
                    "China",
                    "Canada",
                    "Dominican Republic",
                ])),
                Arc::new(Int32Array::from(vec![1, 10, 20, 100])),
            ],
        )?;
        let table = crate::DeltaTable::new_in_memory()
            .write(vec![batch])
            .with_partition_columns(vec!["country"])
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await?;
        let log_store = table.log_store();
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);

        let views = snapshot
            .file_views(log_store.as_ref(), None)
            .try_collect::<Vec<_>>()
            .await?;
        let selected_add = views
            .into_iter()
            .find_map(|view| {
                let add = view.to_add();
                (add.partition_values.get("country") == Some(&Some("Dominican Republic".into())))
                    .then_some(add)
            })
            .expect("expected a file in the Dominican Republic partition");

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_log_store(log_store)
            .with_selected_adds([selected_add])?;

        let plan = provider.scan(&state, None, &[], None).await?;
        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();
        assert_eq!(visitor.num_scanned, Some(1));

        let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let returned_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(returned_rows, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_applies_deletion_vectors() -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.scan_builder().build()?.table_root().clone();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let (selected_file_url, expected_raw_rows, deleted_rows): (Url, usize, usize) = snapshot
            .file_views(log_store.as_ref(), None)
            .map_ok(|view| {
                view.deletion_vector_descriptor().map(|dv| {
                    (
                        table_root.join(view.path_raw()).unwrap(),
                        view.num_records()
                            .expect("expected numRecords stats for table-with-dv file"),
                        dv.cardinality as usize,
                    )
                })
            })
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .next()
            .expect("expected at least one file with a deletion vector");

        let parquet_path = selected_file_url.to_file_path().unwrap();
        let reader = SerializedFileReader::new(File::open(parquet_path)?)?;
        let raw_rows = reader.metadata().file_metadata().num_rows() as usize;
        assert_eq!(raw_rows, expected_raw_rows);
        assert!(deleted_rows > 0);

        let selection = FileSelection::new([selected_file_url.to_string()]);
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_selection(selection);

        let plan = provider.scan(&state, None, &[], None).await?;

        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();
        assert_eq!(visitor.num_scanned, Some(1));

        let batches: Vec<_> = collect_partitioned(plan.clone(), session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let returned_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let expected_returned_rows = expected_raw_rows
            .checked_sub(deleted_rows)
            .expect("deletion vector cardinality should not exceed file row count");
        assert_eq!(returned_rows, expected_returned_rows);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_strict_missing_files_errors() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.scan_builder().build()?.table_root().clone();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let mut selected_file_ids: Vec<String> = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| table_root.join(view.path_raw()).unwrap().to_string())
            .try_collect()
            .await?;
        selected_file_ids.push(
            "https://urluser:urlpassword@example.com/__does_not_exist__.parquet?token=urltoken"
                .to_string(),
        );

        let selection = FileSelection::new(selected_file_ids);
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_selection(selection);

        let err = provider.scan(&state, None, &[], None).await.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("File selection contains"),
            "unexpected error: {err_str}"
        );
        assert!(
            err_str.contains("missing files"),
            "unexpected error: {err_str}"
        );
        assert!(!err_str.contains("urluser"));
        assert!(!err_str.contains("urlpassword"));
        assert!(!err_str.contains("urltoken"));

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_missing_policy_ignore_skips_missing() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.scan_builder().build()?.table_root().clone();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let mut selected_file_ids: Vec<String> = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| table_root.join(view.path_raw()).unwrap().to_string())
            .try_collect()
            .await?;
        selected_file_ids.push(
            "https://example.com/__does_not_exist__.parquet?token=should_not_matter".to_string(),
        );

        let selection = FileSelection::new(selected_file_ids)
            .with_missing_file_policy(MissingFilePolicy::Ignore);
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_selection(selection);

        let plan = provider.scan(&state, None, &[], None).await?;
        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();
        assert_eq!(visitor.num_scanned, Some(1));

        Ok(())
    }

    #[test]
    fn test_file_selection_from_file_paths_normalizes_urls() -> TestResult {
        let cases = vec![
            (
                "file:///tmp/delta",
                "data/part-000.parquet",
                "file:///tmp/delta/data/part-000.parquet",
            ),
            (
                "file:///tmp/delta/",
                "file:///other/table/part-000.parquet",
                "file:///other/table/part-000.parquet",
            ),
            (
                "s3://my-bucket/warehouse/my_table",
                "part-00000-abc.snappy.parquet",
                "s3://my-bucket/warehouse/my_table/part-00000-abc.snappy.parquet",
            ),
            (
                "az://container/path/to/table/",
                "year=2024/part-00000.parquet",
                "az://container/path/to/table/year=2024/part-00000.parquet",
            ),
            (
                "gs://bucket/delta_table",
                "gs://bucket/delta_table/data/part-000.parquet",
                "gs://bucket/delta_table/data/part-000.parquet",
            ),
            (
                "file:///tmp/delta/",
                "data/part space😀.parquet",
                "file:///tmp/delta/data/part%20space%F0%9F%98%80.parquet",
            ),
        ];

        for (table_root, input, expected) in cases {
            let table_root = Url::parse(table_root).unwrap();
            let selection = FileSelection::from_file_paths([input], &table_root)?;
            assert!(selection.file_ids.contains(expected));
            assert_eq!(selection.missing_file_policy, MissingFilePolicy::Error);
        }

        Ok(())
    }

    #[test]
    fn test_file_selection_serde_is_deterministic() -> TestResult {
        let selection = FileSelection::new([
            "file:///tmp/delta/b.parquet".to_string(),
            "file:///tmp/delta/a.parquet".to_string(),
        ]);

        let value = serde_json::to_value(&selection)?;
        let file_ids = value
            .get("file_ids")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>();

        assert_eq!(
            file_ids,
            vec![
                "file:///tmp/delta/a.parquet".to_string(),
                "file:///tmp/delta/b.parquet".to_string(),
            ]
        );

        Ok(())
    }

    fn expected_dv_small()
    -> std::result::Result<Vec<DeletionVectorSelection>, Box<dyn std::error::Error>> {
        let filepath = Url::from_file_path(
            TestTables::WithDvSmall
                .as_path()
                .join("part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"),
        )
        .map_err(|_| "failed to convert expected file path to URL")?
        .to_string();
        Ok(vec![DeletionVectorSelection {
            filepath,
            keep_mask: vec![false, true, true, true, true, true, true, true, true, false],
        }])
    }

    #[tokio::test]
    async fn test_deletion_vectors_with_dv_table() -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let expected = expected_dv_small()?;

        let deletion_vectors = provider.deletion_vectors(&state).await?;
        let deletion_vectors_second = provider.deletion_vectors(&state).await?;

        assert_eq!(deletion_vectors, expected);
        assert_eq!(deletion_vectors_second, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_without_dv_table_is_empty() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let deletion_vectors = provider.deletion_vectors(&state).await?;

        assert!(deletion_vectors.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_registers_log_store_for_fresh_session() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![11, 13]).await?;
        let provider = DeltaScan::builder()
            .with_log_store(table.log_store())
            .build()
            .await?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let deletion_vectors = provider.deletion_vectors(&state).await?;
        assert!(deletion_vectors.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_reject_unsupported_reader_protocol() -> TestResult {
        let table = create_in_memory_id_table_with_reader_protocol(2).await?;
        let provider = DeltaScan::new(
            table.snapshot()?.snapshot().snapshot().clone(),
            DeltaScanConfig::default(),
        )?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let err = provider
            .deletion_vectors(&state)
            .await
            .expect_err("unsupported reader protocol should fail before deletion-vector reads");
        assert!(
            err.to_string().contains("Unsupported"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_with_eager_snapshot() -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder()?.build_storage()?;
        let eager = EagerSnapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::new(eager, DeltaScanConfig::default())?;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let expected = expected_dv_small()?;

        let deletion_vectors = provider.deletion_vectors(&state).await?;
        assert_eq!(deletion_vectors, expected);

        Ok(())
    }

    async fn provider_for_partitioned_table() -> TestResult<(
        crate::DeltaTable,
        Arc<crate::delta_datafusion::table_provider::next::DeltaScan>,
    )> {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await?;

        let provider = crate::delta_datafusion::table_provider::next::DeltaScan::new(
            table.snapshot().unwrap().snapshot().clone(),
            DeltaScanConfig::default().with_schema(test_multi_partitioned_override_schema()),
        )?
        .with_log_store(table.log_store());

        Ok((table, Arc::new(provider)))
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_scan() -> TestResult {
        let (_table, provider) = provider_for_partitioned_table().await?;

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider)?;

        let df = ctx.sql("SELECT number FROM test_table").await?;
        let batches = df.collect().await?;

        assert_eq!(batches[0].columns().len(), 1);
        assert_eq!(
            batches[0].schema().fields()[0].data_type(),
            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_filter() -> TestResult {
        let (_table, provider) = provider_for_partitioned_table().await?;

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider)?;

        let df = ctx
            .sql("SELECT * FROM test_table WHERE number < '1970-01-01T00:00:00.007'")
            .await?;
        let batches = df.collect().await?;

        assert_eq!(batches[0].num_rows(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_filter_aggregate() -> TestResult {
        let (_table, provider) = provider_for_partitioned_table().await?;

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider)?;
        let query = "SELECT count(1) c, max(number) fake_ts FROM test_table WHERE letter != 'a' and number < '2020-01-01T00:00:00Z'";
        let df = ctx.sql(query).await?;
        let batches = df.collect().await?;
        datafusion::assert_batches_eq!(
            [
                "+---+-------------------------+",
                "| c | fake_ts                 |",
                "+---+-------------------------+",
                "| 2 | 1970-01-01T00:00:00.007 |",
                "+---+-------------------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_insert() -> TestResult {
        let (_partitioned_table, provider) = provider_for_partitioned_table().await?;
        let logical_schema = provider.schema();

        let table_dir = tempfile::tempdir()?;
        let table = CreateBuilder::new()
            .with_location(table_dir.path().to_string_lossy())
            .with_columns(
                StructType::try_new(vec![
                    StructField::new(
                        "letter",
                        delta_kernel::schema::DataType::Primitive(
                            delta_kernel::schema::PrimitiveType::String,
                        ),
                        true,
                    ),
                    StructField::new("date", delta_kernel::schema::DataType::DATE, true),
                    StructField::new(
                        "data",
                        delta_kernel::schema::DataType::Primitive(
                            delta_kernel::schema::PrimitiveType::Binary,
                        ),
                        true,
                    ),
                    StructField::new(
                        "number",
                        delta_kernel::schema::DataType::Primitive(
                            delta_kernel::schema::PrimitiveType::Long,
                        ),
                        true,
                    ),
                ])?
                .fields()
                .cloned(),
            )
            .await?;

        let provider = Arc::new(
            crate::delta_datafusion::table_provider::next::DeltaScan::new(
                table.snapshot().unwrap().snapshot().clone(),
                DeltaScanConfig::default().with_schema(test_multi_partitioned_override_schema()),
            )?
            .with_log_store(table.log_store()),
        );

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider.clone())?;
        let state = ctx.state();

        let mut dict_builder = StringDictionaryBuilder::<UInt16Type>::new();
        dict_builder.append("a")?;
        let mut bin_builder = BinaryDictionaryBuilder::<UInt16Type>::new();
        bin_builder.append(b"hello")?;

        let batch = RecordBatch::try_new(
            logical_schema.clone(),
            vec![
                Arc::new(dict_builder.finish()),
                Arc::new(Date32Array::from(vec![0])),
                Arc::new(bin_builder.finish()),
                Arc::new(TimestampMillisecondArray::from(vec![2000])),
            ],
        )?;

        let mem_table = MemTable::try_new(logical_schema.clone(), vec![vec![batch]])?;
        let input = mem_table.scan(&state, None, &[], None).await?;

        let write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await?;

        let batches = collect_partitioned(write_plan, ctx.task_ctx()).await?;

        datafusion::assert_batches_eq!(
            [
                "+-------+",
                "| count |",
                "+-------+",
                "| 1     |",
                "+-------+",
            ],
            &batches[0]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_provider_schema_keeps_configured_file_id_capability() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await?;
        let provider = crate::delta_datafusion::table_provider::next::DeltaScan::new(
            table.snapshot()?.snapshot().clone(),
            DeltaScanConfig::default().with_file_column_name("my_files"),
        )?;

        let schema = provider.schema();
        assert!(schema.column_with_name("my_files").is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_file_column_projection() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await?;
        let provider = Arc::new(
            crate::delta_datafusion::table_provider::next::DeltaScan::new(
                table.snapshot()?.snapshot().clone(),
                DeltaScanConfig::default()
                    .with_schema(test_multi_partitioned_override_schema())
                    .with_file_column_name("my_files"),
            )?
            .with_log_store(table.log_store()),
        );

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider)?;

        let df = ctx
            .sql("SELECT * EXCEPT (my_files) FROM test_table")
            .await?;
        let batches = df.collect().await?;
        let schema = batches[0].schema();

        assert_eq!(schema.fields().len(), 4);
        assert!(schema.column_with_name("my_files").is_none(),);

        let df_file = ctx.sql("SELECT data, my_files FROM test_table").await?;
        let batches_file = df_file.collect().await?;
        let schema_file = batches_file[0].schema();

        assert_eq!(schema_file.fields().len(), 2);
        assert!(schema_file.column_with_name("my_files").is_some());

        Ok(())
    }
}
