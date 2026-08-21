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
use std::collections::HashSet;
use std::{borrow::Cow, fmt, sync::Arc};

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
use crate::kernel::{Add, EagerSnapshot, SendableScanMetadataStream, Snapshot};
use crate::logstore::LogStoreRef;
use crate::protocol::SaveMode;
use crate::table::normalize_table_url;

mod scan;

/// Default column name for the file id column we add to files read from disk.
pub(crate) use crate::delta_datafusion::file_id::FILE_ID_COLUMN_DEFAULT;

/// Policy for selected files that are not active in a scan snapshot.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum MissingSelectedFilePolicy {
    /// Return an error when a selected file is not active in the scan snapshot.
    #[default]
    Error,
    /// Skip selected files that are not active in the scan snapshot.
    ///
    /// Malformed paths, paths outside the table root, protocol failures, object store failures,
    /// and query planning failures still return errors.
    ///
    /// Useful for maintenance rewrites where removed files may be skipped. Use
    /// [`MissingSelectedFilePolicy::Error`] when every selected file must be present.
    Ignore,
}

/// File selection for a [`DeltaScan`] snapshot.
///
/// A selection limits a scan to explicit files, for example paths returned by
/// [`crate::DeltaTable::get_files_by_partitions`] or Add actions produced by maintenance tasks.
///
/// The input identifies files. Metadata comes from the scan snapshot, including
/// deletion vectors, partition values, statistics, column mapping, and tags.
///
/// Empty selections produce empty scans. Duplicate inputs are deduplicated after the scan snapshot
/// is known. Add inputs contribute only their path. The default policy returns an error for files
/// that are not active in the scan snapshot. [`MissingSelectedFilePolicy::Ignore`] skips those
/// files. Query pruning does not mark a selected file as missing.
///
/// Absolute URLs must be under the table root. Paths outside the table root are rejected during
/// scan planning with redacted error output.
/// Username, password, query, and fragment are stripped from URLs before storage.
///
/// Selections with paths are resolved against snapshot metadata before the data scan. Each scan
/// with a file selection requires that metadata pass.
#[derive(Clone, Serialize, Deserialize)]
pub struct FileSelection {
    paths: Vec<String>,
    missing_file_policy: MissingSelectedFilePolicy,
}

/// File selection resolved against the scan snapshot.
///
/// `FileSelection` stores caller input. `ResolvedFileSelection` stores canonical file IDs after
/// that input has been normalized and compared with active files from Delta Kernel scan metadata.
#[derive(Clone, Debug)]
struct ResolvedFileSelection {
    /// Canonical file IDs for selected files that are active in the scan snapshot.
    ///
    /// These values are full file URLs produced from snapshot metadata. They are used to filter
    /// Delta Kernel scan metadata before building the DataFusion scan.
    active_file_ids: HashSet<String>,
    /// Canonical file IDs requested by the user but not active in the scan snapshot.
    ///
    /// These values are produced from the input [`FileSelection`] before scan metadata replay.
    /// They are only returned as errors when [`MissingSelectedFilePolicy::Error`] is set.
    missing_file_ids: Vec<String>,
    /// Policy to apply when `missing_file_ids` is not empty.
    missing_file_policy: MissingSelectedFilePolicy,
}

impl FileSelection {
    /// Select files from Add actions.
    ///
    /// Only the path is used. File metadata is loaded from the scan snapshot.
    pub fn from_adds(adds: impl IntoIterator<Item = Add>) -> Self {
        Self {
            paths: adds.into_iter().map(add_path_for_selection).collect(),
            missing_file_policy: MissingSelectedFilePolicy::Error,
        }
    }

    /// Select files by path.
    ///
    /// Paths may be relative to the table root or absolute URLs under the same table root.
    /// Username, password, query, and fragment are stripped from URLs.
    /// Path validation happens when a scan is planned against a concrete table root.
    pub fn from_file_paths(paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            paths: paths
                .into_iter()
                .map(Into::into)
                .map(sanitize_selection_path)
                .collect(),
            missing_file_policy: MissingSelectedFilePolicy::Error,
        }
    }

    /// Set the policy for selected files that are not active in the scan snapshot.
    pub fn with_missing_file_policy(mut self, policy: MissingSelectedFilePolicy) -> Self {
        self.missing_file_policy = policy;
        self
    }

    fn resolve_input_file_ids(&self, table_root_url: &Url) -> crate::DeltaResult<HashSet<String>> {
        let table_root_url = ensure_table_root_url(table_root_url);
        self.paths
            .iter()
            .map(|path| {
                normalize_path_as_file_id_with_table_root(
                    path,
                    &table_root_url,
                    "selected file path",
                )
            })
            .collect()
    }
}

impl fmt::Debug for FileSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSelection")
            .field("paths", &redacted_paths_for_debug(&self.paths))
            .field("missing_file_policy", &self.missing_file_policy)
            .finish()
    }
}

fn redacted_paths_for_debug(paths: &[String]) -> Vec<String> {
    paths
        .iter()
        .map(|path| redact_url_str_for_error(path))
        .collect()
}

impl ResolvedFileSelection {
    fn new(
        active_file_ids: HashSet<String>,
        missing_file_ids: Vec<String>,
        missing_file_policy: MissingSelectedFilePolicy,
    ) -> Self {
        Self {
            active_file_ids,
            missing_file_ids,
            missing_file_policy,
        }
    }

    fn validate_missing(&self) -> Result<()> {
        if self.missing_file_policy == MissingSelectedFilePolicy::Ignore
            || self.missing_file_ids.is_empty()
        {
            return Ok(());
        }

        let missing_total = self.missing_file_ids.len();
        let missing: Vec<_> = self
            .missing_file_ids
            .iter()
            .take(10)
            .map(|id| redact_url_str_for_error(id))
            .collect();
        let extra = if missing_total > missing.len() {
            format!(" (and {} more)", missing_total - missing.len())
        } else {
            String::new()
        };
        Err(DataFusionError::Plan(format!(
            "File selection contains {missing_total} missing files (showing up to 10, redacted): {}{extra}",
            missing.join(", ")
        )))
    }
}

fn add_path_for_selection(add: Add) -> String {
    let path = add.path;
    if let Ok(mut url) = Url::parse(&path) {
        strip_url_sensitive_parts(&mut url);
        url.to_string()
    } else {
        String::from(Path::from(path))
    }
}

fn sanitize_selection_path(path: String) -> String {
    if let Ok(mut url) = Url::parse(&path) {
        strip_url_sensitive_parts(&mut url);
        url.to_string()
    } else {
        strip_unparsed_url_sensitive_parts(&path)
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
    strip_url_sensitive_parts(&mut root);

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

fn canonical_local_file_url(url: &url::Url) -> Option<url::Url> {
    if url.scheme() != "file" {
        return None;
    }
    let path = url.to_file_path().ok()?;
    let canonical_path = std::fs::canonicalize(path).ok()?;
    Url::from_file_path(canonical_path).ok()
}

fn strip_url_sensitive_parts(url: &mut Url) {
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
}

pub(crate) fn redact_url_for_error(url: &Url) -> String {
    let mut redacted = url.clone();
    strip_url_sensitive_parts(&mut redacted);
    redacted.to_string()
}

pub(crate) fn redact_url_str_for_error(urlish: &str) -> String {
    Url::parse(urlish).map_or_else(
        |_| strip_unparsed_url_sensitive_parts(urlish),
        |url| redact_url_for_error(&url),
    )
}

fn strip_unparsed_url_sensitive_parts(urlish: &str) -> String {
    let without_query = urlish
        .split(['?', '#'])
        .next()
        .unwrap_or(urlish)
        .to_string();
    // Redact authority for strings that Url cannot parse, including malformed URLs
    // and Windows style `scheme://userinfo@host\path` values.
    let Some(authority_start) = without_query.find("://").map(|idx| idx + 3) else {
        return without_query;
    };
    let authority_len = without_query[authority_start..]
        .find(['/', '\\'])
        .unwrap_or(without_query.len() - authority_start);
    let authority_end = authority_start + authority_len;
    let Some(userinfo_end) = without_query[authority_start..authority_end]
        .rfind('@')
        .map(|idx| authority_start + idx + 1)
    else {
        return without_query;
    };

    format!(
        "{}{}",
        &without_query[..authority_start],
        &without_query[userinfo_end..]
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

    if path.is_empty() {
        return Err(DeltaTableError::InvalidTableLocation(format!(
            "{context} is empty"
        )));
    }

    let file_url = Url::parse(path)
        .or_else(|_| table_root_url.join(path))
        .map_err(|e| {
            DeltaTableError::InvalidTableLocation(format!(
                "Failed to normalize {context} '{}' against '{}': {e}",
                redact_url_str_for_error(path),
                redact_url_for_error(table_root_url),
            ))
        })?;

    validate_selected_file_url_under_table_root(&file_url, table_root_url, context, path)
}

fn validate_selected_file_url_under_table_root(
    file_url: &Url,
    table_root_url: &Url,
    context: &'static str,
    original: &str,
) -> crate::DeltaResult<String> {
    let mut original_root_identity = ensure_table_root_url(&normalize_table_url(table_root_url));
    strip_url_sensitive_parts(&mut original_root_identity);
    let canonical_root_identity = canonical_table_root_identity(table_root_url);
    let root_identities = if canonical_root_identity == original_root_identity {
        vec![original_root_identity]
    } else {
        vec![original_root_identity, canonical_root_identity]
    };

    let mut file_identity = file_url.clone();
    strip_url_sensitive_parts(&mut file_identity);
    let mut file_identities = vec![file_identity.clone()];
    if let Some(canonical_file_identity) = canonical_local_file_url(&file_identity)
        && canonical_file_identity != file_identity
    {
        file_identities.push(canonical_file_identity);
    }

    let relative_path = file_identities
        .iter()
        .flat_map(|file_identity| {
            root_identities
                .iter()
                .map(move |root_identity| (file_identity, root_identity))
        })
        .find_map(|(file_identity, root_identity)| {
            let same_store = file_identity.scheme() == root_identity.scheme()
                && file_identity.host_str() == root_identity.host_str()
                && file_identity.port_or_known_default() == root_identity.port_or_known_default();
            same_store
                .then(|| file_identity.path().strip_prefix(root_identity.path()))
                .flatten()
        })
        .ok_or_else(|| {
            DeltaTableError::InvalidTableLocation(format!(
                "{context} '{}' is outside table root '{}'",
                redact_url_str_for_error(original),
                redact_url_for_error(&canonical_table_root_identity(table_root_url))
            ))
        })?;

    let mut normalized = table_root_url.join(relative_path).map_err(|err| {
        DeltaTableError::InvalidTableLocation(format!(
            "Failed to normalize {context} '{}' against '{}': {err}",
            redact_url_str_for_error(original),
            redact_url_for_error(table_root_url),
        ))
    })?;

    // File IDs do not carry query or fragment parts from the input table root.
    normalized.set_query(None);
    normalized.set_fragment(None);
    Ok(normalized.to_string())
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

/// An executable, serializable Delta table scan.
///
/// `DeltaScan` captures everything needed to read a consistent set of data files from a
/// table snapshot — the resolved schemas, optional file-skipping predicates, deletion-vector
/// aware file selection and the originating log store. It is the unit produced by
/// [`TableProviderBuilder`] and consumed by DataFusion's execution layer.
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
    /// Create a new scan over the given `snapshot` using `config`.
    ///
    /// Validates that the snapshot only uses reader features delta-rs supports and resolves
    /// the scan and provider (public) schemas, including the optional file-id column.
    pub fn new(snapshot: impl Into<SnapshotWrapper>, config: DeltaScanConfig) -> Result<Self> {
        let snapshot = snapshot.into();
        Self::validate_supported_reader_features(&snapshot)
            .map_err(crate::DeltaTableError::from)?;
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

    /// Restrict reads to Add action paths. File metadata comes from the scan snapshot.
    pub fn with_adds(self, adds: impl IntoIterator<Item = Add>) -> Self {
        self.with_file_selection(FileSelection::from_adds(adds))
    }

    /// Restrict reads to file paths.
    pub fn with_file_paths(self, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.with_file_selection(FileSelection::from_file_paths(paths))
    }

    /// Restrict reads to a file selection resolved against this scan's snapshot.
    ///
    /// Path validation runs during scan or deletion vector planning. Malformed paths, paths
    /// outside the table root, and missing selected files are reported there.
    pub fn with_file_selection(mut self, selection: FileSelection) -> Self {
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

    fn validate_supported_reader_features(
        snapshot: &SnapshotWrapper,
    ) -> std::result::Result<(), TransactionError> {
        match PROTOCOL.can_read_from_protocol(snapshot.snapshot().protocol()) {
            Ok(()) => Ok(()),
            Err(TransactionError::UnsupportedTableFeatures(features))
                if features.as_slice() == [TableFeature::ColumnMapping]
                    && snapshot
                        .table_configuration()
                        .is_feature_enabled(&TableFeature::ColumnMapping) =>
            {
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn ensure_read_ready(&self, session: &dyn Session) -> Result<()> {
        // DataFusion codec deserialization bypasses DeltaScan::new.
        // Keep protocol checks on read entry points.
        Self::validate_supported_reader_features(&self.snapshot)
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

        let resolved_file_selection = self
            .resolve_file_selection(self.file_selection.as_ref(), engine.clone())
            .await?;
        if resolved_file_selection
            .as_ref()
            .is_some_and(|selection| selection.active_file_ids.is_empty())
        {
            return Ok(Vec::new());
        }

        let stream = self.scan_metadata_stream(&scan_plan, engine.clone());
        scan::replay_deletion_vectors(
            engine,
            &scan_plan,
            &self.config,
            stream,
            resolved_file_selection.as_ref(),
        )
        .await
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

    async fn resolve_file_selection(
        &self,
        selection: Option<&FileSelection>,
        engine: Arc<dyn Engine>,
    ) -> Result<Option<ResolvedFileSelection>> {
        let Some(selection) = selection else {
            return Ok(None);
        };

        let scan_plan =
            KernelScanPlan::try_new(self.snapshot.snapshot(), None, &[], &self.config, None)?;
        let stream = self.scan_metadata_stream(&scan_plan, engine);

        scan::resolve_file_selection(selection, &scan_plan, stream)
            .await
            .map(Some)
    }

    /// Start building a scan/table provider with a fluent [`TableProviderBuilder`].
    pub fn builder() -> TableProviderBuilder {
        TableProviderBuilder::new()
    }
}

#[async_trait::async_trait]
impl TableProvider for DeltaScan {
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

        let resolved_file_selection = self
            .resolve_file_selection(self.file_selection.as_ref(), engine.clone())
            .await?;
        let stream = self.scan_metadata_stream(&scan_plan, engine.clone());

        scan::execution_plan(
            &self.config,
            session,
            scan_plan,
            stream,
            engine,
            limit,
            resolved_file_selection.as_ref(),
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
        array::{
            BooleanArray, Date32Array, Int32Array, Int64Array, StringArray,
            TimestampMillisecondArray,
        },
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
            TestResult, TestTables, make_test_add,
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

    #[test]
    fn test_redact_url_str_for_error_redacts_unparsed_authority() {
        let redacted = redact_url_str_for_error(
            "bad scheme://urluser:urlpassword@example.com\\table\\part.parquet?token=urltoken#frag",
        );

        assert_eq!(redacted, "bad scheme://example.com\\table\\part.parquet");
        for forbidden in ["urluser", "urlpassword", "urltoken", "frag"] {
            assert!(
                !redacted.contains(forbidden),
                "redacted unparsed URL contains {forbidden}: {redacted}"
            );
        }
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
            if let Some(delta_scan_exec) = plan.downcast_ref::<scan::DeltaScanExec>() {
                return self.pre_visit_delta_scan(delta_scan_exec);
            };

            if let Some(datasource_exec) = plan.downcast_ref::<DataSourceExec>() {
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
            let Some(datasource_exec) = plan.downcast_ref::<DataSourceExec>() else {
                return Ok(true);
            };
            let Some(scan_config) = datasource_exec
                .data_source()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(true);
            };
            let Some(parquet_source) = scan_config.file_source.downcast_ref::<ParquetSource>()
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

    async fn create_in_memory_id_table_with_unsupported_reader_protocol()
    -> crate::DeltaResult<crate::DeltaTable> {
        let schema = StructType::try_new(vec![StructField::new(
            "id".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        )])?;
        crate::DeltaTable::new_in_memory()
            .create()
            .with_columns(schema.fields().cloned())
            .with_actions(vec![Action::Protocol(
                ProtocolInner::new(3, 7)
                    .append_reader_features([TableFeature::TypeWidening])
                    .append_writer_features([TableFeature::TypeWidening])
                    .as_kernel(),
            )])
            .await
    }

    #[tokio::test]
    async fn test_direct_scan_rejects_unsupported_reader_protocol() -> TestResult {
        let table = create_in_memory_id_table_with_unsupported_reader_protocol().await?;
        let err = DeltaScan::new(
            table.snapshot()?.snapshot().snapshot().clone(),
            DeltaScanConfig::default(),
        )
        .unwrap_err();

        let err_str = err.to_string();
        assert!(
            err_str.contains("Unsupported table features"),
            "unexpected error: {err_str}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_rejects_unsupported_reader_protocol() -> TestResult {
        let table = create_in_memory_id_table_with_unsupported_reader_protocol().await?;
        let err = DeltaScan::builder()
            .with_snapshot(table.snapshot()?.snapshot().snapshot().clone())
            .build()
            .await
            .unwrap_err();

        let err_str = err.to_string();
        assert!(
            err_str.contains("Unsupported table features"),
            "unexpected error: {err_str}"
        );

        Ok(())
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
    async fn test_delta_scan_serde_accepts_missing_file_selection_field() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = DeltaScan::builder().with_snapshot(snapshot).build().await?;
        let mut serialized = serde_json::to_value(&provider)?;

        serialized
            .as_object_mut()
            .expect("serialized provider was not a JSON object")
            .remove("file_selection");

        let _decoded: DeltaScan = serde_json::from_value(serialized)?;

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
    async fn test_boolean_predicate_does_not_require_minmax_stats() -> TestResult {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, true),
            ArrowField::new("active", ArrowDataType::Boolean, true),
            ArrowField::new("value", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(StringArray::from(vec!["old-1", "old-2"])),
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
        session.register_table("delta_table", Arc::new(provider))?;

        let expected = vec![
            "+----+--------+-------+",
            "| id | active | value |",
            "+----+--------+-------+",
            "| 1  | true   | old-1 |",
            "+----+--------+-------+",
        ];

        let batches = session
            .sql("SELECT id, active, value FROM delta_table WHERE active = true")
            .await?
            .collect()
            .await?;
        assert_batches_sorted_eq!(&expected, &batches);

        let batches = session
            .sql("SELECT id, active, value FROM delta_table WHERE id = 1 AND active = true")
            .await?
            .collect()
            .await?;
        assert_batches_sorted_eq!(&expected, &batches);

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
    async fn test_scan_with_file_selection_reads_only_selected_files() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.inner.table_root().clone();
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

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_paths(selected_file_ids.clone());

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
        let selection = FileSelection::from_file_paths([selected_path.as_str()]);

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
    async fn test_scan_with_file_selection_files_uses_snapshot_metadata_for_mutated_add()
    -> TestResult {
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
        let mut selected_add = selected_add;
        selected_add
            .partition_values
            .insert("country".to_string(), Some("WRONG".to_string()));
        selected_add.size = 1;
        selected_add.stats = Some(r#"{"numRecords":999999}"#.to_string());
        selected_add.tags = Some(std::collections::HashMap::from([(
            "trusted".to_string(),
            Some("false".to_string()),
        )]));

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .build()
            .await?
            .with_log_store(log_store)
            .with_adds([selected_add]);

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
        let expected = vec![
            "+------------+--------------------+-------+",
            "| modified   | country            | value |",
            "+------------+--------------------+-------+",
            "| 2021-02-02 | Dominican Republic | 100   |",
            "+------------+--------------------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_applies_deletion_vectors() -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.inner.table_root().clone();

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

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_paths([selected_file_url.to_string()]);

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
    async fn test_scan_with_file_selection_mutated_add_uses_snapshot_deletion_vector_metadata()
    -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);

        let (mut selected_add, expected_raw_rows, deleted_rows) = snapshot
            .file_views(log_store.as_ref(), None)
            .map_ok(|view| {
                view.deletion_vector_descriptor().map(|dv| {
                    (
                        view.to_add(),
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
        selected_add.deletion_vector = None;
        selected_add.size = 1;
        selected_add.stats = Some(r#"{"numRecords":999999}"#.to_string());
        selected_add.tags = Some(std::collections::HashMap::from([(
            "trusted".to_string(),
            Some("false".to_string()),
        )]));

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .build()
            .await?
            .with_log_store(log_store)
            .with_adds([selected_add]);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let plan = provider.scan(&state, None, &[], None).await?;
        let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let returned_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(returned_rows, expected_raw_rows - deleted_rows);

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_strict_missing_files_errors() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.inner.table_root().clone();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let mut selected_file_ids: Vec<String> = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| table_root.join(view.path_raw()).unwrap().to_string())
            .try_collect()
            .await?;
        selected_file_ids.push(table_root.join("__does_not_exist__.parquet")?.to_string());

        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .build()
            .await?
            .with_file_paths(selected_file_ids);

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
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_missing_policy_ignore_skips_missing() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let table_root = snapshot.inner.table_root().clone();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let mut selected_file_ids: Vec<String> = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| table_root.join(view.path_raw()).unwrap().to_string())
            .try_collect()
            .await?;
        selected_file_ids.push(table_root.join("__does_not_exist__.parquet")?.to_string());

        let selection = FileSelection::from_file_paths(selected_file_ids)
            .with_missing_file_policy(MissingSelectedFilePolicy::Ignore);
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
    async fn test_scan_with_empty_file_selection_returns_empty_scan() -> TestResult {
        let base = TestTables::Simple.table_builder()?.build_storage()?;
        let (log_store, mut operations) = recording_log_store(base);
        let snapshot = Snapshot::try_new(log_store.as_ref(), Default::default(), None).await?;
        drain_recorded_ops(&mut operations).await;

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?
            .with_log_store(log_store)
            .with_file_paths(Vec::<String>::new());

        let plan = provider.scan(&state, None, &[], None).await?;
        let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let returned_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(returned_rows, 0);

        let replay_ops = drain_recorded_ops(&mut operations).await;
        assert!(
            replay_ops.iter().all(|op| !op.is_log_replay_read()),
            "empty file selection read log metadata: {replay_ops:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_file_selection_empty_file_path_errors() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![1, 2]).await?;
        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let provider = DeltaScan::builder()
            .with_log_store(table.log_store())
            .build()
            .await?
            .with_file_paths([""]);

        let err = provider.scan(&state, None, &[], None).await.unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("empty"), "unexpected error: {err_str}");

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_with_duplicate_file_selection_deduplicates() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![1, 2]).await?;
        let log_store = table.log_store();
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let selected_path = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .next()
            .unwrap();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store)
            .build()
            .await?
            .with_file_paths([selected_path.clone(), selected_path]);

        let plan = provider.scan(&state, None, &[], None).await?;
        let mut visitor = DeltaScanVisitor::default();
        visit_execution_plan(plan.as_ref(), &mut visitor).unwrap();
        assert_eq!(visitor.num_scanned, Some(1));

        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_root_file_selection_errors_under_ignore_policy() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![1, 2]).await?;
        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let wrong_root =
            "https://urluser:urlpassword@example.com/other/part.parquet?token=urltoken#frag";

        let provider = DeltaScan::builder()
            .with_log_store(table.log_store())
            .build()
            .await?
            .with_file_selection(
                FileSelection::from_file_paths([wrong_root])
                    .with_missing_file_policy(MissingSelectedFilePolicy::Ignore),
            );

        let err = provider.scan(&state, None, &[], None).await.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("outside table root"),
            "unexpected error: {err_str}"
        );
        assert!(!err_str.contains("urluser"));
        assert!(!err_str.contains("urlpassword"));
        assert!(!err_str.contains("urltoken"));
        assert!(!err_str.contains("frag"));

        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_root_file_selection_errors_under_default_policy() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![1, 2]).await?;
        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let wrong_root =
            "https://urluser:urlpassword@example.com/other/part.parquet?token=urltoken#frag";

        let provider = DeltaScan::builder()
            .with_log_store(table.log_store())
            .build()
            .await?
            .with_file_paths([wrong_root]);

        let err = provider.scan(&state, None, &[], None).await.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("outside table root"),
            "unexpected error: {err_str}"
        );
        assert!(!err_str.contains("urluser"));
        assert!(!err_str.contains("urlpassword"));
        assert!(!err_str.contains("urltoken"));
        assert!(!err_str.contains("frag"));

        Ok(())
    }

    #[tokio::test]
    async fn test_selected_active_file_pruned_by_data_skipping_returns_empty_scan() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![1, 2]).await?;
        let log_store = table.log_store();
        let snapshot = Arc::new(Snapshot::try_new(&log_store, Default::default(), None).await?);
        let selected_path = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .next()
            .unwrap();

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let provider = DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store)
            .with_file_skipping_predicates([col("id").eq(lit(999i64))])
            .build()
            .await?
            .with_file_paths([selected_path]);

        let plan = provider.scan(&state, None, &[], None).await?;
        let batches: Vec<_> = collect_partitioned(plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let returned_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(returned_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_file_selection_resolves_against_provider_snapshot_not_latest() -> TestResult {
        let table = create_in_memory_id_table_with_rows(vec![1, 2]).await?;
        let log_store = table.log_store();
        let old_version = table.version();
        let old_snapshot = table.snapshot()?.snapshot().snapshot().clone();
        let selected_path = old_snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .next()
            .unwrap();

        let overwrite_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "id",
                ArrowDataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![3, 4]))],
        )?;
        let latest_table = table
            .write(vec![overwrite_batch])
            .with_save_mode(SaveMode::Overwrite)
            .await?;
        assert!(latest_table.version() > old_version);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let old_provider = DeltaScan::builder()
            .with_snapshot(old_snapshot)
            .with_log_store(log_store.clone())
            .build()
            .await?
            .with_file_paths([selected_path.clone()]);
        let old_plan = old_provider.scan(&state, None, &[], None).await?;
        let old_batches: Vec<_> = collect_partitioned(old_plan, session.task_ctx())
            .await?
            .into_iter()
            .flatten()
            .collect();
        let expected = vec!["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+"];
        assert_batches_sorted_eq!(&expected, &old_batches);

        let latest_provider = DeltaScan::builder()
            .with_snapshot(latest_table.snapshot()?.snapshot().snapshot().clone())
            .with_log_store(log_store)
            .build()
            .await?
            .with_file_paths([selected_path]);
        let err = latest_provider
            .scan(&state, None, &[], None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("missing files"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[test]
    fn test_normalize_path_as_file_id_accepts_table_root_paths() -> TestResult {
        let cases = vec![
            (
                "file:///tmp/delta",
                "data/part-000.parquet",
                "file:///tmp/delta/data/part-000.parquet",
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
            let file_id = normalize_path_as_file_id(input, &table_root, "file path")?;
            assert_eq!(file_id, expected);
        }

        Ok(())
    }

    #[test]
    fn test_file_selection_serde_preserves_unresolved_input() -> TestResult {
        let selection = FileSelection::from_file_paths([
            "file:///tmp/delta/b.parquet".to_string(),
            "file:///tmp/delta/a.parquet".to_string(),
        ])
        .with_missing_file_policy(MissingSelectedFilePolicy::Ignore);

        let value = serde_json::to_value(&selection)?;
        let file_paths = value
            .get("paths")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        let policy = value
            .get("missing_file_policy")
            .and_then(|v| v.as_str())
            .unwrap();

        assert_eq!(
            file_paths,
            vec![
                "file:///tmp/delta/b.parquet".to_string(),
                "file:///tmp/delta/a.parquet".to_string(),
            ]
        );
        assert_eq!(policy, "Ignore");

        Ok(())
    }

    #[test]
    fn test_file_selection_serde_strips_url_credentials_query_and_fragment() -> TestResult {
        let selection = FileSelection::from_file_paths([
            "https://urluser:urlpassword@example.com/table/part.parquet?token=urltoken#frag",
        ]);
        let serialized = serde_json::to_string(&selection)?;

        assert!(serialized.contains("https://example.com/table/part.parquet"));
        for forbidden in ["urluser", "urlpassword", "urltoken", "frag"] {
            assert!(
                !serialized.contains(forbidden),
                "serialized file selection contains {forbidden}: {serialized}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_file_selection_from_adds_strips_url_credentials_query_and_fragment() -> TestResult {
        let add = make_test_add(
            "https://urluser:urlpassword@example.com/table/part.parquet?token=urltoken#frag",
            &[],
            123,
        );
        let selection = FileSelection::from_adds([add]);
        let serialized = serde_json::to_string(&selection)?;

        assert!(serialized.contains("https://example.com/table/part.parquet"));
        for forbidden in ["urluser", "urlpassword", "urltoken", "frag"] {
            assert!(
                !serialized.contains(forbidden),
                "serialized file selection contains {forbidden}: {serialized}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_file_selection_from_adds_serializes_identity_only() -> TestResult {
        let mut add = make_test_add("part=a/part-000.parquet", &[("part", "a")], 123);
        add.stats = Some(r#"{"numRecords":999}"#.to_string());
        add.tags = Some(std::collections::HashMap::from([(
            "secret-tag".to_string(),
            Some("secret-value".to_string()),
        )]));
        add.size = 999;

        let selection = FileSelection::from_adds([add]);
        let serialized = serde_json::to_string(&selection)?;

        assert!(serialized.contains("part=a/part-000.parquet"));
        for forbidden in [
            "partitionValues",
            "numRecords",
            "secret-tag",
            "secret-value",
            "modificationTime",
            "\"size\"",
        ] {
            assert!(
                !serialized.contains(forbidden),
                "FileSelection::from_adds serialized Add metadata field {forbidden}: {serialized}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_file_selection_debug_redacts_sensitive_url_parts() {
        let selection = FileSelection::from_file_paths([
            "https://urluser:urlpassword@example.com/table/part.parquet?token=urltoken#frag",
        ]);

        let debug = format!("{selection:?}");

        assert!(debug.contains("example.com/table/part.parquet"));
        for forbidden in ["urluser", "urlpassword", "urltoken", "frag"] {
            assert!(
                !debug.contains(forbidden),
                "FileSelection debug output must redact {forbidden}: {debug}"
            );
        }
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
    async fn test_deletion_vectors_file_selection_selects_dv_file() -> TestResult {
        let log_store = TestTables::WithDvSmall.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let expected = expected_dv_small()?;
        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?
            .with_file_paths([expected[0].filepath.clone()]);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let deletion_vectors = provider.deletion_vectors(&state).await?;
        assert_eq!(deletion_vectors, expected);

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_file_selection_without_dv_is_empty() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let selected_path = snapshot
            .file_views(log_store.as_ref(), None)
            .take(1)
            .map_ok(|view| view.path_raw().to_string())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .next()
            .unwrap();
        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?
            .with_log_store(log_store)
            .with_file_paths([selected_path]);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();

        let deletion_vectors = provider.deletion_vectors(&state).await?;
        assert!(deletion_vectors.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_file_selection_strict_missing_errors() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let table_root = snapshot.inner.table_root().clone();
        let missing_path = table_root.join("__does_not_exist__.parquet")?.to_string();
        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?
            .with_log_store(log_store)
            .with_file_paths([missing_path]);

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let err = provider.deletion_vectors(&state).await.unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("File selection contains"),
            "unexpected error: {err_str}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deletion_vectors_file_selection_ignore_missing_is_empty() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let table_root = snapshot.inner.table_root().clone();
        let missing_path = table_root.join("__does_not_exist__.parquet")?.to_string();
        let provider = DeltaScan::new(snapshot, DeltaScanConfig::default())?
            .with_log_store(log_store)
            .with_file_selection(
                FileSelection::from_file_paths([missing_path])
                    .with_missing_file_policy(MissingSelectedFilePolicy::Ignore),
            );

        let session = Arc::new(create_session().into_inner());
        let state = session.state_ref().read().clone();
        let deletion_vectors = provider.deletion_vectors(&state).await?;
        assert!(deletion_vectors.is_empty());

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
