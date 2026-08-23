//! Kernel-based Delta table scanning with optimized query execution.
//!
//! This module provides efficient table scanning using Delta Kernel, integrating with
//! DataFusion's query engine. It supports:
//!
//! - **Physical scan execution** ([`DeltaScanExec`]) - Reads Parquet data files and applies
//!   Delta protocol transformations (column mapping, deletion vectors, partition values)
//! - **Metadata-only scans** ([`DeltaScanMetaExec`]) - Answers queries like `COUNT(*)`
//!   using file statistics without reading data files
//! - **Predicate pushdown** - Pushes filters to both kernel file skipping and Parquet readers
//!   for efficient data pruning
//! - **Multi-store support** - Handles files across different object stores in a single query
//!
//! The scan planning process in [`plan`] determines which files to read and how to apply
//! predicates, while execution plans handle the actual data reading and transformation.

use std::{
    collections::{HashSet, VecDeque},
    pin::Pin,
    sync::Arc,
};

use arrow::datatypes::UInt16Type;
use arrow_array::{
    ArrayRef, DictionaryArray, RecordBatch, StringArray, StringViewArray, UInt16Array,
};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, FieldRef, Schema, SchemaBuilder, SchemaRef};
use chrono::{TimeZone as _, Utc};
use dashmap::DashMap;
use datafusion::{
    catalog::Session,
    common::{
        ColumnStatistics, HashMap, Result, Statistics, ToDFSchema, internal_datafusion_err,
        plan_err, stats::Precision,
    },
    config::TableParquetOptions,
    datasource::physical_plan::{ParquetSource, parquet::CachedParquetFileReaderFactory},
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    physical_plan::{
        ExecutionPlan,
        empty::EmptyExec,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder},
        union::UnionExec,
    },
    prelude::Expr,
};
use datafusion_datasource::{
    PartitionedFile, TableSchema, compute_all_files_statistics, file_groups::FileGroup,
    file_scan_config::FileScanConfigBuilder, source::DataSourceExec,
};
use datafusion_physical_expr_adapter::{
    BatchAdapter, BatchAdapterFactory, PhysicalExprAdapterFactory,
};
use delta_kernel::{
    Engine, Expression, engine::arrow_data::ArrowEngineData, expressions::StructData,
    scan::ScanMetadata, table_features::TableFeature,
};
use futures::{Stream, TryStreamExt as _, future::ready};
use itertools::Itertools as _;
use object_store::{ObjectMeta, path::Path};
use tracing::debug;
use url::Url;

pub use self::exec::DeltaScanExec;
use self::exec_meta::DeltaScanMetaExec;
use self::expr_adapter::{DeltaPhysicalExprAdapterFactory, relax_schema_nested_nullability};
pub(crate) use self::plan::{KernelScanPlan, ProjectedScanContract, supports_filters_pushdown};
use self::replay::{ScanFileContext, ScanFileStream};
use super::{FileSelection, ResolvedFileSelection};
use crate::{
    DeltaTableError,
    delta_datafusion::{
        DeltaScanConfig,
        engine::{AsObjectStoreUrl as _, to_datafusion_scalar},
        file_id::wrap_file_id_value,
        table_provider::next::DeletionVectorSelection,
    },
    kernel::LogicalFileView,
};

mod exec;
mod exec_meta;
mod expr_adapter;
mod plan;
mod replay;

type ScanMetadataStream = Pin<Box<dyn Stream<Item = Result<ScanMetadata, DeltaTableError>> + Send>>;
type PublicFileIdMap = HashMap<String, String>;

struct ReplayedScanFiles {
    files: Vec<ScanFileContext>,
    transforms: HashMap<String, Arc<Expression>>,
    dvs: DashMap<String, Vec<bool>>,
    public_file_ids: PublicFileIdMap,
    metrics: ExecutionPlanMetricsSet,
}

pub(super) async fn execution_plan(
    config: &DeltaScanConfig,
    session: &dyn Session,
    scan_plan: KernelScanPlan,
    stream: ScanMetadataStream,
    engine: Arc<dyn Engine>,
    limit: Option<usize>,
    file_selection: Option<&ResolvedFileSelection>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(selection) = file_selection
        && selection.active_file_ids.is_empty()
    {
        return Ok(Arc::new(EmptyExec::new(
            scan_plan.contract.result_schema.clone(),
        )));
    }

    let replayed = replay_files(engine, &scan_plan, config.clone(), stream, file_selection).await?;

    let file_id_field = scan_plan.contract.file_id_field.clone();
    if scan_plan.is_metadata_only() && !scan_plan.contract.retain_row_index {
        let map_file = |(file_index, f): (usize, &ScanFileContext)| {
            Ok((
                compact_internal_file_id(file_index),
                match &f.stats.num_rows {
                    Precision::Exact(n) => *n,
                    _ => {
                        return plan_err!(
                            "Expected exact row counts in file: {}",
                            super::redact_url_for_error(&f.file_url)
                        );
                    }
                },
            ))
        };

        let maybe_file_rows = replayed
            .files
            .iter()
            .enumerate()
            .map(map_file)
            .try_collect::<_, VecDeque<_>, _>();
        if let Ok(file_rows) = maybe_file_rows {
            let retain_file_id = scan_plan.contract.retain_file_id;
            let ReplayedScanFiles {
                transforms,
                dvs,
                public_file_ids,
                metrics,
                ..
            } = replayed;
            let exec = DeltaScanMetaExec::new(
                Arc::new(scan_plan),
                vec![file_rows],
                Arc::new(transforms),
                Arc::new(dvs),
                Arc::new(public_file_ids),
                retain_file_id.then_some(file_id_field),
                metrics,
            );
            return Ok(Arc::new(exec) as _);
        }
    }

    get_data_scan_plan(session, scan_plan, replayed, limit).await
}

/// Materialize deletion vector keep masks for every file in the scan that has one.
///
/// Deletion vectors are loaded as a side-effect of consuming [`ScanFileStream`].  We drain the
/// full stream here (discarding file contexts, stats, and partition values) because the DV
/// loading tasks are spawned lazily during stream poll.  A dedicated DV-only stream that skips
/// stats parsing is possible but not yet warranted — this path is not latency-sensitive and the
/// file-list is typically small.
///
/// [`ReceiverStreamBuilder::build`] returns a merged stream that includes a JoinSet checker;
/// `.try_collect().await` below will not complete until every spawned DV-loading task has
/// finished, so no results are lost.
pub(super) async fn replay_deletion_vectors(
    engine: Arc<dyn Engine>,
    scan_plan: &KernelScanPlan,
    config: &DeltaScanConfig,
    stream: ScanMetadataStream,
    file_selection: Option<&ResolvedFileSelection>,
) -> Result<Vec<DeletionVectorSelection>> {
    let mut stream = ScanFileStream::new(
        engine,
        &scan_plan.scan,
        config.clone(),
        file_selection.map(|selection| &selection.active_file_ids),
        stream,
    );
    while stream.try_next().await?.is_some() {}

    let dv_stream = stream.dv_stream.build();
    // Only files with `dv_info.has_vector()` spawn tasks, so every item should carry a DV.
    // Guard with a typed error (instead of panic) in case that invariant drifts.
    let dvs: DashMap<_, _> = dv_stream
        .and_then(|(url, dv, num_records)| {
            ready(match dv {
                Some(keep_mask) => normalize_dv_keep_mask_for_api(keep_mask, num_records, &url)
                    .map(|mask| (url.to_string(), mask))
                    .map_err(DeltaTableError::from),
                None => Err(DeltaTableError::generic(
                    "Invariant violation: DV task spawned for file without deletion vector",
                )),
            })
        })
        .try_collect()
        .await?;

    let mut vectors: Vec<_> = dvs
        .into_iter()
        .map(|(filepath, keep_mask)| DeletionVectorSelection {
            filepath,
            keep_mask,
        })
        .collect();
    vectors.sort_unstable_by(|left, right| left.filepath.cmp(&right.filepath));
    Ok(vectors)
}

pub(super) async fn resolve_file_selection(
    selection: &FileSelection,
    scan_plan: &KernelScanPlan,
    stream: ScanMetadataStream,
) -> Result<ResolvedFileSelection> {
    let requested_file_ids =
        resolve_input_file_ids_on_blocking_pool(selection, scan_plan.scan.table_root()).await?;
    if requested_file_ids.is_empty() {
        return Ok(ResolvedFileSelection::new(
            HashSet::new(),
            Vec::new(),
            selection.missing_file_policy,
        ));
    }

    let mut missing_file_ids = requested_file_ids;
    let selected_active_file_ids = collect_selected_active_file_ids(
        scan_plan.scan.table_root(),
        stream,
        &mut missing_file_ids,
    )
    .await?;
    let mut missing_file_ids: Vec<_> = missing_file_ids.into_iter().collect();
    missing_file_ids.sort_unstable();

    let resolved = ResolvedFileSelection::new(
        selected_active_file_ids,
        missing_file_ids,
        selection.missing_file_policy,
    );
    resolved.validate_missing()?;
    Ok(resolved)
}

async fn resolve_input_file_ids_on_blocking_pool(
    selection: &FileSelection,
    table_root: &Url,
) -> Result<HashSet<String>> {
    let selection = selection.clone();
    let table_root = table_root.clone();
    tokio::task::spawn_blocking(move || selection.resolve_input_file_ids(&table_root))
        .await
        .map_err(|err| DataFusionError::External(Box::new(err)))?
        .map_err(DataFusionError::from)
}

async fn collect_selected_active_file_ids(
    table_root: &Url,
    mut stream: ScanMetadataStream,
    missing_file_ids: &mut HashSet<String>,
) -> Result<HashSet<String>> {
    let mut selected_active_file_ids = HashSet::new();

    while let Some(scan_data) = stream.try_next().await? {
        let (data, mut selection_vector) = scan_data.scan_files.into_parts();
        let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)
            .map_err(DeltaTableError::from)?
            .into();
        // Delta Kernel may return a short selection vector. Missing entries are selected.
        selection_vector.resize(batch.num_rows(), true);

        for (idx, selected) in selection_vector.into_iter().enumerate() {
            if selected {
                let file_url = replay::parse_path(
                    table_root,
                    LogicalFileView::new(batch.clone(), idx).path_raw(),
                )?;
                let file_id = file_url.to_string();
                if missing_file_ids.remove(&file_id) {
                    selected_active_file_ids.insert(file_id);
                    if missing_file_ids.is_empty() {
                        return Ok(selected_active_file_ids);
                    }
                }
            }
        }
    }

    Ok(selected_active_file_ids)
}

async fn replay_files(
    engine: Arc<dyn Engine>,
    scan_plan: &KernelScanPlan,
    scan_config: DeltaScanConfig,
    stream: ScanMetadataStream,
    file_selection: Option<&ResolvedFileSelection>,
) -> Result<ReplayedScanFiles> {
    let mut stream = ScanFileStream::new(
        engine,
        &scan_plan.scan,
        scan_config,
        file_selection.map(|selection| &selection.active_file_ids),
        stream,
    );
    let mut files = Vec::new();
    while let Some(file) = stream.try_next().await? {
        files.extend(file);
    }

    let mut public_file_ids = PublicFileIdMap::default();
    if scan_plan.contract.retain_file_id {
        for (file_index, file) in files.iter().enumerate() {
            public_file_ids.insert(
                compact_internal_file_id(file_index),
                file.file_url.to_string(),
            );
        }
    }

    let transforms: HashMap<_, _> = files
        .iter_mut()
        .enumerate()
        .flat_map(|(file_index, file)| {
            file.transform
                .take()
                .map(|t| (compact_internal_file_id(file_index), t))
        })
        .collect();

    let dv_stream = stream.dv_stream.build();
    let dvs_by_url: HashMap<_, _> = dv_stream
        .try_filter_map(|(url, dv, _)| ready(Ok(dv.map(|dv| (url.to_string(), dv)))))
        .try_collect()
        .await?;
    let dvs = remap_deletion_vectors_to_internal_file_ids(&files, dvs_by_url)?;

    let metrics = ExecutionPlanMetricsSet::new();
    MetricBuilder::new(&metrics)
        .global_counter("count_files_scanned")
        .add(stream.metrics.num_scanned);

    Ok(ReplayedScanFiles {
        files,
        transforms,
        dvs,
        public_file_ids,
        metrics,
    })
}

/// Normalize a DV keep mask for `deletion_vectors()`.
///
/// Kernel returns a sparse mask (up to the highest deleted row index). For API output we need one
/// full mask per file, to do this we pad trailing entries with `true` up to `numRecords`. If `numRecords`
/// is missing we fail, because we cannot know the correct full length.
///
/// This is API only. Scan execution does per batch normalization in `exec::consume_dv_mask` and
/// `exec_meta::apply_selection_vector`.
fn normalize_dv_keep_mask_for_api(
    mut mask: Vec<bool>,
    num_records: Option<u64>,
    file_url: &Url,
) -> Result<Vec<bool>> {
    let redacted_url = super::redact_url_for_error(file_url);
    let Some(num_records) = num_records else {
        return plan_err!(
            "Missing numRecords for file with deletion vector: {}",
            redacted_url
        );
    };
    let num_records = usize::try_from(num_records).map_err(|_| {
        DataFusionError::Execution(format!(
            "numRecords does not fit usize for file with deletion vector: {redacted_url}"
        ))
    })?;
    if mask.len() > num_records {
        return plan_err!(
            "Deletion vector mask length {} exceeds numRecords {} for file: {}",
            mask.len(),
            num_records,
            redacted_url
        );
    }
    mask.resize(num_records, true);
    Ok(mask)
}

async fn get_data_scan_plan(
    session: &dyn Session,
    scan_plan: KernelScanPlan,
    replayed: ReplayedScanFiles,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let ReplayedScanFiles {
        files,
        transforms,
        dvs,
        public_file_ids,
        metrics,
    } = replayed;
    let mut partition_stats = HashMap::new();

    // Convert files into DataFusion `PartitionedFile`s grouped by object store.
    // Create one `DataSourceExec` plan for each store.
    // Add a compact scan file id as a partition value for file correlation.
    // The exec maps that id back to the public file path only when the file column is projected.
    let to_partitioned_file = |(file_index, f): (usize, ScanFileContext)| {
        if let Some(part_stata) = &f.partitions {
            update_partition_stats(part_stata, &f.stats, &mut partition_stats)?;
        }
        // We create a PartitionedFile from the ObjectMeta to avoid any surprises in path encoding
        // that may arise from using the 'new' method directly. i.e. the 'new' method encodes paths
        // segments again, which may lead to double-encoding in some cases.
        let mut partitioned_file: PartitionedFile = ObjectMeta {
            location: Path::from_url_path(f.file_url.path())?,
            size: f.size,
            last_modified: Utc.timestamp_nanos(0),
            e_tag: None,
            version: None,
        }
        .into();
        let file_value = wrap_file_id_value(compact_internal_file_id(file_index));
        // NOTE: `PartitionedFile::with_statistics` appends exact stats for partition columns based
        // on `partition_values`, so partition values must be set first.
        partitioned_file.partition_values = vec![file_value.clone()];
        partitioned_file = partitioned_file.with_statistics(Arc::new(f.stats));
        Ok::<_, DataFusionError>((
            f.file_url.as_object_store_url(),
            (partitioned_file, None::<Vec<bool>>),
        ))
    };

    // Group the files by their object store url. Since datafusion assumes that all files in a
    // DataSourceExec are stored in the same object store, we need to create one plan per store
    let partitioned_files = files
        .into_iter()
        .enumerate()
        .map(to_partitioned_file)
        .try_collect::<_, Vec<_>, _>()?;

    let files_by_store = partitioned_files.into_iter().into_group_map();

    // TODO(roeap); not sure exactly how row tracking is implemented in kernel right now
    // so leaving predicate as None for now until we are sure this is safe to do.
    let table_config = scan_plan.table_configuration();
    let predicate = if table_config.is_feature_enabled(&TableFeature::RowTracking) {
        None
    } else {
        scan_plan.parquet_predicate.as_ref()
    };
    let file_id_field = scan_plan.contract.file_id_field.clone();
    let pq_plan = get_read_plan(
        session,
        files_by_store,
        &scan_plan.parquet_read_schema,
        &scan_plan.parquet_predicate_schema,
        limit,
        &file_id_field,
        predicate,
    )
    .await?;

    let transforms = Arc::new(transforms);
    let dvs = Arc::new(dvs);
    let public_file_ids = Arc::new(public_file_ids);
    let exec = DeltaScanExec::new(
        Arc::new(scan_plan),
        pq_plan,
        Arc::clone(&transforms),
        Arc::clone(&dvs),
        Arc::clone(&public_file_ids),
        partition_stats,
        metrics,
    );

    Ok(Arc::new(exec))
}

fn update_partition_stats(
    data: &StructData,
    stats: &Statistics,
    part_stats: &mut HashMap<String, ColumnStatistics>,
) -> Result<()> {
    for (field, stat) in data.fields().iter().zip(data.values().iter()) {
        let (null_count, value) = if stat.is_null() {
            (stats.num_rows, Precision::Absent)
        } else {
            (
                Precision::Exact(0),
                Precision::Exact(to_datafusion_scalar(stat)?),
            )
        };
        if let Some(part_stat) = part_stats.get_mut(field.name()) {
            part_stat.null_count = part_stat.null_count.add(&null_count);
            part_stat.min_value = part_stat.min_value.min(&value);
            part_stat.max_value = part_stat.max_value.max(&value);
        } else {
            part_stats.insert(
                field.name().clone(),
                ColumnStatistics {
                    null_count,
                    min_value: value.clone(),
                    max_value: value,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                    byte_size: Precision::Absent,
                },
            );
        }
    }

    Ok(())
}

type FilesByStore = (ObjectStoreUrl, Vec<(PartitionedFile, Option<Vec<bool>>)>);

fn compact_internal_file_id(file_index: usize) -> String {
    file_index.to_string()
}

fn remap_deletion_vectors_to_internal_file_ids(
    files: &[ScanFileContext],
    mut dvs_by_url: HashMap<String, Vec<bool>>,
) -> Result<DashMap<String, Vec<bool>>> {
    let dvs = DashMap::new();
    for (file_index, file) in files.iter().enumerate() {
        if dvs_by_url.is_empty() {
            break;
        }
        if let Some(dv) = dvs_by_url.remove(file.file_url.as_str()) {
            dvs.insert(compact_internal_file_id(file_index), dv);
        }
    }
    if let Some(file_url) = dvs_by_url.keys().next() {
        let redacted_url = Url::parse(file_url)
            .map(|url| super::redact_url_for_error(&url))
            .unwrap_or_else(|_| file_url.clone());
        return Err(internal_datafusion_err!(
            "missing internal file id mapping for file with deletion vector: {redacted_url}"
        ));
    }
    Ok(dvs)
}

fn public_file_id<'a>(
    public_file_ids: &'a PublicFileIdMap,
    internal_file_id: &str,
) -> Result<&'a str> {
    public_file_ids
        .get(internal_file_id)
        .map(String::as_str)
        .ok_or_else(|| {
            internal_datafusion_err!(
                "missing public file id mapping for internal file id '{internal_file_id}'"
            )
        })
}

fn file_id_array_for_value(
    file_id_field: &FieldRef,
    file_id: &str,
    row_count: usize,
) -> Result<ArrayRef> {
    let keys = UInt16Array::from(vec![0u16; row_count]);
    let values: ArrayRef = match file_id_field.data_type() {
        DataType::Dictionary(_, value_type) if value_type.as_ref() == &DataType::Utf8View => {
            if row_count == 0 {
                Arc::new(StringViewArray::from_iter_values(std::iter::empty::<&str>()))
            } else {
                Arc::new(StringViewArray::from_iter_values([file_id]))
            }
        }
        _ => {
            if row_count == 0 {
                Arc::new(StringArray::from(Vec::<Option<&str>>::new()))
            } else {
                Arc::new(StringArray::from(vec![Some(file_id)]))
            }
        }
    };

    let file_id_array: DictionaryArray<UInt16Type> = DictionaryArray::try_new(keys, values)?;
    Ok(Arc::new(file_id_array))
}

/// Maximum number of distinct values representable by DataFusion's default partition dictionary
/// encoding (`Dictionary<UInt16, _>`).
const MAX_PARTITION_DICT_CARDINALITY: usize = (u16::MAX as usize) + 1;

fn partitioned_files_to_file_groups(
    files: impl IntoIterator<Item = PartitionedFile>,
) -> Vec<FileGroup> {
    partitioned_files_to_file_groups_with_limit(files, MAX_PARTITION_DICT_CARDINALITY)
}

fn partitioned_files_to_file_groups_with_limit(
    files: impl IntoIterator<Item = PartitionedFile>,
    max_files_per_group: usize,
) -> Vec<FileGroup> {
    let file_groups = files
        .into_iter()
        // Each `PartitionedFile` is assigned to exactly one file group. DeltaScanStream stores
        // row ordinal counters per execution partition. Whole file ownership is required for
        // scan row ordinals.
        // Partition values are dictionary encoded using a UInt16 key (DataFusion's default
        // `wrap_partition_type_in_dict`). Keep file groups small enough that the file-id partition
        // dictionary doesn't exceed the key space (one distinct value per file).
        .chunks(max_files_per_group)
        .into_iter()
        .map(|chunk| chunk.collect::<FileGroup>())
        .collect_vec();

    #[cfg(debug_assertions)]
    {
        let mut owner_by_path = HashMap::new();
        for (partition, group) in file_groups.iter().enumerate() {
            for file in group.iter() {
                let path = file.object_meta.location.to_string();
                if let Some(previous_partition) = owner_by_path.insert(path.clone(), partition) {
                    debug_assert_eq!(
                        previous_partition, partition,
                        "file {path} was assigned to multiple scan partitions; row indexes require whole file ownership"
                    );
                }
            }
        }
    }

    file_groups
}

async fn get_read_plan(
    state: &dyn Session,
    files_by_store: impl IntoIterator<Item = FilesByStore>,
    // Schema of physical file columns to read from Parquet (no Delta partitions, no file-id).
    //
    // This is also the schema used for Parquet pruning/pushdown. It may include view types
    // (e.g. Utf8View/BinaryView) depending on `DeltaScanConfig`.
    parquet_read_schema: &SchemaRef,
    // Predicate binding schema used to bind Parquet predicates, including the synthetic file id
    // column when the provider exposes it.
    parquet_predicate_schema: &SchemaRef,
    limit: Option<usize>,
    file_id_field: &FieldRef,
    predicate: Option<&Expr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plans = Vec::new();

    // Relax nested-field nullability on the schema handed to the parquet source:
    // Delta's `nullable = false` is a write-time invariant, and Spark-written files
    // may mark nested fields optional. The logical schema is restored above the
    // parquet scan by DeltaScanExec's transforms.
    let parquet_read_schema = Arc::new(relax_schema_nested_nullability(parquet_read_schema));
    let parquet_read_schema = &parquet_read_schema;

    let pq_options = TableParquetOptions {
        global: state.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    let mut full_read_schema = SchemaBuilder::from(parquet_read_schema.as_ref().clone());
    full_read_schema.push(file_id_field.as_ref().clone().with_nullable(true));
    let full_read_schema = Arc::new(full_read_schema.finish());
    let parquet_predicate_df_schema = parquet_predicate_schema.clone().to_dfschema()?;
    let adapter_factory = Arc::new(DeltaPhysicalExprAdapterFactory);

    for (store_url, files) in files_by_store.into_iter() {
        let reader_factory = Arc::new(CachedParquetFileReaderFactory::new(
            state.runtime_env().object_store(&store_url)?,
            state.runtime_env().cache_manager.get_file_metadata_cache(),
        ));

        // NOTE: In the "next" provider, DataFusion's Parquet scan partition fields are file-id
        // only. Delta partition columns/values are injected via kernel transforms and handled
        // above Parquet, so they are not part of the Parquet partition schema here.
        let table_schema =
            TableSchema::new(parquet_read_schema.clone(), vec![file_id_field.clone()]);
        let full_table_schema = table_schema.table_schema().clone();
        let mut file_source = ParquetSource::new(table_schema)
            .with_table_parquet_options(pq_options.clone())
            .with_parquet_file_reader_factory(reader_factory);

        // TODO(roeap); we might be able to also push selection vectors into the read plan
        // by creating parquet access plans. However we need to make sure this does not
        // interfere with other delta features like row ids.
        let has_selection_vectors = files.iter().any(|(_, sv)| sv.is_some());
        if !has_selection_vectors && let Some(pred) = predicate {
            match state.create_physical_expr(pred.clone(), &parquet_predicate_df_schema) {
                Ok(physical) => match adapter_factory
                    .create(parquet_predicate_schema.clone(), full_read_schema.clone())
                {
                    Ok(adapter) => match adapter.rewrite(physical) {
                        Ok(rewritten) => {
                            file_source = file_source
                                .with_predicate(rewritten)
                                .with_pushdown_filters(true);
                        }
                        Err(err) => {
                            debug!(
                                predicate = ?pred,
                                schema = ?parquet_predicate_schema,
                                error = %err,
                                "Skipping parquet predicate pushdown because predicate adaptation to the read schema failed"
                            );
                        }
                    },
                    Err(err) => {
                        debug!(
                            predicate = ?pred,
                            schema = ?parquet_predicate_schema,
                            error = %err,
                            "Skipping parquet predicate pushdown because predicate adapter creation failed"
                        );
                    }
                },
                Err(err) => {
                    debug!(
                        predicate = ?pred,
                        schema = ?parquet_predicate_schema,
                        error = %err,
                        "Skipping parquet predicate pushdown because predicate binding failed"
                    );
                }
            }
        }

        let file_groups = partitioned_files_to_file_groups(files.into_iter().map(|file| file.0));
        let (file_groups, statistics) =
            compute_all_files_statistics(file_groups, full_table_schema, true, false)?;

        let config = FileScanConfigBuilder::new(store_url, Arc::new(file_source))
            .with_file_groups(file_groups)
            .with_statistics(statistics)
            .with_limit(limit)
            .with_expr_adapter(Some(adapter_factory.clone() as _))
            .build();

        plans.push(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>);
    }

    Ok(match plans.len() {
        0 => Arc::new(EmptyExec::new(full_read_schema.clone())),
        1 => plans.remove(0),
        _ => UnionExec::try_new(plans)?,
    })
}

// Small helper to reuse some code between exec and exec_meta
fn finalize_transformed_batch(
    batch: RecordBatch,
    scan_plan: &KernelScanPlan,
    file_id_col: Option<(ArrayRef, FieldRef)>,
    schema_adapter: &mut SchemaAdapter,
) -> Result<RecordBatch> {
    let result = if let Some(projection) = scan_plan.contract.result_projection.as_ref() {
        batch.project(projection)?
    } else {
        batch
    };
    // NOTE: most data is read properly typed already, however columns added via
    // literals in the transformations may need to be cast to the physical expected type.
    let result = if result.schema_ref().eq(&scan_plan.contract.result_schema) {
        result
    } else {
        let adapted = schema_adapter.adapt(result)?;
        if adapted.schema_ref().eq(&scan_plan.contract.result_schema) {
            adapted
        } else {
            // The batch adapter targets a nullability-relaxed schema (see
            // `expr_adapter`). Restamp to the strict logical schema; this
            // validates actual data nulls rather than declared nullability.
            crate::kernel::cast_record_batch(
                &adapted,
                scan_plan.contract.result_schema.clone(),
                false,
                false,
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        }
    };
    if let Some((arr, field)) = file_id_col {
        let arr = if arr.data_type() != field.data_type() {
            let options = CastOptions {
                safe: true,
                ..Default::default()
            };
            cast_with_options(arr.as_ref(), field.data_type(), &options)?
        } else {
            arr
        };
        let mut columns = result.columns().to_vec();
        columns.push(arr);
        let mut fields = result.schema().fields().to_vec();
        fields.push(field);
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    } else {
        Ok(result)
    }
}

/// Caches a [`BatchAdapter`] for the most recently seen source schema, avoiding
/// repeated expression-tree construction when consecutive batches share the same
/// physical schema (the common case within a single file).
struct SchemaAdapter {
    factory: BatchAdapterFactory,
    /// Single-entry cache: the source schema for the currently cached adapter.
    cached_source: Option<SchemaRef>,
    cached_adapter: Option<BatchAdapter>,
}

impl SchemaAdapter {
    fn new(target_schema: SchemaRef) -> Self {
        Self {
            factory: BatchAdapterFactory::new(target_schema)
                .with_adapter_factory(Arc::new(DeltaPhysicalExprAdapterFactory)),
            cached_source: None,
            cached_adapter: None,
        }
    }

    /// Adapt the batch to the target schema, using a cached adapter when the
    /// source schema matches the previous call.
    fn adapt(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let source_schema = batch.schema();
        let can_reuse = matches!(
            (&self.cached_source, &self.cached_adapter),
            (Some(cached_source), Some(_)) if cached_source.eq(&source_schema)
        );
        let needs_rebuild = !can_reuse;
        if needs_rebuild {
            let adapter = self.factory.make_adapter(&source_schema)?;
            self.cached_source = Some(source_schema);
            self.cached_adapter = Some(adapter);
        }
        match self.cached_adapter.as_ref() {
            Some(adapter) => adapter.adapt_batch(&batch),
            None => plan_err!(
                "schema adapter cache entry missing for source schema: {:?}",
                batch.schema()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow_array::Array;
    use arrow_array::{
        BinaryArray, BinaryViewArray, Int32Array, Int64Array, RecordBatch, RecordBatchOptions,
        StringArray, StringViewArray, StructArray,
    };
    use arrow_schema::{ArrowError, DataType, Field, Fields, Schema};
    use datafusion::{
        error::DataFusionError,
        physical_plan::collect,
        prelude::{col, lit},
    };
    use object_store::{ObjectStoreExt as _, memory::InMemory};
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{
            DeltaScanConfig, MissingSelectedFilePolicy,
            engine::DataFusionEngine,
            session::create_session,
            table_provider::next::{FILE_ID_COLUMN_DEFAULT, FileSelection},
        },
        kernel::Snapshot,
        test_utils::{TestResult, TestTables},
    };

    use super::{plan::build_parquet_predicate_schema, *};

    #[test]
    fn test_partitioned_files_to_file_groups_respects_dictionary_cardinality_limit() {
        let files = (0..=MAX_PARTITION_DICT_CARDINALITY)
            .map(|i| PartitionedFile::new(format!("memory:///f{i}.parquet"), 0))
            .collect_vec();

        let groups = partitioned_files_to_file_groups(files);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), MAX_PARTITION_DICT_CARDINALITY);
        assert_eq!(groups[1].len(), 1);
    }

    #[tokio::test]
    async fn test_resolve_empty_file_selection_does_not_poll_metadata_stream() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let scan_plan =
            KernelScanPlan::try_new(&snapshot, None, &[], &DeltaScanConfig::default(), None)?;
        let stream: ScanMetadataStream = Box::pin(futures::stream::poll_fn(|_| {
            panic!("unexpected metadata stream poll for empty file selection")
        }));

        let resolved = resolve_file_selection(
            &FileSelection::from_file_paths(Vec::<String>::new()),
            &scan_plan,
            stream,
        )
        .await?;

        assert!(resolved.active_file_ids.is_empty());
        assert!(resolved.missing_file_ids.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_resolved_file_selection_plan_does_not_poll_metadata_stream() -> TestResult {
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let scan_plan =
            KernelScanPlan::try_new(&snapshot, None, &[], &DeltaScanConfig::default(), None)?;
        let stream: ScanMetadataStream = Box::pin(futures::stream::poll_fn(|_| {
            panic!("unexpected metadata stream poll for empty file selection execution")
        }));
        let session = create_session().into_inner();
        let state = session.state();
        let engine = DataFusionEngine::new_from_session(&state);
        let selection = ResolvedFileSelection::new(
            std::collections::HashSet::new(),
            Vec::new(),
            MissingSelectedFilePolicy::Error,
        );

        let plan = execution_plan(
            &DeltaScanConfig::default(),
            &state,
            scan_plan,
            stream,
            engine,
            None,
            Some(&selection),
        )
        .await?;

        assert!(plan.is::<EmptyExec>());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_uses_compact_internal_file_id_partition_values() -> TestResult {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = create_session().into_inner();

        let scan = provider.scan(&session.state(), None, &[], None).await?;
        let exec = scan
            .downcast_ref::<DeltaScanExec>()
            .expect("expected DeltaScanExec");
        let data_source = exec.children()[0]
            .downcast_ref::<DataSourceExec>()
            .expect("expected DataSourceExec child");
        let (file_scan_config, _) = data_source
            .downcast_to_file_source::<ParquetSource>()
            .expect("expected parquet file source");

        let internal_file_ids = file_scan_config
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|file| {
                file.partition_values
                    .first()
                    .and_then(|value| value.try_as_str().flatten())
                    .expect("file-id partition value")
            })
            .collect_vec();

        assert!(
            !internal_file_ids.is_empty(),
            "test fixture should plan at least one file"
        );
        for internal_file_id in internal_file_ids {
            assert!(
                internal_file_id.len() <= 20,
                "internal file id should be compact, got {internal_file_id:?}"
            );
            assert!(
                !internal_file_id.contains('/'),
                "internal file id should not carry a full file path, got {internal_file_id:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_public_file_id_uses_file_path_with_compact_internal_ids() -> TestResult {
        let table = TestTables::Simple.table_builder()?.load().await?;
        let provider = table.table_provider().with_file_column("file_id").await?;
        let session = create_session().into_inner();

        session.register_table("delta_table", provider)?;

        let file_id_batches = session
            .sql("SELECT CAST(file_id AS STRING) AS file_id FROM delta_table LIMIT 1")
            .await?
            .collect()
            .await?;
        let file_id = file_id_batches[0].column(0).as_string_view().value(0);
        assert!(
            file_id.starts_with("file://") && file_id.ends_with(".parquet"),
            "public file id should remain a file path, got {file_id:?}"
        );

        let escaped_file_id = file_id.replace('\'', "''");
        let df = session
            .sql(&format!(
                "SELECT id FROM delta_table WHERE file_id = '{escaped_file_id}'"
            ))
            .await?;
        let filtered = df.collect().await?;

        assert!(filtered.iter().map(|batch| batch.num_rows()).sum::<usize>() > 0);
        assert!(filtered[0].schema().column_with_name("file_id").is_none());

        Ok(())
    }

    fn scan_file_context(file_url: &str) -> ScanFileContext {
        ScanFileContext {
            file_url: Url::parse(file_url).expect("valid test URL"),
            size: 0,
            transform: None,
            stats: Statistics::new_unknown(&Schema::empty()),
            partitions: None,
        }
    }

    #[test]
    fn test_remap_deletion_vectors_to_internal_file_ids_uses_compact_keys() -> TestResult {
        let files = vec![
            scan_file_context("s3://bucket/very/long/path/first.parquet"),
            scan_file_context("s3://bucket/very/long/path/second.parquet"),
        ];
        let mut dvs_by_url = HashMap::new();
        dvs_by_url.insert(files[1].file_url.to_string(), vec![true, false, true]);

        let dvs = remap_deletion_vectors_to_internal_file_ids(&files, dvs_by_url)?;

        assert!(dvs.contains_key("1"));
        assert!(!dvs.contains_key(files[1].file_url.as_str()));
        assert_eq!(
            dvs.get("1").expect("compact dv key").as_slice(),
            &[true, false, true]
        );
        Ok(())
    }

    #[test]
    fn test_remap_deletion_vectors_to_internal_file_ids_errors_for_unknown_url() {
        let files = vec![scan_file_context(
            "s3://bucket/very/long/path/first.parquet",
        )];
        let mut dvs_by_url = HashMap::new();
        dvs_by_url.insert(
            "s3://bucket/very/long/path/missing.parquet?X-Amz-Signature=secret-token".to_string(),
            vec![true],
        );

        let err = remap_deletion_vectors_to_internal_file_ids(&files, dvs_by_url).unwrap_err();
        let err = err.to_string();
        assert!(
            err.contains("missing internal file id mapping for file with deletion vector"),
            "unexpected error: {err}"
        );
        assert!(
            !err.contains("secret-token"),
            "error should redact URL query secrets: {err}"
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "row indexes require whole file ownership")]
    fn test_partitioned_files_to_file_groups_rejects_split_file_across_groups_in_debug() {
        let files = vec![
            PartitionedFile::new("memory:///same.parquet", 0),
            PartitionedFile::new("memory:///other.parquet", 0),
            PartitionedFile::new("memory:///same.parquet", 0),
        ];

        let _ = partitioned_files_to_file_groups_with_limit(files, 1);
    }

    #[test]
    fn test_normalize_dv_keep_mask_for_api_pads_short_mask_with_true() {
        let url = Url::parse("file:///tmp/table/file.parquet").unwrap();
        let actual = normalize_dv_keep_mask_for_api(vec![true, false], Some(4), &url).unwrap();
        assert_eq!(actual, vec![true, false, true, true]);
    }

    #[test]
    fn test_normalize_dv_keep_mask_for_api_keeps_equal_length_mask() {
        let url = Url::parse("file:///tmp/table/file.parquet").unwrap();
        let mask = vec![true, false, true];
        let actual = normalize_dv_keep_mask_for_api(mask.clone(), Some(3), &url).unwrap();
        assert_eq!(actual, mask);
    }

    #[test]
    fn test_normalize_dv_keep_mask_for_api_pads_empty_mask_to_all_true() {
        let url = Url::parse("file:///tmp/table/file.parquet").unwrap();
        let actual = normalize_dv_keep_mask_for_api(Vec::new(), Some(3), &url).unwrap();
        assert_eq!(actual, vec![true, true, true]);
    }

    #[test]
    fn test_normalize_dv_keep_mask_for_api_errors_when_mask_longer_than_num_records() {
        let url =
            Url::parse("s3://user:secret@example.com/table/file.parquet?sig=token#frag").unwrap();
        let expected_url = super::super::redact_url_for_error(&url);
        let err = normalize_dv_keep_mask_for_api(vec![true, false, true], Some(2), &url)
            .expect_err("longer mask should error");
        let message = err.to_string();
        assert!(message.contains("exceeds numRecords"));
        assert!(message.contains(&expected_url));
        assert!(!message.contains("sig=token"));
        assert!(!message.contains("secret"));
    }

    #[test]
    fn test_normalize_dv_keep_mask_for_api_errors_when_num_records_missing() {
        let url =
            Url::parse("s3://user:secret@example.com/table/file.parquet?sig=token#frag").unwrap();
        let expected_url = super::super::redact_url_for_error(&url);
        let err = normalize_dv_keep_mask_for_api(vec![true], None, &url)
            .expect_err("missing numRecords should error");
        let message = err.to_string();
        assert!(message.contains("Missing numRecords"));
        assert!(message.contains(&expected_url));
        assert!(!message.contains("sig=token"));
        assert!(!message.contains("secret"));
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn test_normalize_dv_keep_mask_for_api_errors_when_num_records_overflow_usize() {
        // This branch is only reachable on 32-bit targets where u64 may exceed usize.
        let url = Url::parse("file:///tmp/table/file.parquet").unwrap();
        let overflow_num_records = (usize::MAX as u64) + 1;
        let err = normalize_dv_keep_mask_for_api(vec![true], Some(overflow_num_records), &url)
            .expect_err("numRecords that does not fit usize should error");
        assert!(err.to_string().contains("does not fit usize"));
    }

    #[test]
    fn test_schema_adapter_synthesizes_nullable_columns() {
        let source_schema = Arc::new(Schema::new(Fields::empty()));
        let source = RecordBatch::try_new_with_options(
            source_schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(2)),
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let mut adapter = SchemaAdapter::new(target_schema.clone());
        let adapted = adapter.adapt(source).unwrap();

        assert_eq!(adapted.schema().as_ref(), target_schema.as_ref());
        assert_eq!(adapted.num_rows(), 2);
        let id = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id.null_count(), 2);
    }

    #[test]
    fn test_schema_adapter_missing_non_nullable_column_errors() {
        let source_schema = Arc::new(Schema::new(Fields::empty()));
        let source = RecordBatch::try_new_with_options(
            source_schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let mut adapter = SchemaAdapter::new(target_schema);
        let err = adapter
            .adapt(source)
            .expect_err("missing non-nullable columns should error");
        match err {
            DataFusionError::Execution(msg) => {
                assert!(
                    msg.contains("Non-nullable column 'id'"),
                    "expected non-nullable missing-column error, got: {msg}"
                );
                assert!(
                    msg.contains("missing from the physical schema"),
                    "expected missing physical schema detail, got: {msg}"
                );
            }
            other => {
                panic!("expected execution error for missing non-nullable column, got: {other}")
            }
        }
    }

    #[test]
    fn test_schema_adapter_invalid_scalar_cast_errors() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(StringArray::from(vec![Some("not-an-int")]))],
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let mut adapter = SchemaAdapter::new(target_schema);
        let err = adapter
            .adapt(source)
            .expect_err("invalid value cast should fail under DataFusion default cast semantics");
        match err {
            DataFusionError::ArrowError(inner, _) => {
                assert!(
                    matches!(inner.as_ref(), ArrowError::CastError(_)),
                    "expected arrow cast error, got: {inner}"
                );
            }
            other => panic!("expected arrow cast error for invalid scalar cast, got: {other}"),
        }
    }

    #[test]
    fn test_schema_adapter_type_widening() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut adapter = SchemaAdapter::new(target_schema.clone());
        let adapted = adapter.adapt(source).unwrap();

        assert_eq!(adapted.schema().as_ref(), target_schema.as_ref());
        assert_eq!(adapted.num_rows(), 3);
        let id = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.values(), &[1i64, 2, 3]);
    }

    #[test]
    fn test_schema_adapter_overflow_cast_errors() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let source = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1]))],
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let mut adapter = SchemaAdapter::new(target_schema);
        let err = adapter
            .adapt(source)
            .expect_err("overflow cast should fail under DataFusion default cast semantics");
        match err {
            DataFusionError::ArrowError(inner, _) => {
                assert!(
                    matches!(inner.as_ref(), ArrowError::CastError(_)),
                    "expected arrow cast error, got: {inner}"
                );
            }
            other => panic!("expected arrow cast error for overflow cast, got: {other}"),
        }
    }

    #[test]
    fn test_schema_adapter_caches_across_calls() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let target_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut adapter = SchemaAdapter::new(target_schema);

        let batch1 = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(Int32Array::from(vec![2]))],
        )
        .unwrap();

        let _ = adapter.adapt(batch1).unwrap();
        assert!(adapter.cached_source.is_some());

        // Second call with the same schema should hit the cache (no rebuild).
        let adapted = adapter.adapt(batch2).unwrap();
        assert_eq!(adapted.num_rows(), 1);
        let id = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id.values(), &[2i64]);
    }

    #[tokio::test]
    async fn test_parquet_plan() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_data.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_data.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&arrow_schema, &file_id_field);

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 1  | a     | memory:///test_data.parquet |",
            "| 2  | b     | memory:///test_data.parquet |",
            "| 3  | c     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        // respect limits
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            &parquet_predicate_schema,
            Some(1),
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 1  | a     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        // extended schema with missing column
        let arrow_schema_extended = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("value2", DataType::Utf8, true),
        ]));
        let parquet_predicate_schema_extended =
            build_parquet_predicate_schema(&arrow_schema_extended, &file_id_field);
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
            &parquet_predicate_schema_extended,
            Some(1),
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+--------+-----------------------------+",
            "| id | value | value2 | __delta_rs_file_id__        |",
            "+----+-------+--------+-----------------------------+",
            "| 1  | a     |        | memory:///test_data.parquet |",
            "+----+-------+--------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_plan_nested() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let nested_fields: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]
        .into();
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", DataType::Struct(nested_fields.clone()), true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StructArray::try_new(
                    nested_fields,
                    vec![
                        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
                        Arc::new(StringArray::from(vec![Some("aa"), Some("bb"), Some("cc")])),
                    ],
                    None,
                )?),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_data.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_data.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&arrow_schema, &file_id_field);

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+---------------+-----------------------------+",
            "| id | nested        | __delta_rs_file_id__        |",
            "+----+---------------+-----------------------------+",
            "| 1  | {a: a, b: aa} | memory:///test_data.parquet |",
            "| 2  | {a: b, b: bb} | memory:///test_data.parquet |",
            "| 3  | {a: c, b: cc} | memory:///test_data.parquet |",
            "+----+---------------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        let nested_fields_extended: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]
        .into();
        let arrow_schema_extended = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "nested",
                DataType::Struct(nested_fields_extended.clone()),
                true,
            ),
        ]));
        let parquet_predicate_schema_extended =
            build_parquet_predicate_schema(&arrow_schema_extended, &file_id_field);
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
            &parquet_predicate_schema_extended,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+--------------------+-----------------------------+",
            "| id | nested             | __delta_rs_file_id__        |",
            "+----+--------------------+-----------------------------+",
            "| 1  | {a: a, b: aa, c: } | memory:///test_data.parquet |",
            "| 2  | {a: b, b: bb, c: } | memory:///test_data.parquet |",
            "| 3  | {a: c, b: cc, c: } | memory:///test_data.parquet |",
            "+----+--------------------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    /// Reading a file that contains actual null values in a nested field declared
    /// non-nullable in the table schema must fail: the nullability relaxation in
    /// the expression adapter only accommodates Spark-style relaxed on-disk
    /// schemas, while the final `cast_record_batch` in
    /// `finalize_transformed_batch` validates the actual data.
    #[tokio::test]
    async fn test_scan_rejects_actual_nested_nulls() -> TestResult {
        let store_url = Url::parse("memory:///")?;
        let log_store =
            crate::logstore::logstore_for(&store_url, crate::logstore::StorageConfig::default())?;

        // Delta log schema: metadata_col.int_id is NOT nullable.
        let meta_type = crate::kernel::StructType::try_new(vec![crate::kernel::StructField::new(
            "int_id",
            crate::kernel::DataType::Primitive(crate::kernel::PrimitiveType::String),
            false,
        )])?;
        let table = crate::operations::create::CreateBuilder::new()
            .with_log_store(log_store.clone())
            .with_columns(vec![crate::kernel::StructField::new(
                "metadata_col",
                crate::kernel::DataType::Struct(Box::new(meta_type)),
                true,
            )])
            .await?;

        // Parquet file whose data contains an actual null in int_id, registered
        // via a raw Add action to bypass the delta-rs writer.
        let meta_fields = Fields::from(vec![Field::new("int_id", DataType::Utf8, true)]);
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "metadata_col",
            DataType::Struct(meta_fields.clone()),
            true,
        )]));
        let meta = StructArray::try_new(
            meta_fields,
            vec![Arc::new(StringArray::from(vec![Some("t1"), None])) as ArrayRef],
            None,
        )?;
        let batch = RecordBatch::try_new(file_schema.clone(), vec![Arc::new(meta)])?;

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, file_schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let size = buffer.len() as i64;

        let store = log_store.object_store(None);
        store
            .put(&Path::from("part-00000.parquet"), buffer.into())
            .await?;

        crate::kernel::transaction::CommitBuilder::default()
            .with_actions(vec![crate::kernel::Action::Add(crate::kernel::Add {
                path: "part-00000.parquet".to_string(),
                size,
                data_change: true,
                ..Default::default()
            })])
            .build(
                Some(table.snapshot()?),
                log_store.clone(),
                crate::protocol::DeltaOperation::Write {
                    mode: crate::protocol::SaveMode::Append,
                    partition_by: None,
                    predicate: None,
                },
            )
            .await?;

        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let provider = crate::delta_datafusion::table_provider::next::DeltaScan::builder()
            .with_snapshot(snapshot)
            .with_log_store(log_store)
            .await?;

        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store);
        session.register_table("delta_table", provider)?;

        let result = session
            .sql("SELECT * FROM delta_table")
            .await?
            .collect()
            .await;
        assert!(
            result.is_err(),
            "expected error when reading actual nulls in a non-nullable nested field, got: {result:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_plan_multiple_stores() -> TestResult {
        let store_1 = Arc::new(InMemory::new());
        let store_url_1 = Url::parse("first:///")?;
        let store_2 = Arc::new(InMemory::new());
        let store_url_2 = Url::parse("second:///")?;

        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url_1, store_1.clone());
        session
            .runtime_env()
            .register_object_store(&store_url_2, store_2.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let data_1 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )?;
        let data_2 = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(StringArray::from(vec![Some("b")])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data_1)?;
        arrow_writer.close()?;
        let path = Path::from("test_data.parquet");
        store_1.put(&path, buffer.into()).await?;
        let mut file_1: PartitionedFile = store_1.head(&path).await?.into();
        file_1
            .partition_values
            .push(wrap_file_id_value("first:///test_data.parquet"));

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data_2)?;
        arrow_writer.close()?;
        let path = Path::from("test_data.parquet");
        store_2.put(&path, buffer.into()).await?;
        let mut file_2: PartitionedFile = store_2.head(&path).await?.into();
        file_2
            .partition_values
            .push(wrap_file_id_value("second:///test_data.parquet"));

        let files_by_store = vec![
            (
                store_url_1.as_object_store_url(),
                vec![(file_1, None::<Vec<bool>>)],
            ),
            (
                store_url_2.as_object_store_url(),
                vec![(file_2, None::<Vec<bool>>)],
            ),
        ];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&arrow_schema, &file_id_field);

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            None,
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 1  | a     | first:///test_data.parquet  |",
            "| 2  | b     | second:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_plan_predicate() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_data.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_data.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&arrow_schema, &file_id_field);

        let predicate = col("id").eq(lit(2i32));
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 2  | b     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_predicate_pushdown_skips_pushdown_when_logical_rewrite_fails() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let parquet_read_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let logical_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("missing", DataType::Int32, false),
        ]));
        let data = RecordBatch::try_new(
            parquet_read_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer =
            ArrowWriter::try_new(&mut buffer, parquet_read_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_rewrite_failure.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_rewrite_failure.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&logical_schema, &file_id_field);
        let predicate = col("missing").eq(lit(1i32));

        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        let expected = vec![
            "+----+----------------------------------------+",
            "| id | __delta_rs_file_id__                   |",
            "+----+----------------------------------------+",
            "| 1  | memory:///test_rewrite_failure.parquet |",
            "| 2  | memory:///test_rewrite_failure.parquet |",
            "| 3  | memory:///test_rewrite_failure.parquet |",
            "+----+----------------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_predicate_pushdown_allows_view_literal_against_base_parquet_file() -> TestResult {
        use datafusion::scalar::ScalarValue;

        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        // Write a Parquet file with base types, but read it with a view-typed schema.
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let parquet_read_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let data = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                ])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, file_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_view_literal.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_view_literal.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&parquet_read_schema, &file_id_field);

        let predicate = col("name").eq(lit(ScalarValue::Utf8View(Some("bob".to_string()))));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;

        let expected = vec![
            "+----+------+-------------------------------------+",
            "| id | name | __delta_rs_file_id__                |",
            "+----+------+-------------------------------------+",
            "| 2  | bob  | memory:///test_view_literal.parquet |",
            "+----+------+-------------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_predicate_pushdown_allows_sql_literal_against_view_schema() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        // Write a Parquet file with base types, but read it with a view-typed schema.
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let parquet_read_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let data = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                ])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, file_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_sql_literal.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_sql_literal.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&parquet_read_schema, &file_id_field);

        let predicate = col("name").eq(lit("bob"));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;

        let expected = vec![
            "+----+------+------------------------------------+",
            "| id | name | __delta_rs_file_id__               |",
            "+----+------+------------------------------------+",
            "| 2  | bob  | memory:///test_sql_literal.parquet |",
            "+----+------+------------------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn test_predicate_pushdown_allows_physical_column_mapping_names() -> TestResult {
        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let physical_name = "col-3877fd94-0973-4941-ac6b-646849a1ff65";
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(physical_name, DataType::Utf8, true),
        ]));
        let parquet_read_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(physical_name, DataType::Utf8View, true),
        ]));
        let data = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    Some("bob"),
                    Some("charlie"),
                ])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, file_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_column_mapping_pushdown.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values.push(wrap_file_id_value(
            "memory:///test_column_mapping_pushdown.parquet",
        ));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&parquet_read_schema, &file_id_field);

        let predicate = col(physical_name).eq(lit("bob"));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 3);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 2);

        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "bob");

        assert_eq!(batches[0].schema().field(1).name(), physical_name);
        assert_eq!(batches[0].schema().field(2).name(), FILE_ID_COLUMN_DEFAULT);

        Ok(())
    }

    #[tokio::test]
    async fn test_predicate_pushdown_allows_binaryview_literal_against_base_parquet_file()
    -> TestResult {
        use datafusion::scalar::ScalarValue;

        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Binary, true),
        ]));
        let parquet_read_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::BinaryView, true),
        ]));
        let data = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(b"aaa".as_slice()),
                    Some(b"bbb".as_slice()),
                    Some(b"ccc".as_slice()),
                ])),
            ],
        )?;

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, file_schema.clone(), None)?;
        arrow_writer.write(&data)?;
        arrow_writer.close()?;

        let path = Path::from("test_binary_view.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_file_id_value("memory:///test_binary_view.parquet"));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field =
            crate::delta_datafusion::file_id::file_id_field(Some(FILE_ID_COLUMN_DEFAULT));
        let parquet_predicate_schema =
            build_parquet_predicate_schema(&parquet_read_schema, &file_id_field);

        let predicate = col("data").eq(lit(ScalarValue::BinaryView(Some(b"bbb".to_vec()))));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
            &parquet_predicate_schema,
            None,
            &file_id_field,
            Some(&predicate),
        )
        .await?;
        let batches = collect(plan, session.task_ctx()).await?;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 2);

        let data_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap();
        assert_eq!(data_col.value(0), b"bbb");

        assert_eq!(batches[0].num_columns(), 3);
        assert_eq!(batches[0].schema().field(2).name(), FILE_ID_COLUMN_DEFAULT);

        Ok(())
    }
}
