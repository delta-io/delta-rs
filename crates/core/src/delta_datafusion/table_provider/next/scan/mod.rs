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

use std::{collections::VecDeque, pin::Pin, sync::Arc};

use arrow::array::AsArray;
use arrow_array::{ArrayRef, RecordBatch, StructArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, FieldRef, Schema, SchemaBuilder, SchemaRef};
use chrono::{TimeZone as _, Utc};
use dashmap::DashMap;
use datafusion::{
    catalog::Session,
    common::{
        ColumnStatistics, HashMap, Result, Statistics, ToDFSchema, plan_err, stats::Precision,
    },
    config::TableParquetOptions,
    datasource::physical_plan::{ParquetSource, parquet::CachedParquetFileReaderFactory},
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    physical_expr::{PhysicalExpr, PhysicalExprSimplifier},
    physical_expr_adapter::{
        DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
    },
    physical_plan::{
        ExecutionPlan,
        empty::EmptyExec,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder},
        union::UnionExec,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use datafusion_datasource::{
    PartitionedFile, TableSchema, compute_all_files_statistics,
    file_groups::FileGroup,
    file_scan_config::{FileScanConfigBuilder, wrap_partition_value_in_dict},
    source::DataSourceExec,
};
use delta_kernel::{
    Engine, Expression, expressions::StructData, scan::ScanMetadata, table_features::TableFeature,
};
use futures::{Stream, TryStreamExt as _, future::ready};
use itertools::Itertools as _;
use object_store::{ObjectMeta, path::Path};

pub use self::exec::DeltaScanExec;
use self::exec_meta::DeltaScanMetaExec;
pub(crate) use self::plan::{KernelScanPlan, supports_filters_pushdown};
use self::replay::{ScanFileContext, ScanFileStream};
use crate::{
    DeltaTableError,
    delta_datafusion::{
        DeltaScanConfig,
        engine::{AsObjectStoreUrl as _, to_datafusion_scalar},
    },
};

mod exec;
mod exec_meta;
mod plan;
mod replay;

type ScanMetadataStream = Pin<Box<dyn Stream<Item = Result<ScanMetadata, DeltaTableError>> + Send>>;

fn align_file_stats_to_schema(mut stats: Statistics, schema: &SchemaRef) -> Statistics {
    // DataFusion expects `column_statistics` to have exactly one entry per field in the schema the
    // stats refer to. If the widths don't match, don't risk mis-associating stats with the wrong
    // columns (which could lead to incorrect pruning decisions or type errors during constant
    // folding). Fall back to unknown column stats.
    if stats.column_statistics.len() != schema.fields().len() {
        stats.column_statistics = Statistics::unknown_column(schema.as_ref());
        return stats;
    }

    for idx in 0..schema.fields().len() {
        let data_type = schema.field(idx).data_type();
        let col_stats = &mut stats.column_statistics[idx];
        let min_value = std::mem::replace(&mut col_stats.min_value, Precision::Absent);
        let max_value = std::mem::replace(&mut col_stats.max_value, Precision::Absent);
        let sum_value = std::mem::replace(&mut col_stats.sum_value, Precision::Absent);
        col_stats.min_value = align_precision_scalar_to_type(min_value, data_type);
        col_stats.max_value = align_precision_scalar_to_type(max_value, data_type);
        col_stats.sum_value = align_precision_scalar_to_type(sum_value, data_type);
    }
    stats
}

fn align_precision_scalar_to_type(
    precision: Precision<ScalarValue>,
    data_type: &DataType,
) -> Precision<ScalarValue> {
    match precision {
        Precision::Exact(v) => Precision::Exact(align_scalar_to_type(v, data_type)),
        Precision::Inexact(v) => Precision::Inexact(align_scalar_to_type(v, data_type)),
        Precision::Absent => Precision::Absent,
    }
}

fn align_scalar_to_type(value: ScalarValue, data_type: &DataType) -> ScalarValue {
    match (data_type, value) {
        // Base -> view types (needed when schema_force_view_types=true)
        (DataType::Utf8View, ScalarValue::Utf8(v)) => ScalarValue::Utf8View(v),
        (DataType::Utf8View, ScalarValue::LargeUtf8(v)) => ScalarValue::Utf8View(v),
        (DataType::BinaryView, ScalarValue::Binary(v)) => ScalarValue::BinaryView(v),
        (DataType::BinaryView, ScalarValue::LargeBinary(v)) => ScalarValue::BinaryView(v),

        // View -> base types (useful when schema_force_view_types=false)
        (DataType::Utf8, ScalarValue::Utf8View(v)) => ScalarValue::Utf8(v),
        (DataType::Utf8, ScalarValue::LargeUtf8(v)) => ScalarValue::Utf8(v),
        (DataType::LargeUtf8, ScalarValue::Utf8View(v)) => ScalarValue::LargeUtf8(v),
        (DataType::LargeUtf8, ScalarValue::Utf8(v)) => ScalarValue::LargeUtf8(v),
        (DataType::Binary, ScalarValue::BinaryView(v)) => ScalarValue::Binary(v),
        (DataType::Binary, ScalarValue::LargeBinary(v)) => ScalarValue::Binary(v),
        (DataType::LargeBinary, ScalarValue::BinaryView(v)) => ScalarValue::LargeBinary(v),
        (DataType::LargeBinary, ScalarValue::Binary(v)) => ScalarValue::LargeBinary(v),

        // Dictionary values align their inner scalar to the dictionary value type.
        (DataType::Dictionary(_, value_type), ScalarValue::Dictionary(key_type, value)) => {
            let aligned_value = align_scalar_to_type(*value, value_type);
            ScalarValue::Dictionary(key_type, Box::new(aligned_value))
        }

        (_, v) => v,
    }
}

pub(super) async fn execution_plan(
    config: &DeltaScanConfig,
    session: &dyn Session,
    scan_plan: KernelScanPlan,
    stream: ScanMetadataStream,
    engine: Arc<dyn Engine>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (files, transforms, dvs, metrics) =
        replay_files(engine, &scan_plan, config.clone(), stream).await?;

    let file_id_field = config.file_id_field();
    if scan_plan.is_metadata_only() {
        let map_file = |f: &ScanFileContext| {
            Ok((
                f.file_url.to_string(),
                match &f.stats.num_rows {
                    Precision::Exact(n) => *n,
                    _ => {
                        return plan_err!("Expected exact row counts in: {}", f.file_url);
                    }
                },
            ))
        };

        let maybe_file_rows = files
            .iter()
            .map(map_file)
            .try_collect::<_, VecDeque<_>, _>();
        if let Ok(file_rows) = maybe_file_rows {
            let exec = DeltaScanMetaExec::new(
                Arc::new(scan_plan),
                vec![file_rows],
                Arc::new(transforms),
                Arc::new(dvs),
                config.retain_file_id().then_some(file_id_field),
                metrics,
            );
            return Ok(Arc::new(exec) as _);
        }
    }

    get_data_scan_plan(
        session,
        scan_plan,
        files,
        transforms,
        dvs,
        metrics,
        limit,
        file_id_field,
        config.retain_file_id(),
    )
    .await
}

async fn replay_files(
    engine: Arc<dyn Engine>,
    scan_plan: &KernelScanPlan,
    scan_config: DeltaScanConfig,
    stream: ScanMetadataStream,
) -> Result<(
    Vec<ScanFileContext>,
    HashMap<String, Arc<Expression>>,
    DashMap<String, Vec<bool>>,
    ExecutionPlanMetricsSet,
)> {
    let mut stream = ScanFileStream::new(engine, &scan_plan.scan, scan_config, stream);
    let mut files = Vec::new();
    while let Some(file) = stream.try_next().await? {
        files.extend(file);
    }

    let transforms: HashMap<_, _> = files
        .iter_mut()
        .flat_map(|file| {
            file.transform
                .take()
                .map(|t| (file.file_url.to_string(), t))
        })
        .collect();

    let dv_stream = stream.dv_stream.build();
    let dvs: DashMap<_, _> = dv_stream
        .try_filter_map(|(url, dv)| ready(Ok(dv.map(|dv| (url.to_string(), dv)))))
        .try_collect()
        .await?;

    let metrics = ExecutionPlanMetricsSet::new();
    MetricBuilder::new(&metrics)
        .global_counter("count_files_scanned")
        .add(stream.metrics.num_scanned);

    Ok((files, transforms, dvs, metrics))
}

async fn get_data_scan_plan(
    session: &dyn Session,
    scan_plan: KernelScanPlan,
    files: Vec<ScanFileContext>,
    transforms: HashMap<String, Arc<Expression>>,
    dvs: DashMap<String, Vec<bool>>,
    metrics: ExecutionPlanMetricsSet,
    limit: Option<usize>,
    file_id_field: FieldRef,
    retain_file_ids: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut partition_stats = HashMap::new();
    let parquet_read_schema = scan_plan.parquet_read_schema.clone();

    // Convert the files into datafusions `PartitionedFile`s grouped by the object store they are stored in
    // this is used to create a DataSourceExec plan for each store
    // To correlate the data with the original file, we add the file url as a partition value
    // This is required to apply the correct transform to the data in downstream processing.
    let to_partitioned_file = |f: ScanFileContext| {
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
        let file_value =
            wrap_partition_value_in_dict(ScalarValue::Utf8(Some(f.file_url.to_string())));
        // NOTE: `PartitionedFile::with_statistics` appends exact stats for partition columns based
        // on `partition_values`, so partition values must be set first.
        partitioned_file.partition_values = vec![file_value.clone()];
        // DataFusion's ParquetOpener may replace constant columns with literals using file stats.
        // Align scalar types (e.g. Utf8 -> Utf8View) to match the Parquet read schema so literal
        // replacement doesn't produce arrays with mismatched types.
        let stats = align_file_stats_to_schema(f.stats, &parquet_read_schema);
        partitioned_file = partitioned_file.with_statistics(Arc::new(stats));
        Ok::<_, DataFusionError>((
            f.file_url.as_object_store_url(),
            (partitioned_file, None::<Vec<bool>>),
        ))
    };

    // Group the files by their object store url. Since datafusion assumes that all files in a
    // DataSourceExec are stored in the same object store, we need to create one plan per store
    let files_by_store = files
        .into_iter()
        .map(to_partitioned_file)
        .try_collect::<_, Vec<_>, _>()?
        .into_iter()
        .into_group_map();

    // TODO(roeap); not sure exactly how row tracking is implemented in kernel right now
    // so leaving predicate as None for now until we are sure this is safe to do.
    let table_config = scan_plan.table_configuration();
    let predicate = if table_config.is_feature_enabled(&TableFeature::RowTracking) {
        None
    } else {
        scan_plan.parquet_predicate.as_ref()
    };
    let file_id_column = file_id_field.name().clone();
    let pq_plan = get_read_plan(
        session,
        files_by_store,
        &scan_plan.parquet_read_schema,
        limit,
        &file_id_field,
        predicate,
    )
    .await?;

    let exec = DeltaScanExec::new(
        Arc::new(scan_plan),
        pq_plan,
        Arc::new(transforms),
        Arc::new(dvs),
        partition_stats,
        file_id_column,
        retain_file_ids,
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
async fn get_read_plan(
    state: &dyn Session,
    files_by_store: impl IntoIterator<Item = FilesByStore>,
    // Schema of physical file columns to read from Parquet (no Delta partitions, no file-id).
    //
    // This is also the schema used for Parquet pruning/pushdown. It may include view types
    // (e.g. Utf8View/BinaryView) depending on `DeltaScanConfig`.
    parquet_read_schema: &SchemaRef,
    limit: Option<usize>,
    file_id_field: &FieldRef,
    predicate: Option<&Expr>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plans = Vec::new();

    let pq_options = TableParquetOptions {
        global: state.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    let mut full_read_schema = SchemaBuilder::from(parquet_read_schema.as_ref().clone());
    full_read_schema.push(file_id_field.as_ref().clone().with_nullable(true));
    let full_read_schema = Arc::new(full_read_schema.finish());
    let full_read_df_schema = full_read_schema.clone().to_dfschema()?;

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
            // Predicate pushdown can reference the synthetic file-id partition column.
            // Use the full read schema (data columns + file-id) when planning.
            let physical = state.create_physical_expr(pred.clone(), &full_read_df_schema)?;
            file_source = file_source
                .with_predicate(physical)
                .with_pushdown_filters(true);
        }

        let file_group: FileGroup = files.into_iter().map(|file| file.0).collect();
        let (file_groups, statistics) =
            compute_all_files_statistics(vec![file_group], full_table_schema, true, false)?;

        let config = FileScanConfigBuilder::new(store_url, Arc::new(file_source))
            .with_file_groups(file_groups)
            .with_statistics(statistics)
            .with_limit(limit)
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
) -> Result<RecordBatch> {
    let result = if let Some(projection) = scan_plan.result_projection.as_ref() {
        batch.project(projection)?
    } else {
        batch
    };
    // NOTE: most data is read properly typed already, however columns added via
    // literals in the transformations may need to be cast to the physical expected type.
    let result = cast_record_batch(result, &scan_plan.result_schema)?;
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

fn cast_record_batch(batch: RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() == 0 {
        if !target_schema.fields().is_empty() {
            return plan_err!(
                "Cannot cast empty RecordBatch to non-empty schema: {:?}",
                target_schema
            );
        }
        return Ok(batch);
    }

    let options = CastOptions {
        safe: true,
        ..Default::default()
    };
    Ok(cast_with_options(
        &StructArray::from(batch),
        &DataType::Struct(target_schema.fields().clone()),
        &options,
    )?
    .as_struct()
    .into())
}

#[derive(Debug)]
struct DeltaPhysicalExprAdapterFactory;

#[derive(Debug)]
struct DeltaPhysicalExprAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        let inner = DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, physical_file_schema.clone());
        Arc::new(DeltaPhysicalExprAdapter {
            inner,
            physical_file_schema,
        })
    }
}

impl PhysicalExprAdapter for DeltaPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = self.inner.rewrite(expr)?;
        let simplifier = PhysicalExprSimplifier::new(self.physical_file_schema.as_ref());
        simplifier.simplify(expr)
    }
}
#[cfg(test)]
mod tests {
    use arrow_array::{
        BinaryArray, BinaryViewArray, Int32Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::{
        physical_plan::collect,
        prelude::{col, lit},
    };
    use object_store::{ObjectStore as _, memory::InMemory};
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{session::create_session, table_provider::next::FILE_ID_COLUMN_DEFAULT},
        test_utils::TestResult,
    };

    use super::*;

    #[test]
    fn test_align_scalar_to_type_string_views() -> Result<()> {
        let base = ScalarValue::Utf8(Some("a".to_string()));
        assert_eq!(
            align_scalar_to_type(base, &DataType::Utf8View),
            ScalarValue::Utf8View(Some("a".to_string()))
        );

        let base = ScalarValue::LargeUtf8(Some("b".to_string()));
        assert_eq!(
            align_scalar_to_type(base, &DataType::Utf8View),
            ScalarValue::Utf8View(Some("b".to_string()))
        );

        let view = ScalarValue::Utf8View(Some("c".to_string()));
        assert_eq!(
            align_scalar_to_type(view, &DataType::Utf8),
            ScalarValue::Utf8(Some("c".to_string()))
        );

        let view = ScalarValue::Utf8View(Some("d".to_string()));
        assert_eq!(
            align_scalar_to_type(view, &DataType::LargeUtf8),
            ScalarValue::LargeUtf8(Some("d".to_string()))
        );

        Ok(())
    }

    #[test]
    fn test_align_scalar_to_type_binary_views() -> Result<()> {
        let base = ScalarValue::Binary(Some(vec![1, 2, 3]));
        assert_eq!(
            align_scalar_to_type(base, &DataType::BinaryView),
            ScalarValue::BinaryView(Some(vec![1, 2, 3]))
        );

        let base = ScalarValue::LargeBinary(Some(vec![4, 5, 6]));
        assert_eq!(
            align_scalar_to_type(base, &DataType::BinaryView),
            ScalarValue::BinaryView(Some(vec![4, 5, 6]))
        );

        let view = ScalarValue::BinaryView(Some(vec![7, 8, 9]));
        assert_eq!(
            align_scalar_to_type(view, &DataType::Binary),
            ScalarValue::Binary(Some(vec![7, 8, 9]))
        );

        let view = ScalarValue::BinaryView(Some(vec![10, 11, 12]));
        assert_eq!(
            align_scalar_to_type(view, &DataType::LargeBinary),
            ScalarValue::LargeBinary(Some(vec![10, 11, 12]))
        );

        Ok(())
    }

    #[test]
    fn test_align_scalar_to_type_dictionary_inner() -> Result<()> {
        let scalar = ScalarValue::Dictionary(
            Box::new(DataType::UInt16),
            Box::new(ScalarValue::Utf8(Some("a".to_string()))),
        );
        let dt = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8View));
        assert_eq!(
            align_scalar_to_type(scalar, &dt),
            ScalarValue::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(ScalarValue::Utf8View(Some("a".to_string())))
            )
        );

        let scalar = ScalarValue::Dictionary(
            Box::new(DataType::UInt16),
            Box::new(ScalarValue::Utf8View(Some("b".to_string()))),
        );
        let dt = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
        assert_eq!(
            align_scalar_to_type(scalar, &dt),
            ScalarValue::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(ScalarValue::Utf8(Some("b".to_string())))
            )
        );

        Ok(())
    }

    #[test]
    fn test_align_file_stats_to_schema_string_views() -> Result<()> {
        let col_stats = ColumnStatistics::new_unknown()
            .with_min_value(Precision::Exact(ScalarValue::Utf8(Some("a".to_string()))))
            .with_max_value(Precision::Exact(ScalarValue::Utf8(Some("b".to_string()))))
            .with_sum_value(Precision::Exact(ScalarValue::Utf8(Some("c".to_string()))));
        let stats = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![col_stats],
        };

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8View,
            true,
        )]));
        let aligned = align_file_stats_to_schema(stats, &schema);
        assert_eq!(aligned.column_statistics.len(), 1);
        let col_stats = &aligned.column_statistics[0];

        assert_eq!(
            col_stats.min_value,
            Precision::Exact(ScalarValue::Utf8View(Some("a".to_string())))
        );
        assert_eq!(
            col_stats.max_value,
            Precision::Exact(ScalarValue::Utf8View(Some("b".to_string())))
        );
        assert_eq!(
            col_stats.sum_value,
            Precision::Exact(ScalarValue::Utf8View(Some("c".to_string())))
        );

        Ok(())
    }

    #[test]
    fn test_align_file_stats_to_schema_length_mismatch_drops_column_stats() -> Result<()> {
        let col0 = ColumnStatistics::new_unknown()
            .with_min_value(Precision::Exact(ScalarValue::Utf8(Some("a".to_string()))));
        let col1 = ColumnStatistics::new_unknown()
            .with_min_value(Precision::Exact(ScalarValue::Utf8(Some("b".to_string()))));
        let stats = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![col0, col1],
        };

        // Schema has fewer fields than stats. We drop column stats to unknown to avoid mis-association.
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Utf8View,
            true,
        )]));
        let aligned = align_file_stats_to_schema(stats, &schema);
        assert_eq!(aligned.column_statistics.len(), 1);
        assert_eq!(aligned.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[0].max_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[0].sum_value, Precision::Absent);

        // Schema has more fields than stats. We drop column stats to unknown to avoid mis-association.
        let col0 = ColumnStatistics::new_unknown()
            .with_min_value(Precision::Exact(ScalarValue::Utf8(Some("c".to_string()))));
        let stats = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![col0],
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("col0", DataType::Utf8View, true),
            Field::new("col1", DataType::Utf8View, true),
        ]));
        let aligned = align_file_stats_to_schema(stats, &schema);
        assert_eq!(aligned.column_statistics.len(), 2);
        assert_eq!(aligned.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[0].max_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[0].sum_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[1].min_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[1].max_value, Precision::Absent);
        assert_eq!(aligned.column_statistics[1].sum_value, Precision::Absent);

        Ok(())
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
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "memory:///test_data.parquet".to_string(),
            ))));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
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
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
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
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "memory:///test_data.parquet".to_string(),
            ))));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
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
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
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
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "first:///test_data.parquet".to_string(),
            ))));

        let mut buffer = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
        arrow_writer.write(&data_2)?;
        arrow_writer.close()?;
        let path = Path::from("test_data.parquet");
        store_2.put(&path, buffer.into()).await?;
        let mut file_2: PartitionedFile = store_2.head(&path).await?.into();
        file_2
            .partition_values
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "second:///test_data.parquet".to_string(),
            ))));

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

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
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
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "memory:///test_data.parquet".to_string(),
            ))));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let predicate = col("id").eq(lit(2i32));
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
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
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "memory:///test_view_literal.parquet".to_string(),
            ))));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let predicate = col("name").eq(lit(ScalarValue::Utf8View(Some("bob".to_string()))));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
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

        let path = Path::from("test_sql_literal.parquet");
        store.put(&path, buffer.into()).await?;
        let mut file: PartitionedFile = store.head(&path).await?.into();
        file.partition_values
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "memory:///test_sql_literal.parquet".to_string(),
            ))));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let predicate = col("name").eq(lit("bob"));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
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
            .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                "memory:///test_binary_view.parquet".to_string(),
            ))));

        let files_by_store = vec![(
            store_url.as_object_store_url(),
            vec![(file, None::<Vec<bool>>)],
        )];

        let file_id_field = Arc::new(Field::new(
            FILE_ID_COLUMN_DEFAULT,
            DataType::Dictionary(DataType::UInt16.into(), DataType::Utf8.into()),
            false,
        ));

        let predicate = col("data").eq(lit(ScalarValue::BinaryView(Some(b"bbb".to_vec()))));
        let plan = get_read_plan(
            &session.state(),
            files_by_store,
            &parquet_read_schema,
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
