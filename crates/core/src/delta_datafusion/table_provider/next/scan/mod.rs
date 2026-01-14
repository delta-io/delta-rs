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

use arrow_array::{ArrayRef, RecordBatch};

use arrow_schema::{DataType, FieldRef, Schema, SchemaRef};
use chrono::{TimeZone as _, Utc};
use dashmap::DashMap;
use datafusion::{
    catalog::Session,
    common::{
        ColumnStatistics, DFSchema, HashMap, Result, Statistics, ToDFSchema, plan_err,
        stats::Precision,
    },
    config::TableParquetOptions,
    datasource::physical_plan::{ParquetSource, parquet::CachedParquetFileReaderFactory},
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    logical_expr::ExprSchemable,
    physical_expr::planner::logical2physical,
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
        DeltaPhysicalExprAdapterFactory, DeltaScanConfig,
        engine::{AsObjectStoreUrl as _, to_datafusion_scalar},
    },
};

mod exec;
mod exec_meta;
mod plan;
mod replay;

type ScanMetadataStream = Pin<Box<dyn Stream<Item = Result<ScanMetadata, DeltaTableError>> + Send>>;

pub(super) async fn execution_plan(
    config: &DeltaScanConfig,
    session: &dyn Session,
    scan_plan: KernelScanPlan,
    stream: ScanMetadataStream,
    engine: Arc<dyn Engine>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let (files, transforms, dvs, metrics) = replay_files(engine, &scan_plan, stream).await?;

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
            return Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>);
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
        config.enable_parquet_pushdown,
    )
    .await
}

async fn replay_files(
    engine: Arc<dyn Engine>,
    scan_plan: &KernelScanPlan,
    stream: ScanMetadataStream,
) -> Result<(
    Vec<ScanFileContext>,
    HashMap<String, Arc<Expression>>,
    DashMap<String, Vec<bool>>,
    ExecutionPlanMetricsSet,
)> {
    let mut stream = ScanFileStream::new(engine, &scan_plan.scan, stream);
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
    enable_parquet_pushdown: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut partition_stats = HashMap::new();

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
        partitioned_file = partitioned_file.with_statistics(Arc::new(f.stats));
        partitioned_file.partition_values = vec![wrap_partition_value_in_dict(ScalarValue::Utf8(
            Some(f.file_url.to_string()),
        ))];
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
    let has_selection_vectors = files_by_store
        .values()
        .any(|files| files.iter().any(|(_, selection)| selection.is_some()));
    let parquet_pushdown_active =
        enable_parquet_pushdown && predicate.is_some() && !has_selection_vectors;
    let skip_filter_predicate =
        parquet_pushdown_active && scan_plan.parquet_predicate_covers_all_filters;
    let file_id_column = file_id_field.name().clone();
    let pq_plan = get_read_plan(
        session,
        files_by_store,
        &scan_plan.parquet_read_schema,
        limit,
        &file_id_field,
        predicate,
        enable_parquet_pushdown,
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
        skip_filter_predicate,
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
    physical_schema: &SchemaRef,
    limit: Option<usize>,
    file_id_field: &FieldRef,
    predicate: Option<&Expr>,
    enable_parquet_pushdown: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut plans = Vec::new();

    let pq_options = TableParquetOptions {
        global: state.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    for (store_url, files) in files_by_store.into_iter() {
        let reader_factory = Arc::new(CachedParquetFileReaderFactory::new(
            state.runtime_env().object_store(&store_url)?,
            state.runtime_env().cache_manager.get_file_metadata_cache(),
        ));

        let table_schema = TableSchema::new(physical_schema.clone(), vec![file_id_field.clone()]);
        let mut file_source = ParquetSource::new(table_schema)
            .with_table_parquet_options(pq_options.clone())
            .with_parquet_file_reader_factory(reader_factory);

        // TODO(roeap); we might be able to also push selection vectors into the read plan
        // by creating parquet access plans. However we need to make sure this does not
        // interfere with other delta features like row ids.
        let has_selection_vectors = files.iter().any(|(_, sv)| sv.is_some());
        if enable_parquet_pushdown
            && !has_selection_vectors
            && let Some(pred) = predicate
        {
            let pred = normalize_predicate_to_schema(pred.clone(), physical_schema)?;
            let physical = logical2physical(&pred, physical_schema.as_ref());
            file_source = file_source
                .with_predicate(physical)
                .with_pushdown_filters(true);
        }

        let file_group: FileGroup = files.into_iter().map(|file| file.0).collect();
        let (file_groups, mut statistics) =
            compute_all_files_statistics(vec![file_group], physical_schema.clone(), true, false)?;

        statistics
            .column_statistics
            .push(ColumnStatistics::new_unknown());

        let config = FileScanConfigBuilder::new(store_url, Arc::new(file_source))
            .with_file_groups(file_groups)
            .with_statistics(statistics)
            .with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory::new())))
            .with_limit(limit)
            .build();

        plans.push(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>);
    }

    Ok(match plans.len() {
        0 => Arc::new(EmptyExec::new(physical_schema.clone())),
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
    // literals in the transdormations may need to be cast to the physical expected type.
    let result = cast_record_batch(result, &scan_plan.result_schema)?;
    if let Some((arr, field)) = file_id_col {
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
    crate::kernel::schema::cast::cast_record_batch(&batch, Arc::clone(target_schema), true, true)
        .map_err(|err| DataFusionError::External(Box::new(err)))
}

pub(crate) fn normalize_predicate_to_schema(expr: Expr, schema: &SchemaRef) -> Result<Expr> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::BinaryExpr;
    use datafusion::logical_expr::expr::{InList, Like};

    let df_schema = schema.clone().to_dfschema()?;

    expr.transform(|e| match &e {
        Expr::BinaryExpr(binary) => {
            let left_col_type = get_column_type_for_expr(&binary.left, &df_schema);
            let right_col_type = get_column_type_for_expr(&binary.right, &df_schema);

            let new_left = convert_expr_literal_for_target(&binary.left, &right_col_type);
            let new_right = convert_expr_literal_for_target(&binary.right, &left_col_type);

            if new_left.is_some() || new_right.is_some() {
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(new_left.unwrap_or_else(|| (*binary.left).clone())),
                    op: binary.op.clone(),
                    right: Box::new(new_right.unwrap_or_else(|| (*binary.right).clone())),
                })))
            } else {
                Ok(Transformed::no(e))
            }
        }
        Expr::InList(in_list) => {
            if let Some(target_type) = get_column_type_for_expr(&in_list.expr, &df_schema) {
                let mut changed = false;
                let new_list: Vec<_> = in_list
                    .list
                    .iter()
                    .map(|item| {
                        if let Some(converted) =
                            convert_expr_literal_for_target(item, &Some(target_type.clone()))
                        {
                            changed = true;
                            converted
                        } else {
                            item.clone()
                        }
                    })
                    .collect();

                if changed {
                    Ok(Transformed::yes(Expr::InList(InList {
                        expr: in_list.expr.clone(),
                        list: new_list,
                        negated: in_list.negated,
                    })))
                } else {
                    Ok(Transformed::no(e))
                }
            } else {
                Ok(Transformed::no(e))
            }
        }
        Expr::Like(like) => {
            if let Some(target_type) = get_column_type_for_expr(&like.expr, &df_schema) {
                if let Some(new_pattern) =
                    convert_expr_literal_for_target(&like.pattern, &Some(target_type))
                {
                    Ok(Transformed::yes(Expr::Like(Like {
                        negated: like.negated,
                        expr: like.expr.clone(),
                        pattern: Box::new(new_pattern),
                        escape_char: like.escape_char,
                        case_insensitive: like.case_insensitive,
                    })))
                } else {
                    Ok(Transformed::no(e))
                }
            } else {
                Ok(Transformed::no(e))
            }
        }
        Expr::SimilarTo(like) => {
            if let Some(target_type) = get_column_type_for_expr(&like.expr, &df_schema) {
                if let Some(new_pattern) =
                    convert_expr_literal_for_target(&like.pattern, &Some(target_type))
                {
                    Ok(Transformed::yes(Expr::SimilarTo(Like {
                        negated: like.negated,
                        expr: like.expr.clone(),
                        pattern: Box::new(new_pattern),
                        escape_char: like.escape_char,
                        case_insensitive: like.case_insensitive,
                    })))
                } else {
                    Ok(Transformed::no(e))
                }
            } else {
                Ok(Transformed::no(e))
            }
        }
        _ => Ok(Transformed::no(e)),
    })
    .map(|t| t.data)
}

fn get_column_type_for_expr(expr: &Expr, df_schema: &DFSchema) -> Option<DataType> {
    if let Expr::Literal(_, _) = expr {
        return None;
    }
    expr.get_type(df_schema).ok()
}

fn convert_expr_literal_for_target(expr: &Expr, target_type: &Option<DataType>) -> Option<Expr> {
    let target = target_type.as_ref()?;
    if let Expr::Literal(scalar, meta) = expr {
        convert_literal_for_column_type(scalar, target)
            .map(|converted| Expr::Literal(converted, meta.clone()))
    } else {
        None
    }
}

fn convert_literal_for_column_type(
    scalar: &ScalarValue,
    target_type: &DataType,
) -> Option<ScalarValue> {
    match target_type {
        DataType::Utf8View => match scalar {
            ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => {
                Some(ScalarValue::Utf8View(v.clone()))
            }
            _ => None,
        },
        DataType::BinaryView => match scalar {
            ScalarValue::Binary(v) | ScalarValue::LargeBinary(v) => {
                Some(ScalarValue::BinaryView(v.clone()))
            }
            _ => None,
        },
        DataType::Utf8 => match scalar {
            ScalarValue::Utf8View(v) | ScalarValue::LargeUtf8(v) => {
                Some(ScalarValue::Utf8(v.clone()))
            }
            _ => None,
        },
        DataType::LargeUtf8 => match scalar {
            ScalarValue::Utf8View(v) | ScalarValue::Utf8(v) => {
                Some(ScalarValue::LargeUtf8(v.clone()))
            }
            _ => None,
        },
        DataType::Binary => match scalar {
            ScalarValue::BinaryView(v) | ScalarValue::LargeBinary(v) => {
                Some(ScalarValue::Binary(v.clone()))
            }
            _ => None,
        },
        DataType::LargeBinary => match scalar {
            ScalarValue::BinaryView(v) | ScalarValue::Binary(v) => {
                Some(ScalarValue::LargeBinary(v.clone()))
            }
            _ => None,
        },
        DataType::Dictionary(_, value_type) => convert_literal_for_column_type(scalar, value_type),
        _ => None,
    }
}

#[cfg(test)]
fn schema_has_view_types(schema: &SchemaRef) -> bool {
    schema.fields().iter().any(|f| field_has_view_type(f))
}

#[cfg(test)]
fn field_has_view_type(field: &arrow_schema::Field) -> bool {
    match field.data_type() {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::Struct(fields) => fields.iter().any(|f| field_has_view_type(f)),
        DataType::List(inner) | DataType::LargeList(inner) | DataType::ListView(inner) => {
            field_has_view_type(inner)
        }
        _ => false,
    }
}

#[cfg(test)]
fn convert_scalar_view_to_base(scalar: &ScalarValue) -> Option<ScalarValue> {
    match scalar {
        ScalarValue::Utf8View(v) => Some(ScalarValue::Utf8(v.clone())),
        ScalarValue::BinaryView(v) => Some(ScalarValue::Binary(v.clone())),
        ScalarValue::Dictionary(key_type, inner) => {
            convert_scalar_view_to_base(inner).map(|converted_inner| {
                ScalarValue::Dictionary(key_type.clone(), Box::new(converted_inner))
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{BinaryArray, Int32Array, RecordBatch, StringArray, StructArray};
    use arrow_schema::{DataType, Field, Fields, Schema};
    use datafusion::{
        physical_plan::{ExecutionPlan, collect},
        prelude::{col, lit},
    };
    use datafusion_datasource::file::FileSource as _;
    use object_store::{ObjectStore as _, memory::InMemory};
    use parquet::arrow::ArrowWriter;
    use url::Url;

    use crate::{
        assert_batches_sorted_eq,
        delta_datafusion::{session::create_session, table_provider::next::FILE_ID_COLUMN_DEFAULT},
        test_utils::TestResult,
    };

    use super::*;

    fn parquet_source_from_plan(plan: &Arc<dyn ExecutionPlan>) -> &ParquetSource {
        let data_source_exec = plan
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .expect("expected DataSourceExec plan");
        let (_, parquet_source) = data_source_exec
            .downcast_to_file_source::<ParquetSource>()
            .expect("expected ParquetSource in DataSourceExec");
        parquet_source
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
            true,
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
            true,
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
            true,
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

        // extended schema with missing column and different data types
        let arrow_schema_extended = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8View, true),
            Field::new("value2", DataType::Utf8View, true),
        ]));
        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema_extended,
            Some(1),
            &file_id_field,
            None,
            true,
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
        assert!(matches!(
            batches[0].column(1).data_type(),
            DataType::Utf8View
        ));
        assert!(matches!(
            batches[0].column(2).data_type(),
            DataType::Utf8View
        ));

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
            true,
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
            true,
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
            true,
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
            true,
        )
        .await?;

        let parquet_source = parquet_source_from_plan(&plan);
        assert!(parquet_source.filter().is_some());

        let batches = collect(plan, session.task_ctx()).await?;

        let expected = vec![
            "+----+-------+-----------------------------+",
            "| id | value | __delta_rs_file_id__        |",
            "+----+-------+-----------------------------+",
            "| 2  | b     | memory:///test_data.parquet |",
            "+----+-------+-----------------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &batches);

        let plan = get_read_plan(
            &session.state(),
            files_by_store.clone(),
            &arrow_schema,
            None,
            &file_id_field,
            Some(&predicate),
            false,
        )
        .await?;

        let parquet_source = parquet_source_from_plan(&plan);
        assert!(parquet_source.filter().is_none());

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

        Ok(())
    }

    #[tokio::test]
    async fn test_view_literal_conversion_in_predicate_pushdown() -> TestResult {
        use datafusion::scalar::ScalarValue;

        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
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
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
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
            &arrow_schema,
            None,
            &file_id_field,
            Some(&predicate),
            true,
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
    async fn test_binaryview_literal_conversion_in_predicate_pushdown() -> TestResult {
        use datafusion::scalar::ScalarValue;

        let store = Arc::new(InMemory::new());
        let store_url = Url::parse("memory:///")?;
        let session = Arc::new(create_session().into_inner());
        session
            .runtime_env()
            .register_object_store(&store_url, store.clone());

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Binary, true),
        ]));
        let data = RecordBatch::try_new(
            arrow_schema.clone(),
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
        let mut arrow_writer = ArrowWriter::try_new(&mut buffer, arrow_schema.clone(), None)?;
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
            &arrow_schema,
            None,
            &file_id_field,
            Some(&predicate),
            true,
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
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(data_col.value(0), b"bbb");

        assert_eq!(batches[0].num_columns(), 3);
        assert_eq!(batches[0].schema().field(2).name(), FILE_ID_COLUMN_DEFAULT);

        Ok(())
    }

    #[test]
    fn test_convert_dictionary_wrapped_view_literals() {
        use super::convert_scalar_view_to_base;
        use datafusion::scalar::ScalarValue;

        let dict_utf8view = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8View(Some("test".to_string()))),
        );
        let converted = convert_scalar_view_to_base(&dict_utf8view);
        assert!(converted.is_some());
        let expected = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("test".to_string()))),
        );
        assert_eq!(converted.unwrap(), expected);

        let dict_binaryview = ScalarValue::Dictionary(
            Box::new(DataType::UInt16),
            Box::new(ScalarValue::BinaryView(Some(vec![1, 2, 3]))),
        );
        let converted = convert_scalar_view_to_base(&dict_binaryview);
        assert!(converted.is_some());
        let expected = ScalarValue::Dictionary(
            Box::new(DataType::UInt16),
            Box::new(ScalarValue::Binary(Some(vec![1, 2, 3]))),
        );
        assert_eq!(converted.unwrap(), expected);

        let dict_utf8 = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::Utf8(Some("test".to_string()))),
        );
        let converted = convert_scalar_view_to_base(&dict_utf8);
        assert!(converted.is_none());
    }

    #[test]
    fn test_schema_has_view_types() {
        use super::schema_has_view_types;

        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::Binary, true),
        ]));
        assert!(!schema_has_view_types(&base_schema));

        let view_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        assert!(schema_has_view_types(&view_schema));

        let binary_view_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::BinaryView, true),
        ]));
        assert!(schema_has_view_types(&binary_view_schema));

        let nested_view_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "nested",
                DataType::Struct(vec![Field::new("inner", DataType::Utf8View, true)].into()),
                true,
            ),
        ]));
        assert!(schema_has_view_types(&nested_view_schema));
    }

    #[test]
    fn test_normalize_predicate_preserves_view_literals_for_view_schema() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let view_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
        ]));

        let predicate = col("name").eq(lit(ScalarValue::Utf8View(Some("test".to_string()))));
        let normalized = normalize_predicate_to_schema(predicate.clone(), &view_schema).unwrap();

        assert_eq!(predicate, normalized);
    }

    #[test]
    fn test_normalize_predicate_converts_view_literals_for_base_schema() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let base_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let predicate = col("name").eq(lit(ScalarValue::Utf8View(Some("test".to_string()))));
        let normalized = normalize_predicate_to_schema(predicate, &base_schema).unwrap();

        let expected = col("name").eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        assert_eq!(expected, normalized);
    }

    #[test]
    fn test_normalize_predicate_mixed_schema_dictionary_partition_and_view_column() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let mixed_schema = Arc::new(Schema::new(vec![
            Field::new(
                "partition_col",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("data_col", DataType::Utf8View, true),
        ]));

        let part_predicate =
            col("partition_col").eq(lit(ScalarValue::Utf8View(Some("part_value".to_string()))));
        let normalized = normalize_predicate_to_schema(part_predicate, &mixed_schema).unwrap();
        if let Expr::BinaryExpr(binary) = &normalized {
            if let Expr::Literal(scalar, _) = &*binary.right {
                assert!(
                    matches!(scalar, ScalarValue::Utf8(_)),
                    "Partition column literal should be converted to Utf8, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal on right side of binary expr");
            }
        } else {
            panic!("Expected BinaryExpr");
        }

        let data_predicate =
            col("data_col").eq(lit(ScalarValue::Utf8(Some("data_value".to_string()))));
        let normalized = normalize_predicate_to_schema(data_predicate, &mixed_schema).unwrap();
        if let Expr::BinaryExpr(binary) = &normalized {
            if let Expr::Literal(scalar, _) = &*binary.right {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "View column literal should be converted to Utf8View, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal on right side of binary expr");
            }
        } else {
            panic!("Expected BinaryExpr");
        }
    }

    #[test]
    fn test_normalize_predicate_compound_with_mixed_types() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let mixed_schema = Arc::new(Schema::new(vec![
            Field::new(
                "part",
                DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("name", DataType::Utf8View, true),
        ]));

        let compound = col("part")
            .eq(lit(ScalarValue::Utf8View(Some("a".to_string()))))
            .and(col("name").eq(lit(ScalarValue::Utf8(Some("b".to_string())))));
        let normalized = normalize_predicate_to_schema(compound, &mixed_schema).unwrap();

        if let Expr::BinaryExpr(outer) = &normalized {
            if let (Expr::BinaryExpr(left_binary), Expr::BinaryExpr(right_binary)) =
                (&*outer.left, &*outer.right)
            {
                if let Expr::Literal(left_scalar, _) = &*left_binary.right {
                    assert!(
                        matches!(left_scalar, ScalarValue::Utf8(_)),
                        "part literal should be Utf8"
                    );
                }
                if let Expr::Literal(right_scalar, _) = &*right_binary.right {
                    assert!(
                        matches!(right_scalar, ScalarValue::Utf8View(_)),
                        "name literal should be Utf8View"
                    );
                }
            }
        }
    }

    #[test]
    fn test_normalize_predicate_in_list_for_view_column() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));

        let predicate = col("name").in_list(
            vec![
                lit(ScalarValue::Utf8(Some("a".to_string()))),
                lit(ScalarValue::Utf8(Some("b".to_string()))),
                lit(ScalarValue::Utf8(Some("c".to_string()))),
            ],
            false,
        );
        let normalized = normalize_predicate_to_schema(predicate, &schema).unwrap();

        if let Expr::InList(in_list) = &normalized {
            for item in &in_list.list {
                if let Expr::Literal(scalar, _) = item {
                    assert!(
                        matches!(scalar, ScalarValue::Utf8View(_)),
                        "All IN list literals should be Utf8View, got {:?}",
                        scalar
                    );
                }
            }
        } else {
            panic!("Expected InList expression");
        }
    }

    #[test]
    fn test_normalize_predicate_base_to_view_conversion() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let view_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("data", DataType::BinaryView, true),
        ]));

        let utf8_predicate = col("name").eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        let normalized = normalize_predicate_to_schema(utf8_predicate, &view_schema).unwrap();
        let expected = col("name").eq(lit(ScalarValue::Utf8View(Some("test".to_string()))));
        assert_eq!(expected, normalized);

        let binary_predicate = col("data").eq(lit(ScalarValue::Binary(Some(vec![1, 2, 3]))));
        let normalized = normalize_predicate_to_schema(binary_predicate, &view_schema).unwrap();
        let expected = col("data").eq(lit(ScalarValue::BinaryView(Some(vec![1, 2, 3]))));
        assert_eq!(expected, normalized);
    }

    #[test]
    fn test_convert_literal_for_column_type() {
        use super::convert_literal_for_column_type;
        use datafusion::scalar::ScalarValue;

        let utf8_val = ScalarValue::Utf8(Some("test".to_string()));
        assert_eq!(
            convert_literal_for_column_type(&utf8_val, &DataType::Utf8View),
            Some(ScalarValue::Utf8View(Some("test".to_string())))
        );

        let utf8view_val = ScalarValue::Utf8View(Some("test".to_string()));
        assert_eq!(
            convert_literal_for_column_type(&utf8view_val, &DataType::Utf8),
            Some(ScalarValue::Utf8(Some("test".to_string())))
        );

        let dict_type = DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
        assert_eq!(
            convert_literal_for_column_type(&utf8view_val, &dict_type),
            Some(ScalarValue::Utf8(Some("test".to_string())))
        );

        let binary_val = ScalarValue::Binary(Some(vec![1, 2, 3]));
        assert_eq!(
            convert_literal_for_column_type(&binary_val, &DataType::BinaryView),
            Some(ScalarValue::BinaryView(Some(vec![1, 2, 3])))
        );

        let int_val = ScalarValue::Int32(Some(42));
        assert_eq!(
            convert_literal_for_column_type(&int_val, &DataType::Utf8View),
            None
        );
    }

    #[test]
    fn test_normalize_predicate_nested_struct_with_view_types() {
        use super::normalize_predicate_to_schema;
        use datafusion::functions::core::expr_ext::FieldAccessor;
        use datafusion::scalar::ScalarValue;

        let nested_fields: Fields = vec![
            Field::new("inner_name", DataType::Utf8View, true),
            Field::new("inner_value", DataType::Int32, true),
        ]
        .into();
        let nested_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", DataType::Struct(nested_fields), true),
        ]));

        let nested_field_access = col("nested").field("inner_name");
        let predicate = nested_field_access.eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        let normalized = normalize_predicate_to_schema(predicate, &nested_schema).unwrap();

        if let Expr::BinaryExpr(binary) = &normalized {
            if let Expr::Literal(scalar, _) = &*binary.right {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "Nested field literal should be converted to Utf8View, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal on right side");
            }
        } else {
            panic!("Expected BinaryExpr");
        }
    }

    #[test]
    fn test_normalize_predicate_cast_expression() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
        ]));

        let cast_expr = col("name")
            .cast_to(&DataType::Utf8View, &schema.clone().to_dfschema().unwrap())
            .unwrap();
        let predicate = cast_expr.eq(lit(ScalarValue::Utf8(Some("test".to_string()))));
        let normalized = normalize_predicate_to_schema(predicate, &schema).unwrap();

        if let Expr::BinaryExpr(binary) = &normalized {
            if let Expr::Literal(scalar, _) = &*binary.right {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "Cast expression literal should be converted to Utf8View, got {:?}",
                    scalar
                );
            }
        }
    }

    #[test]
    fn test_get_column_type_for_expr_uses_df_type_inference() {
        use super::get_column_type_for_expr;
        use datafusion::common::ToDFSchema;
        use datafusion::functions::core::expr_ext::FieldAccessor;

        let nested_fields: Fields = vec![
            Field::new("inner_name", DataType::Utf8View, true),
            Field::new("inner_value", DataType::Int32, true),
        ]
        .into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("nested", DataType::Struct(nested_fields), true),
        ]));
        let df_schema = schema.to_dfschema().unwrap();

        let col_type = get_column_type_for_expr(&col("name"), &df_schema);
        assert_eq!(col_type, Some(DataType::Utf8View));

        let col_type = get_column_type_for_expr(&col("id"), &df_schema);
        assert_eq!(col_type, Some(DataType::Int32));

        let nested_access = col("nested").field("inner_name");
        let col_type = get_column_type_for_expr(&nested_access, &df_schema);
        assert_eq!(col_type, Some(DataType::Utf8View));

        let nested_int_access = col("nested").field("inner_value");
        let col_type = get_column_type_for_expr(&nested_int_access, &df_schema);
        assert_eq!(col_type, Some(DataType::Int32));

        let literal_type = get_column_type_for_expr(&lit("test"), &df_schema);
        assert_eq!(literal_type, None);
    }

    #[test]
    fn test_normalize_predicate_with_physical_column_names() {
        use super::normalize_predicate_to_schema;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "col-173b4db9-b5ad-427f-9e75-516aae37fbbb",
                DataType::Utf8View,
                true,
            ),
            Field::new(
                "col-3877fd94-0973-4941-ac6b-646849a1ff65",
                DataType::Utf8View,
                true,
            ),
        ]));

        let predicate = col("col-3877fd94-0973-4941-ac6b-646849a1ff65")
            .eq(lit(ScalarValue::Utf8(Some("Timothy Lamb".to_string()))));

        let normalized = normalize_predicate_to_schema(predicate, &schema).unwrap();

        if let Expr::BinaryExpr(binary) = &normalized {
            if let Expr::Literal(scalar, _) = &*binary.right {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "Physical column name predicate should convert Utf8 literal to Utf8View, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal on right side");
            }
        } else {
            panic!("Expected BinaryExpr");
        }
    }

    #[test]
    fn test_get_column_type_with_hyphenated_names() {
        use super::get_column_type_for_expr;
        use datafusion::common::ToDFSchema;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "col-173b4db9-b5ad-427f-9e75-516aae37fbbb",
                DataType::Utf8View,
                true,
            ),
            Field::new("normal_name", DataType::Utf8View, true),
        ]));
        let df_schema = schema.to_dfschema().unwrap();

        let hyphenated_col = col("col-173b4db9-b5ad-427f-9e75-516aae37fbbb");
        let col_type = get_column_type_for_expr(&hyphenated_col, &df_schema);
        assert_eq!(
            col_type,
            Some(DataType::Utf8View),
            "Should find type for hyphenated column name"
        );

        let normal_col = col("normal_name");
        let col_type = get_column_type_for_expr(&normal_col, &df_schema);
        assert_eq!(col_type, Some(DataType::Utf8View));
    }

    #[test]
    fn test_normalize_predicate_like_expression() {
        use super::normalize_predicate_to_schema;
        use datafusion::logical_expr::expr::Like;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));

        let like_expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("%test%".to_string())),
                None,
            )),
            escape_char: None,
            case_insensitive: false,
        });

        let normalized = normalize_predicate_to_schema(like_expr, &schema).unwrap();

        if let Expr::Like(like) = &normalized {
            if let Expr::Literal(scalar, _) = &*like.pattern {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "LIKE pattern should be converted to Utf8View for Utf8View column, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal pattern");
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_normalize_predicate_ilike_expression() {
        use super::normalize_predicate_to_schema;
        use datafusion::logical_expr::expr::Like;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));

        let ilike_expr = Expr::Like(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("%TEST%".to_string())),
                None,
            )),
            escape_char: None,
            case_insensitive: true,
        });

        let normalized = normalize_predicate_to_schema(ilike_expr, &schema).unwrap();

        if let Expr::Like(like) = &normalized {
            assert!(
                like.case_insensitive,
                "Should preserve case_insensitive flag"
            );
            if let Expr::Literal(scalar, _) = &*like.pattern {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "ILIKE pattern should be converted to Utf8View, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal pattern");
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_normalize_predicate_similar_to_expression() {
        use super::normalize_predicate_to_schema;
        use datafusion::logical_expr::expr::Like;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));

        let similar_expr = Expr::SimilarTo(Like {
            negated: false,
            expr: Box::new(col("name")),
            pattern: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("%(test|demo)%".to_string())),
                None,
            )),
            escape_char: None,
            case_insensitive: false,
        });

        let normalized = normalize_predicate_to_schema(similar_expr, &schema).unwrap();

        if let Expr::SimilarTo(like) = &normalized {
            if let Expr::Literal(scalar, _) = &*like.pattern {
                assert!(
                    matches!(scalar, ScalarValue::Utf8View(_)),
                    "SIMILAR TO pattern should be converted to Utf8View, got {:?}",
                    scalar
                );
            } else {
                panic!("Expected literal pattern");
            }
        } else {
            panic!("Expected SimilarTo expression");
        }
    }

    #[test]
    fn test_normalize_predicate_like_preserves_escape_char() {
        use super::normalize_predicate_to_schema;
        use datafusion::logical_expr::expr::Like;
        use datafusion::scalar::ScalarValue;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            true,
        )]));

        let like_expr = Expr::Like(Like {
            negated: true,
            expr: Box::new(col("name")),
            pattern: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("%test\\%pattern%".to_string())),
                None,
            )),
            escape_char: Some('\\'),
            case_insensitive: false,
        });

        let normalized = normalize_predicate_to_schema(like_expr, &schema).unwrap();

        if let Expr::Like(like) = &normalized {
            assert!(like.negated, "Should preserve negated flag");
            assert_eq!(like.escape_char, Some('\\'), "Should preserve escape_char");
        } else {
            panic!("Expected Like expression");
        }
    }
}
