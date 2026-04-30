use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::{Array as _, *};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields};
use arrow_schema::{Field, Schema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::parse_json;
use delta_kernel::expressions::Scalar;
use delta_kernel::scan::scan_row_schema;
use delta_kernel::schema::PrimitiveType;
use delta_kernel::schema::{DataType, SchemaRef as KernelSchemaRef};
use delta_kernel::snapshot::Snapshot as KernelSnapshot;
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::{EvaluationHandler, Expression, ExpressionEvaluator};
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::log::*;

use crate::kernel::ARROW_HANDLER;
use crate::kernel::StructType;
use crate::kernel::arrow::engine_ext::SnapshotExt;
use crate::kernel::arrow::extract::{self as ex};
use crate::kernel::snapshot::stats_projection::{
    FileStatsMaterialization, StatsProjection, StatsSourcePolicy,
};
use crate::{DeltaResult, DeltaTableError};

pin_project! {
    pub(crate) struct ScanRowOutStream<S> {
        stats_schema: KernelSchemaRef,
        partitions_schema: Option<KernelSchemaRef>,
        column_mapping_mode: ColumnMappingMode,
        stats_materialization: FileStatsMaterialization,

        #[pin]
        stream: S,
    }
}

impl<S> ScanRowOutStream<S> {
    pub fn try_new(
        snapshot: Arc<KernelSnapshot>,
        stream: S,
        skip_stats: bool,
    ) -> DeltaResult<Self> {
        let stats_schema = snapshot.stats_schema()?;
        let partitions_schema = snapshot.partitions_schema()?;
        let column_mapping_mode = snapshot.table_configuration().column_mapping_mode();
        let stats_materialization = if skip_stats {
            FileStatsMaterialization::without_stats()
        } else {
            FileStatsMaterialization::compatibility(StatsProjection::full())
        };
        Ok(Self {
            stats_schema,
            partitions_schema,
            column_mapping_mode,
            stats_materialization,
            stream,
        })
    }
}

impl<S> Stream for ScanRowOutStream<S>
where
    S: Stream<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = parse_stats_column_impl(
                    &batch,
                    this.stats_schema.clone(),
                    this.partitions_schema.as_ref(),
                    *this.column_mapping_mode,
                    this.stats_materialization,
                );
                Poll::Ready(Some(result))
            }
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

pub(crate) fn scan_row_in_eval(
    snapshot: &KernelSnapshot,
) -> DeltaResult<Arc<dyn ExpressionEvaluator>> {
    static EXPRESSION: LazyLock<Arc<Expression>> = LazyLock::new(|| {
        Expression::struct_from(
            scan_row_schema()
                .fields()
                .map(|field| Expression::column([field.name.clone()])),
        )
        .into()
    });
    static OUT_TYPE: LazyLock<DataType> =
        LazyLock::new(|| DataType::Struct(Box::new(scan_row_schema().as_ref().clone())));

    let input_schema = snapshot.scan_row_parsed_schema_arrow()?;
    let input_schema = Arc::new(input_schema.as_ref().try_into_kernel()?);

    Ok(ARROW_HANDLER.new_expression_evaluator(
        input_schema,
        EXPRESSION.clone(),
        OUT_TYPE.clone(),
    )?)
}

#[cfg(any(test, feature = "datafusion"))]
pub(crate) fn parse_stats_column_with_schema(
    sn: &KernelSnapshot,
    batch: &RecordBatch,
    stats_schema: KernelSchemaRef,
) -> DeltaResult<RecordBatch> {
    let partitions_schema = sn.partitions_schema()?;
    let column_mapping_mode = sn.table_configuration().column_mapping_mode();
    parse_stats_column_impl(
        batch,
        stats_schema,
        partitions_schema.as_ref(),
        column_mapping_mode,
        &FileStatsMaterialization::compatibility(StatsProjection::full()),
    )
}

fn parse_stats_column_impl(
    batch: &RecordBatch,
    stats_schema: KernelSchemaRef,
    partitions_schema: Option<&KernelSchemaRef>,
    column_mapping_mode: ColumnMappingMode,
    stats_materialization: &FileStatsMaterialization,
) -> DeltaResult<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.num_columns() + 2);
    let mut fields = Vec::with_capacity(batch.num_columns() + 2);

    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if field.name() == "stats_parsed" {
            continue;
        }
        if field.name() == "stats" && !stats_materialization.preserves_raw_stats() {
            continue;
        }
        fields.push(field.clone());
        columns.push(column.clone());
    }

    if let Some(stats_array) = materialize_stats_array(batch, stats_schema, stats_materialization)?
    {
        fields.push(Arc::new(Field::new(
            "stats_parsed",
            stats_array.data_type().to_owned(),
            true,
        )));
        columns.push(stats_array);
    }

    if let Some(partition_schema) = partitions_schema {
        let partition_array = parse_partitions(
            batch,
            partition_schema.as_ref(),
            "fileConstantValues.partitionValues",
            column_mapping_mode,
        )?;
        fields.push(Arc::new(Field::new(
            "partitionValues_parsed",
            partition_array.data_type().to_owned(),
            false,
        )));
        columns.push(Arc::new(partition_array));
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn materialize_stats_array(
    batch: &RecordBatch,
    stats_schema: KernelSchemaRef,
    stats_materialization: &FileStatsMaterialization,
) -> DeltaResult<Option<Arc<StructArray>>> {
    match stats_materialization.stats_source_policy() {
        StatsSourcePolicy::None => Ok(None),
        StatsSourcePolicy::JsonOnly => parse_raw_stats_array(batch, stats_schema).map(Some),
        StatsSourcePolicy::ParsedWithJsonFallback => {
            let requested_schema: arrow_schema::Schema = stats_schema.as_ref().try_into_arrow()?;
            if let Some((idx, _)) = batch.schema_ref().column_with_name("stats_parsed") {
                let parsed = batch
                    .column(idx)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DeltaTableError::SchemaMismatch {
                        msg: "stats_parsed column is not a struct".to_string(),
                    })?;
                match project_struct_array(parsed, requested_schema.fields()) {
                    Ok(projected) => return Ok(Some(Arc::new(projected))),
                    Err(err) => {
                        debug!(
                            "existing stats_parsed did not satisfy requested stats schema; falling back to raw stats: {err}"
                        );
                    }
                }
            }
            parse_raw_stats_array(batch, stats_schema).map(Some)
        }
    }
}

fn parse_raw_stats_array(
    batch: &RecordBatch,
    stats_schema: KernelSchemaRef,
) -> DeltaResult<Arc<StructArray>> {
    let Some((stats_idx, _)) = batch.schema_ref().column_with_name("stats") else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: "stats column not found".to_string(),
        });
    };

    let stats_batch = batch.project(&[stats_idx])?;
    let stats_data = Box::new(ArrowEngineData::new(stats_batch));

    let parsed = parse_json(stats_data, stats_schema)?;
    let parsed: RecordBatch = ArrowEngineData::try_from_engine_data(parsed)?.into();

    Ok(Arc::new(parsed.into()))
}

fn project_struct_array(
    array: &StructArray,
    requested_fields: &Fields,
) -> DeltaResult<StructArray> {
    let ArrowDataType::Struct(existing_fields) = array.data_type() else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: "expected struct array for stats_parsed".to_string(),
        });
    };

    let mut columns = Vec::with_capacity(requested_fields.len());
    for requested_field in requested_fields {
        let existing_idx = existing_fields
            .iter()
            .position(|field| field.name() == requested_field.name())
            .ok_or_else(|| DeltaTableError::SchemaMismatch {
                msg: format!("stats_parsed field {} not found", requested_field.name()),
            })?;
        let existing_column = array.column(existing_idx);
        let column: ArrayRef = match (requested_field.data_type(), existing_column.data_type()) {
            (ArrowDataType::Struct(requested_child_fields), ArrowDataType::Struct(_)) => {
                let child = existing_column
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| DeltaTableError::SchemaMismatch {
                        msg: format!(
                            "stats_parsed field {} is not a struct",
                            requested_field.name()
                        ),
                    })?;
                Arc::new(project_struct_array(child, requested_child_fields)?)
            }
            _ => existing_column.clone(),
        };
        columns.push(column);
    }

    Ok(StructArray::try_new_with_length(
        requested_fields.clone(),
        columns,
        array.nulls().cloned(),
        array.len(),
    )?)
}

pub(crate) fn parse_partitions(
    batch: &RecordBatch,
    partition_schema: &StructType,
    raw_path: &str,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<StructArray> {
    trace!(
        "parse_partitions: batch: {batch:?}\npartition_schema: {partition_schema:?}\npath: {raw_path}"
    );
    let partitions =
        ex::extract_and_cast_opt::<MapArray>(batch, raw_path).ok_or(DeltaTableError::generic(
            "No partitionValues column found in files batch. This is unexpected.",
        ))?;
    let num_rows = batch.num_rows();

    let mut values = partition_schema
        .fields()
        .map(|f| {
            (
                f.physical_name(column_mapping_mode).to_string(),
                Vec::<Scalar>::with_capacity(partitions.len()),
            )
        })
        .collect::<HashMap<_, _>>();

    for i in 0..partitions.len() {
        if partitions.is_null(i) {
            return Err(DeltaTableError::generic(
                "Expected potentially empty partition values map, but found a null value.",
            ));
        }
        let data: HashMap<_, _> = collect_map(&partitions.value(i))
            .ok_or(DeltaTableError::generic(
                "Failed to collect partition values from map array.",
            ))?
            .map(|(k, v)| {
                let field = partition_schema
                    .fields()
                    .find(|field| field.physical_name(column_mapping_mode).eq(k.as_str()))
                    .ok_or(DeltaTableError::generic(format!(
                        "Partition column {k} not found in schema."
                    )))?;
                let field_type = match field.data_type() {
                    DataType::Primitive(p) => Ok(p),
                    _ => Err(DeltaTableError::generic(
                        "nested partitioning values are not supported",
                    )),
                }?;
                Ok::<_, DeltaTableError>((
                    k,
                    v.map(|vv| field_type.parse_scalar(vv.as_str()))
                        .transpose()?
                        .unwrap_or(Scalar::Null(field.data_type().clone())),
                ))
            })
            .collect::<Result<_, _>>()?;

        partition_schema.fields().for_each(|f| {
            let value = data
                .get(f.physical_name(column_mapping_mode))
                .cloned()
                .unwrap_or(Scalar::Null(f.data_type().clone()));
            values
                .get_mut(f.physical_name(column_mapping_mode))
                .unwrap()
                .push(value);
        });
    }

    let columns = partition_schema
        .fields()
        .map(|f| {
            let values = values.get(f.physical_name(column_mapping_mode)).unwrap();
            match f.data_type() {
                DataType::Primitive(p) => {
                    // Safety: we created the Scalars above using the parsing function of the same PrimitiveType
                    // should this fail, it's a bug in our code, and we should panic
                    let arr = match p {
                        PrimitiveType::String => {
                            Arc::new(StringArray::from_iter(values.iter().map(|v| match v {
                                Scalar::String(s) => Some(s.clone()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Long => {
                            Arc::new(Int64Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Long(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Integer => {
                            Arc::new(Int32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Integer(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Short => {
                            Arc::new(Int16Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Short(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Byte => {
                            Arc::new(Int8Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Byte(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Float => {
                            Arc::new(Float32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Float(f) => Some(*f),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Double => {
                            Arc::new(Float64Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Double(f) => Some(*f),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Boolean => {
                            Arc::new(BooleanArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Boolean(b) => Some(*b),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Binary => {
                            Arc::new(BinaryArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Binary(b) => Some(b.clone()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Date => {
                            Arc::new(Date32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Date(d) => Some(*d),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Timestamp => Arc::new(
                            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Timestamp(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_timezone("UTC"),
                        ) as ArrayRef,
                        #[cfg(feature = "nanosecond-timestamps")]
                        PrimitiveType::TimestampNanos => Arc::new(
                            TimestampNanosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::TimestampNanos(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_timezone("UTC"),
                        ) as ArrayRef,
                        PrimitiveType::TimestampNtz => Arc::new(
                            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::TimestampNtz(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            })),
                        ) as ArrayRef,
                        PrimitiveType::Decimal(decimal) => Arc::new(
                            Decimal128Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Decimal(decimal) => Some(decimal.bits()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_precision_and_scale(decimal.precision(), decimal.scale() as i8)?,
                        ) as ArrayRef,
                    };
                    Ok(arr)
                }
                _ => Err(DeltaTableError::generic(
                    "complex partitioning values are not supported",
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(StructArray::try_new_with_length(
        Fields::from(
            partition_schema
                .fields()
                .map(|f| f.try_into_arrow())
                .collect::<Result<Vec<ArrowField>, _>>()?,
        ),
        columns,
        None,
        num_rows,
    )?)
}

fn collect_map(val: &StructArray) -> Option<impl Iterator<Item = (String, Option<String>)> + '_> {
    let keys = val
        .column(0)
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()?;
    let values = val
        .column(1)
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()?;
    Some(
        keys.iter()
            .zip(values.iter())
            .filter_map(|(k, v)| k.map(|kv| (kv.to_string(), v.map(|vv| vv.to_string())))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::MapBuilder;
    use arrow::array::MapFieldNames;
    use arrow::array::StringBuilder;
    use arrow::array::{ArrayRef, new_null_array};
    use arrow::datatypes::DataType as ArrowDataType;
    use arrow::datatypes::Field as ArrowField;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_schema::Field;
    use delta_kernel::scan::scan_row_schema;
    use delta_kernel::schema::{MapType, MetadataValue, SchemaRef, StructField};
    use pretty_assertions::assert_eq;

    use crate::kernel::arrow::engine_ext::{ExpressionEvaluatorExt, SnapshotExt};
    use crate::kernel::snapshot::Snapshot;
    use crate::kernel::snapshot::stats_projection::{FileStatsMaterialization, StatsProjection};
    use crate::test_utils::TestTables;

    fn scan_row_batch_with_stats(raw_stats: &str) -> RecordBatch {
        let schema: ArrowSchema = scan_row_schema().as_ref().try_into_arrow().unwrap();
        let mut columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 1))
            .collect();
        columns[schema.index_of("path").unwrap()] =
            Arc::new(StringArray::from(vec![Some("part-000.parquet")]));
        columns[schema.index_of("size").unwrap()] = Arc::new(Int64Array::from(vec![1]));
        columns[schema.index_of("modificationTime").unwrap()] = Arc::new(Int64Array::from(vec![1]));
        columns[schema.index_of("stats").unwrap()] =
            Arc::new(StringArray::from(vec![Some(raw_stats)]));

        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    fn raw_stats_string(batch: RecordBatch, row: usize) -> Option<String> {
        batch
            .column_by_name("stats")
            .and_then(|col| col.as_string_opt::<i32>())
            .and_then(|col| col.is_valid(row).then(|| col.value(row).to_string()))
    }

    fn num_records_stats_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
        )
    }

    fn value_stats_schema() -> SchemaRef {
        Arc::new(
            StructType::try_new([
                StructField::nullable("numRecords", DataType::LONG),
                StructField::nullable(
                    "minValues",
                    StructType::try_new([StructField::nullable("value", DataType::INTEGER)])
                        .unwrap(),
                ),
                StructField::nullable(
                    "maxValues",
                    StructType::try_new([StructField::nullable("value", DataType::INTEGER)])
                        .unwrap(),
                ),
                StructField::nullable(
                    "nullCount",
                    StructType::try_new([StructField::nullable("value", DataType::LONG)]).unwrap(),
                ),
            ])
            .unwrap(),
        )
    }

    fn append_stats_parsed(batch: &RecordBatch, stats_parsed: StructArray) -> RecordBatch {
        let mut fields = batch.schema().fields().to_vec();
        let mut columns = batch.columns().to_vec();
        fields.push(Arc::new(Field::new(
            "stats_parsed",
            stats_parsed.data_type().clone(),
            true,
        )));
        columns.push(Arc::new(stats_parsed));
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    fn without_raw_stats(batch: &RecordBatch) -> RecordBatch {
        let stats_idx = batch.schema().index_of("stats").unwrap();
        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| (idx != stats_idx).then_some(field.clone()))
            .collect::<Vec<_>>();
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| (idx != stats_idx).then_some(column.clone()))
            .collect::<Vec<_>>();
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
    }

    fn num_records_stats_parsed(num_records: i64) -> StructArray {
        StructArray::from(vec![(
            Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(num_records)])) as ArrayRef,
        )])
    }

    fn value_stats_parsed() -> StructArray {
        let min_values = StructArray::from(vec![(
            Arc::new(Field::new("value", ArrowDataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
        )]);
        let max_values = StructArray::from(vec![(
            Arc::new(Field::new("value", ArrowDataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(9)])) as ArrayRef,
        )]);
        let null_count = StructArray::from(vec![(
            Arc::new(Field::new("value", ArrowDataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef,
        )]);

        StructArray::from(vec![
            (
                Arc::new(Field::new("numRecords", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(11)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "minValues",
                    min_values.data_type().clone(),
                    true,
                )),
                Arc::new(min_values) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "maxValues",
                    max_values.data_type().clone(),
                    true,
                )),
                Arc::new(max_values) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "nullCount",
                    null_count.data_type().clone(),
                    true,
                )),
                Arc::new(null_count) as ArrayRef,
            ),
        ])
    }

    fn stats_parsed_field_names(batch: &RecordBatch) -> Vec<String> {
        let schema = batch.schema();
        let field = schema.field_with_name("stats_parsed").unwrap();
        let ArrowDataType::Struct(fields) = field.data_type() else {
            panic!("stats_parsed should be a struct");
        };
        fields.iter().map(|field| field.name().clone()).collect()
    }

    #[test]
    fn test_physical_partition_name_mapping() -> DeltaResult<()> {
        let physical_partition_name = "col-173b4db9-b5ad-427f-9e75-516aae37fbbb".to_string();
        let schema: SchemaRef =
            scan_row_schema().project(&["path", "size", "fileConstantValues"])?;
        let partition_schema = StructType::try_new(vec![
            StructField::nullable("Company Very Short", DataType::STRING).with_metadata(vec![
                (
                    "delta.columnMapping.id".to_string(),
                    MetadataValue::Number(1),
                ),
                (
                    "delta.columnMapping.physicalName".to_string(),
                    MetadataValue::String(physical_partition_name.clone()),
                ),
            ]),
        ])
        .expect("Failed to initialize partition schema");

        let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
        let file_constant_values: SchemaRef = Arc::new(
            StructType::try_new([
                StructField::nullable("partitionValues", partition_values),
                StructField::nullable("baseRowId", DataType::LONG),
                StructField::nullable("clusteringProvider", DataType::STRING),
            ])
            .expect("Failed to create SchemaRef for file_constant_values"),
        );
        // Inspecting the schema of file_constant_values:
        let _: ArrowSchema = file_constant_values.as_ref().try_into_arrow()?;

        // Constructing complex types in Arrow is hell.
        // Absolute hell.
        //
        // The partition column values that should be coming off the log are:
        //  "col-173b4db9-b5ad-427f-9e75-516aae37fbbb":"BMS"
        let keys_builder = StringBuilder::new();
        let values_builder = StringBuilder::new();
        let map_fields = MapFieldNames {
            entry: "key_value".into(),
            key: "key".into(),
            value: "value".into(),
        };
        let mut partitions =
            MapBuilder::new(Some(map_fields.clone()), keys_builder, values_builder);
        let mut tags = MapBuilder::new(
            Some(map_fields.clone()),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        tags.append(false)?;

        // The partition named in the schema, we need to get the physical name's "rename" out though
        partitions
            .keys()
            .append_value(physical_partition_name.clone());
        partitions.values().append_value("BMS");
        partitions.append(true).unwrap();
        let partitions = partitions.finish();

        let struct_fields = Fields::from(vec![
            Field::new("key", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Utf8, true),
        ]);
        let map_field = Arc::new(ArrowField::new(
            "key_value",
            ArrowDataType::Struct(struct_fields),
            false,
        ));

        let parts = StructArray::from(vec![
            (
                Arc::new(ArrowField::new(
                    "partitionValues",
                    ArrowDataType::Map(map_field.clone(), false),
                    true,
                )),
                Arc::new(partitions) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("baseRowId", ArrowDataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Option::<i64>::None])) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "defaultRowCommitVersion",
                    ArrowDataType::Int64,
                    true,
                )),
                Arc::new(Int64Array::from(vec![Option::<i64>::None])) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "tags",
                    ArrowDataType::Map(map_field, false),
                    true,
                )),
                Arc::new(tags.finish()) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new(
                    "clusteringProvider",
                    ArrowDataType::Utf8,
                    true,
                )),
                Arc::new(StringArray::from(vec![Option::<String>::None])) as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema.as_ref().try_into_arrow()?),
            vec![
                // path
                Arc::new(StringArray::from(vec!["foo.parquet".to_string()])),
                // size
                Arc::new(Int64Array::from(vec![1])),
                // fileConstantValues
                Arc::new(parts),
            ],
        )?;

        let raw_path = "fileConstantValues.partitionValues";
        let partitions =
            parse_partitions(&batch, &partition_schema, raw_path, ColumnMappingMode::Id)?;
        assert_eq!(
            None,
            partitions.column_by_name(&physical_partition_name),
            "Should not have found the physical column name"
        );
        assert_ne!(
            None,
            partitions.column_by_name("Company Very Short"),
            "Should have found the renamed column"
        );
        Ok(())
    }

    #[test]
    fn parse_stats_column_impl_preserves_raw_stats_column() -> DeltaResult<()> {
        let raw_stats = r#"{"maxValues":{"value":9},"numRecords":11,"nullCount":{"value":0},"minValues":{"value":1}}"#;
        let batch = scan_row_batch_with_stats(raw_stats);
        let stats_schema = Arc::new(StructType::try_new([StructField::nullable(
            "numRecords",
            DataType::LONG,
        )])?);

        let projected = parse_stats_column_impl(
            &batch,
            stats_schema,
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::full()),
        )?;

        assert!(projected.schema().column_with_name("stats").is_some());
        assert!(
            projected
                .schema()
                .column_with_name("stats_parsed")
                .is_some()
        );

        Ok(())
    }

    #[test]
    fn parse_stats_column_impl_reuses_existing_stats_parsed_without_raw_json() -> DeltaResult<()> {
        let batch = scan_row_batch_with_stats(r#"{"numRecords":11"#);
        let batch = append_stats_parsed(&without_raw_stats(&batch), num_records_stats_parsed(11));
        let projected = parse_stats_column_impl(
            &batch,
            num_records_stats_schema(),
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::query(StatsProjection::num_records_only()),
        )?;

        assert!(projected.schema().column_with_name("stats").is_none());
        assert_eq!(stats_parsed_field_names(&projected), vec!["numRecords"]);

        Ok(())
    }

    #[test]
    fn parse_stats_column_impl_projects_wide_stats_parsed_to_requested_fields() -> DeltaResult<()> {
        let batch = scan_row_batch_with_stats(r#"{"numRecords":11"#);
        let batch = append_stats_parsed(&batch, value_stats_parsed());
        let projected = parse_stats_column_impl(
            &batch,
            num_records_stats_schema(),
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::query(StatsProjection::num_records_only()),
        )?;

        assert!(projected.schema().column_with_name("stats").is_none());
        assert_eq!(stats_parsed_field_names(&projected), vec!["numRecords"]);

        Ok(())
    }

    #[test]
    fn parse_stats_column_impl_keeps_raw_stats_when_raw_policy_preserves() -> DeltaResult<()> {
        let raw_stats = r#"{"maxValues":{"value":9},"numRecords":11,"nullCount":{"value":0},"minValues":{"value":1}}"#;
        let batch = scan_row_batch_with_stats(raw_stats);
        let batch = append_stats_parsed(&batch, value_stats_parsed());
        let projected = parse_stats_column_impl(
            &batch,
            value_stats_schema(),
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::full()),
        )?;

        assert_eq!(raw_stats_string(projected, 0), Some(raw_stats.to_string()));

        Ok(())
    }

    #[test]
    fn parse_stats_column_impl_errors_on_malformed_stats_json() -> DeltaResult<()> {
        let batch = scan_row_batch_with_stats(r#"{"numRecords":11"#);
        let stats_schema = Arc::new(StructType::try_new([StructField::nullable(
            "numRecords",
            DataType::LONG,
        )])?);

        let err = parse_stats_column_impl(
            &batch,
            stats_schema,
            None,
            ColumnMappingMode::None,
            &FileStatsMaterialization::compatibility(StatsProjection::full()),
        )
        .expect_err("malformed stats JSON should fail parsing");

        assert!(
            err.to_string().contains("json") || err.to_string().contains("JSON"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn parse_stats_column_with_schema_errors_on_malformed_stats_json_for_partial_projection()
    -> DeltaResult<()> {
        let batch = scan_row_batch_with_stats(
            r#"{"numRecords":11,"minValues":{"value":1},"nullCount":{"value":0}"#,
        );
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let stats_schema = Arc::new(StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable(
                "minValues",
                StructType::try_new([StructField::nullable("value", DataType::INTEGER)])?,
            ),
            StructField::nullable(
                "nullCount",
                StructType::try_new([StructField::nullable("value", DataType::LONG)])?,
            ),
        ])?);

        let err = parse_stats_column_with_schema(snapshot.inner.as_ref(), &batch, stats_schema)
            .expect_err("malformed stats JSON should fail partial projection parsing");

        assert!(
            err.to_string().contains("json") || err.to_string().contains("JSON"),
            "unexpected error: {err}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn cached_batches_preserve_raw_stats_for_kernel_roundtrip() -> DeltaResult<()> {
        let raw_stats = r#"{"maxValues":{"value":"z"},"numRecords":11,"nullCount":{"value":0},"minValues":{"value":"a"}}"#;
        let batch = scan_row_batch_with_stats(raw_stats);
        let log_store = TestTables::Simple.table_builder()?.build_storage()?;
        let snapshot = Snapshot::try_new(&log_store, Default::default(), None).await?;
        let projected = parse_stats_column_with_schema(
            snapshot.inner.as_ref(),
            &batch,
            snapshot.inner.stats_schema()?,
        )?;
        // Keep this guard so cached batches still pass through the kernel evaluator
        // without rebuilding raw `stats` from `ToJson`.
        let evaluator = scan_row_in_eval(snapshot.inner.as_ref())?;
        let reparsed = evaluator.evaluate_arrow(projected)?;

        assert_eq!(raw_stats_string(reparsed, 0), Some(raw_stats.to_string()));

        Ok(())
    }
}
