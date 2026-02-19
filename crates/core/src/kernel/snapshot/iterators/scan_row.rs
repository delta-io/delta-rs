use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::{Array as _, *};
use arrow_schema::{Field as ArrowField, Fields};
use arrow_schema::{Field, Schema};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_conversion::TryIntoKernel;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine::parse_json;
use delta_kernel::expressions::Scalar;
use delta_kernel::expressions::UnaryExpressionOp;
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
use crate::{DeltaResult, DeltaTableError};

pin_project! {
    pub(crate) struct ScanRowOutStream<S> {
        stats_schema: KernelSchemaRef,
        partitions_schema: Option<KernelSchemaRef>,
        column_mapping_mode: ColumnMappingMode,

        #[pin]
        stream: S,
    }
}

impl<S> ScanRowOutStream<S> {
    pub fn try_new(snapshot: Arc<KernelSnapshot>, stream: S) -> DeltaResult<Self> {
        let stats_schema = snapshot.stats_schema()?;
        let partitions_schema = snapshot.partitions_schema()?;
        let column_mapping_mode = snapshot.table_configuration().column_mapping_mode();
        Ok(Self {
            stats_schema,
            partitions_schema,
            column_mapping_mode,
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
        Expression::struct_from(scan_row_schema().fields().map(|field| {
            if field.name == "stats" {
                Expression::unary(
                    UnaryExpressionOp::ToJson,
                    Expression::column(["stats_parsed"]),
                )
            } else {
                Expression::column([field.name.clone()])
            }
        }))
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
    )
}

fn parse_stats_column_impl(
    batch: &RecordBatch,
    stats_schema: KernelSchemaRef,
    partitions_schema: Option<&KernelSchemaRef>,
    column_mapping_mode: ColumnMappingMode,
) -> DeltaResult<RecordBatch> {
    let Some((stats_idx, _)) = batch.schema_ref().column_with_name("stats") else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: "stats column not found".to_string(),
        });
    };

    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();

    let stats_batch = batch.project(&[stats_idx])?;
    let stats_data = Box::new(ArrowEngineData::new(stats_batch));

    let parsed = parse_json(stats_data, stats_schema)?;
    let parsed: RecordBatch = ArrowEngineData::try_from_engine_data(parsed)?.into();

    let stats_array: Arc<StructArray> = Arc::new(parsed.into());
    fields[stats_idx] = Arc::new(Field::new(
        "stats_parsed",
        stats_array.data_type().to_owned(),
        true,
    ));
    columns[stats_idx] = stats_array;

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
    use arrow::datatypes::DataType as ArrowDataType;
    use arrow::datatypes::Field as ArrowField;
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow_schema::Field;
    use delta_kernel::schema::{MapType, MetadataValue, SchemaRef, StructField};

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
        .unwrap();

        let partition_values = MapType::new(DataType::STRING, DataType::STRING, true);
        let file_constant_values: SchemaRef = Arc::new(
            StructType::try_new([
                StructField::nullable("partitionValues", partition_values),
                StructField::nullable("baseRowId", DataType::LONG),
            ])
            .unwrap(),
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
}
