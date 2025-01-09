use std::borrow::Cow;
use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_schema::{Field as ArrowField, Fields};
use convert_case::{Case, Casing};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::column_expr;
use delta_kernel::schema::{
    ArrayType, DataType, MapType, PrimitiveType, SchemaRef, SchemaTransform, StructField,
    StructType,
};
use delta_kernel::{Expression, ExpressionEvaluator, ExpressionHandler};

use crate::table::config::TableConfig;
use crate::DeltaResult;
use crate::DeltaTableError;

use super::super::ARROW_HANDLER;

pub(super) struct AddOrdinals;

impl AddOrdinals {
    pub const PATH: usize = 0;
    pub const SIZE: usize = 1;
    pub const MODIFICATION_TIME: usize = 2;
    pub const DATA_CHANGE: usize = 3;
}

pub(super) struct DVOrdinals;

impl DVOrdinals {
    pub const STORAGE_TYPE: usize = 0;
    pub const PATH_OR_INLINE_DV: usize = 1;
    // pub const OFFSET: usize = 2;
    pub const SIZE_IN_BYTES: usize = 3;
    pub const CARDINALITY: usize = 4;
}

impl DVOrdinals {}

lazy_static::lazy_static! {
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Deletion-Vectors
    static ref DV_FIELDS: StructType = StructType::new([
        StructField::new("storageType", DataType::STRING, true),
        StructField::new("pathOrInlineDv", DataType::STRING, true),
        StructField::new("offset", DataType::INTEGER, true),
        StructField::new("sizeInBytes", DataType::INTEGER, true),
        StructField::new("cardinality", DataType::LONG, true),
    ]);
    static ref DV_FIELDS_OUT: StructType = StructType::new([
        StructField::new("storage_type", DataType::STRING, true),
        StructField::new("path_or_inline_dv", DataType::STRING, true),
        StructField::new("offset", DataType::INTEGER, true),
        StructField::new("size_in_bytes", DataType::INTEGER, true),
        StructField::new("cardinality", DataType::LONG, true),
    ]);
}

fn get_add_transform_expr() -> Expression {
    Expression::Struct(vec![
        column_expr!("add.path"),
        column_expr!("add.size"),
        column_expr!("add.modificationTime"),
        column_expr!("add.stats"),
        column_expr!("add.deletionVector"),
        Expression::Struct(vec![column_expr!("add.partitionValues")]),
    ])
}

pub(super) fn extract_adds(
    batch: &RecordBatch,
    stats_schema: &StructType,
    partition_schema: Option<&StructType>,
) -> DeltaResult<RecordBatch> {
    // base expression to extract the add action fields
    let mut columns = vec![
        Expression::column(["add", "path"]),
        Expression::column(["add", "size"]),
        Expression::column(["add", "modificationTime"]),
        Expression::column(["add", "dataChange"]),
        // Expression::column(["add", "stats"]),
        Expression::struct_from(
            stats_schema
                .fields()
                .map(|f| Expression::column(["add", "stats_parsed", f.name().as_str()])),
        ),
    ];
    // fields for the output schema
    let mut out_fields = vec![
        StructField::new("path", DataType::STRING, true),
        StructField::new("size", DataType::LONG, true),
        StructField::new("modification_time", DataType::LONG, true),
        StructField::new("data_change", DataType::BOOLEAN, true),
        // StructField::new("stats", DataType::STRING, true),
        StructField::new("stats_parsed", stats_schema.clone(), true),
    ];

    let has_stats = batch
        .column_by_name("add")
        .and_then(|c| c.as_struct_opt())
        .is_some_and(|c| c.column_by_name("stats").is_some());
    if has_stats {
        columns.push(Expression::column(["add", "stats"]));
        out_fields.push(StructField::new("stats", DataType::STRING, true));
    };

    if let Some(partition_schema) = partition_schema {
        // TODO we assume there are parsed partition values - this maz change in the future
        // when we get the log data directly form kernel.
        columns.push(Expression::column(["add", "partitionValues_parsed"]));
        out_fields.push(StructField::new(
            "partition_values",
            partition_schema.clone(),
            true,
        ));
    }

    let has_dv = batch
        .column_by_name("add")
        .and_then(|c| c.as_struct_opt())
        .is_some_and(|c| c.column_by_name("deletionVector").is_some());
    if has_dv {
        columns.push(Expression::struct_from(DV_FIELDS.fields().map(|f| {
            Expression::column(["add", "deletionVector", f.name().as_str()])
        })));
        out_fields.push(StructField::new(
            "deletion_vector",
            DV_FIELDS_OUT.clone(),
            true,
        ));
    }

    let data_schema: SchemaRef = Arc::new(batch.schema().as_ref().try_into()?);

    // remove non add action rows from record batch before processing
    let filter_expr = Expression::column(["add", "path"]).is_not_null();
    let filter_evaluator = get_evaluator(data_schema.clone(), filter_expr, DataType::BOOLEAN);
    let result = eval_expr(&filter_evaluator, batch)?;
    let predicate = result
        .column(0)
        .as_boolean_opt()
        .ok_or_else(|| DeltaTableError::generic("expected boolean array"))?;
    let filtered_batch = filter_record_batch(batch, predicate)?;

    dbg!(&out_fields);

    let evaluator = get_evaluator(
        data_schema,
        Expression::struct_from(columns),
        DataType::struct_type(out_fields.clone()),
    );
    let result = eval_expr(&evaluator, &filtered_batch)?;

    dbg!("done evaluating");

    let result = fill_with_null(&StructType::new(out_fields.clone()), &result.into())?;
    let mut columns = result.as_struct().columns().to_vec();

    // assert a conssitent stats scehma by imputing missing value with null
    // the stats schema may be different per file, as the table configuration
    // for collecting stats may change over time.
    // let mut columns = result.columns().to_vec();
    // if let Some(stats_col) = result.column_by_name("stats_parsed") {
    //     let stats = stats_col
    //         .as_struct_opt()
    //         .ok_or_else(|| DeltaTableError::generic("expected struct array"))?;
    //     columns[OUT_STATS_ORDINAL] = fill_with_null(&stats_schema, stats)?;
    // };

    if !has_stats {
        out_fields.push(StructField::new("stats", DataType::STRING, true));
        columns.push(null_array(&DataType::STRING, filtered_batch.num_rows())?);
    }

    // ensure consistent schema by adding empty deletion vector data if it is missing in the input
    if !has_dv {
        out_fields.push(StructField::new(
            "deletion_vector",
            DV_FIELDS_OUT.clone(),
            true,
        ));
        columns.push(null_array(
            // safety: we just added a filed to the vec in additions to the ones above,
            out_fields.last().unwrap().data_type(),
            filtered_batch.num_rows(),
        )?);
    }

    let batch_schema = Arc::new((&StructType::new(out_fields.clone())).try_into()?);
    Ok(RecordBatch::try_new(batch_schema, columns)?)
}

struct FieldCasingTransform;
impl<'a> SchemaTransform<'a> for FieldCasingTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        self.recurse_into_struct_field(field)
            .map(|f| Cow::Owned(f.with_name(f.name().to_case(Case::Snake))))
    }
}

pub(super) fn eval_expr(
    evaluator: &Arc<dyn ExpressionEvaluator>,
    data: &RecordBatch,
) -> DeltaResult<RecordBatch> {
    let engine_data = ArrowEngineData::new(data.clone());
    let result = ArrowEngineData::try_from_engine_data(evaluator.evaluate(&engine_data)?)?;
    Ok(result.into())
}

fn fill_with_null(target_schema: &StructType, data: &StructArray) -> DeltaResult<ArrayRef> {
    let fields = Fields::from(
        target_schema
            .fields()
            .map(ArrowField::try_from)
            .collect::<Result<Vec<_>, _>>()?,
    );
    let arrays = target_schema
        .fields()
        .map(|target_field| {
            if let Some(col_arr) = data.column_by_name(target_field.name()) {
                if let DataType::Struct(struct_schema) = target_field.data_type() {
                    let struct_arr = col_arr
                        .as_struct_opt()
                        .ok_or_else(|| DeltaTableError::generic("expected struct array"))?;
                    fill_with_null(struct_schema, struct_arr)
                } else {
                    Ok(col_arr.clone())
                }
            } else {
                if target_field.is_nullable() {
                    null_array(target_field.data_type(), data.len())
                } else {
                    Err(DeltaTableError::generic(format!(
                        "missing non-nullable field: {}",
                        target_field.name()
                    )))
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StructArray::try_new(
        fields,
        arrays,
        data.nulls().cloned(),
    )?))
}

pub(super) fn get_evaluator(
    schema: SchemaRef,
    expression: Expression,
    output_type: DataType,
) -> Arc<dyn ExpressionEvaluator> {
    ARROW_HANDLER.get_evaluator(schema, expression, output_type)
}

fn null_array(data_type: &DataType, num_rows: usize) -> DeltaResult<ArrayRef> {
    use crate::kernel::arrow::LIST_ARRAY_ROOT;
    use arrow_array::*;
    use delta_kernel::schema::PrimitiveType;

    let arr: Arc<dyn arrow::array::Array> = match data_type {
        DataType::Primitive(primitive) => match primitive {
            PrimitiveType::Byte => Arc::new(Int8Array::new_null(num_rows)),
            PrimitiveType::Short => Arc::new(Int16Array::new_null(num_rows)),
            PrimitiveType::Integer => Arc::new(Int32Array::new_null(num_rows)),
            PrimitiveType::Long => Arc::new(Int64Array::new_null(num_rows)),
            PrimitiveType::Float => Arc::new(Float32Array::new_null(num_rows)),
            PrimitiveType::Double => Arc::new(Float64Array::new_null(num_rows)),
            PrimitiveType::String => Arc::new(StringArray::new_null(num_rows)),
            PrimitiveType::Boolean => Arc::new(BooleanArray::new_null(num_rows)),
            PrimitiveType::Timestamp => {
                Arc::new(TimestampMicrosecondArray::new_null(num_rows).with_timezone("UTC"))
            }
            PrimitiveType::TimestampNtz => Arc::new(TimestampMicrosecondArray::new_null(num_rows)),
            PrimitiveType::Date => Arc::new(Date32Array::new_null(num_rows)),
            PrimitiveType::Binary => Arc::new(BinaryArray::new_null(num_rows)),
            PrimitiveType::Decimal(precision, scale) => Arc::new(
                Decimal128Array::new_null(num_rows)
                    .with_precision_and_scale(*precision, *scale as i8)?,
            ),
        },
        DataType::Struct(t) => {
            let fields = Fields::from(
                t.fields()
                    .map(ArrowField::try_from)
                    .collect::<Result<Vec<_>, _>>()?,
            );
            Arc::new(StructArray::new_null(fields, num_rows))
        }
        DataType::Array(t) => {
            let field = ArrowField::new(LIST_ARRAY_ROOT, t.element_type().try_into()?, true);
            Arc::new(ListArray::new_null(Arc::new(field), num_rows))
        }
        DataType::Map { .. } => unimplemented!(),
    };
    Ok(arr)
}
