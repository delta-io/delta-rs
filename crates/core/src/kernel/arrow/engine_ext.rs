//! Utilities for interacting with Kernel APIs using Arrow data structures.
//!
use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef,
};
use delta_kernel::arrow::array::BooleanArray;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::{ColumnName, Scalar, StructData};
use delta_kernel::scan::ScanMetadata;
use delta_kernel::schema::{
    ArrayType, DataType, MapType, PrimitiveType, Schema, SchemaRef, SchemaTransform, StructField,
    StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, TableProperties};
use delta_kernel::{DeltaResult, ExpressionEvaluator, ExpressionRef};

use crate::errors::{DeltaResult as DeltaResultLocal, DeltaTableError};
use crate::kernel::SCAN_ROW_ARROW_SCHEMA;

/// [`ScanMetadata`] contains (1) a [`RecordBatch`] specifying data files to be scanned
/// and (2) a vector of transforms (one transform per scan file) that must be applied to the data read
/// from those files.
pub(crate) struct ScanMetadataArrow {
    /// Record batch with one row per file to scan
    pub scan_files: RecordBatch,

    /// Row-level transformations to apply to data read from files.
    ///
    /// Each entry in this vector corresponds to a row in the `scan_files` data. The entry is an
    /// expression that must be applied to convert the file's data into the logical schema
    /// expected by the scan:
    ///
    /// - `Some(expr)`: Apply this expression to transform the data to match [`Scan::schema()`].
    /// - `None`: No transformation is needed; the data is already in the correct logical form.
    ///
    /// Note: This vector can be indexed by row number.
    scan_file_transforms: Vec<Option<ExpressionRef>>,
}

/// Internal extension traits to the Kernel Snapshot.
///
/// These traits provide additional convenience functionality for working with Kernel snapshots.
/// Some of this may eventually be upstreamed as the kernel implementation matures.
pub(crate) trait SnapshotExt {
    /// Returns the expected file statistics schema for the snapshot.
    fn stats_schema(&self) -> DeltaResult<SchemaRef>;

    /// The expected schema for partition values
    fn partitions_schema(&self) -> DeltaResultLocal<Option<SchemaRef>>;

    /// The scheme expected for the data returned from a scan.
    ///
    /// This is an extended version of the raw schema that includes additional
    /// computations by delta-rs. Specifically the `stats_parsed` and
    /// `partitionValues_parsed` fields are added.
    fn scan_row_parsed_schema_arrow(&self) -> DeltaResultLocal<ArrowSchemaRef>;
}

impl SnapshotExt for Snapshot {
    fn stats_schema(&self) -> DeltaResult<SchemaRef> {
        let partition_columns = self.table_configuration().metadata().partition_columns();
        let column_mapping_mode = self.table_configuration().column_mapping_mode();
        let physical_schema = StructType::try_new(
            self.schema()
                .fields()
                .filter(|field| !partition_columns.contains(field.name()))
                .map(|field| field.make_physical(column_mapping_mode)),
        )?;
        Ok(Arc::new(stats_schema(
            &physical_schema,
            self.table_properties(),
        )))
    }

    fn partitions_schema(&self) -> DeltaResultLocal<Option<SchemaRef>> {
        Ok(partitions_schema(
            self.schema().as_ref(),
            self.table_configuration().metadata().partition_columns(),
        )?
        .map(Arc::new))
    }

    /// Arrow schema for a parsed (including stats_parsed and partitionValues_parsed)
    /// scan row (file data).
    fn scan_row_parsed_schema_arrow(&self) -> DeltaResultLocal<ArrowSchemaRef> {
        let mut fields = SCAN_ROW_ARROW_SCHEMA.fields().to_vec();
        let stats_idx = SCAN_ROW_ARROW_SCHEMA.index_of("stats").unwrap();

        let stats_schema = self.stats_schema()?;
        let stats_schema: ArrowSchema = stats_schema.as_ref().try_into_arrow()?;
        fields[stats_idx] = Arc::new(Field::new(
            "stats_parsed",
            ArrowDataType::Struct(stats_schema.fields().to_owned()),
            true,
        ));

        if let Some(partition_schema) = self.partitions_schema()? {
            let partition_schema: ArrowSchema = partition_schema.as_ref().try_into_arrow()?;
            fields.push(Arc::new(Field::new(
                "partitionValues_parsed",
                ArrowDataType::Struct(partition_schema.fields().to_owned()),
                false,
            )));
        }

        let schema = Arc::new(ArrowSchema::new(fields));
        Ok(schema)
    }
}

fn partitions_schema(
    schema: &StructType,
    partition_columns: &[String],
) -> DeltaResultLocal<Option<StructType>> {
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

/// Generates the expected schema for file statistics.
///
/// The base stats schema is dependent on the current table configuration and derived via:
/// - only fields present in data files are included (use physical names, no partition columns)
/// - if `dataSkippingStatsColumns` is set, include only those columns.
///   Column names may refer to struct fields in which case all child fields are included.
/// - otherwise the first `dataSkippingNumIndexedCols` (default 32) leaf fields are included.
/// - all fields are made nullable.
///
/// For the `nullCount` schema, we consider the whole base schema and convert all leaf fields
/// to data type LONG. Maps, arrays, and variant are considered leaf fields in this case.
///
/// For the min / max schemas, we non-eligible leaf fields from the base schema.
/// Field eligibility is determined by the fields data type via [`is_skipping_eligeble_datatype`].
///
/// The overall schema is then:
/// ```ignored
/// {
///    numRecords: long,
///    nullCount: <derived null count schema>,
///    minValues: <derived min/max schema>,
///    maxValues: <derived min/max schema>,
/// }
/// ```
pub(crate) fn stats_schema(
    physical_file_schema: &Schema,
    table_properties: &TableProperties,
) -> Schema {
    let mut fields = Vec::with_capacity(4);
    fields.push(StructField::nullable("numRecords", DataType::LONG));

    // generate the base stats schema:
    // - make all fields nullable
    // - include fields according to table properties (num_indexed_cols, stats_coliumns, ...)
    let mut base_transform = BaseStatsTransform::new(table_properties);
    if let Some(base_schema) = base_transform.transform_struct(physical_file_schema) {
        let base_schema = base_schema.into_owned();

        // convert all leaf fields to data type LONG for null count
        let mut null_count_transform = NullCountStatsTransform;
        if let Some(null_count_schema) = null_count_transform.transform_struct(&base_schema) {
            fields.push(StructField::nullable(
                "nullCount",
                null_count_schema.into_owned(),
            ));
        };

        // include only min/max skipping eligible fields (data types)
        let mut min_max_transform = MinMaxStatsTransform;
        if let Some(min_max_schema) = min_max_transform.transform_struct(&base_schema) {
            let min_max_schema = min_max_schema.into_owned();
            fields.push(StructField::nullable("minValues", min_max_schema.clone()));
            fields.push(StructField::nullable("maxValues", min_max_schema));
        }
    }

    StructType::try_new(fields).expect("Failed to construct StructType for stats_schema")
}

// Convert a min/max stats schema into a nullcount schema (all leaf fields are LONG)
pub(crate) struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_primitive(&mut self, _ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Owned(PrimitiveType::Long))
    }
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

        if matches!(
            &field.data_type,
            DataType::Array(_) | DataType::Map(_) | DataType::Variant(_)
        ) {
            return Some(Cow::Owned(StructField {
                name: field.name.clone(),
                data_type: DataType::LONG,
                nullable: true,
                metadata: Default::default(),
            }));
        }

        match self.transform(&field.data_type)? {
            Borrowed(_) => Some(Borrowed(field)),
            dt => Some(Owned(StructField {
                name: field.name.clone(),
                data_type: dt.into_owned(),
                nullable: true,
                metadata: Default::default(),
            })),
        }
    }
}

/// Transforms a table schema into a base stats schema.
///
/// Base stats schema in this case refers the subsets of fields in the table schema
/// that may be considered for stats collection. Depending on the type of stats - min/max/nullcount/... -
/// additional transformations may be applied.
///
/// The concrete shape of the schema depends on the table configuration.
/// * `dataSkippingStatsColumns` - used to explicitly specify the columns
///   to be used for data skipping statistics. (takes precedence)
/// * `dataSkippingNumIndexedCols` - used to specify the number of columns
///   to be used for data skipping statistics. Defaults to 32.
///
/// All fields are nullable.
struct BaseStatsTransform {
    n_columns: Option<DataSkippingNumIndexedCols>,
    added_columns: u64,
    column_names: Option<Vec<ColumnName>>,
    path: Vec<String>,
}

impl BaseStatsTransform {
    fn new(props: &TableProperties) -> Self {
        // if data_skipping_stats_columns is specified, it takes precedence
        // over data_skipping_num_indexed_cols, even if that is also specified
        if let Some(columns_names) = &props.data_skipping_stats_columns {
            Self {
                n_columns: None,
                added_columns: 0,
                column_names: Some(columns_names.clone()),
                path: Vec::new(),
            }
        } else {
            Self {
                n_columns: Some(
                    props
                        .data_skipping_num_indexed_cols
                        .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32)),
                ),
                added_columns: 0,
                column_names: None,
                path: Vec::new(),
            }
        }
    }
}

impl<'a> SchemaTransform<'a> for BaseStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

        // Check if the number of columns is set and if the added columns exceed the limit
        // In the constructor we assert this will always be None if column_names are specified
        if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = self.n_columns {
            if self.added_columns >= n_cols {
                return None;
            }
        }

        self.path.push(field.name.clone());
        let data_type = field.data_type();

        // keep the field if it:
        // - is a struct field and we need to traverse its children
        // - OR it is referenced by the column names
        // - OR it is a primitive type / leaf field
        let should_include = matches!(data_type, DataType::Struct(_))
            || self
                .column_names
                .as_ref()
                .map(|ns| should_include_column(&ColumnName::new(&self.path), ns))
                .unwrap_or(true);

        if !should_include {
            self.path.pop();
            return None;
        }

        // increment count only for leaf columns.
        if !matches!(data_type, DataType::Struct(_)) {
            self.added_columns += 1;
        }

        let field = match self.transform(&field.data_type)? {
            Borrowed(_) if field.is_nullable() => Borrowed(field),
            data_type => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: Default::default(),
            }),
        };

        self.path.pop();

        // exclude struct fields with no children
        if matches!(field.data_type(), DataType::Struct(dt) if dt.num_fields() == 0) {
            None
        } else {
            Some(field)
        }
    }
}

// removes all fields with non eligible data types
//
// should only be applied to schema oricessed via `BaseStatsTransform`.
struct MinMaxStatsTransform;

impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    // array and map fields are not eligible for data skipping, so filter them out.
    fn transform_array(&mut self, _: &'a ArrayType) -> Option<Cow<'a, ArrayType>> {
        None
    }
    fn transform_map(&mut self, _: &'a MapType) -> Option<Cow<'a, MapType>> {
        None
    }
    fn transform_variant(&mut self, _: &'a StructType) -> Option<Cow<'a, StructType>> {
        None
    }

    fn transform_primitive(&mut self, ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        if is_skipping_eligeble_datatype(ptype) {
            Some(Cow::Borrowed(ptype))
        } else {
            None
        }
    }
}

// Checks if a column should be included or traversed into.
//
// Returns true if the column name is included in the list of column names
// or if the column name is a prefix of any column name in the list
// or if the column name is a child of any column name in the list
fn should_include_column(column_name: &ColumnName, column_names: &[ColumnName]) -> bool {
    column_names.iter().any(|name| {
        name.as_ref().starts_with(column_name) || column_name.as_ref().starts_with(name)
    })
}

/// Checks if a data type is eligible for min/max file skipping.
/// https://github.com/delta-io/delta/blob/143ab3337121248d2ca6a7d5bc31deae7c8fe4be/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java#L61
fn is_skipping_eligeble_datatype(data_type: &PrimitiveType) -> bool {
    matches!(
        data_type,
        &PrimitiveType::Byte
            | &PrimitiveType::Short
            | &PrimitiveType::Integer
            | &PrimitiveType::Long
            | &PrimitiveType::Float
            | &PrimitiveType::Double
            | &PrimitiveType::Date
            | &PrimitiveType::Timestamp
            | &PrimitiveType::TimestampNtz
            | &PrimitiveType::String
            // | &PrimitiveType::Boolean
            | PrimitiveType::Decimal(_)
    )
}

pub(crate) fn kernel_to_arrow(metadata: ScanMetadata) -> DeltaResult<ScanMetadataArrow> {
    let (underlying_data, selection_vector) = metadata.scan_files.into_parts();
    let scan_file_transforms = metadata
        .scan_file_transforms
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| selection_vector[i].then_some(v))
        .collect();
    let batch = ArrowEngineData::try_from_engine_data(underlying_data)?.into();
    let scan_files = filter_record_batch(&batch, &BooleanArray::from(selection_vector))?;
    Ok(ScanMetadataArrow {
        scan_files,
        scan_file_transforms,
    })
}

/// Internal extension trait for expression evaluators.
///
/// This just abstracts the conversion between Arrow [`RecoedBatch`]es and
/// Kernel's [`ArrowEngineData`].
pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        Ok(ArrowEngineData::try_from_engine_data(T::evaluate(self, &engine_data)?)?.into())
    }
}

/// Extension trait for Kernel's [`StructData`].
///
/// StructData is the data structure contained in a Struct scalar.
/// The exposed API on kernels struct data is very minimal and does not allow
/// for conveniently probing the fields / values contained within [`StructData`].
///
/// This trait therefore adds convenience methods for accessing fields and values.
pub trait StructDataExt {
    /// Returns a reference to the field with the given name, if it exists.
    fn field(&self, name: &str) -> Option<&StructField>;

    /// Returns a reference to the value with the given index, if it exists.
    fn value(&self, index: usize) -> Option<&Scalar>;

    /// Returns the index of the field with the given name, if it exists.
    fn index_of(&self, name: &str) -> Option<usize>;
}

impl StructDataExt for StructData {
    fn field(&self, name: &str) -> Option<&StructField> {
        self.fields().iter().find(|f| f.name() == name)
    }

    fn index_of(&self, name: &str) -> Option<usize> {
        self.fields().iter().position(|f| f.name() == name)
    }

    fn value(&self, index: usize) -> Option<&Scalar> {
        self.values().get(index)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use delta_kernel::arrow::array::Int32Array;
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_conversion::TryIntoKernel;
    use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
    use delta_kernel::expressions::*;
    use delta_kernel::schema::{ArrayType, DataType};
    use delta_kernel::EvaluationHandler;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_evaluate_arrow() {
        let handler = ArrowEvaluationHandler;

        let schema = Schema::new(vec![Field::new("a", ArrowDataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();

        let expression = column_expr!("a");
        let expr = handler
            .new_expression_evaluator(
                Arc::new((&schema).try_into_kernel().unwrap()),
                expression.into(),
                DataType::INTEGER,
            )
            .unwrap();

        let result = expr.evaluate_arrow(batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_include_column() {
        let full_name = vec![ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl4"])];
        let parent = ColumnName::new(["lvl1", "lvl2", "lvl3"]);
        assert!(should_include_column(&parent, &full_name));
        assert!(should_include_column(&full_name[0], &full_name));
        // child fields should also be included
        assert!(should_include_column(&full_name[0], &[parent]));

        let not_parent = ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl5"]);
        assert!(!should_include_column(&not_parent, &full_name));
        let not_parent = ColumnName::new(["lvl1", "lvl3", "lvl4"]);
        assert!(!should_include_column(&not_parent, &full_name));

        let not_parent = ColumnName::new(["lvl1", "lvl2", "lvl4"]);
        assert!(!should_include_column(&not_parent, &full_name));
    }

    #[test]
    fn test_stats_schema_simple() {
        let properties: TableProperties = [("key", "value")].into();
        let file_schema =
            StructType::try_new([StructField::nullable("id", DataType::LONG)]).unwrap();

        let stats_schema = stats_schema(&file_schema, &properties);
        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", file_schema.clone()),
            StructField::nullable("minValues", file_schema.clone()),
            StructField::nullable("maxValues", file_schema),
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_with_non_eligible_field() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a nested logical schema with:
        // - top-level field "id" (LONG) - eligible for data skipping
        // - nested struct "metadata" containing:
        //   - "name" (STRING) - eligible for data skipping
        //   - "tags" (ARRAY) - NOT eligible for data skipping
        //   - "score" (DOUBLE) - eligible for data skipping

        // Create array type for a field that's not eligible for data skipping
        let array_type = DataType::Array(Box::new(ArrayType::new(DataType::STRING, false)));
        let metadata_struct = StructType::try_new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("tags", array_type),
            StructField::nullable("score", DataType::DOUBLE),
        ])
        .unwrap();
        let file_schema = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable(
                "metadata",
                DataType::Struct(Box::new(metadata_struct.clone())),
            ),
        ])
        .unwrap();

        let stats_schema = stats_schema(&file_schema, &properties);

        let expected_null_nested = StructType::try_new([
            StructField::nullable("name", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
            StructField::nullable("score", DataType::LONG),
        ])
        .unwrap();
        let expected_null = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("metadata", DataType::Struct(Box::new(expected_null_nested))),
        ])
        .unwrap();

        let expected_nested = StructType::try_new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("score", DataType::DOUBLE),
        ])
        .unwrap();
        let expected_fields = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("metadata", DataType::Struct(Box::new(expected_nested))),
        ])
        .unwrap();

        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_col_names() {
        let properties: TableProperties = [(
            "delta.dataSkippingStatsColumns".to_string(),
            "`user.info`.name".to_string(),
        )]
        .into();

        let user_struct = StructType::try_new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ])
        .unwrap();
        let file_schema = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user.info", DataType::Struct(Box::new(user_struct.clone()))),
        ])
        .unwrap();

        let stats_schema = stats_schema(&file_schema, &properties);

        let expected_nested =
            StructType::try_new([StructField::nullable("name", DataType::STRING)]).unwrap();
        let expected_fields = StructType::try_new([StructField::nullable(
            "user.info",
            DataType::Struct(Box::new(expected_nested)),
        )])
        .unwrap();
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_n_cols() {
        let properties: TableProperties = [(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "1".to_string(),
        )]
        .into();

        let logical_schema = StructType::try_new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ])
        .unwrap();

        let stats_schema = stats_schema(&logical_schema, &properties);

        let expected_fields =
            StructType::try_new([StructField::nullable("name", DataType::STRING)]).unwrap();
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_different_fields_in_null_vs_minmax() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a schema with fields that have different eligibility for min/max vs null count
        // - "id" (LONG) - eligible for both null count and min/max
        // - "is_active" (BOOLEAN) - eligible for null count but NOT for min/max
        // - "metadata" (BINARY) - eligible for null count but NOT for min/max
        let file_schema = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::BOOLEAN),
            StructField::nullable("metadata", DataType::BINARY),
        ])
        .unwrap();

        let stats_schema = stats_schema(&file_schema, &properties);

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
        ])
        .unwrap();

        // Expected minValues/maxValues schema: only eligible fields (no boolean, no binary)
        let expected_min_max =
            StructType::try_new([StructField::nullable("id", DataType::LONG)]).unwrap();

        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_nested_different_fields_in_null_vs_minmax() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a nested schema where some nested fields are eligible for min/max and others aren't
        let user_struct = StructType::try_new([
            StructField::nullable("name", DataType::STRING), // eligible for min/max
            StructField::nullable("is_admin", DataType::BOOLEAN), // NOT eligible for min/max
            StructField::nullable("age", DataType::INTEGER), // eligible for min/max
            StructField::nullable("profile_pic", DataType::BINARY), // NOT eligible for min/max
        ])
        .unwrap();

        let file_schema = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(user_struct.clone()))),
            StructField::nullable("is_deleted", DataType::BOOLEAN), // NOT eligible for min/max
        ])
        .unwrap();

        let stats_schema = stats_schema(&file_schema, &properties);

        // Expected nullCount schema: all fields converted to LONG, maintaining structure
        let expected_null_user = StructType::try_new([
            StructField::nullable("name", DataType::LONG),
            StructField::nullable("is_admin", DataType::LONG),
            StructField::nullable("age", DataType::LONG),
            StructField::nullable("profile_pic", DataType::LONG),
        ])
        .unwrap();
        let expected_null_count = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_null_user))),
            StructField::nullable("is_deleted", DataType::LONG),
        ])
        .unwrap();

        // Expected minValues/maxValues schema: only eligible fields
        let expected_minmax_user = StructType::try_new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ])
        .unwrap();
        let expected_min_max = StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("user", DataType::Struct(Box::new(expected_minmax_user))),
        ])
        .unwrap();

        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            StructField::nullable("minValues", expected_min_max.clone()),
            StructField::nullable("maxValues", expected_min_max),
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_only_non_eligible_fields() {
        let properties: TableProperties = [("key", "value")].into();

        // Create a schema with only fields that are NOT eligible for min/max skipping
        let file_schema = StructType::try_new([
            StructField::nullable("is_active", DataType::BOOLEAN),
            StructField::nullable("metadata", DataType::BINARY),
            StructField::nullable(
                "tags",
                DataType::Array(Box::new(ArrayType::new(DataType::STRING, false))),
            ),
        ])
        .unwrap();

        let stats_schema = stats_schema(&file_schema, &properties);

        // Expected nullCount schema: all fields converted to LONG
        let expected_null_count = StructType::try_new([
            StructField::nullable("is_active", DataType::LONG),
            StructField::nullable("metadata", DataType::LONG),
            StructField::nullable("tags", DataType::LONG),
        ])
        .unwrap();

        // Expected minValues/maxValues schema: empty since no fields are eligible
        // Since there are no eligible fields, minValues and maxValues should not be present
        let expected = StructType::try_new([
            StructField::nullable("numRecords", DataType::LONG),
            StructField::nullable("nullCount", expected_null_count),
            // No minValues or maxValues fields since no primitive fields are eligible
        ])
        .unwrap();

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_partitions_schema() -> DeltaResultLocal<()> {
        let logical_schema = StructType::try_new([
            StructField::nullable("name", DataType::STRING),
            StructField::nullable("age", DataType::INTEGER),
        ])
        .unwrap();

        let result = partitions_schema(&logical_schema, &[])?;
        assert_eq!(None, result);
        Ok(())
    }

    #[tokio::test]
    async fn test_partition_schema2() {
        let schema = StructType::try_new(vec![
            StructField::new("id", DataType::LONG, true),
            StructField::new("name", DataType::STRING, true),
            StructField::new("date", DataType::DATE, true),
        ])
        .unwrap();

        let partition_columns = vec!["date".to_string()];
        let expected =
            StructType::try_new(vec![StructField::new("date", DataType::DATE, true)]).unwrap();
        assert_eq!(
            partitions_schema(&schema, &partition_columns).unwrap(),
            Some(expected)
        );

        assert_eq!(partitions_schema(&schema, &[]).unwrap(), None);
    }
}
