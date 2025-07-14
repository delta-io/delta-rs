//! Utilities for interacting with Kernel APIs using Arrow data structures.
//!
use std::borrow::Cow;
use std::sync::Arc;

use delta_kernel::arrow::array::BooleanArray;
use delta_kernel::arrow::compute::filter_record_batch;
use delta_kernel::arrow::record_batch::RecordBatch;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::{Scan, ScanMetadata};
use delta_kernel::schema::{
    DataType, PrimitiveType, SchemaRef, SchemaTransform, StructField, StructType,
};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, TableProperties};
use delta_kernel::{
    DeltaResult, Engine, EngineData, ExpressionEvaluator, ExpressionRef, PredicateRef, Version,
};
use itertools::Itertools;

use crate::table::config::TableConfig;

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
    pub scan_file_transforms: Vec<Option<ExpressionRef>>,
}

pub(crate) trait ScanExt {
    /// Get the metadata for a table scan.
    ///
    /// This method handles translation between `EngineData` and `RecordBatch`
    /// and will already apply any selection vectors to the data.
    /// See [`Scan::scan_metadata`] for details.
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;

    fn scan_metadata_from_arrow(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: Box<dyn Iterator<Item = RecordBatch>>,
        existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>>;
}

impl ScanExt for Scan {
    fn scan_metadata_arrow(
        &self,
        engine: &dyn Engine,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        Ok(self
            .scan_metadata(engine)?
            .map_ok(kernel_to_arrow)
            .flatten())
    }

    fn scan_metadata_from_arrow(
        &self,
        engine: &dyn Engine,
        existing_version: Version,
        existing_data: Box<dyn Iterator<Item = RecordBatch>>,
        existing_predicate: Option<PredicateRef>,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<ScanMetadataArrow>>> {
        let engine_iter =
            existing_data.map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>);
        Ok(self
            .scan_metadata_from(engine, existing_version, engine_iter, existing_predicate)?
            .map_ok(kernel_to_arrow)
            .flatten())
    }
}

pub(crate) trait SnapshotExt {
    /// Returns the expected file statistics schema for the snapshot.
    fn stats_schema(&self) -> DeltaResult<SchemaRef>;
}

impl SnapshotExt for Snapshot {
    fn stats_schema(&self) -> DeltaResult<SchemaRef> {
        let physical_schema =
            StructType::new(self.schema().fields().map(|field| field.make_physical()));
        let min_max_transform = MinMaxStatsTransform::new(self.table_properties());
        stats_schema(&physical_schema, min_max_transform)
    }
}

// create a stats schema from our internal representation of the table config.
pub(crate) fn stats_schema_from_config(
    logical_schema: &StructType,
    table_conf: TableConfig<'_>,
) -> DeltaResult<SchemaRef> {
    let physical_schema =
        StructType::new(logical_schema.fields().map(|field| field.make_physical()));
    let min_max_transform = MinMaxStatsTransform::new_from_config(table_conf);
    stats_schema(&physical_schema, min_max_transform)
}

fn stats_schema(
    physical_schema: &StructType,
    mut min_max_transform: MinMaxStatsTransform,
) -> DeltaResult<SchemaRef> {
    let min_max_schema =
        if let Some(min_max_schema) = min_max_transform.transform_struct(physical_schema) {
            min_max_schema.into_owned()
        } else {
            StructType::new(vec![])
        };
    let nullcount_schema =
        if let Some(nullcount_schema) = NullCountStatsTransform.transform_struct(&min_max_schema) {
            nullcount_schema.into_owned()
        } else {
            StructType::new(vec![])
        };
    Ok(Arc::new(StructType::new([
        StructField::nullable("numRecords", DataType::LONG),
        StructField::nullable("nullCount", nullcount_schema),
        StructField::nullable("minValues", min_max_schema.clone()),
        StructField::nullable("maxValues", min_max_schema),
    ])))
}

struct MinMaxStatsTransform {
    n_columns: Option<DataSkippingNumIndexedCols>,
    added_columns: u64,
    column_names: Option<Vec<ColumnName>>,
    path: Vec<String>,
}

impl MinMaxStatsTransform {
    fn new(props: &TableProperties) -> Self {
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

    fn new_from_config(props: TableConfig<'_>) -> Self {
        if let Some(columns_names) = props.stats_columns_kernel() {
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
                        .num_indexed_cols_kernel()
                        .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32)),
                ),
                added_columns: 0,
                column_names: None,
                path: Vec::new(),
            }
        }
    }
}

// Convert a min/max stats schema into a nullcount schema (all leaf fields are LONG)
struct NullCountStatsTransform;
impl<'a> SchemaTransform<'a> for NullCountStatsTransform {
    fn transform_primitive(&mut self, _ptype: &'a PrimitiveType) -> Option<Cow<'a, PrimitiveType>> {
        Some(Cow::Owned(PrimitiveType::Long))
    }
}

fn should_include_column(column_name: &ColumnName, column_names: &[ColumnName]) -> bool {
    column_names
        .iter()
        .any(|name| name.as_ref().starts_with(column_name))
}

impl<'a> SchemaTransform<'a> for MinMaxStatsTransform {
    fn transform_struct_field(&mut self, field: &'a StructField) -> Option<Cow<'a, StructField>> {
        use Cow::*;

        if let Some(DataSkippingNumIndexedCols::NumColumns(n_cols)) = self.n_columns {
            if self.added_columns >= n_cols {
                return None;
            }
        }

        self.path.push(field.name.clone());
        let data_type = field.data_type();

        let should_include = self
            .column_names
            .as_ref()
            .map(|column_names| {
                let col_name = ColumnName::new(&self.path);
                should_include_column(&col_name, column_names)
                    || matches!(data_type, DataType::Struct(_))
            })
            .unwrap_or_else(|| is_skipping_eligeble_datatype(data_type))
            || matches!(data_type, DataType::Struct(_));

        if !should_include {
            self.path.pop();
            return None;
        }

        if is_skipping_eligeble_datatype(data_type) {
            self.added_columns += 1;
        }

        let field = match self.transform(&field.data_type)? {
            Borrowed(_) if field.is_nullable() => Borrowed(field),
            data_type => Owned(StructField {
                name: field.name.clone(),
                data_type: data_type.into_owned(),
                nullable: true,
                metadata: field.metadata.clone(),
            }),
        };

        self.path.pop();
        Some(field)
    }
}

// https://github.com/delta-io/delta/blob/143ab3337121248d2ca6a7d5bc31deae7c8fe4be/kernel/kernel-api/src/main/java/io/delta/kernel/internal/skipping/StatsSchemaHelper.java#L61
fn is_skipping_eligeble_datatype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        &DataType::BYTE
            | &DataType::SHORT
            | &DataType::INTEGER
            | &DataType::LONG
            | &DataType::FLOAT
            | &DataType::DOUBLE
            | &DataType::DATE
            | &DataType::TIMESTAMP
            | &DataType::TIMESTAMP_NTZ
            | &DataType::STRING
            | &DataType::BOOLEAN
            | DataType::Primitive(PrimitiveType::Decimal(_))
    )
}

fn kernel_to_arrow(metadata: ScanMetadata) -> DeltaResult<ScanMetadataArrow> {
    let scan_file_transforms = metadata
        .scan_file_transforms
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| metadata.scan_files.selection_vector[i].then_some(v))
        .collect();
    let batch = ArrowEngineData::try_from_engine_data(metadata.scan_files.data)?.into();
    let scan_files = filter_record_batch(
        &batch,
        &BooleanArray::from(metadata.scan_files.selection_vector),
    )?;
    Ok(ScanMetadataArrow {
        scan_files,
        scan_file_transforms,
    })
}

pub(crate) trait ExpressionEvaluatorExt {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch>;
}

impl<T: ExpressionEvaluator + ?Sized> ExpressionEvaluatorExt for T {
    fn evaluate_arrow(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        let engine_data = ArrowEngineData::new(batch);
        Ok(ArrowEngineData::try_from_engine_data(T::evaluate(self, &engine_data)?)?.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::logstore::LogStoreExt;
    use crate::test_utils::TestTables;

    use super::*;

    use delta_kernel::arrow::array::Int32Array;
    use delta_kernel::arrow::datatypes::{DataType, Field, Schema};
    use delta_kernel::arrow::record_batch::RecordBatch;
    use delta_kernel::engine::arrow_conversion::TryIntoKernel;
    use delta_kernel::engine::arrow_expression::ArrowEvaluationHandler;
    use delta_kernel::schema::{ArrayType, DataType as KernelDataType};
    use delta_kernel::EvaluationHandler;
    use delta_kernel::{expressions::*, Table};
    use pretty_assertions::assert_eq;

    #[test]
    fn test_evaluate_arrow() {
        let handler = ArrowEvaluationHandler;

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let values = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(values)]).unwrap();

        let expression = column_expr!("a");
        let expr = handler.new_expression_evaluator(
            Arc::new((&schema).try_into_kernel().unwrap()),
            expression,
            KernelDataType::INTEGER,
        );

        let result = expr.evaluate_arrow(batch);
        assert!(result.is_ok());
    }

    #[test]
    fn test_should_include_column() {
        let full_name = vec![ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl4"])];
        let parent = ColumnName::new(["lvl1", "lvl2", "lvl3"]);
        assert!(should_include_column(&parent, &full_name));
        assert!(should_include_column(&full_name[0], &full_name));
        let not_parent = ColumnName::new(["lvl1", "lvl2", "lvl3", "lvl5"]);
        assert!(!should_include_column(&not_parent, &full_name));
        let not_parent = ColumnName::new(["lvl1", "lvl3", "lvl4"]);
        assert!(!should_include_column(&not_parent, &full_name));
    }

    #[tokio::test]
    async fn test_stats_schema() {
        let log_store = TestTables::Simple.table_builder().build_storage().unwrap();
        let table = Table::try_from_uri(log_store.table_root_url()).unwrap();
        let engine = log_store.engine(None).await;
        let snapshot = table.snapshot(engine.as_ref(), None).unwrap();
        let stats_schema = snapshot.stats_schema().unwrap();

        let id_field = StructType::new([StructField::nullable("id", KernelDataType::LONG)]);
        let expected = Arc::new(StructType::new([
            StructField::nullable("numRecords", KernelDataType::LONG),
            StructField::nullable("nullCount", id_field.clone()),
            StructField::nullable("minValues", id_field.clone()),
            StructField::nullable("maxValues", id_field.clone()),
        ]));

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_old() {
        let raw = HashMap::from([("key".to_string(), "value".to_string())]);
        let config = TableConfig(&raw);
        let logical_schema = StructType::new([StructField::nullable("id", KernelDataType::LONG)]);

        let stats_schema = stats_schema_from_config(&logical_schema, config).unwrap();

        let expected = Arc::new(StructType::new([
            StructField::nullable("numRecords", KernelDataType::LONG),
            StructField::nullable("nullCount", logical_schema.clone()),
            StructField::nullable("minValues", logical_schema.clone()),
            StructField::nullable("maxValues", logical_schema.clone()),
        ]));

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_old_nested() {
        let raw = HashMap::from([("key".to_string(), "value".to_string())]);
        let config = TableConfig(&raw);

        // Create a nested logical schema with:
        // - top-level field "id" (LONG)
        // - nested struct "user" containing fields "name" (STRING) and "age" (INTEGER)
        let user_struct = StructType::new([
            StructField::nullable("name", KernelDataType::STRING),
            StructField::nullable("age", KernelDataType::INTEGER),
        ]);

        let logical_schema = StructType::new([
            StructField::nullable("id", KernelDataType::LONG),
            StructField::nullable(
                "user",
                KernelDataType::Struct(Box::new(user_struct.clone())),
            ),
        ]);

        let stats_schema = stats_schema_from_config(&logical_schema, config).unwrap();

        // Expected result: The stats schema should maintain the nested structure
        // but make all fields nullable
        let expected_nested = StructType::new([
            StructField::nullable("name", KernelDataType::STRING),
            StructField::nullable("age", KernelDataType::INTEGER),
        ]);

        let expected_fields = StructType::new([
            StructField::nullable("id", KernelDataType::LONG),
            StructField::nullable("user", KernelDataType::Struct(Box::new(expected_nested))),
        ]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = Arc::new(StructType::new([
            StructField::nullable("numRecords", KernelDataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ]));

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_old_with_nonskippable_field() {
        let raw = HashMap::from([("key".to_string(), "value".to_string())]);
        let config = TableConfig(&raw);

        // Create a nested logical schema with:
        // - top-level field "id" (LONG) - eligible for data skipping
        // - nested struct "metadata" containing:
        //   - "name" (STRING) - eligible for data skipping
        //   - "tags" (ARRAY) - NOT eligible for data skipping
        //   - "score" (DOUBLE) - eligible for data skipping

        // Create array type for a field that's not eligible for data skipping
        let array_type =
            KernelDataType::Array(Box::new(ArrayType::new(KernelDataType::STRING, false)));

        let metadata_struct = StructType::new([
            StructField::nullable("name", KernelDataType::STRING),
            StructField::nullable("tags", array_type),
            StructField::nullable("score", KernelDataType::DOUBLE),
        ]);

        let logical_schema = StructType::new([
            StructField::nullable("id", KernelDataType::LONG),
            StructField::nullable(
                "metadata",
                KernelDataType::Struct(Box::new(metadata_struct.clone())),
            ),
        ]);

        let stats_schema = stats_schema_from_config(&logical_schema, config).unwrap();

        // Expected result: The stats schema should maintain the structure
        // but exclude fields not eligible for data skipping (array type)
        let expected_nested = StructType::new([
            StructField::nullable("name", KernelDataType::STRING),
            // "tags" field should be excluded as it's an array type
            StructField::nullable("score", KernelDataType::DOUBLE),
        ]);

        let expected_fields = StructType::new([
            StructField::nullable("id", KernelDataType::LONG),
            StructField::nullable(
                "metadata",
                KernelDataType::Struct(Box::new(expected_nested)),
            ),
        ]);

        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = Arc::new(StructType::new([
            StructField::nullable("numRecords", KernelDataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ]));

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_old_col_names() {
        let raw = HashMap::from([(
            "delta.dataSkippingStatsColumns".to_string(),
            "`user.info`.name".to_string(),
        )]);
        let config = TableConfig(&raw);

        let user_struct = StructType::new([
            StructField::nullable("name", KernelDataType::STRING),
            StructField::nullable("age", KernelDataType::INTEGER),
        ]);
        let logical_schema = StructType::new([
            StructField::nullable("id", KernelDataType::LONG),
            StructField::nullable(
                "user.info",
                KernelDataType::Struct(Box::new(user_struct.clone())),
            ),
        ]);

        let stats_schema = stats_schema_from_config(&logical_schema, config).unwrap();

        let expected_nested =
            StructType::new([StructField::nullable("name", KernelDataType::STRING)]);
        let expected_fields = StructType::new([StructField::nullable(
            "user.info",
            KernelDataType::Struct(Box::new(expected_nested)),
        )]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = Arc::new(StructType::new([
            StructField::nullable("numRecords", KernelDataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ]));

        assert_eq!(&expected, &stats_schema);
    }

    #[test]
    fn test_stats_schema_old_n_cols() {
        let raw = HashMap::from([(
            "delta.dataSkippingNumIndexedCols".to_string(),
            "1".to_string(),
        )]);
        let config = TableConfig(&raw);

        let logical_schema = StructType::new([
            StructField::nullable("name", KernelDataType::STRING),
            StructField::nullable("age", KernelDataType::INTEGER),
        ]);

        let stats_schema = stats_schema_from_config(&logical_schema, config).unwrap();

        let expected_fields =
            StructType::new([StructField::nullable("name", KernelDataType::STRING)]);
        let null_count = NullCountStatsTransform
            .transform_struct(&expected_fields)
            .unwrap()
            .into_owned();

        let expected = Arc::new(StructType::new([
            StructField::nullable("numRecords", KernelDataType::LONG),
            StructField::nullable("nullCount", null_count),
            StructField::nullable("minValues", expected_fields.clone()),
            StructField::nullable("maxValues", expected_fields.clone()),
        ]));

        assert_eq!(&expected, &stats_schema);
    }
}
