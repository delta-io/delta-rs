//! Execution node that rewrites logical record batches into their physical (column-mapped)
//! form just before the write sink.
//!
//! Column mapping stores data in Parquet under *physical* column names (random `col-<uuid>` in
//! `name` mode), each tagged with a Parquet `field_id`, while the rest of delta-rs works on the
//! *logical* schema. This node renames each table column to its physical name and attaches the
//! `field_id`, passing non-table columns (e.g. the CDC `_change_type` marker) through unchanged.
//!
//! Only names and metadata change — Arrow types and buffers are preserved (so Large/View types
//! survive), making the rewrite effectively zero-copy.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayData, ArrayRef, RecordBatch, make_array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use delta_kernel::schema::{
    DataType as KernelDataType, SchemaRef as KernelSchemaRef, StructField, StructType,
};
use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_features::ColumnMappingMode;
use futures::{Stream, StreamExt};

use crate::errors::{DeltaResult, DeltaTableError};

/// Arrow field metadata key recognized by the Parquet writer to emit a `field_id`.
const PARQUET_FIELD_ID_META_KEY: &str = "PARQUET:field_id";

/// Length of the random data-file directory prefix on column-mapped tables (delta-spark default).
const DEFAULT_RANDOM_PREFIX_LENGTH: usize = 2;

/// Shared column-mapping write state, derived once from the table configuration and reused by
/// every write entry point — the DataFusion plan path (via [`ColumnMappingState::wrap_plan`]) and
/// the `DeltaDataSink` stream path (via [`ColumnMappingState::transform_batch`]) — so they all
/// emit physical names/ids, physical partition keys, and random-prefixed paths consistently.
#[derive(Clone)]
pub(crate) struct ColumnMappingState {
    mode: ColumnMappingMode,
    /// Kernel table schema carrying the `delta.columnMapping.*` annotations.
    logical_schema: KernelSchemaRef,
}

impl ColumnMappingState {
    /// Build the state from a table configuration, returning `None` when column mapping is off.
    pub(crate) fn from_table_config(table_config: &TableConfiguration) -> Option<Self> {
        let mode = table_config.column_mapping_mode();
        (mode != ColumnMappingMode::None).then(|| Self {
            mode,
            logical_schema: table_config.logical_schema(),
        })
    }

    /// Physical Arrow schema for `input`: table columns renamed to physical names with `field_id`
    /// metadata, non-table columns passed through.
    pub(crate) fn physical_schema(&self, input: &SchemaRef) -> SchemaRef {
        Arc::new(Schema::new(physical_fields(
            input.fields(),
            &self.logical_schema,
            self.mode,
        )))
    }

    /// Wrap a write plan so its output batches are emitted physically.
    pub(crate) fn wrap_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        ColumnMappingExec::try_new(plan, self.logical_schema.as_ref(), self.mode)
    }

    /// Rewrite a single batch into its physical form (for stream-based writers like `DeltaDataSink`).
    pub(crate) fn transform_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        apply_column_mapping(batch, &self.physical_schema(&batch.schema()))
    }

    /// Translate logical partition column names to physical. Errors (rather than silently using
    /// the logical name) if a partition column is missing from the table schema.
    pub(crate) fn physical_partition_columns(
        &self,
        partition_columns: &[String],
    ) -> DeltaResult<Vec<String>> {
        partition_columns
            .iter()
            .map(|name| {
                self.logical_schema
                    .field(name)
                    .map(|field| field.physical_name(self.mode).to_string())
                    .ok_or_else(|| {
                        DeltaTableError::Generic(format!(
                            "partition column '{name}' not found in the table schema"
                        ))
                    })
            })
            .collect()
    }

    /// Directory-prefix length for data files on column-mapped tables.
    pub(crate) fn random_prefix_length(&self) -> usize {
        DEFAULT_RANDOM_PREFIX_LENGTH
    }
}

/// Execution node that casts logical record batches to their physical, column-mapped form.
///
/// See the [module docs](self) for details. Constructed via [`ColumnMappingExec::try_new`],
/// which is a no-op (returns the input plan unchanged) when column mapping is disabled.
#[derive(Debug)]
struct ColumnMappingExec {
    input: Arc<dyn ExecutionPlan>,
    /// Output schema with physical column names and `field_id` metadata.
    physical_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl ColumnMappingExec {
    /// Wrap `input` so its output batches are rewritten to the physical schema.
    ///
    /// Returns `input` unchanged when `mode` is [`ColumnMappingMode::None`], so callers can
    /// invoke this unconditionally. `logical_schema` is the kernel table schema carrying the
    /// `delta.columnMapping.*` annotations used to resolve physical names and field ids.
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        logical_schema: &StructType,
        mode: ColumnMappingMode,
    ) -> DeltaResult<Arc<dyn ExecutionPlan>> {
        if mode == ColumnMappingMode::None {
            return Ok(input);
        }

        let physical_fields = physical_fields(input.schema().fields(), logical_schema, mode);
        let physical_schema = Arc::new(Schema::new(physical_fields));

        let properties = PlanProperties::new(
            EquivalenceProperties::new(physical_schema.clone()),
            input.properties().partitioning.clone(),
            input.properties().emission_type,
            input.properties().boundedness,
        );

        Ok(Arc::new(Self {
            input,
            physical_schema,
            properties: Arc::new(properties),
        }))
    }
}

impl DisplayAs for ColumnMappingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::TreeRender
            | DisplayFormatType::Verbose => {
                write!(f, "ColumnMappingExec")
            }
        }
    }
}

impl ExecutionPlan for ColumnMappingExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ColumnMappingExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "ColumnMappingExec wrong number of children: expected 1, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            physical_schema: self.physical_schema.clone(),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(ColumnMappingStream {
            schema: self.physical_schema.clone(),
            input: self.input.execute(partition, context)?,
        }))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Stream that rewrites each input batch into the physical schema.
struct ColumnMappingStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
}

impl Stream for ColumnMappingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(Some(apply_column_mapping(&batch, &self.schema)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ColumnMappingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Rebuild `batch` under `physical_schema`, reusing the underlying buffers and only changing
/// (possibly nested) field names and metadata. Columns are matched positionally; name-checked
/// adapters (`cast_record_batch`, `RecordBatch::with_schema`) reject the logical→physical rename.
fn apply_column_mapping(batch: &RecordBatch, physical_schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = physical_schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| retype_array(array, field.data_type()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(DataFusionError::from)?;
    RecordBatch::try_new(physical_schema.clone(), columns).map_err(DataFusionError::from)
}

/// Compute the physical Arrow fields for `arrow_fields` against the column-mapping
/// annotations in `logical_schema`. Fields not present in `logical_schema` (e.g. the CDC
/// `_change_type` marker) are passed through unchanged.
fn physical_fields(
    arrow_fields: &Fields,
    logical_schema: &StructType,
    mode: ColumnMappingMode,
) -> Vec<Field> {
    arrow_fields
        .iter()
        .map(|field| match logical_schema.field(field.name()) {
            Some(kernel_field) => physical_field(field, kernel_field, mode),
            None => field.as_ref().clone(),
        })
        .collect()
}

/// Clone a field's metadata, dropping any inherited `PARQUET:field_id`. Field ids are table-owned
/// (the column-mapping id is written as the Parquet `field_id`, per PROTOCOL.md), so a stale id
/// carried in on input batches must not survive into the written file. All other metadata is kept.
fn metadata_without_field_id(field: &Field) -> HashMap<String, String> {
    field
        .metadata()
        .iter()
        .filter(|(key, _)| key.as_str() != PARQUET_FIELD_ID_META_KEY)
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Build the physical Arrow field for a single logical field: rename to the physical name,
/// attach the table-owned `field_id` metadata, and recurse into nested types — all while
/// preserving the input Arrow data type.
fn physical_field(field: &Field, kernel_field: &StructField, mode: ColumnMappingMode) -> Field {
    // Strip parquet field ids
    let mut metadata = metadata_without_field_id(field);
    if let Some(id) = kernel_field.column_mapping_id() {
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
    }
    let data_type = physical_data_type(field.data_type(), kernel_field.data_type(), mode);
    Field::new(
        kernel_field.physical_name(mode),
        data_type,
        field.is_nullable(),
    )
    .with_metadata(metadata)
}

/// Recurse through nested Arrow types, renaming reachable struct fields to their physical names.
/// Names come from `kernel_type`, but the representation stays `arrow_type`'s: converting the
/// kernel type to Arrow (`make_physical().try_into_arrow()`) would canonicalize away the batch's
/// View/Large types and force a copy.
fn physical_data_type(
    arrow_type: &DataType,
    kernel_type: &KernelDataType,
    mode: ColumnMappingMode,
) -> DataType {
    match kernel_type {
        KernelDataType::Struct(kernel_struct) => {
            let DataType::Struct(arrow_fields) = arrow_type else {
                return arrow_type.clone();
            };
            DataType::Struct(physical_fields(arrow_fields, kernel_struct, mode).into())
        }
        KernelDataType::Array(kernel_array) => {
            let element_kernel = kernel_array.element_type();
            match arrow_type {
                DataType::List(element) => {
                    DataType::List(rebuild_element(element, element_kernel, mode))
                }
                DataType::LargeList(element) => {
                    DataType::LargeList(rebuild_element(element, element_kernel, mode))
                }
                DataType::ListView(element) => {
                    DataType::ListView(rebuild_element(element, element_kernel, mode))
                }
                DataType::LargeListView(element) => {
                    DataType::LargeListView(rebuild_element(element, element_kernel, mode))
                }
                DataType::FixedSizeList(element, len) => {
                    DataType::FixedSizeList(rebuild_element(element, element_kernel, mode), *len)
                }
                _ => arrow_type.clone(),
            }
        }
        KernelDataType::Map(kernel_map) => {
            let DataType::Map(entries, sorted) = arrow_type else {
                return arrow_type.clone();
            };
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return arrow_type.clone();
            };
            // Map entries are a struct of [key, value]; recurse into both by position,
            // keeping the synthetic "key"/"value"/"entries" names as-is.
            let mut new_fields: Vec<Field> = entry_fields
                .iter()
                .map(|f| {
                    Field::new(f.name(), f.data_type().clone(), f.is_nullable())
                        .with_metadata(metadata_without_field_id(f))
                })
                .collect();
            if let Some(key) = new_fields.get_mut(0) {
                *key = key.clone().with_data_type(physical_data_type(
                    key.data_type(),
                    kernel_map.key_type(),
                    mode,
                ));
            }
            if let Some(value) = new_fields.get_mut(1) {
                *value = value.clone().with_data_type(physical_data_type(
                    value.data_type(),
                    kernel_map.value_type(),
                    mode,
                ));
            }
            let new_entries = Field::new(
                entries.name(),
                DataType::Struct(new_fields.into()),
                entries.is_nullable(),
            )
            .with_metadata(metadata_without_field_id(entries));
            DataType::Map(Arc::new(new_entries), *sorted)
        }
        KernelDataType::Primitive(_) | KernelDataType::Variant(_) => arrow_type.clone(),
    }
}

/// Rebuild a list/array element field, preserving its name and nullability while recursing
/// into its (possibly nested) value type.
fn rebuild_element(
    element: &Arc<Field>,
    element_kernel: &KernelDataType,
    mode: ColumnMappingMode,
) -> Arc<Field> {
    let data_type = physical_data_type(element.data_type(), element_kernel, mode);
    Arc::new(
        Field::new(element.name(), data_type, element.is_nullable())
            .with_metadata(metadata_without_field_id(element)),
    )
}

/// Reinterpret `array` under `target` data type, reusing all buffers and offsets and only
/// fixing up nested field names. A fast path returns the input untouched when the types
/// already match (the common case for primitive leaves and pass-through columns).
fn retype_array(
    array: &ArrayRef,
    target: &DataType,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    if array.data_type() == target {
        return Ok(array.clone());
    }

    let data = array.to_data();
    let new_children: Vec<ArrayData> = match target {
        DataType::Struct(fields) => data
            .child_data()
            .iter()
            .zip(fields.iter())
            .map(|(child, field)| {
                retype_array(&make_array(child.clone()), field.data_type()).map(|a| a.to_data())
            })
            .collect::<std::result::Result<_, _>>()?,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => {
            vec![
                retype_array(&make_array(data.child_data()[0].clone()), field.data_type())?
                    .to_data(),
            ]
        }
        _ => data.child_data().to_vec(),
    };

    let rebuilt = data
        .into_builder()
        .data_type(target.clone())
        .child_data(new_children)
        .build()?;
    Ok(make_array(rebuilt))
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::{Int32Array, ListArray, StringArray, StringViewArray, StructArray};
    use arrow_buffer::OffsetBuffer;
    use delta_kernel::schema::ArrayType;

    use super::*;
    use crate::test_utils::{column_mapping_test_field, column_mapping_test_field_with_type};

    /// Logical kernel schema carrying column-mapping annotations:
    /// ```text
    ///   id:    int                        -> col-id    (id 1)
    ///   name:  string                     -> col-name  (id 2)
    ///   addr:  struct<street:string>      -> col-addr  (id 3) { street -> col-street (id 4) }
    ///   items: array<struct<sku:string>>  -> col-items (id 5) { ..struct { sku -> col-sku (id 6) } }
    /// ```
    fn logical_kernel_schema() -> StructType {
        let addr = KernelDataType::Struct(Box::new(
            StructType::try_new([column_mapping_test_field_with_type(
                "street",
                "col-street",
                4,
                KernelDataType::STRING,
            )])
            .unwrap(),
        ));
        let item_struct = KernelDataType::Struct(Box::new(
            StructType::try_new([column_mapping_test_field_with_type(
                "sku",
                "col-sku",
                6,
                KernelDataType::STRING,
            )])
            .unwrap(),
        ));
        let items = KernelDataType::Array(Box::new(ArrayType::new(item_struct, true)));
        StructType::try_new([
            column_mapping_test_field("id", "col-id", 1),
            column_mapping_test_field_with_type("name", "col-name", 2, KernelDataType::STRING),
            column_mapping_test_field_with_type("addr", "col-addr", 3, addr),
            column_mapping_test_field_with_type("items", "col-items", 5, items),
        ])
        .unwrap()
    }

    /// Logical Arrow batch. `name` deliberately uses `Utf8View` to prove the rewrite preserves the
    /// input Arrow representation rather than canonicalizing it to `Utf8`.
    fn logical_batch() -> RecordBatch {
        let addr = StructArray::new(
            Fields::from(vec![Field::new("street", DataType::Utf8, true)]),
            vec![Arc::new(StringArray::from(vec!["s1", "s2"])) as ArrayRef],
            None,
        );
        // items: [ [{sku:a},{sku:b}], [{sku:c}] ]
        let item_struct = StructArray::new(
            Fields::from(vec![Field::new("sku", DataType::Utf8, true)]),
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef],
            None,
        );
        // The list-element wrapper carries a stale field id on input; it must be stripped (the
        // wrapper is not a column-mapped table field).
        let items = ListArray::new(
            Arc::new(
                Field::new("item", item_struct.data_type().clone(), true)
                    .with_metadata(stale_field_id()),
            ),
            OffsetBuffer::from_lengths([2usize, 1]),
            Arc::new(item_struct),
            None,
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            // `name` carries a stale field id on input; the table-owned id (2) must win.
            Field::new("name", DataType::Utf8View, true).with_metadata(stale_field_id()),
            Field::new("addr", addr.data_type().clone(), true),
            Field::new("items", items.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringViewArray::from(vec!["n1", "n2"])),
                Arc::new(addr),
                Arc::new(items),
            ],
        )
        .unwrap()
    }

    fn field_id(field: &Field) -> Option<&str> {
        field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .map(String::as_str)
    }

    /// A stale, wrong field id as it might arrive on an input batch (e.g. read from another file).
    fn stale_field_id() -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "999".to_string())])
    }

    /// Exercises the full nested rewrite: top-level + nested struct + struct-inside-list renames,
    /// `field_id` assignment at every level, `Utf8View` preservation, and data integrity. Both
    /// modes are asserted identical (physical name + id resolution is mode-independent), which is
    /// the only thing the `id`-vs-`name` distinction changes on the write path.
    #[test]
    fn rewrites_nested_columns_to_physical_in_both_modes() {
        let kernel_schema = logical_kernel_schema();
        let batch = logical_batch();

        for mode in [ColumnMappingMode::Name, ColumnMappingMode::Id] {
            let physical_schema = Arc::new(Schema::new(physical_fields(
                batch.schema().fields(),
                &kernel_schema,
                mode,
            )));
            let out = apply_column_mapping(&batch, &physical_schema).unwrap();
            let fields = out.schema();

            // Top-level physical names + field ids.
            assert_eq!(fields.field(0).name(), "col-id");
            assert_eq!(field_id(fields.field(0)), Some("1"), "{mode:?}");
            assert_eq!(fields.field(1).name(), "col-name");
            assert_eq!(field_id(fields.field(1)), Some("2"), "{mode:?}");
            // Utf8View representation survives (not canonicalized to Utf8).
            assert_eq!(fields.field(1).data_type(), &DataType::Utf8View, "{mode:?}");

            // Nested struct field renamed + field id; child type preserved.
            assert_eq!(fields.field(2).name(), "col-addr");
            assert_eq!(field_id(fields.field(2)), Some("3"), "{mode:?}");
            let DataType::Struct(addr_fields) = fields.field(2).data_type() else {
                panic!("addr should be a struct");
            };
            assert_eq!(addr_fields[0].name(), "col-street");
            assert_eq!(field_id(addr_fields[0].as_ref()), Some("4"), "{mode:?}");

            // Struct nested INSIDE a list is renamed via the list/element path.
            assert_eq!(fields.field(3).name(), "col-items");
            assert_eq!(field_id(fields.field(3)), Some("5"), "{mode:?}");
            let DataType::List(item_field) = fields.field(3).data_type() else {
                panic!("items should be a list");
            };
            // The synthetic element wrapper must not carry a field id (the stale input id is
            // stripped and no table id applies to it).
            assert_eq!(field_id(item_field), None, "{mode:?}");
            let DataType::Struct(item_fields) = item_field.data_type() else {
                panic!("list element should be a struct");
            };
            assert_eq!(item_fields[0].name(), "col-sku");
            assert_eq!(field_id(item_fields[0].as_ref()), Some("6"), "{mode:?}");

            // Data is preserved through the buffer reinterpret.
            assert_eq!(out.column(0).as_primitive::<Int32Type>().values(), &[1, 2]);
            let names: Vec<&str> = out.column(1).as_string_view().iter().flatten().collect();
            assert_eq!(names, vec!["n1", "n2"]);
            let streets: Vec<&str> = out
                .column(2)
                .as_struct()
                .column(0)
                .as_string::<i32>()
                .iter()
                .flatten()
                .collect();
            assert_eq!(streets, vec!["s1", "s2"]);
            let skus: Vec<&str> = out
                .column(3)
                .as_list::<i32>()
                .values()
                .as_struct()
                .column(0)
                .as_string::<i32>()
                .iter()
                .flatten()
                .collect();
            assert_eq!(skus, vec!["a", "b", "c"]);
        }
    }

    /// Columns absent from the logical schema (e.g. the CDC `_change_type` marker) pass through
    /// unchanged.
    #[test]
    fn passes_through_non_table_columns() {
        let kernel_schema =
            StructType::try_new([column_mapping_test_field("id", "col-id", 1)]).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("_change_type", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["insert"])),
            ],
        )
        .unwrap();

        let physical_schema = Arc::new(Schema::new(physical_fields(
            batch.schema().fields(),
            &kernel_schema,
            ColumnMappingMode::Name,
        )));
        let out = apply_column_mapping(&batch, &physical_schema).unwrap();

        assert_eq!(out.schema().field(0).name(), "col-id");
        assert_eq!(out.schema().field(1).name(), "_change_type");
        assert_eq!(field_id(out.schema().field(1)), None);
    }

    #[test]
    fn physical_partition_columns_errors_on_unknown_column() {
        let logical_schema =
            Arc::new(StructType::try_new([column_mapping_test_field("p", "col-p", 1)]).unwrap());
        let state = ColumnMappingState {
            mode: ColumnMappingMode::Name,
            logical_schema,
        };
        // Known columns translate to their physical names.
        assert_eq!(
            state
                .physical_partition_columns(&["p".to_string()])
                .unwrap(),
            vec!["col-p".to_string()]
        );
        // An unknown partition column fails loudly instead of silently using the logical name.
        let err = state
            .physical_partition_columns(&["missing".to_string()])
            .expect_err("unknown partition column should error");
        match err {
            DeltaTableError::Generic(message) => assert_eq!(
                message,
                "partition column 'missing' not found in the table schema"
            ),
            other => panic!("expected a Generic partition error, got: {other:?}"),
        }
    }
}
