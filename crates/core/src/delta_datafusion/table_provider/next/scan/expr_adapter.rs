//! Delta-aware physical expression adaptation for parquet scans.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};

/// A [`PhysicalExprAdapterFactory`] that relaxes nested-field nullability on the
/// logical (table) schema before delegating to DataFusion's default adapter.
///
/// In Delta, `nullable = false` is a write-time invariant: engines such as Spark
/// write parquet files whose nested fields are physically optional even when the
/// table schema declares them non-nullable. DataFusion's default adapter rejects
/// nullable -> non-nullable nested struct field casts at plan time, so we hand it
/// a nullability-relaxed target schema instead. The data is guaranteed non-null
/// by the writer, and the logical output schema is restored above the parquet
/// scan by [`DeltaScanExec`](super::DeltaScanExec)'s transforms.
#[derive(Debug)]
pub(crate) struct DeltaPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DeltaPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        DefaultPhysicalExprAdapterFactory.create(
            Arc::new(relax_schema_nested_nullability(&logical_file_schema)),
            physical_file_schema,
        )
    }
}

/// Relax nullability of all *nested* fields. Top-level field nullability is kept
/// as is: the default adapter already tolerates nullable file columns feeding
/// non-nullable top-level table columns, and keeping it strict preserves the
/// "non-nullable column missing from file" error for missing top-level columns.
pub(super) fn relax_schema_nested_nullability(schema: &Schema) -> Schema {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| relax_field_type(f.as_ref()))
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Relax nullability within the field's data type, keeping the field's own
/// nullability unchanged.
fn relax_field_type(field: &Field) -> FieldRef {
    Arc::new(
        field
            .clone()
            .with_data_type(relax_nested_nullability(field.data_type())),
    )
}

/// Relax nullability within the field's data type and mark the field itself as
/// nullable.
fn relax_field(field: &FieldRef) -> FieldRef {
    Arc::new(relax_field_type(field).as_ref().clone().with_nullable(true))
}

fn relax_nested_nullability(dt: &DataType) -> DataType {
    match dt {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(relax_field).collect()),
        DataType::List(f) => DataType::List(relax_field(f)),
        DataType::LargeList(f) => DataType::LargeList(relax_field(f)),
        DataType::ListView(f) => DataType::ListView(relax_field(f)),
        DataType::LargeListView(f) => DataType::LargeListView(relax_field(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(relax_field(f), *n),
        DataType::Map(entries, sorted) => {
            // Arrow requires map entries and keys to be non-nullable; only the
            // value field may be relaxed.
            let entries_type = match entries.data_type() {
                DataType::Struct(kv) if kv.len() == 2 => DataType::Struct(Fields::from(vec![
                    relax_field_type(kv[0].as_ref()),
                    relax_field(&kv[1]),
                ])),
                other => relax_nested_nullability(other),
            };
            DataType::Map(
                Arc::new(entries.as_ref().clone().with_data_type(entries_type)),
                *sorted,
            )
        }
        DataType::Dictionary(k, v) => {
            DataType::Dictionary(k.clone(), Box::new(relax_nested_nullability(v)))
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relax_keeps_top_level_nullability_and_relaxes_nested_fields() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "meta",
                DataType::Struct(Fields::from(vec![Field::new(
                    "int_id",
                    DataType::Utf8,
                    false,
                )])),
                true,
            ),
        ]);

        let relaxed = relax_schema_nested_nullability(&schema);

        // Top-level nullability is preserved.
        assert!(!relaxed.field(0).is_nullable());
        assert!(relaxed.field(1).is_nullable());

        // Nested struct field is relaxed to nullable.
        let DataType::Struct(fields) = relaxed.field(1).data_type() else {
            panic!("expected struct");
        };
        assert!(fields[0].is_nullable());
    }

    #[test]
    fn relax_map_keeps_entries_and_key_non_nullable() {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new(
                    "values",
                    DataType::Struct(Fields::from(vec![Field::new("v", DataType::Int32, false)])),
                    false,
                ),
            ])),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "m",
            DataType::Map(Arc::new(entries), false),
            true,
        )]);

        let relaxed = relax_schema_nested_nullability(&schema);
        let DataType::Map(entries, _) = relaxed.field(0).data_type() else {
            panic!("expected map");
        };
        assert!(!entries.is_nullable());
        let DataType::Struct(kv) = entries.data_type() else {
            panic!("expected struct entries");
        };
        assert!(!kv[0].is_nullable(), "map keys must stay non-nullable");
        assert!(kv[1].is_nullable(), "map values should be relaxed");
        let DataType::Struct(value_fields) = kv[1].data_type() else {
            panic!("expected struct values");
        };
        assert!(value_fields[0].is_nullable());
    }

    #[test]
    fn relax_list_variants_relax_element_nullability() {
        let elem = Arc::new(Field::new("item", DataType::Int32, false));
        let schema = Schema::new(vec![
            Field::new("large", DataType::LargeList(elem.clone()), true),
            Field::new("view", DataType::ListView(elem.clone()), true),
            Field::new("large_view", DataType::LargeListView(elem.clone()), true),
            Field::new("fixed", DataType::FixedSizeList(elem.clone(), 2), true),
        ]);

        let relaxed = relax_schema_nested_nullability(&schema);

        for field in relaxed.fields() {
            let inner = match field.data_type() {
                DataType::LargeList(f) | DataType::ListView(f) | DataType::LargeListView(f) => f,
                DataType::FixedSizeList(f, n) => {
                    assert_eq!(*n, 2, "list size must be preserved");
                    f
                }
                other => panic!("unexpected data type: {other:?}"),
            };
            assert!(
                inner.is_nullable(),
                "element of {} should be relaxed",
                field.name()
            );
        }
    }

    #[test]
    fn relax_map_with_non_standard_entries_falls_back_to_generic_relaxation() {
        // Entries struct with three fields does not match the key/value shape,
        // so relaxation falls back to the generic nested handling.
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ])),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "m",
            DataType::Map(Arc::new(entries), false),
            true,
        )]);

        let relaxed = relax_schema_nested_nullability(&schema);
        let DataType::Map(entries, _) = relaxed.field(0).data_type() else {
            panic!("expected map");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected struct entries");
        };
        assert!(fields.iter().all(|f| f.is_nullable()));
    }
}
