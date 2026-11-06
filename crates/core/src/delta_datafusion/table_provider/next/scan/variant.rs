//! Convert per-file Variant storage to the binary representation expected by Kernel.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, MapArray, RecordBatch,
    StructArray, make_array,
};
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{
    Result, internal_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};
use parquet::variant::{VariantArray, VariantType, unshred_variant};

/// File schemas carry the Parquet VARIANT extension marker. Use that marker,
/// rather than guessing from struct child names, to identify Variant storage.
#[derive(Debug)]
pub(super) struct VariantSchemaAdapter {
    adapter: Arc<dyn PhysicalExprAdapter>,
    columns: HashMap<usize, (FieldRef, FieldRef)>,
}

impl VariantSchemaAdapter {
    pub(super) fn create(
        logical: SchemaRef,
        physical: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        let mut columns = HashMap::new();
        let fields = physical
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let Ok(target) = logical.field_with_name(field.name()) else {
                    return Ok(field.clone());
                };
                let normalized = Arc::new(normalize_field(field, target)?);
                if normalized != *field {
                    columns.insert(i, (field.clone(), normalized.clone()));
                }
                Ok(normalized)
            })
            .collect::<Result<Vec<_>>>()?;
        if columns.is_empty() {
            return DefaultPhysicalExprAdapterFactory.create(logical, physical);
        }
        let normalized = Arc::new(Schema::new_with_metadata(
            fields,
            physical.metadata().clone(),
        ));
        Ok(Arc::new(Self {
            adapter: DefaultPhysicalExprAdapterFactory.create(logical, normalized)?,
            columns,
        }))
    }
}

impl PhysicalExprAdapter for VariantSchemaAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // First adapt indices and ordinary schema evolution against the normalized
        // schema. Then place normalization at the physical column boundary, before
        // any struct cast can project away typed_value.
        self.adapter
            .rewrite(expr)?
            .transform(|expr| {
                if let Some(column) = expr.downcast_ref::<Column>()
                    && let Some((source, target)) = self.columns.get(&column.index())
                {
                    return Ok(Transformed::yes(Arc::new(NormalizeVariantExpr {
                        input: expr,
                        source: source.clone(),
                        target: target.clone(),
                    })
                        as Arc<dyn PhysicalExpr>));
                }
                Ok(Transformed::no(expr))
            })
            .data()
    }
}

fn normalize_field(source: &Field, target: &Field) -> Result<Field> {
    if source.has_valid_extension_type::<VariantType>() {
        // Kernel's Variant storage type is canonical metadata/value. Never use
        // the table's two-field schema to read a shredded file directly.
        return Ok(source
            .clone()
            .with_data_type(target.data_type().clone())
            .with_metadata(target.metadata().clone()));
    }
    let data_type = match (source.data_type(), target.data_type()) {
        (DataType::Struct(fields), DataType::Struct(targets)) => DataType::Struct(
            fields
                .iter()
                .map(
                    |field| match targets.iter().find(|target| target.name() == field.name()) {
                        Some(target) => normalize_field(field, target).map(Arc::new),
                        None => Ok(field.clone()),
                    },
                )
                .collect::<Result<_>>()?,
        ),
        (DataType::List(field), DataType::List(target)) => {
            DataType::List(Arc::new(normalize_field(field, target)?))
        }
        (DataType::LargeList(field), DataType::LargeList(target)) => {
            DataType::LargeList(Arc::new(normalize_field(field, target)?))
        }
        (DataType::FixedSizeList(field, n), DataType::FixedSizeList(target, _)) => {
            DataType::FixedSizeList(Arc::new(normalize_field(field, target)?), *n)
        }
        (DataType::Map(field, sorted), DataType::Map(target, _)) => {
            DataType::Map(Arc::new(normalize_field(field, target)?), *sorted)
        }
        _ => source.data_type().clone(),
    };
    Ok(source.clone().with_data_type(data_type))
}

fn normalize_array(input: &ArrayRef, source: &Field, target: &Field) -> Result<ArrayRef> {
    if source == target {
        return Ok(input.clone());
    }
    if source.has_valid_extension_type::<VariantType>() {
        let variants = VariantArray::try_new(input.as_ref())?;
        let binary: ArrayRef = Arc::new(unshred_variant(&variants)?.into_inner());
        return Ok(cast(&binary, target.data_type())?);
    }
    Ok(match (source.data_type(), target.data_type()) {
        (DataType::Struct(sources), DataType::Struct(targets)) => {
            let array = input.as_struct();
            let columns = array
                .columns()
                .iter()
                .zip(sources)
                .zip(targets)
                .map(|((child, source), target)| {
                    // Values beneath a null parent are undefined. Mask them before
                    // decoding a nested Variant, rather than inspecting its payload.
                    let child = if source != target && array.null_count() > 0 {
                        make_array(
                            child
                                .to_data()
                                .into_builder()
                                .nulls(NullBuffer::union(child.nulls(), array.nulls()))
                                .build()?,
                        )
                    } else {
                        child.clone()
                    };
                    normalize_array(&child, source, target)
                })
                .collect::<Result<_>>()?;
            Arc::new(StructArray::try_new(
                targets.clone(),
                columns,
                array.nulls().cloned(),
            )?)
        }
        (DataType::List(source), DataType::List(target)) => {
            let array = input.as_list::<i32>();
            Arc::new(ListArray::try_new(
                target.clone(),
                array.offsets().clone(),
                normalize_array(array.values(), source, target)?,
                array.nulls().cloned(),
            )?)
        }
        (DataType::LargeList(source), DataType::LargeList(target)) => {
            let array = input.as_list::<i64>();
            Arc::new(LargeListArray::try_new(
                target.clone(),
                array.offsets().clone(),
                normalize_array(array.values(), source, target)?,
                array.nulls().cloned(),
            )?)
        }
        (DataType::FixedSizeList(source, n), DataType::FixedSizeList(target, _)) => {
            let array = input.as_fixed_size_list();
            Arc::new(FixedSizeListArray::try_new(
                target.clone(),
                *n,
                normalize_array(array.values(), source, target)?,
                array.nulls().cloned(),
            )?)
        }
        (DataType::Map(source, _), DataType::Map(target, sorted)) => {
            let array = input.as_map();
            let entries: ArrayRef = Arc::new(array.entries().clone());
            let entries = normalize_array(&entries, source, target)?;
            Arc::new(MapArray::try_new(
                target.clone(),
                array.offsets().clone(),
                entries.as_struct().clone(),
                array.nulls().cloned(),
                *sorted,
            )?)
        }
        _ => {
            return internal_err!(
                "unexpected Variant normalization from {} to {}",
                source.data_type(),
                target.data_type()
            );
        }
    })
}

#[derive(Debug, Eq)]
struct NormalizeVariantExpr {
    input: Arc<dyn PhysicalExpr>,
    source: FieldRef,
    target: FieldRef,
}

impl PartialEq for NormalizeVariantExpr {
    fn eq(&self, other: &Self) -> bool {
        self.input.as_ref() == other.input.as_ref()
            && self.source == other.source
            && self.target == other.target
    }
}
impl Hash for NormalizeVariantExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.as_ref().hash(state);
        self.source.hash(state);
        self.target.hash(state);
    }
}
impl fmt::Display for NormalizeVariantExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unshred_variant({})", self.input)
    }
}
impl PhysicalExpr for NormalizeVariantExpr {
    fn data_type(&self, _schema: &Schema) -> Result<DataType> {
        Ok(self.target.data_type().clone())
    }
    fn nullable(&self, _schema: &Schema) -> Result<bool> {
        Ok(self.target.is_nullable())
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(ColumnarValue::Array(normalize_array(
            &input,
            &self.source,
            &self.target,
        )?))
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [input]: [Arc<dyn PhysicalExpr>; 1] = children.try_into().map_err(|_| {
            datafusion::common::internal_datafusion_err!("Variant normalization expects one child")
        })?;
        Ok(Arc::new(Self {
            input,
            source: self.source.clone(),
            target: self.target.clone(),
        }))
    }
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, Int32Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::variant::VariantArrayBuilder;

    #[test]
    fn ordinary_struct_with_variant_field_names_is_not_decoded() -> Result<()> {
        let fields = vec![
            Arc::new(Field::new("metadata", DataType::Binary, false)),
            Arc::new(Field::new("value", DataType::Binary, false)),
            Arc::new(Field::new("typed_value", DataType::Int32, true)),
        ];
        let array = StructArray::try_new(
            fields.clone().into(),
            vec![
                Arc::new(BinaryArray::from_vec(vec![b"not variant metadata"])),
                Arc::new(BinaryArray::from_vec(vec![b"keep"])),
                Arc::new(Int32Array::from(vec![1])),
            ],
            None,
        )?;
        let physical = Arc::new(Schema::new(vec![Field::new(
            "body",
            array.data_type().clone(),
            true,
        )]));
        let logical = Arc::new(Schema::new(vec![Field::new(
            "body",
            DataType::Struct(fields[..2].to_vec().into()),
            true,
        )]));
        let batch = RecordBatch::try_new(physical.clone(), vec![Arc::new(array)])?;
        let expr = VariantSchemaAdapter::create(logical, physical)?
            .rewrite(Arc::new(Column::new("body", 0)))?;
        let result = expr.evaluate(&batch)?.into_array(1)?;
        assert_eq!(
            result.as_struct().column(1).as_binary::<i32>().value(0),
            b"keep"
        );
        Ok(())
    }

    #[test]
    fn null_parent_hides_undefined_variant_payload() -> Result<()> {
        let batch = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
            "../test/tests/data/variant-shredded/part-00000.parquet",
        )?)?
        .build()?
        .next()
        .unwrap()?;
        let body = batch.column_by_name("body").unwrap().as_struct();
        let mut children = body.columns().to_vec();
        let index = body
            .fields()
            .iter()
            .position(|field| field.name() == "metadata")
            .unwrap();
        children[index] = cast(
            &BinaryArray::from_vec(vec![b"" as &[u8]]),
            body.column(index).data_type(),
        )?;
        let invalid = StructArray::try_new(body.fields().clone(), children, None)?;
        let source_child = batch
            .schema()
            .field_with_name("body")?
            .clone()
            .with_name("v");
        let source = Field::new(
            "parent",
            DataType::Struct(vec![Arc::new(source_child)].into()),
            true,
        );
        let binary = VariantArrayBuilder::new(0).build();
        let target_child = Field::new("v", binary.data_type().clone(), true);
        let target = Field::new(
            "parent",
            DataType::Struct(vec![Arc::new(target_child)].into()),
            true,
        );
        let input: ArrayRef = Arc::new(StructArray::try_new(
            match source.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            vec![Arc::new(invalid)],
            Some(NullBuffer::from(vec![false])),
        )?);
        let target = normalize_field(&source, &target)?;
        let result = normalize_array(&input, &source, &target)?;
        assert_eq!(result.null_count(), 1);
        assert_eq!(result.as_struct().column(0).null_count(), 1);
        Ok(())
    }
}
