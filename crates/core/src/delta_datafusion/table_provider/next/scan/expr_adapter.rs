//! Read-path schema-evolution adapter.
//!
//! This adapter restores the old behavior by routing every column whose physical type differs
//! from its logical type through delta-rs's own recursive cast, instead of DataFusion's.
//!
//! Upstream DataFusion fix: <https://github.com/apache/datafusion/pull/23914> (merged to `main`
//! 2026-08-21, unreleased at the time of writing), tracking
//! <https://github.com/apache/datafusion/issues/20835>. Once this crate builds against a
//! DataFusion release containing that fix, this module and its wiring in `get_read_plan` can be
//! deleted in favour of `DefaultPhysicalExprAdapterFactory`.

use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{FieldRef, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{Result as DFResult, ScalarValue, exec_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, lit};
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};

use crate::kernel::schema::cast::cast_array_to_field;

#[derive(Debug)]
pub(crate) struct DeltaExprAdapterFactory;

impl PhysicalExprAdapterFactory for DeltaExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> DFResult<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(DeltaExprAdapter {
            logical_file_schema,
            physical_file_schema,
        }))
    }
}

#[derive(Debug)]
struct DeltaExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for DeltaExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> DFResult<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            let Some(col) = e.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::no(e));
            };

            let Ok(logical_field) = self.logical_file_schema.field_with_name(col.name()) else {
                return Ok(Transformed::no(e));
            };

            let physical_idx = match self.physical_file_schema.index_of(col.name()) {
                Ok(idx) => idx,
                Err(_) if !logical_field.is_nullable() => {
                    return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        col.name()
                    );
                }
                // missing entirely: read it as nulls.
                Err(_) => {
                    let null = ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(Transformed::yes(lit(null)));
                }
            };

            if logical_field.data_type() == self.physical_file_schema.field(physical_idx).data_type()
            {
                if col.index() == physical_idx {
                    return Ok(Transformed::no(e));
                }
                return Ok(Transformed::yes(Arc::new(Column::new(
                    col.name(),
                    physical_idx,
                ))));
            }

            // Types differ. Cast with delta-rs's own recursive, Map-aware cast rather than
            // DataFusion's.
            let resolved = if col.index() == physical_idx {
                col.clone()
            } else {
                Column::new(col.name(), physical_idx)
            };
            Ok(Transformed::yes(Arc::new(DeltaCastColumn {
                expr: Arc::new(resolved),
                target_field: Arc::new(logical_field.clone()),
            })))
        })
        .data()
    }
}

/// Evaluates `expr` and casts the result to `target_field`, null-filling fields the file
/// predates.
#[derive(Debug, Clone, Eq)]
struct DeltaCastColumn {
    expr: Arc<dyn PhysicalExpr>,
    target_field: FieldRef,
}

impl PartialEq for DeltaCastColumn {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.target_field == other.target_field
    }
}

impl Hash for DeltaCastColumn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.target_field.hash(state);
    }
}

impl Display for DeltaCastColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "delta_cast({}, {})", self.expr, self.target_field.name())
    }
}

impl PhysicalExpr for DeltaCastColumn {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let array = self.expr.evaluate(batch)?.into_array(batch.num_rows())?;
        let cast = cast_array_to_field(&array, &self.target_field, false, true)?;
        Ok(ColumnarValue::Array(cast))
    }

    fn return_field(&self, _input_schema: &Schema) -> DFResult<FieldRef> {
        Ok(Arc::clone(&self.target_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return exec_err!("DeltaCastColumn expects exactly one child");
        }
        Ok(Arc::new(Self {
            expr: children.remove(0),
            target_field: Arc::clone(&self.target_field),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}
