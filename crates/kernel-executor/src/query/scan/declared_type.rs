//! Restores a column's declared type above a nullability-relaxed parquet read.
//!
//! DataFusion's `CastExpr` refuses to cast a nullable nested field to a non-nullable one, when
//! the plan is built and again when it runs. Arrow's own cast allows it and validates the data
//! instead: `StructArray::try_new` and the list and map constructors reject NULLs under a
//! non-nullable field. [`DeclaredTypeCast`] is that arrow cast as a physical expression.
//!
//! The parquet reader leaves a NULL struct's non-nullable children non-null, with placeholder
//! values. The cast first unions each struct's NULLs into its children, as the kernel's default
//! engine does after every parquet read, so `add.path` is NULL where `add` is.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray as _, RecordBatch};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::common::Result as DFResult;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use delta_kernel::engine::arrow_data::fix_nested_null_masks;

/// Casts `input` to `field`'s type with arrow's cast, so a NULL under a non-nullable nested
/// field is an execution error rather than a refused plan. A struct's NULLs are unioned into its
/// children first.
#[derive(Debug, Eq)]
pub(super) struct DeclaredTypeCast {
    input: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl DeclaredTypeCast {
    pub(super) fn new(input: Arc<dyn PhysicalExpr>, field: FieldRef) -> Self {
        Self { input, field }
    }
}

impl PartialEq for DeclaredTypeCast {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input) && self.field.eq(&other.field)
    }
}

impl Hash for DeclaredTypeCast {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.field.hash(state);
    }
}

impl fmt::Display for DeclaredTypeCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST({} AS {})", self.input, self.field.data_type())
    }
}

impl PhysicalExpr for DeclaredTypeCast {
    fn return_field(&self, _input_schema: &Schema) -> DFResult<FieldRef> {
        Ok(Arc::clone(&self.field))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let array = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        let array = match array.as_struct_opt() {
            Some(array) => Arc::new(fix_nested_null_masks(array.clone())) as ArrayRef,
            None => array,
        };
        let options = CastOptions {
            safe: false,
            ..Default::default()
        };
        let array = cast_with_options(&array, self.field.data_type(), &options)?;
        Ok(ColumnarValue::Array(array))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|children| {
            datafusion::common::internal_datafusion_err!(
                "DeclaredTypeCast expects one child, got {}",
                children.len()
            )
        })?;
        Ok(Arc::new(Self::new(input, Arc::clone(&self.field))))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CAST(")?;
        self.input.fmt_sql(f)?;
        write!(f, " AS {})", self.field.data_type())
    }
}
