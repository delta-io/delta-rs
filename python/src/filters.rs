//! Compile DNF tuple filters into DataFusion row-filter expressions.
//!
//! This is the row-level counterpart of the kernel file-pruning predicates
//! built in `kernel_dnf_predicate`: the same `(column, op, value)` tuples, but
//! compiled to a DataFusion [`Expr`] evaluated per row by the engine.

use std::str::FromStr;

use delta_kernel::expressions::Scalar;
use delta_kernel::schema::{PrimitiveType, StructType};
use deltalake::datafusion::logical_expr::{Expr, expr_fn::ident, expr_fn::in_list, lit};
use deltalake::delta_datafusion::engine::to_datafusion_scalar;
use deltalake::errors::DeltaTableError;
use deltalake::partitions::FilterOp;
use pyo3::FromPyObject;
use pyo3::pybacked::PyBackedStr;

/// A filter value crossing the FFI with nulls preserved: unlike the partition
/// string encoding, `None` stays a null and `""` stays an empty string.
#[derive(FromPyObject)]
pub(crate) enum PyRowFilterValue {
    Single(Option<PyBackedStr>),
    Multiple(Vec<Option<PyBackedStr>>),
}

pub(crate) type PyRowFilterConjunction = Vec<(PyBackedStr, PyBackedStr, PyRowFilterValue)>;

/// Compile filters in disjunctive normal form -- an OR across conjunctions
/// (AND groups) of `(column, op, value)` literals -- into a DataFusion [`Expr`].
///
/// `=` / `!=` against a null value become IS [NOT] NULL, matching the
/// tuple-filter tradition of the file listing APIs (in SQL, `= NULL` matches
/// nothing). Everything else follows SQL semantics, including three-valued
/// NULL logic for `not in`.
pub(crate) fn dnf_to_datafusion_expr(
    dnf: &[PyRowFilterConjunction],
    table_schema: &StructType,
) -> Result<Expr, DeltaTableError> {
    if dnf.is_empty() {
        return Err(DeltaTableError::Generic(
            "empty filter; pass no filter to match all rows".to_string(),
        ));
    }
    let groups = dnf
        .iter()
        .map(|conjunction| conjunction_to_expr(conjunction, table_schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(groups.into_iter().reduce(|a, b| a.or(b)).unwrap())
}

fn conjunction_to_expr(
    literals: &PyRowFilterConjunction,
    table_schema: &StructType,
) -> Result<Expr, DeltaTableError> {
    if literals.is_empty() {
        return Err(DeltaTableError::Generic(
            "empty conjunction in filter; pass no filter to match all rows".to_string(),
        ));
    }
    let exprs = literals
        .iter()
        .map(|literal| literal_to_expr(literal, table_schema))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(exprs.into_iter().reduce(|a, b| a.and(b)).unwrap())
}

fn literal_to_expr(
    (column, op, value): &(PyBackedStr, PyBackedStr, PyRowFilterValue),
    table_schema: &StructType,
) -> Result<Expr, DeltaTableError> {
    let column: &str = column.as_ref();
    let Some(field) = table_schema.field(column) else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: format!("Field '{column}' is not a root table field."),
        });
    };
    let Some(dt) = field.data_type().as_primitive_opt() else {
        return Err(DeltaTableError::SchemaMismatch {
            msg: format!("Field '{}' is not a primitive type", field.name()),
        });
    };

    // pyarrow's tuple filters accept `==` as an alias for `=`
    let op_str: &str = op.as_ref();
    let op = FilterOp::from_str(if op_str == "==" { "=" } else { op_str })?;

    let col = ident(field.name());
    Ok(match (op, value) {
        (FilterOp::Eq, PyRowFilterValue::Single(raw)) => match parse_row_scalar(dt, raw)? {
            scalar if scalar.is_null() => col.is_null(),
            scalar => col.eq(scalar_lit(&scalar)?),
        },
        (FilterOp::Ne, PyRowFilterValue::Single(raw)) => match parse_row_scalar(dt, raw)? {
            scalar if scalar.is_null() => col.is_not_null(),
            scalar => col.not_eq(scalar_lit(&scalar)?),
        },
        (FilterOp::Lt, PyRowFilterValue::Single(raw)) => {
            col.lt(scalar_lit(&parse_row_scalar(dt, raw)?)?)
        }
        (FilterOp::Le, PyRowFilterValue::Single(raw)) => {
            col.lt_eq(scalar_lit(&parse_row_scalar(dt, raw)?)?)
        }
        (FilterOp::Gt, PyRowFilterValue::Single(raw)) => {
            col.gt(scalar_lit(&parse_row_scalar(dt, raw)?)?)
        }
        (FilterOp::Ge, PyRowFilterValue::Single(raw)) => {
            col.gt_eq(scalar_lit(&parse_row_scalar(dt, raw)?)?)
        }
        (op @ (FilterOp::In | FilterOp::NotIn), PyRowFilterValue::Multiple(raws)) => {
            let values = raws
                .iter()
                .map(|raw| scalar_lit(&parse_row_scalar(dt, raw)?))
                .collect::<Result<Vec<_>, _>>()?;
            in_list(col, values, matches!(op, FilterOp::NotIn))
        }
        (op, _) => {
            return Err(DeltaTableError::Generic(format!(
                "Invalid filter on column {column:?}: operator {op} takes {}",
                match op {
                    FilterOp::In | FilterOp::NotIn => "a list of values",
                    _ => "a single value",
                }
            )));
        }
    })
}

/// Parse a transported filter value into a typed kernel [`Scalar`].
///
/// `parse_scalar` follows the partition value serialization rules where an
/// empty string means null; for row filters an empty string is data, so it is
/// only accepted for string columns and kept as an empty string.
fn parse_row_scalar(
    dt: &PrimitiveType,
    raw: &Option<PyBackedStr>,
) -> Result<Scalar, DeltaTableError> {
    match raw {
        None => Ok(Scalar::Null(dt.clone().into())),
        Some(raw) if raw.is_empty() => match dt {
            PrimitiveType::String => Ok(Scalar::String(String::new())),
            other => Err(DeltaTableError::Generic(format!(
                "cannot parse empty string filter value as {other}"
            ))),
        },
        Some(raw) => Ok(dt.parse_scalar(raw.as_ref())?),
    }
}

fn scalar_lit(scalar: &Scalar) -> Result<Expr, DeltaTableError> {
    Ok(lit(to_datafusion_scalar(scalar)?))
}
