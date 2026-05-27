use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use chrono::TimeZone;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use object_store::ObjectMeta;
use object_store::path::Path;
use serde_json::Value;
use tracing::log;

use crate::DeltaResult;
use crate::delta_datafusion::cdf::CHANGE_TYPE_COL;
use crate::delta_datafusion::cdf::{CdcDataSpec, FileAction};
use crate::delta_datafusion::{get_null_of_arrow_type, to_correct_scalar_value};

pub fn map_action_to_scalar<F: FileAction>(
    action: &F,
    part: &str,
    schema: SchemaRef,
) -> DeltaResult<ScalarValue> {
    Ok(action
        .partition_values()?
        .get(part)
        .map(|val| {
            schema
                .field_with_name(part)
                .map(|field| match val {
                    Some(value) => to_correct_scalar_value(
                        &Value::String(value.to_string()),
                        field.data_type(),
                    )
                    .unwrap_or(Some(ScalarValue::Null))
                    .unwrap_or(ScalarValue::Null),
                    None => get_null_of_arrow_type(field.data_type()).unwrap_or(ScalarValue::Null),
                })
                .unwrap_or(ScalarValue::Null)
        })
        .unwrap_or(ScalarValue::Null))
}

/// Coerce a single partition value for the *pruning* decision only.
///
/// Unlike [`map_action_to_scalar`], this never collapses a coercion failure into
/// [`ScalarValue::Null`], because in the pruning path that would let an
/// unexpected/invalid partition encoding be treated as `NULL` and wrongly pruned
/// (silently dropping live data). The return values are:
///
/// * `Ok(Some(scalar))` -- the raw value was coerced successfully, including the
///   case of a genuine partition `NULL`.
/// * `Ok(None)` -- the value could not be coerced (missing key, unrepresentable
///   value, or unknown/unsupported encoding). The caller MUST keep the file.
fn map_action_to_scalar_for_pruning<F: FileAction>(
    action: &F,
    part: &str,
    schema: &SchemaRef,
) -> DeltaResult<Option<ScalarValue>> {
    let Some(val) = action.partition_values()?.get(part) else {
        // The partition column is not present in this action's partition values;
        // we cannot make a pruning decision, so keep the file.
        return Ok(None);
    };
    let Ok(field) = schema.field_with_name(part) else {
        // No matching field in the table schema -- treat as un-coercible.
        return Ok(None);
    };
    match val {
        // A genuine partition NULL. If the type itself is unsupported, fall back
        // to "could not coerce" so the file is kept.
        None => Ok(get_null_of_arrow_type(field.data_type()).ok()),
        // A non-null raw value. `to_correct_scalar_value` returns `Ok(None)` when
        // the value cannot be represented (e.g. array/object) and `Err` on parse
        // failures; both must be treated as "could not coerce".
        Some(value) => Ok(
            to_correct_scalar_value(&Value::String(value.to_string()), field.data_type())
                .ok()
                .flatten(),
        ),
    }
}

pub fn create_spec_partition_values<F: FileAction>(
    spec: &CdcDataSpec<F>,
    action_type: Option<&ScalarValue>,
) -> Vec<ScalarValue> {
    let mut spec_partition_values = action_type.cloned().map(|at| vec![at]).unwrap_or_default();
    spec_partition_values.push(ScalarValue::UInt64(Some(spec.version)));
    spec_partition_values.push(ScalarValue::TimestampMillisecond(
        Some(spec.timestamp),
        None,
    ));
    spec_partition_values
}

pub fn create_partition_values<F: FileAction>(
    schema: SchemaRef,
    specs: Vec<CdcDataSpec<F>>,
    table_partition_cols: &[String],
    action_type: Option<ScalarValue>,
) -> DeltaResult<HashMap<Vec<ScalarValue>, Vec<PartitionedFile>>> {
    let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

    for spec in specs {
        let spec_partition_values = create_spec_partition_values(&spec, action_type.as_ref());

        for action in spec.actions {
            let partition_values = table_partition_cols
                .iter()
                .map(|part| map_action_to_scalar(&action, part, schema.clone()))
                .collect::<DeltaResult<Vec<ScalarValue>>>()?;

            let mut new_part_values = spec_partition_values.clone();
            new_part_values.extend(partition_values);

            let part = PartitionedFile {
                object_meta: ObjectMeta {
                    location: Path::parse(action.path().as_str())?,
                    size: action.size()? as u64,
                    e_tag: None,
                    last_modified: chrono::Utc.timestamp_nanos(0),
                    version: None,
                },
                partition_values: new_part_values.clone(),
                range: None,
                statistics: None,
                ordering: None,
                extensions: None,
                metadata_size_hint: None,
            };

            file_groups.entry(new_part_values).or_default().push(part);
        }
    }
    Ok(file_groups)
}

pub fn create_cdc_schema(mut schema_fields: Vec<Arc<Field>>, include_type: bool) -> SchemaRef {
    if include_type {
        schema_fields.push(Field::new(CHANGE_TYPE_COL, DataType::Utf8, true).into());
    }
    Arc::new(Schema::new(schema_fields))
}

/// Everything needed to evaluate a partition-only predicate against the
/// `partitionValues` of a single [`FileAction`], so non-matching files can be
/// dropped before they ever become parquet file groups.
pub struct PartitionPruningPredicate {
    /// Physical expression over [`Self::partition_schema`], evaluating to a boolean.
    pub predicate: Arc<dyn PhysicalExpr>,
    /// Schema describing exactly the partition columns referenced by the predicate.
    pub partition_schema: SchemaRef,
    /// Full table input schema, used to coerce raw partition strings to scalars.
    pub table_schema: SchemaRef,
}

impl PartitionPruningPredicate {
    /// Decide whether a file must be kept based on its partition values.
    ///
    /// Returns `Ok(true)` when the file should be retained and `Ok(false)` only
    /// when the predicate conclusively does not match. Pruning is conservative:
    /// partition values are constant per file, so the predicate either fully
    /// matches or it does not, but a file is also kept whenever the decision is
    /// uncertain. Specifically, if any referenced partition value cannot be
    /// coerced to a scalar (an unexpected/invalid encoding, a missing key, or an
    /// unsupported type), the file is kept rather than risk wrongly pruning live
    /// data by treating the value as `NULL`.
    fn should_keep<F: FileAction>(&self, action: &F) -> DeltaResult<bool> {
        let mut columns = Vec::with_capacity(self.partition_schema.fields().len());
        for field in self.partition_schema.fields() {
            match map_action_to_scalar_for_pruning(
                action,
                field.name(),
                &self.table_schema,
            )? {
                Some(scalar) => columns.push(scalar.to_array_of_size(1)?),
                // Could not coerce this partition value: be conservative and keep
                // the file instead of evaluating the predicate against a NULL.
                None => return Ok(true),
            }
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.partition_schema), columns)?;
        let evaluated = self.predicate.evaluate(&batch)?;
        let matches = match evaluated {
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        crate::DeltaTableError::generic(
                            "partition pruning predicate did not evaluate to a boolean",
                        )
                    })?;
                array.len() == 1 && array.is_valid(0) && array.value(0)
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) => value,
            ColumnarValue::Scalar(_) => false,
        };
        Ok(matches)
    }
}

/// Drop any action whose `partitionValues` do not satisfy the partition-only
/// `predicate`, leaving the remaining specs (and their order) untouched.
///
/// Pruning is purely an optimization, so this **fails open**: if an action does
/// not expose its partition values (e.g. a `Remove` without extended file
/// metadata) or per-file predicate evaluation errors for any reason, the file is
/// kept and the error is logged at debug. A pruning failure must never drop a
/// file that would otherwise be read.
pub fn prune_specs_by_partition<F: FileAction>(
    specs: Vec<CdcDataSpec<F>>,
    predicate: &PartitionPruningPredicate,
) -> DeltaResult<Vec<CdcDataSpec<F>>> {
    let mut pruned = Vec::with_capacity(specs.len());
    for spec in specs {
        let (version, timestamp, actions) = spec.into_parts();
        let mut kept = Vec::with_capacity(actions.len());
        for action in actions {
            // Unknown partition values (e.g. Remove without extended metadata)
            // cannot be evaluated, so keep the file to stay correct.
            if action.partition_values().is_err() {
                kept.push(action);
                continue;
            }
            match predicate.should_keep(&action) {
                Ok(true) => kept.push(action),
                Ok(false) => {}
                Err(e) => {
                    // Fail open: evaluating the pruning predicate failed, so keep
                    // the file rather than risk dropping data that should be read.
                    log::debug!(
                        "load_cdf: keeping file '{}', partition pruning evaluation failed: {e}",
                        action.path()
                    );
                    kept.push(action);
                }
            }
        }
        if !kept.is_empty() {
            pruned.push(CdcDataSpec::new(version, timestamp, kept));
        }
    }
    Ok(pruned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::Add;
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;

    /// Build an `Add` with a single `id` partition value (or no `id` key when
    /// `value` is `None` via the outer `Option`).
    fn add_with_id_partition(path: &str, value: Option<Option<&str>>) -> Add {
        let mut partition_values = HashMap::new();
        if let Some(v) = value {
            partition_values.insert("id".to_string(), v.map(|s| s.to_string()));
        }
        Add {
            path: path.to_string(),
            partition_values,
            size: 1,
            modification_time: 0,
            data_change: true,
            ..Default::default()
        }
    }

    /// Compile `id = 5` over an `Int32` `id` partition column into a
    /// `PartitionPruningPredicate`.
    fn id_eq_5_predicate() -> PartitionPruningPredicate {
        let table_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let partition_schema = Arc::clone(&table_schema);
        let df_schema = partition_schema.as_ref().clone().try_into().unwrap();
        let ctx = SessionContext::new();
        let predicate = ctx
            .state()
            .create_physical_expr(col("id").eq(lit(5_i32)), &df_schema)
            .unwrap();
        PartitionPruningPredicate {
            predicate,
            partition_schema,
            table_schema,
        }
    }

    fn kept_paths(specs: Vec<CdcDataSpec<Add>>) -> Vec<String> {
        let mut paths: Vec<String> = specs
            .into_iter()
            .flat_map(|s| s.into_parts().2)
            .map(|a| a.path)
            .collect();
        paths.sort();
        paths
    }

    /// Fix: a partition value that cannot be coerced to the column type must NOT be
    /// pruned. Collapsing the failure to `ScalarValue::Null` would let `id = 5`
    /// evaluate to "not matching" and silently drop live data.
    #[test]
    fn pruning_keeps_file_when_partition_value_cannot_be_coerced() {
        let predicate = id_eq_5_predicate();
        let specs = vec![CdcDataSpec::new(
            0,
            0,
            vec![
                add_with_id_partition("match.parquet", Some(Some("5"))),
                add_with_id_partition("no_match.parquet", Some(Some("7"))),
                // Not coercible to Int32 -> must be kept (fail conservative).
                add_with_id_partition("garbage.parquet", Some(Some("not_a_number"))),
            ],
        )];

        let kept = kept_paths(prune_specs_by_partition(specs, &predicate).unwrap());
        assert_eq!(
            kept,
            vec!["garbage.parquet".to_string(), "match.parquet".to_string()],
            "un-coercible partition value must be kept, matching value must be kept, \
             non-matching value must be pruned"
        );
    }

    /// A genuine partition NULL is distinct from a coercion failure: `id = 5` does
    /// not match NULL, so the file is correctly pruned.
    #[test]
    fn pruning_drops_file_with_genuine_null_partition_value() {
        let predicate = id_eq_5_predicate();
        let specs = vec![CdcDataSpec::new(
            0,
            0,
            vec![
                add_with_id_partition("null.parquet", Some(None)),
                add_with_id_partition("match.parquet", Some(Some("5"))),
            ],
        )];

        let kept = kept_paths(prune_specs_by_partition(specs, &predicate).unwrap());
        assert_eq!(
            kept,
            vec!["match.parquet".to_string()],
            "a real NULL partition value does not satisfy id = 5 and must be pruned"
        );
    }

    /// If a partition column referenced by the predicate is absent from an action's
    /// partition values, the pruning decision is uncertain and the file is kept.
    #[test]
    fn pruning_keeps_file_when_partition_key_missing() {
        let predicate = id_eq_5_predicate();
        let specs = vec![CdcDataSpec::new(
            0,
            0,
            vec![add_with_id_partition("no_key.parquet", None)],
        )];

        let kept = kept_paths(prune_specs_by_partition(specs, &predicate).unwrap());
        assert_eq!(
            kept,
            vec!["no_key.parquet".to_string()],
            "missing partition key must keep the file rather than prune on a NULL"
        );
    }

    /// A malformed value in a partition column the predicate does NOT reference must
    /// not affect pruning. Because the pruning schema only contains the referenced
    /// column (`id`), the unreferenced `region` value is never coerced and cannot make
    /// the file fail open: `id = 5` still prunes a file whose `id` is 7.
    #[test]
    fn pruning_ignores_malformed_value_in_unreferenced_column() {
        let predicate = id_eq_5_predicate();

        let mut partition_values = HashMap::new();
        partition_values.insert("id".to_string(), Some("7".to_string()));
        partition_values.insert("region".to_string(), Some("not_an_int".to_string()));
        let add = Add {
            path: "no_match.parquet".to_string(),
            partition_values,
            size: 1,
            modification_time: 0,
            data_change: true,
            ..Default::default()
        };

        let specs = vec![CdcDataSpec::new(0, 0, vec![add])];
        let kept = kept_paths(prune_specs_by_partition(specs, &predicate).unwrap());
        assert!(
            kept.is_empty(),
            "file with id=7 must be pruned by id = 5; a malformed value in the \
             unreferenced `region` column must not keep it"
        );
    }
}
