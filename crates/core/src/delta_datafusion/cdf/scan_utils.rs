use std::collections::HashMap;
use std::sync::Arc;

use crate::delta_datafusion::cdf::{CHANGE_TYPE_COL, CdcDataSpec, FileAction, ResolvedPair};
use crate::delta_datafusion::{get_null_of_arrow_type, to_correct_scalar_value};
use crate::kernel::StorageType;
use crate::{DeltaResult, DeltaTableError};
use arrow_array::{Array, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use delta_kernel::Engine;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use object_store::path::Path;
use object_store::{DynObjectStore, ObjectStore};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::file::metadata::ParquetMetaData;
use serde_json::Value;
use tracing::{log, warn};
use url::Url;

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
        Some(value) => Ok(to_correct_scalar_value(
            &Value::String(value.to_string()),
            field.data_type(),
        )
        .ok()
        .flatten()),
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

pub async fn create_partition_values<F: FileAction>(
    schema: SchemaRef,
    specs: Vec<CdcDataSpec<F>>,
    table_partition_cols: &[String],
    action_type: Option<ScalarValue>,
    engine: Arc<dyn Engine>,
    root_object_store: Arc<dyn ObjectStore>,
    table_root: Url,
    parquet_metadata_cache: Arc<DashMap<String, Arc<ParquetMetaData>>>,
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
            let mut part_file = PartitionedFile::new(action.path(), action.size()? as u64)
                .with_partition_values(new_part_values.clone());

            if action.has_deletion_vector()
                && let Some(access_plan) = create_file_scan_plan(
                    Arc::clone(&engine),
                    action,
                    table_root.clone(),
                    Arc::clone(&root_object_store),
                    Arc::clone(&parquet_metadata_cache),
                )
                .await?
            {
                part_file = part_file.with_extension(access_plan);
            }
            file_groups
                .entry(new_part_values)
                .or_default()
                .push(part_file);
        }
    }
    Ok(file_groups)
}

pub async fn extend_groups_with_pairs(
    schema: SchemaRef,
    pairs: Vec<ResolvedPair>,
    table_partition_cols: &[String],
    add_groups: &mut HashMap<Vec<ScalarValue>, Vec<PartitionedFile>>,
    remove_groups: &mut HashMap<Vec<ScalarValue>, Vec<PartitionedFile>>,
    engine: Arc<dyn Engine>,
    root_object_store: Arc<dyn ObjectStore>,
    table_root: Url,
    parquet_metadata_cache: Arc<DashMap<String, Arc<ParquetMetaData>>>,
) -> DeltaResult<()> {
    for mut pair in pairs {
        let (deletes, inserts) = resolve_dv_pair(
            pair.add.deletion_vector.take(),
            pair.rm_dv.take(),
            &engine,
            &table_root,
        )?;

        let table_partition_values = table_partition_cols
            .iter()
            .map(|part| map_action_to_scalar(&pair.add, part, schema.clone()))
            .collect::<DeltaResult<Vec<ScalarValue>>>()?;

        if !deletes.is_empty() {
            let access_plan = access_plan_for_selection(
                deletes,
                &pair.add.path,
                pair.add.size as u64,
                &table_root,
                Arc::clone(&root_object_store),
                Arc::clone(&parquet_metadata_cache),
            )
            .await?;

            let mut part_values = vec![
                ScalarValue::Utf8(Some("delete".to_string())),
                ScalarValue::UInt64(Some(pair.version)),
                ScalarValue::TimestampMillisecond(Some(pair.timestamp), None),
            ];
            part_values.extend_from_slice(&table_partition_values);
            // part_values.extend(table_partition_values.clone());

            let part_file = PartitionedFile::new(&pair.add.path, pair.add.size as u64)
                .with_partition_values(part_values.clone())
                .with_extension(access_plan);
            remove_groups
                .entry(part_values)
                .or_default()
                .push(part_file);
        }
        if !inserts.is_empty() {
            let access_plan = access_plan_for_selection(
                inserts,
                &pair.add.path,
                pair.add.size as u64,
                &table_root,
                Arc::clone(&root_object_store),
                Arc::clone(&parquet_metadata_cache),
            )
            .await?;

            let mut part_values = vec![
                ScalarValue::Utf8(Some("insert".to_string())),
                ScalarValue::UInt64(Some(pair.version)),
                ScalarValue::TimestampMillisecond(Some(pair.timestamp), None),
            ];
            part_values.extend_from_slice(&table_partition_values);

            let part_file = PartitionedFile::new(pair.add.path, pair.add.size as u64)
                .with_partition_values(part_values.clone())
                .with_extension(access_plan);
            add_groups.entry(part_values).or_default().push(part_file);
        }
    }
    Ok(())
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
            match map_action_to_scalar_for_pruning(action, field.name(), &self.table_schema)? {
                Some(scalar) => columns.push(scalar.to_array_of_size(1)?),
                // Could not coerce this partition value: be conservative and keep
                // the file instead of evaluating the predicate against a NULL.
                None => return Ok(true),
            }
        }

        // A constant partition-only predicate (e.g. `false` after simplification)
        // references no columns, so `columns` is empty. Arrow rejects a zero-column
        // batch without an explicit row count, so evaluate it over one synthetic row;
        // the predicate is constant, so a single row decides all files identically.
        let batch = if columns.is_empty() {
            RecordBatch::try_new_with_options(
                Arc::clone(&self.partition_schema),
                columns,
                &RecordBatchOptions::new().with_row_count(Some(1)),
            )?
        } else {
            RecordBatch::try_new(Arc::clone(&self.partition_schema), columns)?
        };
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
    // A constant predicate has an empty pruning schema and needs no partition values,
    // so the "unreadable partition values -> keep" pre-check below must be skipped:
    // otherwise a `WHERE false` predicate would keep files (e.g. a Remove without
    // extended metadata) it could safely prune. When the schema references columns,
    // keep the pre-check so we don't route every such action through should_keep's
    // error/fail-open path.
    let needs_partition_values = !predicate.partition_schema.fields().is_empty();
    let mut pruned = Vec::with_capacity(specs.len());
    for spec in specs {
        let (version, timestamp, actions) = spec.into_parts();
        let mut kept = Vec::with_capacity(actions.len());
        for action in actions {
            // For a predicate that references partition columns, an action that cannot
            // expose its partition values (e.g. a Remove without extended metadata)
            // cannot be evaluated, so keep the file to stay correct.
            if needs_partition_values && action.partition_values().is_err() {
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

impl TryInto<DeletionVectorDescriptor> for crate::kernel::DeletionVectorDescriptor {
    type Error = DeltaTableError;

    fn try_into(self) -> DeltaResult<DeletionVectorDescriptor> {
        let storage_type = match self.storage_type {
            StorageType::UuidRelativePath => DeletionVectorStorageType::PersistedRelative,
            StorageType::Inline => DeletionVectorStorageType::Inline,
            StorageType::AbsolutePath => DeletionVectorStorageType::PersistedAbsolute,
        };

        Ok(DeletionVectorDescriptor::try_new(
            storage_type,
            self.path_or_inline_dv,
            self.offset,
            self.size_in_bytes,
            self.cardinality,
        )?)
    }
}

#[inline]
fn treemap_to_bools_with(treemap: roaring::RoaringTreemap, set_bit: bool) -> Vec<bool> {
    #[inline]
    fn combine(high_bits: u32, low_bits: u32) -> usize {
        ((u64::from(high_bits) << 32) | u64::from(low_bits)) as usize
    }

    match treemap.max() {
        Some(max) => {
            let mut result = vec![!set_bit; max as usize + 1];
            let bitmaps = treemap.bitmaps();
            for (index, bitmap) in bitmaps {
                for bit in bitmap.iter() {
                    let vec_index = combine(index, bit);
                    result[vec_index] = set_bit;
                }
            }
            result
        }
        None => {
            // empty set, return empty vec
            vec![]
        }
    }
}

#[inline]
async fn read_parquet_metadata(
    engine_objectstore: Arc<DynObjectStore>,
    file_path: Path,
    file_size: u64,
) -> DeltaResult<Arc<ParquetMetaData>> {
    let mut object_reader = ParquetObjectReader::new(engine_objectstore, file_path);
    if file_size > 0 {
        object_reader = object_reader.with_file_size(file_size);
    }
    let arrow_opts = ArrowReaderOptions::new();
    let arrow_metadata_reader =
        ArrowReaderMetadata::load_async(&mut object_reader, arrow_opts).await?;
    Ok(Arc::clone(arrow_metadata_reader.metadata()))
}

#[inline]
async fn get_metadata_from_cache(
    parquet_metadata_cache: Arc<DashMap<String, Arc<ParquetMetaData>>>,
    engine_objectstore: Arc<DynObjectStore>,
    file_path: &Path,
    file_size: u64,
) -> DeltaResult<Arc<ParquetMetaData>> {
    if let Some(pmd) = parquet_metadata_cache.get(file_path.as_ref()) {
        Ok(Arc::clone(pmd.value()))
    } else {
        let new_pmd =
            read_parquet_metadata(engine_objectstore, file_path.clone(), file_size).await?;
        parquet_metadata_cache.insert(file_path.to_string(), Arc::clone(&new_pmd));
        Ok(new_pmd)
    }
}

#[inline]
fn read_dv_treemap(
    dv: Option<crate::kernel::DeletionVectorDescriptor>,
    engine: &Arc<dyn Engine>,
    table_root_url: &Url,
) -> DeltaResult<roaring::RoaringTreemap> {
    match dv {
        Some(dv) => {
            let kernel_dv: DeletionVectorDescriptor = dv.try_into()?;
            Ok(kernel_dv.read(engine.storage_handler(), table_root_url)?)
        }
        None => Ok(roaring::RoaringTreemap::new()),
    }
}

#[inline]
pub(crate) fn resolve_dv_pair(
    add_dv: Option<crate::kernel::DeletionVectorDescriptor>,
    rm_dv: Option<crate::kernel::DeletionVectorDescriptor>,
    engine: &Arc<dyn Engine>,
    table_root_url: &Url,
) -> DeltaResult<(roaring::RoaringTreemap, roaring::RoaringTreemap)> {
    let add_tm = read_dv_treemap(add_dv, engine, table_root_url)?;
    let rm_tm = read_dv_treemap(rm_dv, engine, table_root_url)?;
    let deletes = &add_tm - &rm_tm;
    let inserts = &rm_tm - &add_tm;
    Ok((deletes, inserts))
}

#[inline]
fn access_plan_from_selection(
    selection: Vec<bool>,
    parquet_metadata: &ParquetMetaData,
    file_path: &Path,
) -> ParquetAccessPlan {
    let mut access_plan = ParquetAccessPlan::new_all(parquet_metadata.num_row_groups());
    let mut offset = 0usize;
    for (rg_idx, rg) in parquet_metadata.row_groups().iter().enumerate() {
        let num_rows = rg.num_rows() as usize;
        if let Some(slice) = selection.get(offset..(offset + num_rows)) {
            let keep = BooleanArray::from(slice.to_vec());
            if keep.true_count() == 0 {
                access_plan.skip(rg_idx);
            } else if keep.false_count() == 0 {
                access_plan.scan(rg_idx);
            } else {
                let selection = RowSelection::from_filters(&[keep]);
                access_plan.set(rg_idx, RowGroupAccess::Selection(selection));
            }
            offset += num_rows;
        } else {
            warn!(
                "Row group indexes were out of bounds: {offset} to {} for {file_path}",
                offset + num_rows
            );
        }
    }
    access_plan
}

#[inline]
fn total_rows(parquet_metadata: &ParquetMetaData) -> usize {
    parquet_metadata
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as usize)
        .sum()
}

pub(crate) async fn access_plan_for_selection(
    selection: roaring::RoaringTreemap,
    file_path_str: &str,
    file_size: u64,
    table_root_url: &Url,
    root_object_store: Arc<DynObjectStore>,
    parquet_metadata_cache: Arc<DashMap<String, Arc<ParquetMetaData>>>,
) -> DeltaResult<ParquetAccessPlan> {
    let tp = table_root_url.path().strip_prefix('/').unwrap();
    let file_path = Path::parse(format!("{tp}{file_path_str}"))?;
    let parquet_metadata = get_metadata_from_cache(
        parquet_metadata_cache,
        root_object_store,
        &file_path,
        file_size,
    )
    .await?;
    let mut sel = treemap_to_bools_with(selection, true);
    sel.resize(total_rows(&parquet_metadata), false);
    Ok(access_plan_from_selection(
        sel,
        &parquet_metadata,
        &file_path,
    ))
}

async fn create_file_scan_plan<F: FileAction>(
    engine: Arc<dyn Engine>,
    file_action: F,
    table_root_url: Url,
    root_object_store: Arc<DynObjectStore>,
    parquet_metadata_cache: Arc<DashMap<String, Arc<ParquetMetaData>>>,
) -> DeltaResult<Option<ParquetAccessPlan>> {
    if let Some(dv) = file_action.deletion_vector() {
        let tp = table_root_url.path().strip_prefix('/').unwrap();
        let dv_path = format!("{}{}", tp, file_action.path());
        let file_path = Path::parse(dv_path)?;

        let raw_tree_map = read_dv_treemap(Some(dv), &engine, &table_root_url)?;
        let mut tree_map = treemap_to_bools_with(raw_tree_map, false);

        let parquet_metadata = get_metadata_from_cache(
            parquet_metadata_cache,
            root_object_store,
            &file_path,
            file_action.size()? as u64,
        )
        .await?;
        tree_map.resize(total_rows(&parquet_metadata), true);
        return Ok(Some(access_plan_from_selection(
            tree_map,
            &parquet_metadata,
            &file_path,
        )));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{Add, Remove};
    use arrow_select::concat::concat_batches;
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::SessionContext;
    use delta_kernel::engine::arrow_data::EngineDataArrowExt;
    use delta_kernel::table_changes::TableChanges;
    use delta_kernel_default_engine::DefaultEngine;
    use itertools::Itertools;
    use object_store::local::LocalFileSystem;
    use std::collections::HashMap;

    /// A constant `false` pruning predicate over an empty schema.
    fn constant_false_predicate() -> PartitionPruningPredicate {
        let table_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let partition_schema: SchemaRef = Arc::new(Schema::new(Vec::<Field>::new()));
        let df_schema = partition_schema.as_ref().clone().try_into().unwrap();
        let predicate = SessionContext::new()
            .state()
            .create_physical_expr(lit(false), &df_schema)
            .unwrap();
        PartitionPruningPredicate {
            predicate,
            partition_schema,
            table_schema,
        }
    }

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

    /// Integration guard for the referenced-columns-only fix: the pruning schema is
    /// *derived from the helper* over a table with two partition columns, and the
    /// predicate references only `id`. A malformed value in the unreferenced `region`
    /// column (here `Int32`, so `"not_an_int"` is a real coercion failure) must not
    /// keep the file. With the original bug — building the schema from *all* partition
    /// columns — `region` would be coerced, fail, and fail open, keeping `id=7`.
    #[test]
    fn pruning_schema_excludes_unreferenced_columns_so_malformed_values_are_ignored() {
        use crate::delta_datafusion::extract_partition_only_predicate;

        let partition_columns = vec!["id".to_string(), "region".to_string()];
        let table_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("region", DataType::Int32, true),
        ]));

        // Build the pruning predicate exactly as the load_cdf builder does: derive the
        // schema from the helper's referenced columns, not from every partition column.
        let (pred_expr, referenced) =
            extract_partition_only_predicate(col("id").eq(lit(5_i32)), &partition_columns)
                .unwrap()
                .expect("partition-only predicate");
        assert_eq!(
            referenced,
            vec!["id".to_string()],
            "region must not be referenced"
        );

        let partition_fields = referenced
            .iter()
            .map(|name| table_schema.field_with_name(name).cloned().unwrap())
            .collect::<Vec<_>>();
        let partition_schema: SchemaRef = Arc::new(Schema::new(partition_fields));
        let df_schema = partition_schema.as_ref().clone().try_into().unwrap();
        let predicate = SessionContext::new()
            .state()
            .create_physical_expr(pred_expr, &df_schema)
            .unwrap();
        let predicate = PartitionPruningPredicate {
            predicate,
            partition_schema,
            table_schema,
        };

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
            "id=7 must be pruned by id=5; the unreferenced malformed `region` must be \
             absent from the pruning schema and must not keep the file: {kept:?}"
        );
    }

    /// A partition-only *constant* predicate (e.g. `WHERE false`, or `p = 'x' AND false`
    /// after simplification) references no partition columns, so the pruning schema is
    /// empty. Evaluation must still work and prune correctly instead of erroring on a
    /// zero-column `RecordBatch` (which would fail open and wrongly keep every file).
    #[test]
    fn pruning_handles_constant_predicate_with_no_referenced_columns() {
        let predicate = constant_false_predicate();

        let specs = vec![CdcDataSpec::new(
            0,
            0,
            vec![
                add_with_id_partition("a.parquet", Some(Some("5"))),
                add_with_id_partition("b.parquet", Some(Some("7"))),
            ],
        )];

        let kept = kept_paths(prune_specs_by_partition(specs, &predicate).unwrap());
        assert!(
            kept.is_empty(),
            "a constant `false` predicate must prune every file, not fail open and keep them: {kept:?}"
        );
    }

    /// A constant `false` predicate has an empty pruning schema, so it needs no partition
    /// values and must prune even a `Remove` that lacks extended partition metadata (whose
    /// `partition_values()` errors). The pre-check that keeps actions with unreadable
    /// partition values must be bypassed when the pruning schema has no fields.
    #[test]
    fn constant_false_predicate_prunes_remove_without_partition_metadata() {
        let predicate = constant_false_predicate();

        let remove = Remove {
            path: "r.parquet".to_string(),
            data_change: true,
            extended_file_metadata: Some(false),
            partition_values: None,
            size: None,
            ..Default::default()
        };
        // This Remove genuinely cannot expose partition values.
        assert!(remove.partition_values().is_err());

        let specs = vec![CdcDataSpec::new(0, 0, vec![remove])];
        let kept: Vec<String> = prune_specs_by_partition(specs, &predicate)
            .unwrap()
            .into_iter()
            .flat_map(|s| s.into_parts().2)
            .map(|r| r.path)
            .collect();
        assert!(
            kept.is_empty(),
            "constant `false` must prune even a Remove without partition metadata: {kept:?}"
        );
    }

    #[tokio::test]
    async fn dv_scan_plan_creation() -> DeltaResult<()> {
        let ctx = SessionContext::new();
        let fs = Arc::new(LocalFileSystem::new());
        let engine = DefaultEngine::builder(fs.clone()).build();

        let table_url = delta_kernel::try_parse_uri(
            "C:\\Users\\shcar\\IdeaProjects\\spark-tests\\cdc_dv_table\\",
        )?;
        let cdc = TableChanges::try_new(table_url, &engine, 6, None)?
            .into_scan_builder()
            .build()?;

        let batches: Vec<RecordBatch> = cdc
            .execute(Arc::new(engine))?
            .map(EngineDataArrowExt::try_into_record_batch)
            .try_collect()?;
        let total = batches.iter().map(|b| b.num_rows()).sum::<usize>();
        dbg!(total);
        let all_batches = concat_batches(&batches[0].schema().clone(), &batches);
        ctx.register_batch("kernel", all_batches?)?;
        ctx.sql("select row_number() over (partition by birthyear) as row_num, name, age, birthyear from kernel order by birthyear, name").await?.show().await?;
        Ok(())
    }
}
