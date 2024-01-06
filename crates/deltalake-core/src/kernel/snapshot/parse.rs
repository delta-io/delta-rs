use arrow::array::StructArray;
use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, MapArray, StringArray};

use super::extract::{
    extract_and_cast, extract_and_cast_opt, read_bool, read_bool_opt, read_primitive,
    read_primitive_opt, read_str, read_str_opt, ProvidesColumnByName,
};
use crate::{
    kernel::{Add, DeletionVectorDescriptor, Remove},
    DeltaResult,
};

pub(super) fn extract_adds(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Add>> {
    let mut result = Vec::new();

    if let Some(arr) = extract_and_cast_opt::<StructArray>(array, "add") {
        let path = extract_and_cast::<StringArray>(arr, "path")?;
        let pvs = extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = extract_and_cast::<Int64Array>(arr, "size")?;
        let modification_time = extract_and_cast::<Int64Array>(arr, "modificationTime")?;
        let data_change = extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let stats = extract_and_cast::<StringArray>(arr, "stats")?;
        let tags = extract_and_cast_opt::<MapArray>(arr, "tags");
        let dv = extract_and_cast_opt::<StructArray>(arr, "deletionVector");

        let get_dv: Box<dyn Fn(usize) -> Option<DeletionVectorDescriptor>> = if let Some(d) = dv {
            let storage_type = extract_and_cast::<StringArray>(d, "storageType")?;
            let path_or_inline_dv = extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
            let offset = extract_and_cast::<Int32Array>(d, "offset")?;
            let size_in_bytes = extract_and_cast::<Int32Array>(d, "sizeInBytes")?;
            let cardinality = extract_and_cast::<Int64Array>(d, "cardinality")?;

            Box::new(|idx: usize| {
                if read_str(storage_type, idx).is_ok() {
                    Some(DeletionVectorDescriptor {
                        storage_type: std::str::FromStr::from_str(
                            read_str(storage_type, idx).ok()?,
                        )
                        .ok()?,
                        path_or_inline_dv: read_str(path_or_inline_dv, idx).ok()?.to_string(),
                        offset: read_primitive_opt(offset, idx),
                        size_in_bytes: read_primitive(size_in_bytes, idx).ok()?,
                        cardinality: read_primitive(cardinality, idx).ok()?,
                    })
                } else {
                    None
                }
            })
        } else {
            Box::new(|_| None)
        };

        for i in 0..arr.len() {
            if arr.is_valid(i) {
                result.push(Add {
                    path: read_str(path, i)?.to_string(),
                    size: read_primitive(size, i)?,
                    modification_time: read_primitive(modification_time, i)?,
                    data_change: read_bool(data_change, i)?,
                    stats: read_str_opt(stats, i).map(|s| s.to_string()),
                    partition_values: pvs
                        .and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect()))
                        .unwrap_or_default(),
                    tags: tags.and_then(|t| collect_map(&t.value(i)).map(|m| m.collect())),
                    deletion_vector: get_dv(i),
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                    partition_values_parsed: None,
                    stats_parsed: None,
                });
            }
        }
    }

    Ok(result)
}

pub(super) fn extract_removes(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Remove>> {
    let mut result = Vec::new();

    if let Some(arr) = extract_and_cast_opt::<StructArray>(array, "remove") {
        let path = extract_and_cast::<StringArray>(arr, "path")?;
        let data_change = extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let deletion_timestamp = extract_and_cast::<Int64Array>(arr, "deletionTimestamp")?;

        let extended_file_metadata =
            extract_and_cast_opt::<BooleanArray>(arr, "extendedFileMetadata");
        let pvs = extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = extract_and_cast_opt::<Int64Array>(arr, "size");
        let tags = extract_and_cast_opt::<MapArray>(arr, "tags");
        let dv = extract_and_cast_opt::<StructArray>(arr, "deletionVector");

        let get_dv: Box<dyn Fn(usize) -> Option<DeletionVectorDescriptor>> = if let Some(d) = dv {
            let storage_type = extract_and_cast::<StringArray>(d, "storageType")?;
            let path_or_inline_dv = extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
            let offset = extract_and_cast::<Int32Array>(d, "offset")?;
            let size_in_bytes = extract_and_cast::<Int32Array>(d, "sizeInBytes")?;
            let cardinality = extract_and_cast::<Int64Array>(d, "cardinality")?;

            Box::new(|idx: usize| {
                if read_str(storage_type, idx).is_ok() {
                    Some(DeletionVectorDescriptor {
                        storage_type: std::str::FromStr::from_str(
                            read_str(storage_type, idx).ok()?,
                        )
                        .ok()?,
                        path_or_inline_dv: read_str(path_or_inline_dv, idx).ok()?.to_string(),
                        offset: read_primitive_opt(offset, idx),
                        size_in_bytes: read_primitive(size_in_bytes, idx).ok()?,
                        cardinality: read_primitive(cardinality, idx).ok()?,
                    })
                } else {
                    None
                }
            })
        } else {
            Box::new(|_| None)
        };

        for i in 0..arr.len() {
            if arr.is_valid(i) {
                result.push(Remove {
                    path: read_str(path, i)?.to_string(),
                    data_change: read_bool(data_change, i)?,
                    deletion_timestamp: read_primitive_opt(deletion_timestamp, i),
                    extended_file_metadata: extended_file_metadata
                        .and_then(|e| read_bool_opt(e, i)),
                    size: size.and_then(|s| read_primitive_opt(s, i)),
                    partition_values: pvs
                        .and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect())),
                    tags: tags.and_then(|t| collect_map(&t.value(i)).map(|m| m.collect())),
                    deletion_vector: get_dv(i),
                    base_row_id: None,
                    default_row_commit_version: None,
                });
            }
        }
    }

    Ok(result)
}

fn collect_map(val: &StructArray) -> Option<impl Iterator<Item = (String, Option<String>)> + '_> {
    let keys = val
        .column(0)
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()?;
    let values = val
        .column(1)
        .as_ref()
        .as_any()
        .downcast_ref::<StringArray>()?;
    Some(
        keys.iter()
            .zip(values.iter())
            .filter_map(|(k, v)| k.map(|kv| (kv.to_string(), v.map(|vv| vv.to_string())))),
    )
}
