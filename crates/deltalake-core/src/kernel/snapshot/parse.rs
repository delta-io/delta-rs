use arrow::array::StructArray;
use arrow_array::{Array, BooleanArray, Int32Array, Int64Array, MapArray, StringArray};

use super::extract::{
    extract_and_cast, extract_and_cast_opt, read_bool, read_primitive, read_primitive_opt,
    read_str, read_str_opt, ProvidesColumnByName,
};
use crate::{
    kernel::{Add, DeletionVectorDescriptor},
    DeltaResult,
};

pub(super) fn extract_adds<'a>(array: &'a dyn ProvidesColumnByName) -> DeltaResult<Vec<Add>> {
    let mut result = Vec::new();

    if let Some(arr) = extract_and_cast_opt::<StructArray>(array, "add") {
        let path = extract_and_cast::<StringArray>(arr, "path")?;
        let pvs = extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = extract_and_cast::<Int64Array>(arr, "size")?;
        let modification_time = extract_and_cast::<Int64Array>(arr, "modificationTime")?;
        let data_change = extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let stats = extract_and_cast::<StringArray>(arr, "stats")?;
        let tags = extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let dv = extract_and_cast_opt::<StructArray>(arr, "deletionVector");

        for i in 0..arr.len() {
            let deletion_vector = dv.and_then(|d| {
                let storage_type = extract_and_cast_opt::<StringArray>(d, "storageType")?;
                let path_or_inline_dv = extract_and_cast_opt::<StringArray>(d, "pathOrInlineDv")?;
                let offset = extract_and_cast_opt::<Int32Array>(d, "offset")?;
                let size_in_bytes = extract_and_cast_opt::<Int32Array>(d, "sizeInBytes")?;
                let cardinality = extract_and_cast_opt::<Int64Array>(d, "cardinality")?;
                if read_str(storage_type, i).is_ok() {
                    Some(DeletionVectorDescriptor {
                        storage_type: std::str::FromStr::from_str(read_str(storage_type, i).ok()?)
                            .ok()?,
                        path_or_inline_dv: read_str(path_or_inline_dv, i).ok()?.to_string(),
                        offset: read_primitive_opt(offset, i),
                        size_in_bytes: read_primitive(size_in_bytes, i).ok()?,
                        cardinality: read_primitive(cardinality, i).ok()?,
                    })
                } else {
                    None
                }
            });

            result.push(Add {
                path: read_str(path, i)?.to_string(),
                size: read_primitive(size, i)?,
                modification_time: read_primitive(modification_time, i)?,
                data_change: read_bool(data_change, i)?,
                stats: read_str_opt(stats, i).map(|s| s.to_string()),
                partition_values: pvs
                    .and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect()))
                    .unwrap_or_default(),
                tags: tags.and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect())),
                deletion_vector,
                base_row_id: None,
                default_row_commit_version: None,
                clustering_provider: None,
                partition_values_parsed: None,
                stats_parsed: None,
            });
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
