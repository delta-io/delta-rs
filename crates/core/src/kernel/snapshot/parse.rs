//! Utilities for converting Arrow arrays into Delta data structures.

use arrow_array::{
    Array, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
};
use percent_encoding::percent_decode_str;

use crate::kernel::arrow::extract::{self as ex, ProvidesColumnByName};
use crate::kernel::{Add, AddCDCFile, DeletionVectorDescriptor, Metadata, Protocol, Remove};
use crate::{DeltaResult, DeltaTableError};

pub(super) fn read_metadata(batch: &dyn ProvidesColumnByName) -> DeltaResult<Option<Metadata>> {
    if let Some(arr) = ex::extract_and_cast_opt::<StructArray>(batch, "metaData") {
        // Stop early if all values are null
        if arr.null_count() == arr.len() {
            return Ok(None);
        }

        let id = ex::extract_and_cast::<StringArray>(arr, "id")?;
        let name = ex::extract_and_cast::<StringArray>(arr, "name")?;
        let description = ex::extract_and_cast::<StringArray>(arr, "description")?;
        // let format = ex::extract_and_cast::<StringArray>(arr, "format")?;
        let schema_string = ex::extract_and_cast::<StringArray>(arr, "schemaString")?;
        let partition_columns = ex::extract_and_cast_opt::<ListArray>(arr, "partitionColumns");
        let configuration = ex::extract_and_cast_opt::<MapArray>(arr, "configuration");
        let created_time = ex::extract_and_cast::<Int64Array>(arr, "createdTime")?;

        for idx in 0..arr.len() {
            if arr.is_valid(idx) {
                return Ok(Some(Metadata {
                    id: ex::read_str(id, idx)?.to_string(),
                    name: ex::read_str_opt(name, idx).map(|s| s.to_string()),
                    description: ex::read_str_opt(description, idx).map(|s| s.to_string()),
                    format: Default::default(),
                    schema_string: ex::read_str(schema_string, idx)?.to_string(),
                    partition_columns: collect_string_list(&partition_columns, idx)
                        .unwrap_or_default(),
                    configuration: configuration
                        .and_then(|pv| collect_map(&pv.value(idx)).map(|m| m.collect()))
                        .unwrap_or_default(),
                    created_time: ex::read_primitive_opt(created_time, idx),
                }));
            }
        }
    }
    Ok(None)
}

pub(super) fn read_protocol(batch: &dyn ProvidesColumnByName) -> DeltaResult<Option<Protocol>> {
    if let Some(arr) = ex::extract_and_cast_opt::<StructArray>(batch, "protocol") {
        // Stop early if all values are null
        if arr.null_count() == arr.len() {
            return Ok(None);
        }

        let min_reader_version = ex::extract_and_cast::<Int32Array>(arr, "minReaderVersion")?;
        let min_writer_version = ex::extract_and_cast::<Int32Array>(arr, "minWriterVersion")?;
        let maybe_reader_features = ex::extract_and_cast_opt::<ListArray>(arr, "readerFeatures");
        let maybe_writer_features = ex::extract_and_cast_opt::<ListArray>(arr, "writerFeatures");

        for idx in 0..arr.len() {
            if arr.is_valid(idx) {
                return Ok(Some(Protocol {
                    min_reader_version: ex::read_primitive(min_reader_version, idx)?,
                    min_writer_version: ex::read_primitive(min_writer_version, idx)?,
                    reader_features: collect_string_list(&maybe_reader_features, idx)
                        .map(|v| v.into_iter().map(Into::into).collect()),
                    writer_features: collect_string_list(&maybe_writer_features, idx)
                        .map(|v| v.into_iter().map(Into::into).collect()),
                }));
            }
        }
    }
    Ok(None)
}

pub(super) fn read_adds(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Add>> {
    let mut result = Vec::new();

    if let Some(arr) = ex::extract_and_cast_opt::<StructArray>(array, "add") {
        let path = ex::extract_and_cast::<StringArray>(arr, "path")?;
        let pvs = ex::extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = ex::extract_and_cast::<Int64Array>(arr, "size")?;
        let modification_time = ex::extract_and_cast::<Int64Array>(arr, "modificationTime")?;
        let data_change = ex::extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let stats = ex::extract_and_cast_opt::<StringArray>(arr, "stats");
        let tags = ex::extract_and_cast_opt::<MapArray>(arr, "tags");
        let dv = ex::extract_and_cast_opt::<StructArray>(arr, "deletionVector");

        let get_dv: Box<dyn Fn(usize) -> Option<DeletionVectorDescriptor>> = if let Some(d) = dv {
            let storage_type = ex::extract_and_cast::<StringArray>(d, "storageType")?;
            let path_or_inline_dv = ex::extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
            let offset = ex::extract_and_cast::<Int32Array>(d, "offset")?;
            let size_in_bytes = ex::extract_and_cast::<Int32Array>(d, "sizeInBytes")?;
            let cardinality = ex::extract_and_cast::<Int64Array>(d, "cardinality")?;

            Box::new(|idx: usize| {
                if ex::read_str(storage_type, idx).is_ok() {
                    Some(DeletionVectorDescriptor {
                        storage_type: std::str::FromStr::from_str(
                            ex::read_str(storage_type, idx).ok()?,
                        )
                        .ok()?,
                        path_or_inline_dv: ex::read_str(path_or_inline_dv, idx).ok()?.to_string(),
                        offset: ex::read_primitive_opt(offset, idx),
                        size_in_bytes: ex::read_primitive(size_in_bytes, idx).ok()?,
                        cardinality: ex::read_primitive(cardinality, idx).ok()?,
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
                let path_ = ex::read_str(path, i)?;
                let path_ = percent_decode_str(path_)
                    .decode_utf8()
                    .map_err(|_| DeltaTableError::Generic("illegal path encoding".into()))?
                    .to_string();
                result.push(Add {
                    path: path_,
                    size: ex::read_primitive(size, i)?,
                    modification_time: ex::read_primitive(modification_time, i)?,
                    data_change: ex::read_bool(data_change, i)?,
                    stats: stats
                        .and_then(|stats| ex::read_str_opt(stats, i).map(|s| s.to_string())),
                    partition_values: pvs
                        .and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect()))
                        .unwrap_or_default(),
                    tags: tags.and_then(|t| collect_map(&t.value(i)).map(|m| m.collect())),
                    deletion_vector: get_dv(i),
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                    stats_parsed: None,
                });
            }
        }
    }

    Ok(result)
}

pub(super) fn read_cdf_adds(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<AddCDCFile>> {
    let mut result = Vec::new();

    if let Some(arr) = ex::extract_and_cast_opt::<StructArray>(array, "cdc") {
        // Stop early if all values are null
        if arr.null_count() == arr.len() {
            return Ok(result);
        }

        let path = ex::extract_and_cast::<StringArray>(arr, "path")?;
        let pvs = ex::extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = ex::extract_and_cast::<Int64Array>(arr, "size")?;
        let data_change = ex::extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let tags = ex::extract_and_cast_opt::<MapArray>(arr, "tags");

        for i in 0..arr.len() {
            if arr.is_valid(i) {
                let path_ = ex::read_str(path, i)?;
                let path_ = percent_decode_str(path_)
                    .decode_utf8()
                    .map_err(|_| DeltaTableError::Generic("illegal path encoding".into()))?
                    .to_string();
                result.push(AddCDCFile {
                    path: path_,
                    size: ex::read_primitive(size, i)?,
                    data_change: ex::read_bool(data_change, i)?,
                    partition_values: pvs
                        .and_then(|pv| collect_map(&pv.value(i)).map(|m| m.collect()))
                        .unwrap_or_default(),
                    tags: tags.and_then(|t| collect_map(&t.value(i)).map(|m| m.collect())),
                });
            }
        }
    }

    Ok(result)
}

pub(super) fn read_removes(array: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Remove>> {
    let mut result = Vec::new();

    if let Some(arr) = ex::extract_and_cast_opt::<StructArray>(array, "remove") {
        // Stop early if all values are null
        if arr.null_count() == arr.len() {
            return Ok(result);
        }

        let path = ex::extract_and_cast::<StringArray>(arr, "path")?;
        let data_change = ex::extract_and_cast::<BooleanArray>(arr, "dataChange")?;
        let deletion_timestamp = ex::extract_and_cast::<Int64Array>(arr, "deletionTimestamp")?;

        let extended_file_metadata =
            ex::extract_and_cast_opt::<BooleanArray>(arr, "extendedFileMetadata");
        let pvs = ex::extract_and_cast_opt::<MapArray>(arr, "partitionValues");
        let size = ex::extract_and_cast_opt::<Int64Array>(arr, "size");
        let tags = ex::extract_and_cast_opt::<MapArray>(arr, "tags");
        let dv = ex::extract_and_cast_opt::<StructArray>(arr, "deletionVector");

        let get_dv: Box<dyn Fn(usize) -> Option<DeletionVectorDescriptor>> = if let Some(d) = dv {
            let storage_type = ex::extract_and_cast::<StringArray>(d, "storageType")?;
            let path_or_inline_dv = ex::extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
            let offset = ex::extract_and_cast::<Int32Array>(d, "offset")?;
            let size_in_bytes = ex::extract_and_cast::<Int32Array>(d, "sizeInBytes")?;
            let cardinality = ex::extract_and_cast::<Int64Array>(d, "cardinality")?;

            Box::new(|idx: usize| {
                if ex::read_str(storage_type, idx).is_ok() {
                    Some(DeletionVectorDescriptor {
                        storage_type: std::str::FromStr::from_str(
                            ex::read_str(storage_type, idx).ok()?,
                        )
                        .ok()?,
                        path_or_inline_dv: ex::read_str(path_or_inline_dv, idx).ok()?.to_string(),
                        offset: ex::read_primitive_opt(offset, idx),
                        size_in_bytes: ex::read_primitive(size_in_bytes, idx).ok()?,
                        cardinality: ex::read_primitive(cardinality, idx).ok()?,
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
                let path_ = ex::read_str(path, i)?;
                let path_ = percent_decode_str(path_)
                    .decode_utf8()
                    .map_err(|_| DeltaTableError::Generic("illegal path encoding".into()))?
                    .to_string();
                result.push(Remove {
                    path: path_,
                    data_change: ex::read_bool(data_change, i)?,
                    deletion_timestamp: ex::read_primitive_opt(deletion_timestamp, i),
                    extended_file_metadata: extended_file_metadata
                        .and_then(|e| ex::read_bool_opt(e, i)),
                    size: size.and_then(|s| ex::read_primitive_opt(s, i)),
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

pub(super) fn collect_map(
    val: &StructArray,
) -> Option<impl Iterator<Item = (String, Option<String>)> + '_> {
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

fn collect_string_list(arr: &Option<&ListArray>, idx: usize) -> Option<Vec<String>> {
    arr.and_then(|val| {
        let values = val.value(idx);
        let values = values.as_ref().as_any().downcast_ref::<StringArray>()?;
        Some(
            values
                .iter()
                .filter_map(|v| v.map(|vv| vv.to_string()))
                .collect(),
        )
    })
}
