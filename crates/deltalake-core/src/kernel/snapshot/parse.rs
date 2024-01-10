use std::{
    io::{BufRead, BufReader, Cursor},
    task::Poll,
};

use arrow_array::{
    new_null_array, Array, BooleanArray, Int32Array, Int64Array, ListArray, MapArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_json::{reader::Decoder, ReaderBuilder};
use arrow_schema::{ArrowError, SchemaRef as ArrowSchemaRef};
use arrow_select::concat::concat_batches;
use bytes::{Buf, Bytes};
use futures::{ready, Stream, StreamExt};
use object_store::Result as ObjectStoreResult;
use percent_encoding::percent_decode_str;

use super::extract::{
    extract_and_cast, extract_and_cast_opt, read_bool, read_bool_opt, read_primitive,
    read_primitive_opt, read_str, read_str_opt, ProvidesColumnByName,
};
use crate::{
    kernel::{Add, DeletionVectorDescriptor, Metadata, Remove},
    DeltaResult, DeltaTableConfig, DeltaTableError,
};

#[inline]
pub(super) fn get_reader(data: &[u8]) -> BufReader<Cursor<&[u8]>> {
    BufReader::new(Cursor::new(data))
}

pub(super) fn read_metadata(batch: &dyn ProvidesColumnByName) -> DeltaResult<Option<Metadata>> {
    if let Some(arr) = extract_and_cast_opt::<StructArray>(batch, "metaData") {
        let id = extract_and_cast::<StringArray>(arr, "id")?;
        let name = extract_and_cast::<StringArray>(arr, "name")?;
        let description = extract_and_cast::<StringArray>(arr, "description")?;
        // let format = extract_and_cast::<StringArray>(arr, "format")?;
        let schema_string = extract_and_cast::<StringArray>(arr, "schemaString")?;
        let partition_columns = extract_and_cast_opt::<ListArray>(arr, "partitionColumns");
        let configuration = extract_and_cast_opt::<MapArray>(arr, "configuration");
        let created_time = extract_and_cast::<Int64Array>(arr, "createdTime")?;

        for idx in 0..arr.len() {
            if arr.is_valid(idx) {
                return Ok(Some(Metadata {
                    id: read_str(id, idx)?.to_string(),
                    name: read_str_opt(name, idx).map(|s| s.to_string()),
                    description: read_str_opt(description, idx).map(|s| s.to_string()),
                    format: Default::default(),
                    schema_string: read_str(schema_string, idx)?.to_string(),
                    partition_columns: collect_string_list(&partition_columns, idx)
                        .unwrap_or_default(),
                    configuration: configuration
                        .and_then(|pv| collect_map(&pv.value(idx)).map(|m| m.collect()))
                        .unwrap_or_default(),
                    created_time: read_primitive_opt(created_time, idx),
                }));
            }
        }
    }
    Ok(None)
}

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
                let path_ = read_str(path, i)?;
                let path_ = percent_decode_str(path_)
                    .decode_utf8()
                    .map_err(|_| DeltaTableError::Generic("illegal path encoding".into()))?
                    .to_string();
                result.push(Add {
                    path: path_,
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
                let path_ = read_str(path, i)?;
                let path_ = percent_decode_str(path_)
                    .decode_utf8()
                    .map_err(|_| DeltaTableError::Generic("illegal path encoding".into()))?
                    .to_string();
                result.push(Remove {
                    path: path_,
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

fn insert_nulls(
    batches: &mut Vec<RecordBatch>,
    null_count: usize,
    schema: ArrowSchemaRef,
) -> Result<(), ArrowError> {
    let columns = schema
        .fields
        .iter()
        .map(|field| new_null_array(field.data_type(), null_count))
        .collect();
    batches.push(RecordBatch::try_new(schema, columns)?);
    Ok(())
}

pub(super) fn parse_json(
    json_strings: &StringArray,
    output_schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<RecordBatch> {
    let mut decoder = ReaderBuilder::new(output_schema.clone())
        .with_batch_size(config.log_batch_size)
        .build_decoder()?;
    let mut batches = Vec::new();

    let mut null_count = 0;
    let mut value_count = 0;
    let mut value_start = 0;

    for it in 0..json_strings.len() {
        if json_strings.is_null(it) {
            if value_count > 0 {
                let slice = json_strings.slice(value_start, value_count);
                let batch = read_from_json(get_reader(slice.value_data()), &mut decoder)
                    .collect::<Result<Vec<_>, _>>()?;
                batches.extend(batch);
                value_count = 0;
            }
            null_count += 1;
            continue;
        }
        if value_count == 0 {
            value_start = it;
        }
        if null_count > 0 {
            insert_nulls(&mut batches, null_count, output_schema.clone())?;
            null_count = 0;
        }
        value_count += 1;
    }

    if null_count > 0 {
        insert_nulls(&mut batches, null_count, output_schema.clone())?;
    }

    if value_count > 0 {
        let slice = json_strings.slice(value_start, value_count);
        let batch = read_from_json(get_reader(slice.value_data()), &mut decoder)
            .collect::<Result<Vec<_>, _>>()?;
        batches.extend(batch);
    }

    Ok(concat_batches(&output_schema, &batches)?)
}

pub(super) fn decode_stream<S: Stream<Item = ObjectStoreResult<Bytes>> + Unpin>(
    mut decoder: Decoder,
    mut input: S,
) -> impl Stream<Item = Result<RecordBatch, DeltaTableError>> {
    let mut buffered = Bytes::new();
    futures::stream::poll_fn(move |cx| {
        loop {
            if buffered.is_empty() {
                buffered = match ready!(input.poll_next_unpin(cx)) {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                    None => break,
                };
            }
            let decoded = match decoder.decode(buffered.as_ref()) {
                Ok(decoded) => decoded,
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            };
            let read = buffered.len();
            buffered.advance(decoded);
            if decoded != read {
                break;
            }
        }

        Poll::Ready(decoder.flush().map_err(DeltaTableError::from).transpose())
    })
}

pub(super) fn read_from_json<'a, R: BufRead + 'a>(
    mut reader: R,
    decoder: &'a mut Decoder,
) -> impl Iterator<Item = Result<RecordBatch, DeltaTableError>> + '_ {
    let mut next = move || {
        loop {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break; // Input exhausted
            }
            let read = buf.len();
            let decoded = decoder.decode(buf)?;

            reader.consume(decoded);
            if decoded != read {
                break; // Read batch size
            }
        }
        decoder.flush()
    };
    std::iter::from_fn(move || next().map_err(DeltaTableError::from).transpose())
}
