use std::collections::HashMap;
use std::str::FromStr;

use arrow_array::{
    BooleanArray, Int32Array, Int64Array, ListArray, MapArray, RecordBatch, StringArray,
    StructArray,
};
use either::Either;
use fix_hidden_lifetime_bug::fix_hidden_lifetime_bug;
use itertools::izip;
use serde::{Deserialize, Serialize};

use super::{error::Error, DeltaResult};

#[fix_hidden_lifetime_bug]
#[allow(dead_code)]
pub(crate) fn parse_actions<'a>(
    batch: &RecordBatch,
    types: impl IntoIterator<Item = &'a ActionType>,
) -> DeltaResult<impl Iterator<Item = Action>> {
    Ok(types
        .into_iter()
        .filter_map(|action| parse_action(batch, action).ok())
        .flatten())
}

#[fix_hidden_lifetime_bug]
pub(crate) fn parse_action(
    batch: &RecordBatch,
    action_type: &ActionType,
) -> DeltaResult<impl Iterator<Item = Action>> {
    let column_name = match action_type {
        ActionType::Metadata => "metaData",
        ActionType::Protocol => "protocol",
        ActionType::Add => "add",
        ActionType::Remove => "remove",
        _ => unimplemented!(),
    };

    let arr = batch
        .column_by_name(column_name)
        .ok_or(Error::MissingColumn(column_name.into()))?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(Error::UnexpectedColumnType(
            "Cannot downcast to StructArray".into(),
        ))?;

    match action_type {
        ActionType::Metadata => parse_action_metadata(arr),
        ActionType::Protocol => parse_action_protocol(arr),
        ActionType::Add => parse_actions_add(arr),
        ActionType::Remove => parse_actions_remove(arr),
        _ => todo!(),
    }
}

fn parse_action_metadata(arr: &StructArray) -> DeltaResult<Box<dyn Iterator<Item = Action>>> {
    let ids = cast_struct_column::<StringArray>(arr, "id")?;
    let schema_strings = cast_struct_column::<StringArray>(arr, "schemaString")?;
    let metadata = ids
        .into_iter()
        .zip(schema_strings)
        .filter_map(|(maybe_id, maybe_schema_string)| {
            if let (Some(id), Some(schema_string)) = (maybe_id, maybe_schema_string) {
                Some(Metadata::new(
                    id,
                    Format {
                        provider: "parquet".into(),
                        options: Default::default(),
                    },
                    schema_string,
                    Vec::<String>::new(),
                    None,
                ))
            } else {
                None
            }
        })
        .next();

    if metadata.is_none() {
        return Ok(Box::new(std::iter::empty()));
    }
    let mut metadata = metadata.unwrap();

    metadata.partition_columns = cast_struct_column::<ListArray>(arr, "partitionColumns")
        .ok()
        .map(|arr| {
            arr.iter()
                .filter_map(|it| {
                    if let Some(features) = it {
                        let vals = features
                            .as_any()
                            .downcast_ref::<StringArray>()?
                            .iter()
                            .filter_map(|v| v.map(|inner| inner.to_owned()))
                            .collect::<Vec<_>>();
                        Some(vals)
                    } else {
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    metadata.name = cast_struct_column::<StringArray>(arr, "name")
        .ok()
        .and_then(|arr| {
            arr.iter()
                .flat_map(|maybe| maybe.map(|v| v.to_string()))
                .next()
        });
    metadata.description = cast_struct_column::<StringArray>(arr, "description")
        .ok()
        .and_then(|arr| {
            arr.iter()
                .flat_map(|maybe| maybe.map(|v| v.to_string()))
                .next()
        });
    metadata.created_time = cast_struct_column::<Int64Array>(arr, "createdTime")
        .ok()
        .and_then(|arr| arr.iter().flatten().next());

    if let Ok(config) = cast_struct_column::<MapArray>(arr, "configuration") {
        let keys = config
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::MissingData("expected key column in map".into()))?;
        let values = config
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(Error::MissingData("expected value column in map".into()))?;
        metadata.configuration = keys
            .into_iter()
            .zip(values)
            .filter_map(|(k, v)| k.map(|key| (key.to_string(), v.map(|vv| vv.to_string()))))
            .collect::<HashMap<_, _>>();
    };

    Ok(Box::new(std::iter::once(Action::Metadata(metadata))))
}

fn parse_action_protocol(arr: &StructArray) -> DeltaResult<Box<dyn Iterator<Item = Action>>> {
    let min_reader = cast_struct_column::<Int32Array>(arr, "minReaderVersion")?;
    let min_writer = cast_struct_column::<Int32Array>(arr, "minWriterVersion")?;
    let protocol = min_reader
        .into_iter()
        .zip(min_writer)
        .filter_map(|(r, w)| {
            if let (Some(min_reader_version), Some(min_wrriter_version)) = (r, w) {
                Some(Protocol::new(min_reader_version, min_wrriter_version))
            } else {
                None
            }
        })
        .next();

    if protocol.is_none() {
        return Ok(Box::new(std::iter::empty()));
    }
    let mut protocol = protocol.unwrap();

    protocol.reader_features = cast_struct_column::<ListArray>(arr, "readerFeatures")
        .ok()
        .map(|arr| {
            arr.iter()
                .filter_map(|it| {
                    if let Some(features) = it {
                        let vals = features
                            .as_any()
                            .downcast_ref::<StringArray>()?
                            .iter()
                            .filter_map(|v| v.map(|inner| inner.to_owned()))
                            .collect::<Vec<_>>();
                        Some(vals)
                    } else {
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>()
        });

    protocol.writer_features = cast_struct_column::<ListArray>(arr, "writerFeatures")
        .ok()
        .map(|arr| {
            arr.iter()
                .filter_map(|it| {
                    if let Some(features) = it {
                        let vals = features
                            .as_any()
                            .downcast_ref::<StringArray>()?
                            .iter()
                            .filter_map(|v| v.map(|inner| inner.to_string()))
                            .collect::<Vec<_>>();
                        Some(vals)
                    } else {
                        None
                    }
                })
                .flatten()
                .collect::<Vec<_>>()
        });

    Ok(Box::new(std::iter::once(Action::Protocol(protocol))))
}

fn parse_actions_add(arr: &StructArray) -> DeltaResult<Box<dyn Iterator<Item = Action> + '_>> {
    let paths = cast_struct_column::<StringArray>(arr, "path")?;
    let sizes = cast_struct_column::<Int64Array>(arr, "size")?;
    let modification_times = cast_struct_column::<Int64Array>(arr, "modificationTime")?;
    let data_changes = cast_struct_column::<BooleanArray>(arr, "dataChange")?;
    let partition_values = cast_struct_column::<MapArray>(arr, "partitionValues")?
        .iter()
        .map(|data| data.map(|d| struct_array_to_map(&d).unwrap()));

    let tags = if let Ok(stats) = cast_struct_column::<MapArray>(arr, "tags") {
        Either::Left(
            stats
                .iter()
                .map(|data| data.map(|d| struct_array_to_map(&d).unwrap())),
        )
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let stats = if let Ok(stats) = cast_struct_column::<StringArray>(arr, "stats") {
        Either::Left(stats.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let base_row_ids = if let Ok(row_ids) = cast_struct_column::<Int64Array>(arr, "baseRowId") {
        Either::Left(row_ids.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let commit_versions =
        if let Ok(versions) = cast_struct_column::<Int64Array>(arr, "defaultRowCommitVersion") {
            Either::Left(versions.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(sizes.len()))
        };

    let deletion_vectors = if let Ok(dvs) = cast_struct_column::<StructArray>(arr, "deletionVector")
    {
        Either::Left(parse_dv(dvs)?)
    } else {
        Either::Right(std::iter::repeat(None).take(sizes.len()))
    };

    let zipped = izip!(
        paths,
        sizes,
        modification_times,
        data_changes,
        partition_values,
        stats,
        tags,
        base_row_ids,
        commit_versions,
        deletion_vectors,
    );
    let zipped = zipped.map(
        |(
            maybe_paths,
            maybe_size,
            maybe_modification_time,
            maybe_data_change,
            partition_values,
            stat,
            tags,
            base_row_id,
            default_row_commit_version,
            deletion_vector,
        )| {
            if let (Some(path), Some(size), Some(modification_time), Some(data_change)) = (
                maybe_paths,
                maybe_size,
                maybe_modification_time,
                maybe_data_change,
            ) {
                Some(Add {
                    path: path.into(),
                    size,
                    modification_time,
                    data_change,
                    partition_values: partition_values.unwrap_or_default(),
                    stats: stat.map(|v| v.to_string()),
                    tags,
                    base_row_id,
                    default_row_commit_version,
                    deletion_vector,
                    stats_parsed: None,
                    partition_values_parsed: None,
                })
            } else {
                None
            }
        },
    );

    Ok(Box::new(zipped.flatten().map(Action::Add)))
}

fn parse_actions_remove(arr: &StructArray) -> DeltaResult<Box<dyn Iterator<Item = Action> + '_>> {
    let paths = cast_struct_column::<StringArray>(arr, "path")?;
    let data_changes = cast_struct_column::<BooleanArray>(arr, "dataChange")?;

    let deletion_timestamps =
        if let Ok(ts) = cast_struct_column::<Int64Array>(arr, "deletionTimestamp") {
            Either::Left(ts.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let extended_file_metadata =
        if let Ok(metas) = cast_struct_column::<BooleanArray>(arr, "extendedFileMetadata") {
            Either::Left(metas.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let partition_values =
        if let Ok(values) = cast_struct_column::<MapArray>(arr, "partitionValues") {
            Either::Left(
                values
                    .iter()
                    .map(|data| data.map(|d| struct_array_to_map(&d).unwrap())),
            )
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let sizes = if let Ok(size) = cast_struct_column::<Int64Array>(arr, "size") {
        Either::Left(size.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let tags = if let Ok(tags) = cast_struct_column::<MapArray>(arr, "tags") {
        Either::Left(
            tags.iter()
                .map(|data| data.map(|d| struct_array_to_map(&d).unwrap())),
        )
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let deletion_vectors = if let Ok(dvs) = cast_struct_column::<StructArray>(arr, "deletionVector")
    {
        Either::Left(parse_dv(dvs)?)
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let base_row_ids = if let Ok(row_ids) = cast_struct_column::<Int64Array>(arr, "baseRowId") {
        Either::Left(row_ids.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(data_changes.len()))
    };

    let commit_versions =
        if let Ok(row_ids) = cast_struct_column::<Int64Array>(arr, "defaultRowCommitVersion") {
            Either::Left(row_ids.into_iter())
        } else {
            Either::Right(std::iter::repeat(None).take(data_changes.len()))
        };

    let zipped = izip!(
        paths,
        data_changes,
        deletion_timestamps,
        extended_file_metadata,
        partition_values,
        sizes,
        tags,
        deletion_vectors,
        base_row_ids,
        commit_versions,
    );

    let zipped = zipped.map(
        |(
            maybe_paths,
            maybe_data_change,
            deletion_timestamp,
            extended_file_metadata,
            partition_values,
            size,
            tags,
            deletion_vector,
            base_row_id,
            default_row_commit_version,
        )| {
            if let (Some(path), Some(data_change)) = (maybe_paths, maybe_data_change) {
                Some(Remove {
                    path: path.into(),
                    data_change,
                    deletion_timestamp,
                    extended_file_metadata,
                    partition_values,
                    size,
                    tags,
                    deletion_vector,
                    base_row_id,
                    default_row_commit_version,
                })
            } else {
                None
            }
        },
    );

    Ok(Box::new(zipped.flatten().map(Action::Remove)))
}

fn parse_dv(
    arr: &StructArray,
) -> DeltaResult<impl Iterator<Item = Option<DeletionVectorDescriptor>> + '_> {
    let storage_types = cast_struct_column::<StringArray>(arr, "storageType")?;
    let paths_or_inlines = cast_struct_column::<StringArray>(arr, "pathOrInlineDv")?;
    let sizes_in_bytes = cast_struct_column::<Int32Array>(arr, "sizeInBytes")?;
    let cardinalities = cast_struct_column::<Int64Array>(arr, "cardinality")?;

    let offsets = if let Ok(offsets) = cast_struct_column::<Int32Array>(arr, "offset") {
        Either::Left(offsets.into_iter())
    } else {
        Either::Right(std::iter::repeat(None).take(cardinalities.len()))
    };

    let zipped = izip!(
        storage_types,
        paths_or_inlines,
        sizes_in_bytes,
        cardinalities,
        offsets,
    );

    Ok(zipped.map(
        |(maybe_type, maybe_path_or_inline_dv, maybe_size_in_bytes, maybe_cardinality, offset)| {
            if let (
                Some(storage_type),
                Some(path_or_inline_dv),
                Some(size_in_bytes),
                Some(cardinality),
            ) = (
                maybe_type,
                maybe_path_or_inline_dv,
                maybe_size_in_bytes,
                maybe_cardinality,
            ) {
                Some(DeletionVectorDescriptor {
                    storage_type: StorageType::from_str(storage_type).unwrap(),
                    path_or_inline_dv: path_or_inline_dv.into(),
                    size_in_bytes,
                    cardinality,
                    offset,
                })
            } else {
                None
            }
        },
    ))
}

fn cast_struct_column<T: 'static>(arr: &StructArray, name: impl AsRef<str>) -> DeltaResult<&T> {
    arr.column_by_name(name.as_ref())
        .ok_or(Error::MissingColumn(name.as_ref().into()))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or(Error::UnexpectedColumnType(
            "Cannot downcast to expected type".into(),
        ))
}

fn struct_array_to_map(arr: &StructArray) -> DeltaResult<HashMap<String, Option<String>>> {
    let keys = cast_struct_column::<StringArray>(arr, "key")?;
    let values = cast_struct_column::<StringArray>(arr, "value")?;
    Ok(keys
        .into_iter()
        .zip(values)
        .filter_map(|(k, v)| k.map(|key| (key.to_string(), v.map(|vv| vv.to_string()))))
        .collect())
}

#[cfg(all(test, feature = "default-client"))]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::actions::Protocol;
    use crate::client::json::DefaultJsonHandler;
    use crate::executor::tokio::TokioBackgroundExecutor;
    use crate::JsonHandler;

    fn action_batch() -> RecordBatch {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let json_strings: StringArray = vec![
            r#"{"add":{"path":"part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet","partitionValues":{},"size":635,"modificationTime":1677811178336,"dataChange":true,"stats":"{\"numRecords\":10,\"minValues\":{\"value\":0},\"maxValues\":{\"value\":9},\"nullCount\":{\"value\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1677811178336000","MIN_INSERTION_TIME":"1677811178336000","MAX_INSERTION_TIME":"1677811178336000","OPTIMIZE_TARGET_SIZE":"268435456"}}}"#,
            r#"{"commitInfo":{"timestamp":1677811178585,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"10","numOutputBytes":"635"},"engineInfo":"Databricks-Runtime/<unknown>","txnId":"a6a94671-55ef-450e-9546-b8465b9147de"}}"#,
            r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#,
            r#"{"metaData":{"id":"testId","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true","delta.columnMapping.mode":"none"},"createdTime":1677811175819}}"#,
        ]
        .into();
        let output_schema = Arc::new(get_log_schema());
        handler.parse_json(json_strings, output_schema).unwrap()
    }

    #[test]
    fn test_parse_protocol() {
        let batch = action_batch();
        let action = parse_action(&batch, &ActionType::Protocol)
            .unwrap()
            .collect::<Vec<_>>();
        let expected = Action::Protocol(Protocol {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(vec!["deletionVectors".into()]),
            writer_features: Some(vec!["deletionVectors".into()]),
        });
        assert_eq!(action[0], expected)
    }

    #[test]
    fn test_parse_metadata() {
        let batch = action_batch();
        let action = parse_action(&batch, &ActionType::Metadata)
            .unwrap()
            .collect::<Vec<_>>();
        let configuration = HashMap::from_iter([
            (
                "delta.enableDeletionVectors".to_string(),
                Some("true".to_string()),
            ),
            (
                "delta.columnMapping.mode".to_string(),
                Some("none".to_string()),
            ),
        ]);
        let expected = Action::Metadata(Metadata {
            id: "testId".into(),
            name: None,
            description: None,
            format: Format {
                provider: "parquet".into(),
                options: Default::default(),
            },
            schema_string: r#"{"type":"struct","fields":[{"name":"value","type":"integer","nullable":true,"metadata":{}}]}"#.to_string(),
            partition_columns: Vec::new(),
            created_time: Some(1677811175819),
            configuration,
        });
        assert_eq!(action[0], expected)
    }

    #[test]
    fn test_parse_add_partitioned() {
        let store = Arc::new(LocalFileSystem::new());
        let handler = DefaultJsonHandler::new(store, Arc::new(TokioBackgroundExecutor::new()));

        let json_strings: StringArray = vec![
            r#"{"commitInfo":{"timestamp":1670892998177,"operation":"WRITE","operationParameters":{"mode":"Append","partitionBy":"[\"c1\",\"c2\"]"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"3","numOutputRows":"3","numOutputBytes":"1356"},"engineInfo":"Apache-Spark/3.3.1 Delta-Lake/2.2.0","txnId":"046a258f-45e3-4657-b0bf-abfb0f76681c"}}"#,
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"aff5cb91-8cd9-4195-aef9-446908507302","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["c1","c2"],"configuration":{},"createdTime":1670892997849}}"#,
            r#"{"add":{"path":"c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet","partitionValues":{"c1":"4","c2":"c"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":5},\"maxValues\":{\"c3\":5},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet","partitionValues":{"c1":"5","c2":"b"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":6},\"maxValues\":{\"c3\":6},\"nullCount\":{\"c3\":0}}"}}"#,
            r#"{"add":{"path":"c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet","partitionValues":{"c1":"6","c2":"a"},"size":452,"modificationTime":1670892998135,"dataChange":true,"stats":"{\"numRecords\":1,\"minValues\":{\"c3\":4},\"maxValues\":{\"c3\":4},\"nullCount\":{\"c3\":0}}"}}"#,
        ]
        .into();
        let output_schema = Arc::new(get_log_schema());
        let batch = handler.parse_json(json_strings, output_schema).unwrap();

        let actions = parse_action(&batch, &ActionType::Add)
            .unwrap()
            .collect::<Vec<_>>();
        println!("{:?}", actions)
    }
}
