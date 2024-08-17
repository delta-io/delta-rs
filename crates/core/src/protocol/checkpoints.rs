//! Implementation for writing delta checkpoints.

use std::collections::HashMap;
use std::iter::Iterator;

use arrow_json::ReaderBuilder;
use arrow_schema::ArrowError;

use chrono::{Datelike, NaiveDate, NaiveDateTime, Utc};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use lazy_static::lazy_static;
use object_store::{Error, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use regex::Regex;
use serde_json::Value;
use tracing::{debug, error};

use super::{time_utils, ProtocolError};
use crate::kernel::arrow::delta_log_schema_for_table;
use crate::kernel::{
    Action, Add as AddAction, DataType, PrimitiveType, Protocol, Remove, StructField,
};
use crate::logstore::LogStore;
use crate::table::state::DeltaTableState;
use crate::table::{get_partition_col_data_types, CheckPoint, CheckPointBuilder};
use crate::{open_table_with_version, DeltaTable};

type SchemaPath = Vec<String>;

/// Error returned when there is an error during creating a checkpoint.
#[derive(thiserror::Error, Debug)]
enum CheckpointError {
    /// Error returned when a string formatted partition value cannot be parsed to its appropriate
    /// data type.
    #[error("Partition value {0} cannot be parsed from string.")]
    PartitionValueNotParseable(String),

    /// Caller attempt to create a checkpoint for a version which does not exist on the table state
    #[error("Attempted to create a checkpoint for a version {0} that does not match the table state {1}")]
    StaleTableVersion(i64, i64),

    /// Error returned when the parquet writer fails while writing the checkpoint.
    #[error("Failed to write parquet: {}", .source)]
    Parquet {
        /// Parquet error details returned when writing the checkpoint failed.
        #[from]
        source: ParquetError,
    },

    /// Error returned when converting the schema to Arrow format failed.
    #[error("Failed to convert into Arrow schema: {}", .source)]
    Arrow {
        /// Arrow error details returned when converting the schema in Arrow format failed
        #[from]
        source: ArrowError,
    },

    #[error("missing required action type in snapshot: {0}")]
    MissingActionType(String),
}

impl From<CheckpointError> for ProtocolError {
    fn from(value: CheckpointError) -> Self {
        match value {
            CheckpointError::PartitionValueNotParseable(_) => Self::InvalidField(value.to_string()),
            CheckpointError::Arrow { source } => Self::Arrow { source },
            CheckpointError::StaleTableVersion(..) => Self::Generic(value.to_string()),
            CheckpointError::Parquet { source } => Self::ParquetParseError { source },
            CheckpointError::MissingActionType(_) => Self::Generic(value.to_string()),
        }
    }
}

use core::str::Utf8Error;
impl From<Utf8Error> for ProtocolError {
    fn from(value: Utf8Error) -> Self {
        Self::Generic(value.to_string())
    }
}

/// The record batch size for checkpoint parquet file
pub const CHECKPOINT_RECORD_BATCH_SIZE: usize = 5000;

/// Creates checkpoint at current table version
pub async fn create_checkpoint(table: &DeltaTable) -> Result<(), ProtocolError> {
    create_checkpoint_for(
        table.version(),
        table.snapshot().map_err(|_| ProtocolError::NoMetaData)?,
        table.log_store.as_ref(),
    )
    .await?;
    Ok(())
}

/// Delete expires log files before given version from table. The table log retention is based on
/// the `logRetentionDuration` property of the Delta Table, 30 days by default.
pub async fn cleanup_metadata(table: &DeltaTable) -> Result<usize, ProtocolError> {
    let log_retention_timestamp = Utc::now().timestamp_millis()
        - table
            .snapshot()
            .map_err(|_| ProtocolError::NoMetaData)?
            .table_config()
            .log_retention_duration()
            .as_millis() as i64;
    cleanup_expired_logs_for(
        table.version(),
        table.log_store.as_ref(),
        log_retention_timestamp,
    )
    .await
}

/// Loads table from given `table_uri` at given `version` and creates checkpoint for it.
/// The `cleanup` param decides whether to run metadata cleanup of obsolete logs.
/// If it's empty then the table's `enableExpiredLogCleanup` is used.
pub async fn create_checkpoint_from_table_uri_and_cleanup(
    table_uri: &str,
    version: i64,
    cleanup: Option<bool>,
) -> Result<(), ProtocolError> {
    let table = open_table_with_version(table_uri, version)
        .await
        .map_err(|err| ProtocolError::Generic(err.to_string()))?;
    let snapshot = table.snapshot().map_err(|_| ProtocolError::NoMetaData)?;
    create_checkpoint_for(version, snapshot, table.log_store.as_ref()).await?;

    let enable_expired_log_cleanup =
        cleanup.unwrap_or_else(|| snapshot.table_config().enable_expired_log_cleanup());

    if table.version() >= 0 && enable_expired_log_cleanup {
        let deleted_log_num = cleanup_metadata(&table).await?;
        debug!("Deleted {:?} log files.", deleted_log_num);
    }

    Ok(())
}

/// Creates checkpoint for a given table version, table state and object store
pub async fn create_checkpoint_for(
    version: i64,
    state: &DeltaTableState,
    log_store: &dyn LogStore,
) -> Result<(), ProtocolError> {
    if version != state.version() {
        error!(
            "create_checkpoint_for called with version {version} but table state contains: {}. The table state may need to be reloaded",
            state.version()
        );
        return Err(CheckpointError::StaleTableVersion(version, state.version()).into());
    }

    // TODO: checkpoints _can_ be multi-part... haven't actually found a good reference for
    // an appropriate split point yet though so only writing a single part currently.
    // See https://github.com/delta-io/delta-rs/issues/288
    let last_checkpoint_path = log_store.log_path().child("_last_checkpoint");

    debug!("Writing parquet bytes to checkpoint buffer.");
    let tombstones = state
        .unexpired_tombstones(log_store.object_store().clone())
        .await
        .map_err(|_| ProtocolError::Generic("filed to get tombstones".into()))?
        .collect::<Vec<_>>();
    let (checkpoint, parquet_bytes) = parquet_bytes_from_state(state, tombstones)?;

    let file_name = format!("{version:020}.checkpoint.parquet");
    let checkpoint_path = log_store.log_path().child(file_name);

    let object_store = log_store.object_store();
    debug!("Writing checkpoint to {:?}.", checkpoint_path);
    object_store
        .put(&checkpoint_path, parquet_bytes.into())
        .await?;

    let last_checkpoint_content: Value = serde_json::to_value(checkpoint)?;
    let last_checkpoint_content = bytes::Bytes::from(serde_json::to_vec(&last_checkpoint_content)?);

    debug!("Writing _last_checkpoint to {:?}.", last_checkpoint_path);
    object_store
        .put(&last_checkpoint_path, last_checkpoint_content.into())
        .await?;

    Ok(())
}

/// Deletes all delta log commits that are older than the cutoff time
/// and less than the specified version.
pub async fn cleanup_expired_logs_for(
    until_version: i64,
    log_store: &dyn LogStore,
    cutoff_timestamp: i64,
) -> Result<usize, ProtocolError> {
    lazy_static! {
        static ref DELTA_LOG_REGEX: Regex =
            Regex::new(r"_delta_log/(\d{20})\.(json|checkpoint|json.tmp).*$").unwrap();
    }

    let object_store = log_store.object_store();
    let maybe_last_checkpoint = object_store
        .get(&log_store.log_path().child("_last_checkpoint"))
        .await;

    if let Err(Error::NotFound { path: _, source: _ }) = maybe_last_checkpoint {
        return Ok(0);
    }

    let last_checkpoint = maybe_last_checkpoint?.bytes().await?;
    let last_checkpoint: CheckPoint = serde_json::from_slice(&last_checkpoint)?;
    let until_version = i64::min(until_version, last_checkpoint.version);

    // Feed a stream of candidate deletion files directly into the delete_stream
    // function to try to improve the speed of cleanup and reduce the need for
    // intermediate memory.
    let object_store = log_store.object_store();
    let deleted = object_store
        .delete_stream(
            object_store
                .list(Some(log_store.log_path()))
                // This predicate function will filter out any locations that don't
                // match the given timestamp range
                .filter_map(|meta: Result<crate::ObjectMeta, _>| async move {
                    if meta.is_err() {
                        error!("Error received while cleaning up expired logs: {:?}", meta);
                        return None;
                    }
                    let meta = meta.unwrap();
                    let ts = meta.last_modified.timestamp_millis();

                    match DELTA_LOG_REGEX.captures(meta.location.as_ref()) {
                        Some(captures) => {
                            let log_ver_str = captures.get(1).unwrap().as_str();
                            let log_ver: i64 = log_ver_str.parse().unwrap();
                            if log_ver < until_version && ts <= cutoff_timestamp {
                                // This location is ready to be deleted
                                Some(Ok(meta.location))
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                })
                .boxed(),
        )
        .try_collect::<Vec<_>>()
        .await?;

    debug!("Deleted {} expired logs", deleted.len());
    Ok(deleted.len())
}

fn parquet_bytes_from_state(
    state: &DeltaTableState,
    mut tombstones: Vec<Remove>,
) -> Result<(CheckPoint, bytes::Bytes), ProtocolError> {
    let current_metadata = state.metadata();
    let schema = current_metadata.schema()?;

    let partition_col_data_types = get_partition_col_data_types(&schema, current_metadata);

    // Collect a map of paths that require special stats conversion.
    let mut stats_conversions: Vec<(SchemaPath, DataType)> = Vec::new();
    let fields = schema.fields().collect_vec();
    collect_stats_conversions(&mut stats_conversions, fields.as_slice());

    // if any, tombstones do not include extended file metadata, we must omit the extended metadata fields from the remove schema
    // See https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file
    //
    // DBR version 8.x and greater have different behaviors of reading the parquet file depending
    // on the `extended_file_metadata` flag, hence this is safer to set `extended_file_metadata=false`
    // and omit metadata columns if at least one remove action has `extended_file_metadata=false`.
    // We've added the additional check on `size.is_some` because in delta-spark the primitive long type
    // is used, hence we want to omit possible errors when `extended_file_metadata=true`, but `size=null`
    let use_extended_remove_schema = tombstones
        .iter()
        .all(|r| r.extended_file_metadata == Some(true) && r.size.is_some());

    // If use_extended_remove_schema=false for some of the tombstones, then it should be for each.
    if !use_extended_remove_schema {
        for remove in tombstones.iter_mut() {
            remove.extended_file_metadata = Some(false);
        }
    }
    let files = state.file_actions().unwrap();
    // protocol
    let jsons = std::iter::once(Action::Protocol(Protocol {
        min_reader_version: state.protocol().min_reader_version,
        min_writer_version: state.protocol().min_writer_version,
        writer_features: if state.protocol().min_writer_version >= 7 {
            Some(state.protocol().writer_features.clone().unwrap_or_default())
        } else {
            None
        },
        reader_features: if state.protocol().min_reader_version >= 3 {
            Some(state.protocol().reader_features.clone().unwrap_or_default())
        } else {
            None
        },
    }))
    // metaData
    .chain(std::iter::once(Action::Metadata(current_metadata.clone())))
    // txns
    .chain(
        state
            .app_transaction_version()
            .map_err(|_| CheckpointError::MissingActionType("txn".to_string()))?
            .map(Action::Txn),
    )
    // removes
    .chain(tombstones.iter().map(|r| {
        let mut r = (*r).clone();

        // As a "new writer", we should always set `extendedFileMetadata` when writing, and include/ignore the other three fields accordingly.
        // https://github.com/delta-io/delta/blob/fb0452c2fb142310211c6d3604eefb767bb4a134/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L311-L314
        if r.extended_file_metadata.is_none() {
            r.extended_file_metadata = Some(false);
        }

        Action::Remove(r)
    }))
    .map(|a| serde_json::to_value(a).map_err(ProtocolError::from))
    // adds
    .chain(files.iter().map(|f| {
        checkpoint_add_from_state(f, partition_col_data_types.as_slice(), &stats_conversions)
    }));

    // Create the arrow schema that represents the Checkpoint parquet file.
    let arrow_schema = delta_log_schema_for_table(
        (&schema).try_into()?,
        current_metadata.partition_columns.as_slice(),
        use_extended_remove_schema,
    );

    debug!("Writing to checkpoint parquet buffer...");
    // Write the Checkpoint parquet file.
    let mut bytes = vec![];
    let mut writer = ArrowWriter::try_new(
        &mut bytes,
        arrow_schema.clone(),
        Some(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        ),
    )?;
    let mut decoder = ReaderBuilder::new(arrow_schema)
        .with_batch_size(CHECKPOINT_RECORD_BATCH_SIZE)
        .build_decoder()?;
    let jsons = jsons.collect::<Result<Vec<serde_json::Value>, _>>()?;
    decoder.serialize(&jsons)?;

    while let Some(batch) = decoder.flush()? {
        writer.write(&batch)?;
    }

    let _ = writer.close()?;
    debug!("Finished writing checkpoint parquet buffer.");

    let checkpoint = CheckPointBuilder::new(state.version(), jsons.len() as i64)
        .with_size_in_bytes(bytes.len() as i64)
        .build();
    Ok((checkpoint, bytes::Bytes::from(bytes)))
}

fn checkpoint_add_from_state(
    add: &AddAction,
    partition_col_data_types: &[(&String, &DataType)],
    stats_conversions: &[(SchemaPath, DataType)],
) -> Result<Value, ProtocolError> {
    let mut v = serde_json::to_value(Action::Add(add.clone()))
        .map_err(|err| ArrowError::JsonError(err.to_string()))?;

    v["add"]["dataChange"] = Value::Bool(false);

    if !add.partition_values.is_empty() {
        let mut partition_values_parsed: HashMap<String, Value> = HashMap::new();

        for (field_name, data_type) in partition_col_data_types.iter() {
            if let Some(string_value) = add.partition_values.get(*field_name) {
                let v = typed_partition_value_from_option_string(string_value, data_type)?;

                partition_values_parsed.insert(field_name.to_string(), v);
            }
        }

        let partition_values_parsed = serde_json::to_value(partition_values_parsed)
            .map_err(|err| ArrowError::JsonError(err.to_string()))?;
        v["add"]["partitionValues_parsed"] = partition_values_parsed;
    }

    if let Ok(Some(stats)) = add.get_stats() {
        let mut stats =
            serde_json::to_value(stats).map_err(|err| ArrowError::JsonError(err.to_string()))?;
        let min_values = stats.get_mut("minValues").and_then(|v| v.as_object_mut());

        if let Some(min_values) = min_values {
            for (path, data_type) in stats_conversions {
                apply_stats_conversion(min_values, path.as_slice(), data_type)
            }
        }

        let max_values = stats.get_mut("maxValues").and_then(|v| v.as_object_mut());
        if let Some(max_values) = max_values {
            for (path, data_type) in stats_conversions {
                apply_stats_conversion(max_values, path.as_slice(), data_type)
            }
        }

        v["add"]["stats_parsed"] = stats;
    }
    Ok(v)
}

fn typed_partition_value_from_string(
    string_value: &str,
    data_type: &DataType,
) -> Result<Value, ProtocolError> {
    match data_type {
        DataType::Primitive(primitive_type) => match primitive_type {
            PrimitiveType::String | PrimitiveType::Binary => Ok(string_value.to_owned().into()),
            PrimitiveType::Long
            | PrimitiveType::Integer
            | PrimitiveType::Short
            | PrimitiveType::Byte => Ok(string_value
                .parse::<i64>()
                .map_err(|_| CheckpointError::PartitionValueNotParseable(string_value.to_owned()))?
                .into()),
            PrimitiveType::Boolean => Ok(string_value
                .parse::<bool>()
                .map_err(|_| CheckpointError::PartitionValueNotParseable(string_value.to_owned()))?
                .into()),
            PrimitiveType::Float | PrimitiveType::Double => Ok(string_value
                .parse::<f64>()
                .map_err(|_| CheckpointError::PartitionValueNotParseable(string_value.to_owned()))?
                .into()),
            PrimitiveType::Date => {
                let d = NaiveDate::parse_from_str(string_value, "%Y-%m-%d").map_err(|_| {
                    CheckpointError::PartitionValueNotParseable(string_value.to_owned())
                })?;
                // day 0 is 1970-01-01 (719163 days from ce)
                Ok((d.num_days_from_ce() - 719_163).into())
            }
            PrimitiveType::Timestamp | PrimitiveType::TimestampNtz => {
                let ts = NaiveDateTime::parse_from_str(string_value, "%Y-%m-%d %H:%M:%S.%6f");
                let ts: NaiveDateTime = match ts {
                    Ok(_) => ts,
                    Err(_) => NaiveDateTime::parse_from_str(string_value, "%Y-%m-%d %H:%M:%S"),
                }
                .map_err(|_| {
                    CheckpointError::PartitionValueNotParseable(string_value.to_owned())
                })?;
                Ok((ts.and_utc().timestamp_millis() * 1000).into())
            }
            s => unimplemented!(
                "Primitive type {} is not supported for partition column values.",
                s
            ),
        },
        d => unimplemented!(
            "Data type {:?} is not supported for partition column values.",
            d
        ),
    }
}

fn typed_partition_value_from_option_string(
    string_value: &Option<String>,
    data_type: &DataType,
) -> Result<Value, ProtocolError> {
    match string_value {
        Some(s) => {
            if s.is_empty() {
                Ok(Value::Null) // empty string should be deserialized as null
            } else {
                typed_partition_value_from_string(s, data_type)
            }
        }
        None => Ok(Value::Null),
    }
}

fn collect_stats_conversions(paths: &mut Vec<(SchemaPath, DataType)>, fields: &[&StructField]) {
    let mut _path = SchemaPath::new();
    fields
        .iter()
        .for_each(|f| collect_field_conversion(&mut _path, paths, f));
}

fn collect_field_conversion(
    current_path: &mut SchemaPath,
    all_paths: &mut Vec<(SchemaPath, DataType)>,
    field: &StructField,
) {
    match field.data_type() {
        DataType::Primitive(PrimitiveType::Timestamp) => {
            let mut key_path = current_path.clone();
            key_path.push(field.name().to_owned());
            all_paths.push((key_path, field.data_type().to_owned()));
        }
        DataType::Struct(struct_field) => {
            let struct_fields = struct_field.fields();
            current_path.push(field.name().to_owned());
            struct_fields.for_each(|f| collect_field_conversion(current_path, all_paths, f));
            current_path.pop();
        }
        _ => { /* noop */ }
    }
}

fn apply_stats_conversion(
    context: &mut serde_json::Map<String, Value>,
    path: &[String],
    data_type: &DataType,
) {
    if path.len() == 1 {
        if let DataType::Primitive(PrimitiveType::Timestamp) = data_type {
            let v = context.get_mut(&path[0]);

            if let Some(v) = v {
                let ts = v
                    .as_str()
                    .and_then(|s| time_utils::timestamp_micros_from_stats_string(s).ok())
                    .map(|n| Value::Number(serde_json::Number::from(n)));

                if let Some(ts) = ts {
                    *v = ts;
                }
            }
        }
    } else {
        let next_context = context.get_mut(&path[0]).and_then(|v| v.as_object_mut());
        if let Some(next_context) = next_context {
            apply_stats_conversion(next_context, &path[1..], data_type);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, StructBuilder};
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::Schema as ArrowSchema;
    use chrono::Duration;
    use lazy_static::lazy_static;
    use object_store::path::Path;
    use serde_json::json;

    use super::*;
    use crate::kernel::StructType;
    use crate::operations::transaction::{CommitBuilder, TableReference};
    use crate::operations::DeltaOps;
    use crate::protocol::Metadata;
    use crate::writer::test_utils::get_delta_schema;

    #[tokio::test]
    async fn test_create_checkpoint_for() {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(crate::protocol::SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_schema().unwrap(), &table_schema);
        let res =
            create_checkpoint_for(0, table.snapshot().unwrap(), table.log_store.as_ref()).await;
        assert!(res.is_ok());

        // Look at the "files" and verify that the _last_checkpoint has the right version
        let path = Path::from("_delta_log/_last_checkpoint");
        let last_checkpoint = table
            .object_store()
            .get(&path)
            .await
            .expect("Failed to get the _last_checkpoint")
            .bytes()
            .await
            .expect("Failed to get bytes for _last_checkpoint");
        let last_checkpoint: CheckPoint = serde_json::from_slice(&last_checkpoint).expect("Fail");
        assert_eq!(last_checkpoint.version, 0);
    }

    /// This test validates that a checkpoint can be written and re-read with the minimum viable
    /// Metadata. There was a bug which didn't handle the optionality of createdTime.
    #[tokio::test]
    async fn test_create_checkpoint_with_metadata() {
        let table_schema = get_delta_schema();

        let mut table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(crate::protocol::SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_schema().unwrap(), &table_schema);

        let part_cols: Vec<String> = vec![];
        let metadata = Metadata::try_new(table_schema, part_cols, HashMap::new()).unwrap();
        let actions = vec![Action::Metadata(metadata)];

        let epoch_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;

        let operation = crate::protocol::DeltaOperation::StreamingUpdate {
            output_mode: crate::protocol::OutputMode::Append,
            query_id: "test".into(),
            epoch_id,
        };
        let v = CommitBuilder::default()
            .with_actions(actions)
            .build(
                table.state.as_ref().map(|f| f as &dyn TableReference),
                table.log_store(),
                operation,
            )
            .await
            .unwrap()
            .version();

        assert_eq!(1, v, "Expected the commit to create table version 1");
        table.load().await.expect("Failed to reload table");
        assert_eq!(
            table.version(),
            1,
            "The loaded version of the table is not up to date"
        );

        let res = create_checkpoint_for(
            table.version(),
            table.state.as_ref().unwrap(),
            table.log_store.as_ref(),
        )
        .await;
        assert!(res.is_ok());

        // Look at the "files" and verify that the _last_checkpoint has the right version
        let path = Path::from("_delta_log/_last_checkpoint");
        let last_checkpoint = table
            .object_store()
            .get(&path)
            .await
            .expect("Failed to get the _last_checkpoint")
            .bytes()
            .await
            .expect("Failed to get bytes for _last_checkpoint");
        let last_checkpoint: CheckPoint = serde_json::from_slice(&last_checkpoint).expect("Fail");
        assert_eq!(last_checkpoint.version, 1);

        // If the regression exists, this will fail
        table.load().await.expect("Failed to reload the table, this likely means that the optional createdTime was not actually optional");
        assert_eq!(
            1,
            table.version(),
            "The reloaded table doesn't have the right version"
        );
    }

    #[tokio::test]
    async fn test_create_checkpoint_for_invalid_version() {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(crate::protocol::SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_schema().unwrap(), &table_schema);
        match create_checkpoint_for(1, table.snapshot().unwrap(), table.log_store.as_ref()).await {
            Ok(_) => {
                /*
                 * If a checkpoint is allowed to be created here, it will use the passed in
                 * version, but _last_checkpoint is generated from the table state will point to a
                 * version 0 checkpoint.
                 * E.g.
                 *
                 * Path { raw: "_delta_log/00000000000000000000.json" }
                 * Path { raw: "_delta_log/00000000000000000001.checkpoint.parquet" }
                 * Path { raw: "_delta_log/_last_checkpoint" }
                 *
                 */
                panic!(
                    "We should not allow creating a checkpoint for a version which doesn't exist!"
                );
            }
            Err(_) => { /* We should expect an error in the "right" case */ }
        }
    }

    #[test]
    fn typed_partition_value_from_string_test() {
        let string_value: Value = "Hello World!".into();
        assert_eq!(
            string_value,
            typed_partition_value_from_option_string(
                &Some("Hello World!".to_string()),
                &DataType::Primitive(PrimitiveType::String),
            )
            .unwrap()
        );

        let bool_value: Value = true.into();
        assert_eq!(
            bool_value,
            typed_partition_value_from_option_string(
                &Some("true".to_string()),
                &DataType::Primitive(PrimitiveType::Boolean),
            )
            .unwrap()
        );

        let number_value: Value = 42.into();
        assert_eq!(
            number_value,
            typed_partition_value_from_option_string(
                &Some("42".to_string()),
                &DataType::Primitive(PrimitiveType::Integer),
            )
            .unwrap()
        );

        for (s, v) in [
            ("2021-08-08", 18_847),
            ("1970-01-02", 1),
            ("1970-01-01", 0),
            ("1969-12-31", -1),
            ("1-01-01", -719_162),
        ] {
            let date_value: Value = v.into();
            assert_eq!(
                date_value,
                typed_partition_value_from_option_string(
                    &Some(s.to_string()),
                    &DataType::Primitive(PrimitiveType::Date),
                )
                .unwrap()
            );
        }

        for (s, v) in [
            ("2021-08-08 01:00:01.000000", 1628384401000000i64),
            ("2021-08-08 01:00:01", 1628384401000000i64),
            ("1970-01-02 12:59:59.000000", 133199000000i64),
            ("1970-01-02 12:59:59", 133199000000i64),
            ("1970-01-01 13:00:01.000000", 46801000000i64),
            ("1970-01-01 13:00:01", 46801000000i64),
            ("1969-12-31 00:00:00", -86400000000i64),
            ("1677-09-21 00:12:44", -9223372036000000i64),
        ] {
            let timestamp_value: Value = v.into();
            assert_eq!(
                timestamp_value,
                typed_partition_value_from_option_string(
                    &Some(s.to_string()),
                    &DataType::Primitive(PrimitiveType::Timestamp),
                )
                .unwrap()
            );
        }

        let binary_value: Value = "\u{2081}\u{2082}\u{2083}\u{2084}".into();
        assert_eq!(
            binary_value,
            typed_partition_value_from_option_string(
                &Some("₁₂₃₄".to_string()),
                &DataType::Primitive(PrimitiveType::Binary),
            )
            .unwrap()
        );
    }

    #[test]
    fn null_partition_value_from_string_test() {
        assert_eq!(
            Value::Null,
            typed_partition_value_from_option_string(
                &None,
                &DataType::Primitive(PrimitiveType::Integer),
            )
            .unwrap()
        );

        // empty string should be treated as null
        assert_eq!(
            Value::Null,
            typed_partition_value_from_option_string(
                &Some("".to_string()),
                &DataType::Primitive(PrimitiveType::Integer),
            )
            .unwrap()
        );
    }

    #[test]
    fn collect_stats_conversions_test() {
        let delta_schema: StructType = serde_json::from_value(SCHEMA.clone()).unwrap();
        let fields = delta_schema.fields().collect_vec();
        let mut paths = Vec::new();
        collect_stats_conversions(&mut paths, fields.as_slice());

        assert_eq!(2, paths.len());
        assert_eq!(
            (
                vec!["some_struct".to_string(), "struct_timestamp".to_string()],
                DataType::Primitive(PrimitiveType::Timestamp)
            ),
            paths[0]
        );
        assert_eq!(
            (
                vec!["some_timestamp".to_string()],
                DataType::Primitive(PrimitiveType::Timestamp)
            ),
            paths[1]
        );
    }

    async fn setup_table() -> DeltaTable {
        use arrow_schema::{DataType, Field};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Utf8,
            false,
        )]));

        let data =
            vec![Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])) as ArrayRef];
        let batches = vec![RecordBatch::try_new(schema.clone(), data).unwrap()];

        let table = DeltaOps::new_in_memory()
            .write(batches.clone())
            .await
            .unwrap();

        DeltaOps(table)
            .write(batches)
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_cleanup_no_checkpoints() {
        // Test that metadata clean up does not corrupt the table when no checkpoints exist
        let table = setup_table().await;

        let log_retention_timestamp = (Utc::now().timestamp_millis()
            + Duration::days(31).num_milliseconds())
            - table
                .snapshot()
                .unwrap()
                .table_config()
                .log_retention_duration()
                .as_millis() as i64;
        let count = cleanup_expired_logs_for(
            table.version(),
            table.log_store().as_ref(),
            log_retention_timestamp,
        )
        .await
        .unwrap();
        assert_eq!(count, 0);
        println!("{:?}", count);

        let path = Path::from("_delta_log/00000000000000000000.json");
        let res = table.log_store().object_store().get(&path).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_cleanup_with_checkpoints() {
        let table = setup_table().await;
        create_checkpoint(&table).await.unwrap();

        let log_retention_timestamp = (Utc::now().timestamp_millis()
            + Duration::days(32).num_milliseconds())
            - table
                .snapshot()
                .unwrap()
                .table_config()
                .log_retention_duration()
                .as_millis() as i64;
        let count = cleanup_expired_logs_for(
            table.version(),
            table.log_store().as_ref(),
            log_retention_timestamp,
        )
        .await
        .unwrap();
        assert_eq!(count, 1);

        let log_store = table.log_store();

        let path = log_store.log_path().child("00000000000000000000.json");
        let res = table.log_store().object_store().get(&path).await;
        assert!(res.is_err());

        let path = log_store
            .log_path()
            .child("00000000000000000001.checkpoint.parquet");
        let res = table.log_store().object_store().get(&path).await;
        assert!(res.is_ok());

        let path = log_store.log_path().child("00000000000000000001.json");
        let res = table.log_store().object_store().get(&path).await;
        assert!(res.is_ok());
    }

    #[test]
    fn apply_stats_conversion_test() {
        let mut stats = STATS_JSON.clone();

        let min_values = stats.get_mut("minValues").unwrap().as_object_mut().unwrap();

        apply_stats_conversion(
            min_values,
            &["some_struct".to_string(), "struct_string".to_string()],
            &DataType::Primitive(PrimitiveType::String),
        );
        apply_stats_conversion(
            min_values,
            &["some_struct".to_string(), "struct_timestamp".to_string()],
            &DataType::Primitive(PrimitiveType::Timestamp),
        );
        apply_stats_conversion(
            min_values,
            &["some_string".to_string()],
            &DataType::Primitive(PrimitiveType::String),
        );
        apply_stats_conversion(
            min_values,
            &["some_timestamp".to_string()],
            &DataType::Primitive(PrimitiveType::Timestamp),
        );

        let max_values = stats.get_mut("maxValues").unwrap().as_object_mut().unwrap();

        apply_stats_conversion(
            max_values,
            &["some_struct".to_string(), "struct_string".to_string()],
            &DataType::Primitive(PrimitiveType::String),
        );
        apply_stats_conversion(
            max_values,
            &["some_struct".to_string(), "struct_timestamp".to_string()],
            &DataType::Primitive(PrimitiveType::Timestamp),
        );
        apply_stats_conversion(
            max_values,
            &["some_string".to_string()],
            &DataType::Primitive(PrimitiveType::String),
        );
        apply_stats_conversion(
            max_values,
            &["some_timestamp".to_string()],
            &DataType::Primitive(PrimitiveType::Timestamp),
        );

        // minValues
        assert_eq!(
            "A",
            stats["minValues"]["some_struct"]["struct_string"]
                .as_str()
                .unwrap()
        );
        assert_eq!(
            1627668684594000i64,
            stats["minValues"]["some_struct"]["struct_timestamp"]
                .as_i64()
                .unwrap()
        );
        assert_eq!("P", stats["minValues"]["some_string"].as_str().unwrap());
        assert_eq!(
            1627668684594000i64,
            stats["minValues"]["some_timestamp"].as_i64().unwrap()
        );

        // maxValues
        assert_eq!(
            "B",
            stats["maxValues"]["some_struct"]["struct_string"]
                .as_str()
                .unwrap()
        );
        assert_eq!(
            1627668685594000i64,
            stats["maxValues"]["some_struct"]["struct_timestamp"]
                .as_i64()
                .unwrap()
        );
        assert_eq!("Q", stats["maxValues"]["some_string"].as_str().unwrap());
        assert_eq!(
            1627668685594000i64,
            stats["maxValues"]["some_timestamp"].as_i64().unwrap()
        );
    }

    #[tokio::test]
    async fn test_struct_with_single_list_field() {
        // you need another column otherwise the entire stats struct is empty
        // which also fails parquet write during checkpoint
        let other_column_array: ArrayRef = Arc::new(Int32Array::from(vec![1]));

        let mut list_item_builder = Int32Builder::new();
        list_item_builder.append_value(1);

        let mut list_in_struct_builder = ListBuilder::new(list_item_builder);
        list_in_struct_builder.append(true);

        let mut struct_builder = StructBuilder::new(
            vec![arrow_schema::Field::new(
                "list_in_struct",
                arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Int32,
                    true,
                ))),
                true,
            )],
            vec![Box::new(list_in_struct_builder)],
        );
        struct_builder.append(true);

        let struct_with_list_array: ArrayRef = Arc::new(struct_builder.finish());
        let batch = RecordBatch::try_from_iter(vec![
            ("other_column", other_column_array),
            ("struct_with_list", struct_with_list_array),
        ])
        .unwrap();
        let table = DeltaOps::new_in_memory().write(vec![batch]).await.unwrap();

        create_checkpoint(&table).await.unwrap();
    }

    lazy_static! {
        static ref SCHEMA: Value = json!({
            "type": "struct",
            "fields": [
                {
                    "name": "some_struct",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "struct_string",
                                "type": "string",
                                "nullable": true, "metadata": {}
                            },
                            {
                                "name": "struct_timestamp",
                                "type": "timestamp",
                                "nullable": true, "metadata": {}
                            }]
                    },
                    "nullable": true, "metadata": {}
                },
                { "name": "some_string", "type": "string", "nullable": true, "metadata": {} },
                { "name": "some_timestamp", "type": "timestamp", "nullable": true, "metadata": {} },
            ]
        });
        static ref STATS_JSON: Value = json!({
            "minValues": {
                "some_struct": {
                    "struct_string": "A",
                    "struct_timestamp": "2021-07-30T18:11:24.594Z"
                },
                "some_string": "P",
                "some_timestamp": "2021-07-30T18:11:24.594Z"
            },
            "maxValues": {
                "some_struct": {
                    "struct_string": "B",
                    "struct_timestamp": "2021-07-30T18:11:25.594Z"
                },
                "some_string": "Q",
                "some_timestamp": "2021-07-30T18:11:25.594Z"
            }
        });
    }
}
