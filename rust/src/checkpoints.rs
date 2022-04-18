//! Implementation for writing delta checkpoints.

use arrow::datatypes::Schema as ArrowSchema;
use arrow::error::ArrowError;
use arrow::json::reader::Decoder;
use chrono::Datelike;
use chrono::Duration;
use chrono::Utc;
use chrono::MIN_DATETIME;
use futures::StreamExt;
use lazy_static::lazy_static;
use log::*;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::writer::InMemoryWriteableCursor;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::Iterator;
use std::ops::Add;

use super::action;
use super::delta_arrow::delta_log_schema_for_table;
use super::open_table_with_version;
use super::schema::*;
use super::storage::{ObjectMeta, StorageBackend, StorageError};
use super::table_state::DeltaTableState;
use super::time_utils;
use super::{CheckPoint, DeltaTableError};
use crate::DeltaTable;

/// Error returned when there is an error during creating a checkpoint.
#[derive(thiserror::Error, Debug)]
pub enum CheckpointError {
    /// Error returned when the DeltaTableState does not contain a metadata action.
    #[error("DeltaTableMetadata not present in DeltaTableState")]
    MissingMetaData,
    /// Error returned when a string formatted partition value cannot be parsed to its appropriate
    /// data type.
    #[error("Partition value {0} cannot be parsed from string.")]
    PartitionValueNotParseable(String),
    /// Passthrough error returned when calling DeltaTable.
    #[error("DeltaTableError: {source}")]
    DeltaTable {
        /// The source DeltaTableError.
        #[from]
        source: DeltaTableError,
    },
    /// Error returned when the parquet writer fails while writing the checkpoint.
    #[error("Failed to write parquet: {}", .source)]
    ParquetError {
        /// Parquet error details returned when writing the checkpoint failed.
        #[from]
        source: ParquetError,
    },
    /// Error returned when converting the schema to Arrow format failed.
    #[error("Failed to convert into Arrow schema: {}", .source)]
    ArrowError {
        /// Arrow error details returned when converting the schema in Arrow format failed
        #[from]
        source: ArrowError,
    },
    /// Passthrough error returned when calling StorageBackend.
    #[error("StorageError: {source}")]
    Storage {
        /// The source StorageError.
        #[from]
        source: StorageError,
    },
    /// Passthrough error returned by serde_json.
    #[error("serde_json::Error: {source}")]
    JSONSerialization {
        /// The source serde_json::Error.
        #[from]
        source: serde_json::Error,
    },
}

/// The record batch size for checkpoint parquet file
pub const CHECKPOINT_RECORD_BATCH_SIZE: usize = 5000;

impl From<CheckpointError> for ArrowError {
    fn from(error: CheckpointError) -> Self {
        ArrowError::from_external_error(Box::new(error))
    }
}

/// Creates checkpoint at `table.version` for given `table`.
pub async fn create_checkpoint(table: &DeltaTable) -> Result<(), CheckpointError> {
    create_checkpoint_for(
        table.version,
        table.get_state(),
        table.storage.as_ref(),
        &table.table_uri,
    )
    .await?;

    Ok(())
}

/// Delete expires log files before given version from table. The table log retention is based on
/// the `logRetentionDuration` property of the Delta Table, 30 days by default.
pub async fn cleanup_metadata(table: &DeltaTable) -> Result<i32, DeltaTableError> {
    let log_retention_timestamp =
        Utc::now().timestamp_millis() - table.get_state().log_retention_millis();
    cleanup_expired_logs_for(
        table.version + 1,
        table.storage.as_ref(),
        log_retention_timestamp,
        &table.table_uri,
    )
    .await
}

/// Loads table from given `table_uri` at given `version` and creates checkpoint for it.
/// The `cleanup` param decides whether to run metadata cleanup of obsolete logs.
/// If it's empty then the table's `enableExpiredLogCleanup` is used.
pub async fn create_checkpoint_from_table_uri_and_cleanup(
    table_uri: &str,
    version: DeltaDataTypeVersion,
    cleanup: Option<bool>,
) -> Result<(), CheckpointError> {
    let table = open_table_with_version(table_uri, version).await?;
    create_checkpoint_for(
        version,
        table.get_state(),
        table.storage.as_ref(),
        table_uri,
    )
    .await?;

    let enable_expired_log_cleanup =
        cleanup.unwrap_or_else(|| table.get_state().enable_expired_log_cleanup());

    if table.version >= 0 && enable_expired_log_cleanup {
        let deleted_log_num = cleanup_metadata(&table).await?;
        debug!("Deleted {:?} log files.", deleted_log_num);
    }

    Ok(())
}

async fn create_checkpoint_for(
    version: DeltaDataTypeVersion,
    state: &DeltaTableState,
    storage: &dyn StorageBackend,
    table_uri: &str,
) -> Result<(), CheckpointError> {
    // TODO: checkpoints _can_ be multi-part... haven't actually found a good reference for
    // an appropriate split point yet though so only writing a single part currently.
    // See https://github.com/delta-io/delta-rs/issues/288

    let delta_log_uri = storage.join_path(table_uri, "_delta_log");
    let last_checkpoint_uri = storage.join_path(&delta_log_uri, "_last_checkpoint");

    debug!("Writing parquet bytes to checkpoint buffer.");
    let parquet_bytes = parquet_bytes_from_state(state)?;

    let size = parquet_bytes.len() as i64;

    let checkpoint = CheckPoint::new(version, size, None);

    let file_name = format!("{:020}.checkpoint.parquet", version);
    let checkpoint_uri = storage.join_path(&delta_log_uri, &file_name);

    debug!("Writing checkpoint to {:?}.", checkpoint_uri);
    storage.put_obj(&checkpoint_uri, &parquet_bytes).await?;

    let last_checkpoint_content: Value = serde_json::to_value(&checkpoint)?;
    let last_checkpoint_content = serde_json::to_string(&last_checkpoint_content)?;

    debug!("Writing _last_checkpoint to {:?}.", last_checkpoint_uri);
    storage
        .put_obj(&last_checkpoint_uri, last_checkpoint_content.as_bytes())
        .await?;

    Ok(())
}

async fn flush_delete_files<T: Fn(&(DeltaDataTypeVersion, ObjectMeta)) -> bool>(
    storage: &dyn StorageBackend,
    maybe_delete_files: &mut Vec<(DeltaDataTypeVersion, ObjectMeta)>,
    files_to_delete: &mut Vec<(DeltaDataTypeVersion, ObjectMeta)>,
    should_delete_file: T,
) -> Result<i32, DeltaTableError> {
    if !maybe_delete_files.is_empty() && should_delete_file(maybe_delete_files.last().unwrap()) {
        files_to_delete.append(maybe_delete_files);
    }

    let deleted = files_to_delete
        .iter_mut()
        .map(|file| async move {
            match storage.delete_obj(&file.1.path).await {
                Ok(_) => Ok(1),
                Err(e) => Err(DeltaTableError::from(e)),
            }
        })
        .collect::<Vec<_>>();

    let mut deleted_num = 0;
    for x in deleted {
        match x.await {
            Ok(_) => deleted_num += 1,
            Err(e) => return Err(e),
        }
    }

    files_to_delete.clear();

    Ok(deleted_num)
}

async fn cleanup_expired_logs_for(
    until_version: DeltaDataTypeVersion,
    storage: &dyn StorageBackend,
    log_retention_timestamp: i64,
    table_uri: &str,
) -> Result<i32, DeltaTableError> {
    lazy_static! {
        static ref DELTA_LOG_REGEX: Regex =
            Regex::new(r#"^*[/\\]_delta_log[/\\](\d{20})\.(json|checkpoint)*$"#).unwrap();
    }

    let mut deleted_log_num = 0;

    // Get file objects from table.
    let log_uri = storage.join_path(table_uri, "_delta_log");
    let mut candidates: Vec<(DeltaDataTypeVersion, ObjectMeta)> = Vec::new();
    let mut stream = storage.list_objs(&log_uri).await?;
    while let Some(obj_meta) = stream.next().await {
        let obj_meta = obj_meta?;

        let ts = obj_meta.modified.timestamp_millis();

        if let Some(captures) = DELTA_LOG_REGEX.captures(&obj_meta.path) {
            let log_ver_str = captures.get(1).unwrap().as_str();
            let log_ver: DeltaDataTypeVersion = log_ver_str.parse().unwrap();
            if log_ver < until_version && ts <= log_retention_timestamp {
                candidates.push((log_ver, obj_meta));
            }
        }
    }

    // Sort files by file object version.
    candidates.sort_by(|a, b| a.0.cmp(&b.0));

    let mut last_file: (i64, ObjectMeta) = (
        0,
        ObjectMeta {
            path: String::new(),
            modified: MIN_DATETIME,
        },
    );
    let file_needs_time_adjustment =
        |current_file: &(i64, ObjectMeta), last_file: &(i64, ObjectMeta)| {
            last_file.0 < current_file.0
                && last_file.1.modified.timestamp() >= current_file.1.modified.timestamp()
        };

    let should_delete_file = |file: &(i64, ObjectMeta)| {
        file.1.modified.timestamp() <= log_retention_timestamp && file.0 < until_version
    };

    let mut maybe_delete_files: Vec<(DeltaDataTypeVersion, ObjectMeta)> = Vec::new();
    let mut files_to_delete: Vec<(DeltaDataTypeVersion, ObjectMeta)> = Vec::new();

    // Init
    if !candidates.is_empty() {
        let removed = candidates.remove(0);
        last_file = (removed.0, removed.1.clone());
        maybe_delete_files.push(removed);
    }

    let mut current_file: (DeltaDataTypeVersion, ObjectMeta);
    loop {
        if candidates.is_empty() {
            deleted_log_num += flush_delete_files(
                storage,
                &mut maybe_delete_files,
                &mut files_to_delete,
                should_delete_file,
            )
            .await?;

            return Ok(deleted_log_num);
        }
        current_file = candidates.remove(0);

        if file_needs_time_adjustment(&current_file, &last_file) {
            let updated = (
                current_file.0,
                ObjectMeta {
                    path: current_file.1.path.clone(),
                    modified: last_file.1.modified.add(Duration::seconds(1)),
                },
            );
            maybe_delete_files.push(updated);
            last_file = (
                maybe_delete_files.last().unwrap().0,
                maybe_delete_files.last().unwrap().1.clone(),
            );
        } else {
            let deleted = flush_delete_files(
                storage,
                &mut maybe_delete_files,
                &mut files_to_delete,
                should_delete_file,
            )
            .await?;
            if deleted == 0 {
                return Ok(deleted_log_num);
            }
            deleted_log_num += deleted;

            maybe_delete_files.push(current_file.clone());
            last_file = current_file;
        }
    }
}

fn parquet_bytes_from_state(state: &DeltaTableState) -> Result<Vec<u8>, CheckpointError> {
    let current_metadata = state
        .current_metadata()
        .ok_or(CheckpointError::MissingMetaData)?;

    let partition_col_data_types = current_metadata.get_partition_col_data_types();

    // Collect a map of paths that require special stats conversion.
    let mut stats_conversions: Vec<(SchemaPath, SchemaDataType)> = Vec::new();
    collect_stats_conversions(&mut stats_conversions, current_metadata.schema.get_fields());

    let mut tombstones = state.unexpired_tombstones().cloned().collect::<Vec<_>>();

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

    // protocol
    let mut jsons = std::iter::once(action::Action::protocol(action::Protocol {
        min_reader_version: state.min_reader_version(),
        min_writer_version: state.min_writer_version(),
    }))
    // metaData
    .chain(std::iter::once(action::Action::metaData(
        action::MetaData::try_from(current_metadata.clone())?,
    )))
    // txns
    .chain(
        state
            .app_transaction_version()
            .iter()
            .map(|(app_id, version)| {
                action::Action::txn(action::Txn {
                    app_id: app_id.clone(),
                    version: *version,
                    last_updated: None,
                })
            }),
    )
    // removes
    .chain(tombstones.iter().map(|r| {
        let mut r = (*r).clone();

        // As a "new writer", we should always set `extendedFileMetadata` when writing, and include/ignore the other three fields accordingly.
        // https://github.com/delta-io/delta/blob/fb0452c2fb142310211c6d3604eefb767bb4a134/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L311-L314
        if None == r.extended_file_metadata {
            r.extended_file_metadata = Some(false);
        }

        action::Action::remove(r)
    }))
    .map(|a| serde_json::to_value(a).map_err(ArrowError::from))
    // adds
    .chain(state.files().iter().map(|f| {
        checkpoint_add_from_state(f, partition_col_data_types.as_slice(), &stats_conversions)
    }));

    // Create the arrow schema that represents the Checkpoint parquet file.
    let arrow_schema = delta_log_schema_for_table(
        <ArrowSchema as TryFrom<&Schema>>::try_from(&current_metadata.schema)?,
        current_metadata.partition_columns.as_slice(),
        use_extended_remove_schema,
    );

    debug!("Writing to checkpoint parquet buffer...");
    // Write the Checkpoint parquet file.
    let writeable_cursor = InMemoryWriteableCursor::default();
    let mut writer = ArrowWriter::try_new(writeable_cursor.clone(), arrow_schema.clone(), None)?;
    let decoder = Decoder::new(arrow_schema, CHECKPOINT_RECORD_BATCH_SIZE, None);
    while let Some(batch) = decoder.next_batch(&mut jsons)? {
        writer.write(&batch)?;
    }
    let _ = writer.close()?;
    debug!("Finished writing checkpoint parquet buffer.");

    Ok(writeable_cursor.data())
}

fn checkpoint_add_from_state(
    add: &action::Add,
    partition_col_data_types: &[(&str, &SchemaDataType)],
    stats_conversions: &[(SchemaPath, SchemaDataType)],
) -> Result<Value, ArrowError> {
    let mut v = serde_json::to_value(action::Action::add(add.clone()))?;

    v["add"]["dataChange"] = Value::Bool(false);

    if !add.partition_values.is_empty() {
        let mut partition_values_parsed: HashMap<String, Value> = HashMap::new();

        for (field_name, data_type) in partition_col_data_types.iter() {
            if let Some(string_value) = add.partition_values.get(*field_name) {
                let v = typed_partition_value_from_option_string(string_value, data_type)?;

                partition_values_parsed.insert(field_name.to_string(), v);
            }
        }

        let partition_values_parsed = serde_json::to_value(partition_values_parsed)?;
        v["add"]["partitionValues_parsed"] = partition_values_parsed;
    }

    if let Ok(Some(stats)) = add.get_stats() {
        let mut stats = serde_json::to_value(stats)?;
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
    data_type: &SchemaDataType,
) -> Result<Value, CheckpointError> {
    match data_type {
        SchemaDataType::primitive(primitive_type) => match primitive_type.as_str() {
            "string" | "binary" => Ok(string_value.to_owned().into()),
            "long" | "integer" | "short" | "byte" => Ok(string_value
                .parse::<i64>()
                .map_err(|_| CheckpointError::PartitionValueNotParseable(string_value.to_owned()))?
                .into()),
            "boolean" => Ok(string_value
                .parse::<bool>()
                .map_err(|_| CheckpointError::PartitionValueNotParseable(string_value.to_owned()))?
                .into()),
            "float" | "double" => Ok(string_value
                .parse::<f64>()
                .map_err(|_| CheckpointError::PartitionValueNotParseable(string_value.to_owned()))?
                .into()),
            "date" => {
                let d = chrono::naive::NaiveDate::parse_from_str(string_value, "%Y-%m-%d")
                    .map_err(|_| {
                        CheckpointError::PartitionValueNotParseable(string_value.to_owned())
                    })?;
                // day 0 is 1970-01-01 (719163 days from ce)
                Ok((d.num_days_from_ce() - 719_163).into())
            }
            "timestamp" => {
                let ts =
                    chrono::naive::NaiveDateTime::parse_from_str(string_value, "%Y-%m-%d %H:%M:%S")
                        .map_err(|_| {
                            CheckpointError::PartitionValueNotParseable(string_value.to_owned())
                        })?;
                Ok((ts.timestamp_millis() * 1000).into())
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
    data_type: &SchemaDataType,
) -> Result<Value, CheckpointError> {
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

type SchemaPath = Vec<String>;

fn collect_stats_conversions(
    paths: &mut Vec<(SchemaPath, SchemaDataType)>,
    fields: &[SchemaField],
) {
    let mut _path = SchemaPath::new();
    fields
        .iter()
        .for_each(|f| collect_field_conversion(&mut _path, paths, f));
}

fn collect_field_conversion(
    current_path: &mut SchemaPath,
    all_paths: &mut Vec<(SchemaPath, SchemaDataType)>,
    field: &SchemaField,
) {
    match field.get_type() {
        SchemaDataType::primitive(type_name) => {
            if let "timestamp" = type_name.as_str() {
                let mut key_path = current_path.clone();
                key_path.push(field.get_name().to_owned());
                all_paths.push((key_path, field.get_type().to_owned()));
            }
        }
        SchemaDataType::r#struct(struct_field) => {
            let struct_fields = struct_field.get_fields();
            current_path.push(field.get_name().to_owned());
            struct_fields
                .iter()
                .for_each(|f| collect_field_conversion(current_path, all_paths, f));
            current_path.pop();
        }
        _ => { /* noop */ }
    }
}

fn apply_stats_conversion(
    context: &mut serde_json::Map<String, Value>,
    path: &[String],
    data_type: &SchemaDataType,
) {
    if path.len() == 1 {
        match data_type {
            SchemaDataType::primitive(type_name) if type_name == "timestamp" => {
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
            _ => { /* noop */ }
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
    use super::*;
    use lazy_static::lazy_static;
    use std::time::Duration;
    use uuid::Uuid;

    #[test]
    fn typed_partition_value_from_string_test() {
        let string_value: Value = "Hello World!".into();
        assert_eq!(
            string_value,
            typed_partition_value_from_option_string(
                &Some("Hello World!".to_string()),
                &SchemaDataType::primitive("string".to_string()),
            )
            .unwrap()
        );

        let bool_value: Value = true.into();
        assert_eq!(
            bool_value,
            typed_partition_value_from_option_string(
                &Some("true".to_string()),
                &SchemaDataType::primitive("boolean".to_string()),
            )
            .unwrap()
        );

        let number_value: Value = 42.into();
        assert_eq!(
            number_value,
            typed_partition_value_from_option_string(
                &Some("42".to_string()),
                &SchemaDataType::primitive("integer".to_string()),
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
                    &SchemaDataType::primitive("date".to_string()),
                )
                .unwrap()
            );
        }

        for (s, v) in [
            ("2021-08-08 01:00:01", 1628384401000000i64),
            ("1970-01-02 12:59:59", 133199000000i64),
            ("1970-01-01 13:00:01", 46801000000i64),
            ("1969-12-31 00:00:00", -86400000000i64),
            ("1677-09-21 00:12:44", -9223372036000000i64),
        ] {
            let timestamp_value: Value = v.into();
            assert_eq!(
                timestamp_value,
                typed_partition_value_from_option_string(
                    &Some(s.to_string()),
                    &SchemaDataType::primitive("timestamp".to_string()),
                )
                .unwrap()
            );
        }

        let binary_value: Value = "\u{2081}\u{2082}\u{2083}\u{2084}".into();
        assert_eq!(
            binary_value,
            typed_partition_value_from_option_string(
                &Some("₁₂₃₄".to_string()),
                &SchemaDataType::primitive("binary".to_string()),
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
                &SchemaDataType::primitive("integer".to_string()),
            )
            .unwrap()
        );

        // empty string should be treated as null
        assert_eq!(
            Value::Null,
            typed_partition_value_from_option_string(
                &Some("".to_string()),
                &SchemaDataType::primitive("integer".to_string()),
            )
            .unwrap()
        );
    }

    #[test]
    fn collect_stats_conversions_test() {
        let delta_schema: Schema = serde_json::from_value(SCHEMA.clone()).unwrap();
        let fields = delta_schema.get_fields();
        let mut paths = Vec::new();

        collect_stats_conversions(&mut paths, fields.as_slice());

        assert_eq!(2, paths.len());
        assert_eq!(
            (
                vec!["some_struct".to_string(), "struct_timestamp".to_string()],
                SchemaDataType::primitive("timestamp".to_string())
            ),
            paths[0]
        );
        assert_eq!(
            (
                vec!["some_timestamp".to_string()],
                SchemaDataType::primitive("timestamp".to_string())
            ),
            paths[1]
        );
    }

    #[test]
    fn apply_stats_conversion_test() {
        let mut stats = STATS_JSON.clone();

        let min_values = stats.get_mut("minValues").unwrap().as_object_mut().unwrap();

        apply_stats_conversion(
            min_values,
            &["some_struct".to_string(), "struct_string".to_string()],
            &SchemaDataType::primitive("string".to_string()),
        );
        apply_stats_conversion(
            min_values,
            &["some_struct".to_string(), "struct_timestamp".to_string()],
            &SchemaDataType::primitive("timestamp".to_string()),
        );
        apply_stats_conversion(
            min_values,
            &["some_string".to_string()],
            &SchemaDataType::primitive("string".to_string()),
        );
        apply_stats_conversion(
            min_values,
            &["some_timestamp".to_string()],
            &SchemaDataType::primitive("timestamp".to_string()),
        );

        let max_values = stats.get_mut("maxValues").unwrap().as_object_mut().unwrap();

        apply_stats_conversion(
            max_values,
            &["some_struct".to_string(), "struct_string".to_string()],
            &SchemaDataType::primitive("string".to_string()),
        );
        apply_stats_conversion(
            max_values,
            &["some_struct".to_string(), "struct_timestamp".to_string()],
            &SchemaDataType::primitive("timestamp".to_string()),
        );
        apply_stats_conversion(
            max_values,
            &["some_string".to_string()],
            &SchemaDataType::primitive("string".to_string()),
        );
        apply_stats_conversion(
            max_values,
            &["some_timestamp".to_string()],
            &SchemaDataType::primitive("timestamp".to_string()),
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

    // Last-Modified for S3 could not be altered by user, hence using system pauses which makes
    // test to run longer but reliable
    async fn cleanup_metadata_test(table_path: &str) {
        let log_path = |version| format!("{}/_delta_log/{:020}.json", table_path, version);
        let backend = crate::storage::get_backend_for_uri(&table_path).unwrap();

        // we don't need to actually populate files with content as cleanup works only with file's metadata
        backend.put_obj(&log_path(0), &[]).await.unwrap();

        // since we cannot alter s3 object metadata, we mimic it with pauses
        // also we forced to use 2 seconds since Last-Modified is stored in seconds
        std::thread::sleep(Duration::from_secs(2));
        backend.put_obj(&log_path(1), &[]).await.unwrap();

        std::thread::sleep(Duration::from_secs(3));
        backend.put_obj(&log_path(2), &[]).await.unwrap();

        let v0time = backend.head_obj(&log_path(0)).await.unwrap().modified;
        let v1time = backend.head_obj(&log_path(1)).await.unwrap().modified;
        let v2time = backend.head_obj(&log_path(2)).await.unwrap().modified;

        // we choose the retention timestamp to be between v1 and v2 so v2 will be kept but other removed.
        let retention_timestamp =
            v1time.timestamp_millis() + (v2time.timestamp_millis() - v1time.timestamp_millis()) / 2;

        assert!(retention_timestamp > v0time.timestamp_millis());
        assert!(retention_timestamp > v1time.timestamp_millis());
        assert!(retention_timestamp < v2time.timestamp_millis());

        let removed = crate::checkpoints::cleanup_expired_logs_for(
            3,
            backend.as_ref(),
            retention_timestamp,
            &table_path,
        )
        .await
        .unwrap();

        assert_eq!(removed, 2);
        assert!(backend.head_obj(&log_path(0)).await.is_err());
        assert!(backend.head_obj(&log_path(1)).await.is_err());
        assert!(backend.head_obj(&log_path(2)).await.is_ok());

        // after test cleanup
        backend.delete_obj(&log_path(2)).await.unwrap();
    }

    #[tokio::test]
    async fn cleanup_metadata_fs_test() {
        let table_path = format!("./tests/data/md_cleanup/{}", Uuid::new_v4());
        cleanup_metadata_test(&table_path).await;
        std::fs::remove_dir_all(&table_path).unwrap();
    }

    #[cfg(feature = "s3")]
    mod cleanup_metadata_s3_test {
        use super::*;

        #[tokio::test]
        async fn cleanup_metadata_s3_test() {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
            let table_path = format!("s3://deltars/md_cleanup/{}", Uuid::new_v4());
            cleanup_metadata_test(&table_path).await;
        }
    }
}
