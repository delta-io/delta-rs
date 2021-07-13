//! Implementation for writing delta checkpoints.

use arrow::datatypes::Schema as ArrowSchema;
use arrow::error::ArrowError;
use arrow::json::reader::Decoder;
use log::*;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::writer::InMemoryWriteableCursor;
use serde_json::Value;
use std::collections::HashMap;
use std::convert::TryFrom;

use super::action;
use super::delta_arrow::delta_log_schema_for_table;
use super::open_table_with_version;
use super::schema::*;
use super::storage;
use super::storage::{StorageBackend, StorageError};
use super::{CheckPoint, DeltaTableError, DeltaTableState};

/// Error returned when the CheckPointWriter is unable to write a checkpoint.
#[derive(thiserror::Error, Debug)]
pub enum CheckPointWriterError {
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

impl From<CheckPointWriterError> for ArrowError {
    fn from(error: CheckPointWriterError) -> Self {
        ArrowError::from_external_error(Box::new(error))
    }
}

/// Struct for writing checkpoints to the delta log.
pub struct CheckPointWriter {
    table_uri: String,
    delta_log_uri: String,
    last_checkpoint_uri: String,
    storage: Box<dyn StorageBackend>,
}

impl CheckPointWriter {
    /// Creates a new CheckPointWriter.
    pub fn new(table_uri: &str, storage: Box<dyn StorageBackend>) -> Self {
        let delta_log_uri = storage.join_path(table_uri, "_delta_log");
        let last_checkpoint_uri = storage.join_path(delta_log_uri.as_str(), "_last_checkpoint");

        Self {
            table_uri: table_uri.to_string(),
            delta_log_uri,
            last_checkpoint_uri,
            storage,
        }
    }

    /// Creates a new CheckPointWriter for the table URI.
    pub fn new_for_table_uri(table_uri: &str) -> Result<Self, CheckPointWriterError> {
        let storage_backend = storage::get_backend_for_uri(table_uri)?;

        Ok(Self::new(table_uri, storage_backend))
    }

    /// Creates a new checkpoint at the specified version.
    /// NOTE: This method loads a new instance of delta table to determine the state to
    /// checkpoint.
    pub async fn create_checkpoint_for_version(
        &self,
        version: DeltaDataTypeVersion,
    ) -> Result<(), CheckPointWriterError> {
        let table = open_table_with_version(self.table_uri.as_str(), version).await?;

        self.create_checkpoint_from_state(version, table.get_state())
            .await
    }

    /// Creates a new checkpoint at the specified version from the given DeltaTableState.
    pub async fn create_checkpoint_from_state(
        &self,
        version: DeltaDataTypeVersion,
        state: &DeltaTableState,
    ) -> Result<(), CheckPointWriterError> {
        // TODO: checkpoints _can_ be multi-part... haven't actually found a good reference for
        // an appropriate split point yet though so only writing a single part currently.
        // See https://github.com/delta-io/delta-rs/issues/288

        info!("Writing parquet bytes to checkpoint buffer.");
        let parquet_bytes = self.parquet_bytes_from_state(state)?;

        let size = parquet_bytes.len() as i64;

        let checkpoint = CheckPoint::new(version, size, None);

        let file_name = format!("{:020}.checkpoint.parquet", version);
        let checkpoint_uri = self.storage.join_path(&self.delta_log_uri, &file_name);

        info!("Writing checkpoint to {:?}.", checkpoint_uri);
        self.storage
            .put_obj(&checkpoint_uri, &parquet_bytes)
            .await?;

        let last_checkpoint_content: Value = serde_json::to_value(&checkpoint)?;
        let last_checkpoint_content = serde_json::to_string(&last_checkpoint_content)?;

        info!(
            "Writing _last_checkpoint to {:?}.",
            self.last_checkpoint_uri
        );
        self.storage
            .put_obj(
                self.last_checkpoint_uri.as_str(),
                last_checkpoint_content.as_bytes(),
            )
            .await?;

        Ok(())
    }

    fn parquet_bytes_from_state(
        &self,
        state: &DeltaTableState,
    ) -> Result<Vec<u8>, CheckPointWriterError> {
        let current_metadata = state
            .current_metadata()
            .ok_or(CheckPointWriterError::MissingMetaData)?;

        // Collect partition fields along with their data type from the current schema.
        // JSON add actions contain a `partitionValues` field which is a map<string, string>.
        // When loading `partitionValues_parsed` we have to convert the stringified partition values back to the correct data type.
        let data_types: Vec<(&str, &SchemaDataType)> = current_metadata
            .schema
            .get_fields()
            .iter()
            .filter_map(|f| {
                if current_metadata
                    .partition_columns
                    .iter()
                    .any(|s| s.as_str() == f.get_name())
                {
                    Some((f.get_name(), f.get_type()))
                } else {
                    None
                }
            })
            .collect();

        // protocol
        let mut jsons = std::iter::once(action::Action::protocol(action::Protocol {
            min_reader_version: state.min_reader_version(),
            min_writer_version: state.min_writer_version(),
        }))
        // metadata
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
        .chain(
            state
                .tombstones()
                .iter()
                .map(|f| action::Action::remove(f.clone())),
        )
        .map(|a| serde_json::to_value(a).map_err(ArrowError::from))
        // adds
        .chain(
            state
                .files()
                .iter()
                .map(|f| checkpoint_add_from_state(f, data_types.as_slice())),
        );

        debug!("Preparing checkpoint parquet buffer.");
        // Create the arrow schema that represents the Checkpoint parquet file.
        let arrow_schema = delta_log_schema_for_table(
            <ArrowSchema as TryFrom<&Schema>>::try_from(&current_metadata.schema)?,
            current_metadata.partition_columns.as_slice(),
        );
        debug!("Writing to checkpoint parquet buffer...");
        // Write the Checkpoint parquet file.
        let writeable_cursor = InMemoryWriteableCursor::default();
        let mut writer =
            ArrowWriter::try_new(writeable_cursor.clone(), arrow_schema.clone(), None)?;
        let batch_size = state.app_transaction_version().len()
            + state.tombstones().len()
            + state.files().len()
            + 2; // 1 (protocol) + 1 (metadata)
        let decoder = Decoder::new(arrow_schema, batch_size, None);
        while let Some(batch) = decoder.next_batch(&mut jsons)? {
            writer.write(&batch)?;
        }
        let _ = writer.close()?;
        debug!("Finished writing checkpoint parquet buffer.");

        Ok(writeable_cursor.data())
    }
}

fn checkpoint_add_from_state(
    add: &action::Add,
    data_types: &[(&str, &SchemaDataType)],
) -> Result<Value, ArrowError> {
    let mut v = serde_json::to_value(action::Action::add(add.clone()))?;

    if !add.partition_values.is_empty() && add.partition_values_parsed.is_none() {
        let mut partition_values_parsed: HashMap<String, Value> = HashMap::new();

        for (field_name, data_type) in data_types.iter() {
            if let Some(string_value) = add.partition_values.get(*field_name) {
                let v = typed_partition_value_from_string(string_value, data_type)?;

                partition_values_parsed.insert(field_name.to_string(), v);
            }
        }

        let partition_values_parsed = serde_json::to_value(partition_values_parsed)?;
        v["add"]["partitionValues_parsed"] = partition_values_parsed;
    }
    if let Ok(Some(stats)) = add.get_stats() {
        let stats = serde_json::to_value(stats)?;
        v["add"]["stats_parsed"] = stats;
    }
    Ok(v)
}

fn typed_partition_value_from_string(
    string_value: &str,
    data_type: &SchemaDataType,
) -> Result<Value, CheckPointWriterError> {
    match data_type {
        SchemaDataType::primitive(primitive_type) => match primitive_type.as_str() {
            "string" => Ok(string_value.to_owned().into()),
            "long" | "integer" | "short" | "byte" => Ok(string_value
                .parse::<i64>()
                .map_err(|_| {
                    CheckPointWriterError::PartitionValueNotParseable(string_value.to_owned())
                })?
                .into()),
            "boolean" => Ok(string_value
                .parse::<bool>()
                .map_err(|_| {
                    CheckPointWriterError::PartitionValueNotParseable(string_value.to_owned())
                })?
                .into()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_partition_value_from_string_test() {
        let string_value: Value = "Hello World!".into();
        assert_eq!(
            string_value,
            typed_partition_value_from_string(
                &"Hello World!".to_string(),
                &SchemaDataType::primitive("string".to_string())
            )
            .unwrap()
        );

        let bool_value: Value = true.into();
        assert_eq!(
            bool_value,
            typed_partition_value_from_string(
                &"true".to_string(),
                &SchemaDataType::primitive("boolean".to_string())
            )
            .unwrap()
        );

        let number_value: Value = 42.into();
        assert_eq!(
            number_value,
            typed_partition_value_from_string(
                &"42".to_string(),
                &SchemaDataType::primitive("integer".to_string())
            )
            .unwrap()
        );
    }
}
