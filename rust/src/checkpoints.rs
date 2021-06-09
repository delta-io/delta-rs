//! Implementation for writing delta checkpoints.

use arrow::datatypes::Schema as ArrowSchema;
use arrow::error::ArrowError;
use arrow::json::reader::ReaderBuilder;
use log::*;
use parquet::arrow::ArrowWriter;
use parquet::errors::ParquetError;
use parquet::file::writer::InMemoryWriteableCursor;
use serde::Serialize;
use std::convert::TryFrom;
use std::io::Cursor;
use std::sync::Arc;

use super::action;
use super::open_table_with_version;
use super::schema::*;
use super::storage::{StorageBackend, StorageError};
use super::{CheckPoint, DeltaTableError, DeltaTableState};

/// Error returned when the CheckPointWriter is unable to write a checkpoint.
#[derive(thiserror::Error, Debug)]
pub enum CheckPointWriterError {
    /// Error returned when the DeltaTableState does not contain a metadata action.
    #[error("DeltaTableMetadata not present in DeltaTableState")]
    MissingMetaData,
    /// Error returned when creating the checkpoint schema.
    #[error("DeltaLogSchemaError: {source}")]
    DeltaLogSchema {
        /// The source DeltaLogSchemaError
        #[from]
        source: DeltaLogSchemaError,
    },
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

/// Struct for writing checkpoints to the delta log.
pub struct CheckPointWriter {
    table_uri: String,
    delta_log_uri: String,
    last_checkpoint_uri: String,
    storage: Box<dyn StorageBackend>,
    schema_factory: DeltaLogSchemaFactory,
}

impl CheckPointWriter {
    /// Creates a new CheckPointWriter.
    pub fn new(table_uri: &str, storage: Box<dyn StorageBackend>) -> Self {
        let delta_log_uri = storage.join_path(table_uri, "_delta_log");
        let last_checkpoint_uri = storage.join_path(delta_log_uri.as_str(), "_last_checkpoint");
        let schema_factory = DeltaLogSchemaFactory::new();

        Self {
            table_uri: table_uri.to_string(),
            delta_log_uri,
            last_checkpoint_uri,
            storage,
            schema_factory,
        }
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

        let last_checkpoint_content: serde_json::Value = serde_json::to_value(&checkpoint)?;
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

        // let jsons: Iterator<Item = Result<serde_json::Value, CheckPointWriterError>> = [
        //     action::Action::protocol(action::Protocol {
        //         min_reader_version: state.min_reader_version(),
        //         min_writer_version: state.min_writer_version(),
        //     }),
        //     action::Action::metaData(action::MetaData::try_from(current_metadata.clone())?),
        // ];
        //

        //         let things: Vec<action::Action> = state
        //             .files()
        //             .iter()
        //             .map(|f| action::Action::add(f.clone()))
        //             .collect();

        //         let jsons: dyn Iterator<Item = action::Action> = [
        //             action::Action::protocol(action::Protocol {
        //                 min_reader_version: state.min_reader_version(),
        //                 min_writer_version: state.min_writer_version(),
        //             }),
        //             action::Action::metaData(action::MetaData::try_from(current_metadata.clone())?),
        //         ]
        //         .chain(state.files().iter().map(|f| action::Action::add(f.clone())));

        // jsons
        //     .iter()
        //     .chain(state.files().iter().map(|f| action::Action::add(f.clone())))
        //     .chain(
        //         state
        //             .tombstones()
        //             .iter()
        //             .map(|t| action::Action::remove(t.clone())),
        //     )
        //     .chain(
        //         state
        //             .app_transaction_version()
        //             .iter()
        //             .map(|(app_id, version)| {
        //                 action::Action::txn(action::Txn {
        //                     app_id: app_id.clone(),
        //                     version: *version,
        //                     last_updated: None,
        //                 })
        //             }),
        //     )
        //     .map(|action| Ok(action.into()));

        let mut json_buffer: Vec<u8> = Vec::new();

        let protocol = action::Action::protocol(action::Protocol {
            min_reader_version: state.min_reader_version(),
            min_writer_version: state.min_writer_version(),
        });
        extend_json_byte_buffer(&mut json_buffer, &protocol)?;

        let metadata =
            action::Action::metaData(action::MetaData::try_from(current_metadata.clone())?);
        extend_json_byte_buffer(&mut json_buffer, &metadata)?;

        for add in state.files() {
            let add = action::Action::add(add.clone());
            extend_json_byte_buffer(&mut json_buffer, &add)?;
        }

        for remove in state.tombstones() {
            let remove = action::Action::remove(remove.clone());
            extend_json_byte_buffer(&mut json_buffer, &remove)?;
        }

        for (app_id, version) in state.app_transaction_version().iter() {
            let txn = action::Action::txn(action::Txn {
                app_id: app_id.clone(),
                version: *version,
                last_updated: None,
            });
            extend_json_byte_buffer(&mut json_buffer, &txn)?;
        }

        let checkpoint_schema = self.schema_factory.delta_log_schema_for_table(
            &current_metadata.schema,
            current_metadata.partition_columns.as_slice(),
        )?;
        let arrow_checkpoint_schema: ArrowSchema =
            <ArrowSchema as TryFrom<&Schema>>::try_from(&checkpoint_schema)?;
        let arrow_schema = Arc::new(arrow_checkpoint_schema);

        let cursor = Cursor::new(json_buffer);

        let mut json_reader = ReaderBuilder::new()
            .with_schema(arrow_schema.clone())
            .build(cursor)?;

        debug!("Preparing checkpoint parquet buffer.");

        let writeable_cursor = InMemoryWriteableCursor::default();
        let mut writer = ArrowWriter::try_new(writeable_cursor.clone(), arrow_schema, None)?;

        debug!("Writing to checkpoint parquet buffer...");

        while let Some(batch) = json_reader.next()? {
            writer.write(&batch)?;
        }

        let _ = writer.close()?;

        debug!("Finished writing checkpoint parquet buffer.");

        Ok(writeable_cursor.data())
    }
}

fn extend_json_byte_buffer<T>(
    json_byte_buffer: &mut Vec<u8>,
    json_value: &T,
) -> Result<(), serde_json::error::Error>
where
    T: ?Sized + Serialize,
{
    json_byte_buffer.extend(serde_json::to_vec(json_value)?);
    json_byte_buffer.push(b'\n');

    Ok(())
}
