#![deny(clippy::all)]
use std::collections::HashMap;

use napi::bindgen_prelude::*;

use chrono::{DateTime, FixedOffset, Utc};
use deltalake::action;
use deltalake::action::Action;
use deltalake::action::{ColumnCountStat, ColumnValueStat, DeltaOperation, SaveMode, Stats};
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::get_backend_for_uri;
use deltalake::partitions::PartitionFilter;
use deltalake::storage;
use deltalake::DeltaDataTypeLong;
use deltalake::DeltaDataTypeTimestamp;
use deltalake::DeltaTableMetaData;
use deltalake::DeltaTransactionOptions;
use deltalake::Schema;
use deltalake::{arrow, StorageBackend};

#[macro_use]
use napi_derive::napi;

pub struct JsDeltaTableError(napi::bindgen_prelude::Error);

impl From<JsDeltaTableError> for napi::bindgen_prelude::Error {
  fn from(err: JsDeltaTableError) -> Self {
    err.0
  }
}

impl JsDeltaTableError {
  fn new_err<T: std::fmt::Display>(msg: T) -> Error {
    Error::new(
      napi::bindgen_prelude::Status::GenericFailure,
      msg.to_string(),
    )
  }
  fn from_arrow(err: arrow::error::ArrowError) -> Error {
    JsDeltaTableError::new_err(err.to_string())
  }

  fn from_data_catalog(err: deltalake::DataCatalogError) -> Error {
    JsDeltaTableError::new_err(err.to_string())
  }

  fn from_raw(err: deltalake::DeltaTableError) -> Error {
    JsDeltaTableError::new_err(err.to_string())
  }

  fn from_storage(err: deltalake::StorageError) -> Error {
    JsDeltaTableError::new_err(err.to_string())
  }

  fn from_chrono(err: chrono::ParseError) -> Error {
    JsDeltaTableError::new_err(format!("Parse date and time string failed: {}", err))
  }
}

enum PartitionFilterValue<'a> {
  Single(&'a str),
  Multiple(Vec<&'a str>),
}

#[napi(js_name = "DeltaTable")]
struct JsDeltaTable {
  _table: deltalake::DeltaTable,
}

#[napi(object, js_name = "Metadata")]
struct JsDeltaTableMetaData {
  pub id: String,
  pub name: Option<String>,
  pub description: Option<String>,
  pub partition_columns: Vec<String>,
  pub created_time: Option<i64>,
  pub configuration: HashMap<String, Option<String>>,
}

/// Create the Delta Table from a path with an optional version.
/// Multiple StorageBackends are currently supported: AWS S3, Azure Data Lake Storage Gen2, Google Cloud Storage (GCS) and local URI.
/// Depending on the storage backend used, you could provide options values using the `storage_options` parameter.
///
/// @param table_uri - the path of the DeltaTable
/// @param version - version of the DeltaTable
/// @param storage_options - a dictionary of the options to use for the storage backend
/// @returns A new `DeltaTable` instance
#[napi]
pub async fn new_delta_table(
  table_uri: String,
  version: Option<i64>,
  storage_options: Option<HashMap<String, String>>,
) -> Result<JsDeltaTable> {
  let mut table =
    deltalake::DeltaTableBuilder::from_uri(&table_uri).map_err(JsDeltaTableError::from_raw)?;

  if let Some(storage_options) = storage_options {
    let backend = deltalake::get_backend_for_uri_with_options(&table_uri, storage_options)
      .map_err(JsDeltaTableError::from_storage)?;
    table = table.with_storage_backend(backend)
  }
  if let Some(version) = version {
    table = table.with_version(version)
  }
  let table = table.load().await.map_err(JsDeltaTableError::from_raw)?;
  Ok(JsDeltaTable { _table: table })
}
