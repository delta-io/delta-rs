extern crate rust_dataframe;

use std::fs::File;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use rust_dataframe::dataframe::DataFrame;

use crate as delta;
use crate::DeltaTableError;

pub trait DeltaDataframe {
    fn from_loaded_delta_table(table: delta::DeltaTable) -> Result<DataFrame, DeltaTableError>;
    fn from_delta_table(path: &str) -> Result<DataFrame, DeltaTableError>;
    fn from_delta_table_with_version(
        path: &str,
        version: delta::DeltaDataTypeVersion,
    ) -> Result<DataFrame, DeltaTableError>;
}

impl DeltaDataframe for DataFrame {
    fn from_loaded_delta_table(
        delta_table: delta::DeltaTable,
    ) -> Result<DataFrame, DeltaTableError> {
        let mut batches = vec![];
        let mut schema = None;
        let table_path = Path::new(&delta_table.table_path);

        for fname in delta_table.get_files() {
            let fpath = table_path.join(fname);
            let file = File::open(&fpath).map_err(|e| DeltaTableError::MissingDataFile {
                source: e,
                path: String::from(fpath.to_str().unwrap()),
            })?;

            let file_reader = SerializedFileReader::new(file)?;
            let mut arrow_reader = ParquetFileArrowReader::new(Rc::new(file_reader));

            if schema.is_none() {
                schema = Some(Arc::new(arrow_reader.get_schema()?));
            }

            let mut record_batch_reader = arrow_reader.get_record_reader(1024)?;
            while let Ok(Some(batch)) = record_batch_reader.next_batch() {
                batches.push(batch);
            }
        }

        Ok(Self::from_table(
            rust_dataframe::table::Table::from_record_batches(schema.unwrap().clone(), batches),
        ))
    }

    fn from_delta_table(path: &str) -> Result<DataFrame, DeltaTableError> {
        let delta_table = delta::open_table(path)?;
        return Self::from_loaded_delta_table(delta_table);
    }

    fn from_delta_table_with_version(
        path: &str,
        version: delta::DeltaDataTypeVersion,
    ) -> Result<DataFrame, DeltaTableError> {
        let delta_table = delta::open_table_with_version(path, version)?;
        return Self::from_loaded_delta_table(delta_table);
    }
}
