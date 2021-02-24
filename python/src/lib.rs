#![deny(warnings)]

extern crate arrow;
extern crate deltalake;
extern crate pyo3;

use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::collections::HashMap;

create_exception!(deltalake, PyDeltaTableError, PyException);

impl PyDeltaTableError {
    fn from_raw(err: deltalake::DeltaTableError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    fn from_tokio(err: tokio::io::Error) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }
}

#[inline]
fn rt() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(PyDeltaTableError::from_tokio)
}

#[pyclass]
struct RawDeltaTable {
    _table: deltalake::DeltaTable,
}

#[pyclass]
struct DeltaTableSchema {
    #[pyo3(get)]
    schema: Vec<SchemaField>,
}

#[pyclass]
#[derive(Clone)]
struct SchemaField {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    rtype: String,
    #[pyo3(get)]
    nullable: bool,
    #[pyo3(get)]
    metadata: HashMap<String, String>,
}

impl From<&deltalake::SchemaField> for SchemaField {
    fn from(f: &deltalake::SchemaField) -> Self {
        SchemaField {
            name: f.get_name().to_string(),
            rtype: f.get_type().to_json().to_string(),
            nullable: f.is_nullable(),
            metadata: f.get_metadata().clone(),
        }
    }
}

impl From<&ArrowField> for SchemaField {
    fn from(f: &ArrowField) -> Self {
        SchemaField {
            name: f.name().to_string(),
            rtype: f.to_json().to_string(),
            nullable: f.is_nullable(),
            metadata: HashMap::new(),
        }
    }
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    fn new(table_path: &str) -> PyResult<Self> {
        let table = rt()?
            .block_on(deltalake::open_table(&table_path))
            .map_err(PyDeltaTableError::from_raw)?;
        Ok(RawDeltaTable { _table: table })
    }

    pub fn table_path(&self) -> PyResult<&str> {
        Ok(&self._table.table_path)
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version)
    }

    pub fn load_version(&mut self, version: deltalake::DeltaDataTypeVersion) -> PyResult<()> {
        Ok(rt()?
            .block_on(self._table.load_version(version))
            .map_err(PyDeltaTableError::from_raw)?)
    }

    pub fn files(&self) -> PyResult<Vec<String>> {
        Ok(self._table.get_files().to_vec())
    }

    pub fn file_paths(&self) -> PyResult<Vec<String>> {
        Ok(self._table.get_file_paths())
    }

    pub fn schema(&self, format: &str) -> PyResult<DeltaTableSchema> {
        let schema = match &format[..] {
            "ARROW" => {
                <ArrowSchema as From<&deltalake::Schema>>::from(self._table.schema().unwrap())
                    .fields()
                    .iter()
                    .map(SchemaField::from)
                    .collect()
            }
            "DELTA" => self
                ._table
                .schema()
                .unwrap()
                .get_fields()
                .iter()
                .map(SchemaField::from)
                .collect(),
            _ => panic!("Unknown Format for the schema: {}", format),
        };
        Ok(DeltaTableSchema { schema })
    }
}

#[pyfunction]
fn rust_core_version() -> &'static str {
    deltalake::crate_version()
}

#[pymodule]
// module name need to match project name
fn deltalake(py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    m.add_function(pyo3::wrap_pyfunction!(rust_core_version, m)?)?;
    m.add_class::<RawDeltaTable>()?;
    m.add("DeltaTableError", py.get_type::<PyDeltaTableError>())?;
    Ok(())
}
