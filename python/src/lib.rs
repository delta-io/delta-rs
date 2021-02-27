#![deny(warnings)]

extern crate arrow;
extern crate pyo3;

use arrow::datatypes::Schema as ArrowSchema;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

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

    pub fn schema_json(&self) -> PyResult<String> {
        let schema = self
            ._table
            .schema()
            .ok_or_else(|| PyDeltaTableError::new_err("Table schema not found"))?;
        Ok(serde_json::to_string(&schema)
            .map_err(|_| PyDeltaTableError::new_err("Got invalid table schema"))?)
    }

    pub fn arrow_schema_json(&self) -> PyResult<String> {
        let schema = self
            ._table
            .schema()
            .ok_or_else(|| PyDeltaTableError::new_err("Table schema not found"))?;
        Ok(serde_json::to_string(
            &<ArrowSchema as From<&deltalake::Schema>>::from(schema).to_json(),
        )
        .map_err(|_| PyDeltaTableError::new_err("Got invalid table schema"))?)
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
