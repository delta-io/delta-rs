#![deny(warnings)]

extern crate deltalake;
extern crate pyo3;

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

#[pyclass]
struct RawDeltaTable {
    _rt: tokio::runtime::Runtime,
    _table: deltalake::DeltaTable,
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    fn new(table_path: &str) -> PyResult<Self> {
        let mut rt = tokio::runtime::Runtime::new().map_err(PyDeltaTableError::from_tokio)?;
        let table = rt
            .block_on(deltalake::open_table(&table_path))
            .map_err(PyDeltaTableError::from_raw)?;
        Ok(RawDeltaTable {
            _rt: rt,
            _table: table,
        })
    }

    pub fn table_path(&self) -> PyResult<&str> {
        Ok(&self._table.table_path)
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version)
    }

    pub fn load_version(&mut self, version: deltalake::DeltaDataTypeVersion) -> PyResult<()> {
        Ok(self
            ._rt
            .block_on(self._table.load_version(version))
            .map_err(PyDeltaTableError::from_raw)?)
    }

    pub fn files(&self) -> PyResult<Vec<String>> {
        Ok(self._table.get_files().to_vec())
    }

    pub fn file_paths(&self) -> PyResult<Vec<String>> {
        Ok(self._table.get_file_paths())
    }
}

#[pymodule]
// module name need to match project name
fn deltalake(py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    m.add_class::<RawDeltaTable>()?;
    m.add("DeltaTableError", py.get_type::<PyDeltaTableError>())?;
    Ok(())
}
