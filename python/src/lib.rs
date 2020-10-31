extern crate deltalake;
extern crate pyo3;

use self::pyo3::create_exception;
use self::pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(deltalake, PyDeltaTableError, PyException);

impl PyDeltaTableError {
    fn new(err: deltalake::DeltaTableError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }
}

#[pyclass]
struct RawDeltaTable {
    _table: deltalake::DeltaTable,
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    fn new(table_path: &str) -> PyResult<Self> {
        let table = deltalake::open_table(&table_path).map_err(PyDeltaTableError::new)?;
        Ok(RawDeltaTable { _table: table })
    }

    pub fn table_path(&self) -> PyResult<&str> {
        Ok(&self._table.table_path)
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version)
    }

    pub fn load_version(&mut self, version: deltalake::DeltaDataTypeVersion) -> PyResult<()> {
        Ok(self
            ._table
            .load_version(version)
            .map_err(PyDeltaTableError::new)?)
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
    m.add_class::<RawDeltaTable>()?;
    m.add("DeltaTableError", py.get_type::<PyDeltaTableError>())?;
    Ok(())
}
