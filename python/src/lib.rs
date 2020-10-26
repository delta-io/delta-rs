#[macro_use]
extern crate pyo3;
extern crate delta;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

#[pyclass]
struct DeltaTable {
    _table: delta::DeltaTable,
}

#[pymethods]
impl DeltaTable {
    #[new]
    fn new(table_path: &str) -> Self {
        let table = delta::open_table(&table_path).unwrap();
        DeltaTable { _table: table }
    }

    pub fn table_path(&self) -> PyResult<&str> {
        Ok(&self._table.table_path)
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version)
    }

    pub fn files(&self) -> PyResult<Vec<String>> {
        Ok(self._table.get_files().to_vec())
    }
}

#[pymodule]
fn delta(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DeltaTable>()?;
    Ok(())
}
