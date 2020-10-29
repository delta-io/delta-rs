extern crate delta;
extern crate pyo3;

use std::path::Path;

use pyo3::prelude::*;

#[pyclass]
struct RawDeltaTable {
    _table: delta::DeltaTable,
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    fn new(table_path: &str) -> PyResult<Self> {
        let table = delta::open_table(&table_path).unwrap();
        Ok(RawDeltaTable { _table: table })
    }

    pub fn table_path(&self) -> PyResult<&str> {
        Ok(&self._table.table_path)
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version)
    }

    #[args(full_path = "false")]
    pub fn files(&self, full_path: bool) -> PyResult<Vec<String>> {
        let list = self._table.get_files().to_vec();
        if full_path {
            Ok(list
                .iter()
                .map(|fpath| {
                    Path::new(&self._table.table_path)
                        .join(fpath)
                        .to_string_lossy()
                        .into_owned()
                })
                .collect())
        } else {
            Ok(list)
        }
    }
}

#[pymodule]
// module name need to match project name
fn deltalake(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RawDeltaTable>()?;
    Ok(())
}
