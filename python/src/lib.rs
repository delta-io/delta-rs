#![deny(warnings)]

extern crate pyo3;

use chrono::{DateTime, FixedOffset, Utc};
use deltalake::action::Stats;
use deltalake::action::{ColumnCountStat, ColumnValueStat};
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use deltalake::partitions::PartitionFilter;
use deltalake::storage;
use deltalake::{arrow, StorageBackend};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple, PyType};
use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryFrom;

create_exception!(deltalake, PyDeltaTableError, PyException);

impl PyDeltaTableError {
    fn from_arrow(err: arrow::error::ArrowError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    fn from_data_catalog(err: deltalake::DataCatalogError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    fn from_raw(err: deltalake::DeltaTableError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    fn from_storage(err: deltalake::StorageError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    fn from_tokio(err: tokio::io::Error) -> pyo3::PyErr {
        PyDeltaTableError::new_err(err.to_string())
    }

    fn from_chrono(err: chrono::ParseError) -> pyo3::PyErr {
        PyDeltaTableError::new_err(format!(
            "Parse date and time string failed: {}",
            err.to_string()
        ))
    }
}

#[inline]
fn rt() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(PyDeltaTableError::from_tokio)
}

#[derive(FromPyObject)]
enum PartitionFilterValue<'a> {
    Single(&'a str),
    Multiple(Vec<&'a str>),
}

#[pyclass]
struct RawDeltaTable {
    _table: deltalake::DeltaTable,
}

#[pyclass]
struct RawDeltaTableMetaData {
    #[pyo3(get)]
    id: String,
    #[pyo3(get)]
    name: Option<String>,
    #[pyo3(get)]
    description: Option<String>,
    #[pyo3(get)]
    partition_columns: Vec<String>,
    #[pyo3(get)]
    created_time: Option<deltalake::DeltaDataTypeTimestamp>,
    #[pyo3(get)]
    configuration: HashMap<String, Option<String>>,
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    fn new(table_uri: &str, version: Option<deltalake::DeltaDataTypeLong>) -> PyResult<Self> {
        let table = match version {
            None => rt()?.block_on(deltalake::open_table(table_uri)),
            Some(version) => rt()?.block_on(deltalake::open_table_with_version(table_uri, version)),
        }
        .map_err(PyDeltaTableError::from_raw)?;
        Ok(RawDeltaTable { _table: table })
    }

    #[classmethod]
    fn get_table_uri_from_data_catalog(
        _cls: &PyType,
        data_catalog: &str,
        database_name: &str,
        table_name: &str,
        data_catalog_id: Option<String>,
    ) -> PyResult<String> {
        let data_catalog = deltalake::data_catalog::get_data_catalog(data_catalog)
            .map_err(PyDeltaTableError::from_data_catalog)?;
        let table_uri = rt()?
            .block_on(data_catalog.get_table_storage_location(
                data_catalog_id,
                database_name,
                table_name,
            ))
            .map_err(PyDeltaTableError::from_data_catalog)?;

        Ok(table_uri)
    }

    pub fn table_uri(&self) -> PyResult<&str> {
        Ok(&self._table.table_uri)
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version)
    }

    pub fn metadata(&self) -> PyResult<RawDeltaTableMetaData> {
        let metadata = self
            ._table
            .get_metadata()
            .map_err(PyDeltaTableError::from_raw)?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            partition_columns: metadata.partition_columns.clone(),
            created_time: metadata.created_time,
            configuration: metadata.configuration.clone(),
        })
    }

    pub fn load_version(&mut self, version: deltalake::DeltaDataTypeVersion) -> PyResult<()> {
        rt()?
            .block_on(self._table.load_version(version))
            .map_err(PyDeltaTableError::from_raw)
    }

    pub fn load_with_datetime(&mut self, ds: &str) -> PyResult<()> {
        let datetime = DateTime::<Utc>::from(
            DateTime::<FixedOffset>::parse_from_rfc3339(ds)
                .map_err(PyDeltaTableError::from_chrono)?,
        );
        rt()?
            .block_on(self._table.load_with_datetime(datetime))
            .map_err(PyDeltaTableError::from_raw)
    }

    pub fn files_by_partitions(
        &self,
        partitions_filters: Vec<(&str, &str, PartitionFilterValue)>,
    ) -> PyResult<Vec<String>> {
        let partition_filters: Result<Vec<PartitionFilter<&str>>, deltalake::DeltaTableError> =
            partitions_filters
                .into_iter()
                .map(|filter| match filter {
                    (key, op, PartitionFilterValue::Single(v)) => {
                        PartitionFilter::try_from((key, op, v))
                    }
                    (key, op, PartitionFilterValue::Multiple(v)) => {
                        PartitionFilter::try_from((key, op, v))
                    }
                })
                .collect();
        match partition_filters {
            Ok(filters) => Ok(self
                ._table
                .get_file_uris_by_partitions(&filters)
                .map_err(PyDeltaTableError::from_raw)?),
            Err(err) => Err(PyDeltaTableError::from_raw(err)),
        }
    }

    pub fn files(&self) -> PyResult<Vec<String>> {
        Ok(self
            ._table
            .get_files_iter()
            .map(|f| f.to_string())
            .collect())
    }

    pub fn file_uris(&self) -> PyResult<Vec<String>> {
        Ok(self._table.get_file_uris().collect())
    }

    pub fn schema_json(&self) -> PyResult<String> {
        let schema = self
            ._table
            .get_schema()
            .map_err(PyDeltaTableError::from_raw)?;
        serde_json::to_string(&schema)
            .map_err(|_| PyDeltaTableError::new_err("Got invalid table schema"))
    }

    /// Run the Vacuum command on the Delta Table: list and delete files no longer referenced by the Delta table and are older than the retention threshold.
    pub fn vacuum(&mut self, dry_run: bool, retention_hours: Option<u64>) -> PyResult<Vec<String>> {
        rt()?
            .block_on(self._table.vacuum(retention_hours, dry_run))
            .map_err(PyDeltaTableError::from_raw)
    }

    // Run the History command on the Delta Table: Returns provenance information, including the operation, user, and so on, for each write to a table.
    pub fn history(&mut self, limit: Option<usize>) -> PyResult<Vec<String>> {
        let history = self
            ._table
            .history(limit)
            .map_err(PyDeltaTableError::from_raw)?;
        Ok(history
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect())
    }

    pub fn arrow_schema_json(&self) -> PyResult<String> {
        let schema = self
            ._table
            .get_schema()
            .map_err(PyDeltaTableError::from_raw)?;
        serde_json::to_string(
            &<ArrowSchema as TryFrom<&deltalake::Schema>>::try_from(schema)
                .map_err(PyDeltaTableError::from_arrow)?
                .to_json(),
        )
        .map_err(|_| PyDeltaTableError::new_err("Got invalid table schema"))
    }

    pub fn update_incremental(&mut self) -> PyResult<()> {
        rt()?
            .block_on(self._table.update_incremental())
            .map_err(PyDeltaTableError::from_raw)
    }

    pub fn dataset_partitions<'py>(
        &mut self,
        py: Python<'py>,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<(String, Option<&'py PyAny>)>> {
        let path_set = match partition_filters {
            Some(filters) => Some(HashSet::<_>::from_iter(
                self.files_by_partitions(filters)?.iter().cloned(),
            )),
            None => None,
        };

        self._table
            .get_file_uris()
            .zip(self._table.get_partition_values())
            .zip(self._table.get_stats())
            .filter(|((path, _), _)| match &path_set {
                Some(path_set) => path_set.contains(path),
                None => true,
            })
            .map(|((path, partition_values), stats)| {
                let stats = stats.map_err(|err| PyDeltaTableError::from_raw(err))?;
                let expression = filestats_to_expression(py, partition_values, stats)?;
                Ok((path, expression))
            })
            .collect()
    }
}

fn json_value_to_py(value: &serde_json::Value, py: Python) -> PyObject {
    match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(val) => val.to_object(py),
        serde_json::Value::Number(val) => {
            if val.is_f64() {
                return val.as_f64().expect("not an f64").to_object(py);
            } else if val.is_i64() {
                return val.as_i64().expect("not an i64").to_object(py);
            } else {
                return val.as_u64().expect("not an u64").to_object(py);
            }
        }
        serde_json::Value::String(val) => val.to_object(py),
        _ => py.None(),
    }
}

/// Create expression that file statistics guarantee to be true.
///
/// PyArrow uses this expression to determine which Dataset fragments may be
/// skipped during a scan.
fn filestats_to_expression<'py>(
    py: Python<'py>,
    partitions_values: &HashMap<String, Option<String>>,
    stats: Option<Stats>,
) -> PyResult<Option<&'py PyAny>> {
    let ds = PyModule::import(py, "pyarrow.dataset")?;
    let field = ds.getattr("field")?;
    let mut expressions: Vec<PyResult<&PyAny>> = Vec::new();

    for (column, value) in partitions_values.iter() {
        if let Some(value) = value {
            expressions.push(
                field
                    .call1((column,))?
                    .call_method1("__eq__", (value.to_object(py),)),
            );
        }
    }

    if let Some(stats) = stats {
        for (column, minimum) in stats.min_values.iter().filter_map(|(k, v)| match v {
            ColumnValueStat::Value(val) => Some((k.clone(), json_value_to_py(val, py))),
            // Ignoring nested values (I think that's what this does...)
            _ => None,
        }) {
            expressions.push(field.call1((column,))?.call_method1("__gt__", (minimum,)));
        }

        for (column, maximum) in stats.max_values.iter().filter_map(|(k, v)| match v {
            ColumnValueStat::Value(val) => Some((k.clone(), json_value_to_py(val, py))),
            // Ignoring nested values (I think that's what this does...)
            _ => None,
        }) {
            expressions.push(field.call1((column,))?.call_method1("__lt__", (maximum,)));
        }

        for (column, null_count) in stats.null_count.iter().filter_map(|(k, v)| match v {
            ColumnCountStat::Value(val) => Some((k, val)),
            // Ignoring nested values (I think that's what this does...)
            _ => None,
        }) {
            if *null_count == stats.num_records {
                expressions.push(field.call1((column.clone(),))?.call_method0("is_null"));
            }

            if *null_count == 0 {
                expressions.push(
                    field
                        .call1((column.clone(),))?
                        .call_method0("is_null")?
                        .call_method0("__invert__"),
                );
            }
        }
    }

    if expressions.len() == 0 {
        return Ok(None);
    } else {
        return expressions
            .into_iter()
            .reduce(|accum, item| accum?.getattr("__and__")?.call1((item?,)))
            .transpose();
    }
}

#[pyclass]
pub struct DeltaStorageFsBackend {
    _storage: Box<dyn StorageBackend>,
}

#[pymethods]
impl DeltaStorageFsBackend {
    #[new]
    fn new(table_uri: &str) -> PyResult<Self> {
        let storage =
            storage::get_backend_for_uri(table_uri).map_err(PyDeltaTableError::from_storage)?;
        Ok(Self { _storage: storage })
    }

    fn normalize_path(&self, path: &str) -> PyResult<String> {
        Ok(self._storage.trim_path(path))
    }

    fn head_obj<'py>(&mut self, py: Python<'py>, path: &str) -> PyResult<&'py PyTuple> {
        let obj = rt()?
            .block_on(self._storage.head_obj(path))
            .map_err(PyDeltaTableError::from_storage)?;
        Ok(PyTuple::new(
            py,
            &[obj.path, obj.modified.timestamp().to_string()],
        ))
    }

    fn get_obj<'py>(&mut self, py: Python<'py>, path: &str) -> PyResult<&'py PyBytes> {
        let obj = rt()?
            .block_on(self._storage.get_obj(path))
            .map_err(PyDeltaTableError::from_storage)?;
        Ok(PyBytes::new(py, &obj))
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
    m.add_class::<RawDeltaTableMetaData>()?;
    m.add_class::<DeltaStorageFsBackend>()?;
    m.add("PyDeltaTableError", py.get_type::<PyDeltaTableError>())?;
    Ok(())
}
