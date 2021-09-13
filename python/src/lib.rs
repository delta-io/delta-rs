#![deny(warnings)]

extern crate arrow;
extern crate pyo3;

use arrow::datatypes::Schema as ArrowSchema;
use chrono::{DateTime, FixedOffset, Utc};
use deltalake::partitions::PartitionFilter;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::collections::HashMap;
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
    created_time: deltalake::DeltaDataTypeTimestamp,
    #[pyo3(get)]
    configuration: HashMap<String, Option<String>>,
}

#[pyclass]
#[derive(Clone)]
struct RawCommitInfoJobInfo {
    #[pyo3(get)]
    job_id: String,
    #[pyo3(get)]
    job_name: String,
    #[pyo3(get)]
    run_id: String,
    #[pyo3(get)]
    job_owner_id: String,
    #[pyo3(get)]
    trigger_type: String,
}

#[pyclass]
#[derive(Clone)]
struct RawCommitInfoNotebookInfo {
    #[pyo3(get)]
    notebook_id: String,
}

#[pyclass]
#[derive(Clone)]
struct RawDeltaTableCommitInfo {
    #[pyo3(get)]
    version: Option<deltalake::DeltaDataTypeLong>,
    #[pyo3(get)]
    timestamp: deltalake::DeltaDataTypeTimestamp,
    #[pyo3(get)]
    user_id: Option<String>,
    #[pyo3(get)]
    user_name: Option<String>,
    #[pyo3(get)]
    operation: String,
    #[pyo3(get)]
    operation_parameters: HashMap<String, Option<String>>,
    #[pyo3(get)]
    job: Option<RawCommitInfoJobInfo>,
    #[pyo3(get)]
    notebook: Option<RawCommitInfoNotebookInfo>,
    #[pyo3(get)]
    cluster_id: Option<String>,
    #[pyo3(get)]
    read_version: Option<deltalake::DeltaDataTypeLong>,
    #[pyo3(get)]
    isolation_level: Option<String>,
    #[pyo3(get)]
    is_blind_append: Option<bool>,
    #[pyo3(get)]
    operation_metrics: Option<HashMap<String, Option<String>>>,
    #[pyo3(get)]
    user_metadata: Option<String>,
    #[pyo3(get)]
    tags: Option<HashMap<String, Option<String>>>,
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
        Ok(self._table.get_file_uris())
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
    pub fn vacuum(&mut self, dry_run: bool, retention_hours: u64) -> PyResult<Vec<String>> {
        rt()?
            .block_on(self._table.vacuum(retention_hours, dry_run))
            .map_err(PyDeltaTableError::from_raw)
    }

    // Run the History command on the Delta Table: Returns provenance information, including the operation, user, and so on, for each write to a table.
    pub fn history(&mut self, limit: Option<usize>) -> PyResult<Vec<RawDeltaTableCommitInfo>> {
        let history = self
            ._table
            .history(limit)
            .map_err(PyDeltaTableError::from_raw)?;
        Ok(history
            .iter()
            .map(|c| RawDeltaTableCommitInfo {
                version: c.version,
                timestamp: c.timestamp,
                user_id: c.user_id.clone(),
                user_name: c.user_name.clone(),
                operation: c.operation.clone(),
                operation_parameters: c.operation_parameters.clone(),
                job: match &c.job {
                    Some(j) => Some(RawCommitInfoJobInfo {
                        job_id: j.job_id.clone(),
                        job_name: j.job_name.clone(),
                        run_id: j.run_id.clone(),
                        job_owner_id: j.job_owner_id.clone(),
                        trigger_type: j.trigger_type.clone(),
                    }),
                    None => None,
                },
                notebook: match &c.notebook {
                    Some(n) => Some(RawCommitInfoNotebookInfo {
                        notebook_id: n.notebook_id.clone(),
                    }),
                    None => None,
                },
                cluster_id: c.cluster_id.clone(),
                read_version: c.read_version.clone(),
                isolation_level: c.isolation_level.clone(),
                is_blind_append: c.is_blind_append.clone(),
                operation_metrics: c.operation_metrics.clone(),
                user_metadata: c.user_metadata.clone(),
                tags: c.tags.clone()
            })
            .collect::<Vec<RawDeltaTableCommitInfo>>())
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
    m.add_class::<RawDeltaTableCommitInfo>()?;
    m.add("DeltaTableError", py.get_type::<PyDeltaTableError>())?;
    Ok(())
}
