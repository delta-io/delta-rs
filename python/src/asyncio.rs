use crate::filesystem;
use crate::rt;
use crate::FsConfig;
use crate::PartitionFilterValue;
use crate::PyArrowType;
use crate::PyDeltaTableError;
use crate::RawDeltaTable;
use crate::RawDeltaTableMetaData;
use crate::RecordBatch;

use chrono::Utc;
use chrono::{DateTime, FixedOffset};
use deltalake::arrow::datatypes::Schema as ArrowSchema;
use pyo3::prelude::*;
use pyo3::types::PyFrozenSet;
use pyo3::types::PyType;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

struct RawAsyncDeltaTableCore {
    mutex: Mutex<RawDeltaTable>,
}

#[pyclass]
pub struct RawDeltaTableAsync(Arc<RawAsyncDeltaTableCore>);

#[pymethods]
impl RawDeltaTableAsync {
    #[new]
    #[pyo3(signature = (table_uri, version = None, storage_options = None, without_files = false, init = true))]
    fn new(
        table_uri: &str,
        version: Option<deltalake::DeltaDataTypeLong>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        init: bool,
    ) -> PyResult<Self> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri);
        let options = storage_options.clone().unwrap_or_default();
        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        if let Some(version) = version {
            builder = builder.with_version(version)
        }

        if without_files {
            builder = builder.without_files()
        }

        let table = if init {
            rt()?
                .block_on(builder.load())
                .map_err(PyDeltaTableError::from_raw)?
        } else {
            builder.build().map_err(PyDeltaTableError::from_raw)?
        };
        Ok(RawDeltaTableAsync(Arc::new(RawAsyncDeltaTableCore {
            mutex: Mutex::new(RawDeltaTable {
                _table: table,
                _config: FsConfig {
                    root_url: table_uri.into(),
                    options,
                },
            }),
        })))
    }

    fn load<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .load()
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    #[classmethod]
    fn get_table_uri_from_data_catalog<'a>(
        _cls: &PyType,
        data_catalog: String,
        database_name: String,
        table_name: String,
        data_catalog_id: Option<String>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let data_catalog = deltalake::data_catalog::get_data_catalog(&data_catalog)
            .map_err(PyDeltaTableError::from_data_catalog)?;
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let table_uri = data_catalog
                .get_table_storage_location(data_catalog_id, &database_name, &table_name)
                .await
                .map_err(PyDeltaTableError::from_data_catalog)?;

            Ok(table_uri)
        })
    }

    pub fn table_uri(&self) -> PyResult<String> {
        rt()?.block_on(self.0.mutex.lock()).table_uri()
    }

    pub fn version(&self) -> PyResult<i64> {
        rt()?.block_on(self.0.mutex.lock()).version()
    }

    pub fn metadata(&self) -> PyResult<RawDeltaTableMetaData> {
        rt()?.block_on(self.0.mutex.lock()).metadata()
    }

    pub fn protocol_versions(&self) -> PyResult<(i32, i32)> {
        rt()?.block_on(self.0.mutex.lock()).protocol_versions()
    }

    pub fn load_version<'a>(
        &mut self,
        version: deltalake::DeltaDataTypeVersion,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .load_version(version)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    pub fn load_with_datetime<'a>(&mut self, ds: &str, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        let datetime = DateTime::<Utc>::from(
            DateTime::<FixedOffset>::parse_from_rfc3339(ds)
                .map_err(PyDeltaTableError::from_chrono)?,
        );
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .load_with_datetime(datetime)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    pub fn files_by_partitions(
        &self,
        partitions_filters: Vec<(&str, &str, PartitionFilterValue)>,
    ) -> PyResult<Vec<String>> {
        rt()?
            .block_on(self.0.mutex.lock())
            .files_by_partitions(partitions_filters)
    }

    pub fn files(
        &self,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        rt()?.block_on(self.0.mutex.lock()).files(partition_filters)
    }

    pub fn file_uris(
        &self,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        rt()?
            .block_on(self.0.mutex.lock())
            .file_uris(partition_filters)
    }

    #[getter]
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        rt()?.block_on(self.0.mutex.lock()).schema(py)
    }

    #[pyo3(signature = (dry_run, retention_hours = None, enforce_retention_duration = true))]
    pub fn vacuum(
        &mut self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
    ) -> PyResult<Vec<String>> {
        rt()?.block_on(self.0.mutex.lock()).vacuum(
            dry_run,
            retention_hours,
            enforce_retention_duration,
        )
    }

    pub fn history<'a>(&mut self, limit: Option<usize>, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let history = this
                .mutex
                .lock()
                .await
                ._table
                .history(limit)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            let result: Vec<String> = history
                .iter()
                .map(|c| serde_json::to_string(c).unwrap())
                .collect();
            Ok(result)
        })
    }

    pub fn arrow_schema_json(&self) -> PyResult<String> {
        rt()?.block_on(self.0.mutex.lock()).arrow_schema_json()
    }

    pub fn update_incremental<'a>(&mut self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let this = Arc::clone(&self.0);
        pyo3_asyncio::tokio::future_into_py(py, async move {
            this.mutex
                .lock()
                .await
                ._table
                .update_incremental(None)
                .await
                .map_err(PyDeltaTableError::from_raw)?;
            Ok(())
        })
    }

    pub fn dataset_partitions<'py>(
        &mut self,
        py: Python<'py>,
        schema: PyArrowType<ArrowSchema>,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<(String, Option<&'py PyAny>)>> {
        rt()?
            .block_on(self.0.mutex.lock())
            .dataset_partitions(py, schema, partition_filters)
    }

    pub fn get_active_partitions<'py>(
        &self,
        partitions_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        py: Python<'py>,
    ) -> PyResult<&'py PyFrozenSet> {
        rt()?
            .block_on(self.0.mutex.lock())
            .get_active_partitions(partitions_filters, py)
    }

    pub fn get_py_storage_backend(&self) -> PyResult<filesystem::DeltaFileSystemHandler> {
        rt()?.block_on(self.0.mutex.lock()).get_py_storage_backend()
    }

    pub fn get_add_actions(&self, flatten: bool) -> PyResult<PyArrowType<RecordBatch>> {
        rt()?.block_on(self.0.mutex.lock()).get_add_actions(flatten)
    }
}
