use std::sync::Arc;

use deltalake::{
    datafusion::{catalog::TableProvider, prelude::SessionContext},
    delta_datafusion::{DeltaScanConfig, DeltaScanNext, DeltaSessionContext},
};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatchReader;

use crate::{RawDeltaTable, convert_stream_to_reader, error::PythonError, utils::rt};

/// PyQueryBuilder supports the _experimental_ `QueryBuilder` Python interface which allows users
/// to take advantage of the [Apache DataFusion](https://datafusion.apache.org) engine already
/// present in the Python package.
#[pyclass(module = "deltalake._internal")]
#[derive(Default)]
pub(crate) struct PyQueryBuilder {
    /// DataFusion [SessionContext] to hold mappings of registered tables
    ctx: SessionContext,
}

#[pymethods]
impl PyQueryBuilder {
    #[new]
    pub fn new() -> Self {
        let delta_ctx = DeltaSessionContext::new();
        let ctx = delta_ctx.into_inner();

        PyQueryBuilder { ctx }
    }

    /// Register the given [RawDeltaTable] into the [SessionContext] using the provided
    /// `table_name`
    ///
    /// Once called, the provided `delta_table` will be referenceable in SQL queries so long as
    /// another table of the same name is not registered over it.
    pub fn register(&self, table_name: &str, delta_table: &RawDeltaTable) -> PyResult<()> {
        let snapshot = delta_table.cloned_state()?;
        let log_store = delta_table.log_store()?;
        let url = log_store.root_url();

        self.ctx
            .register_object_store(url, log_store.root_object_store(None));

        let config = DeltaScanConfig::new().with_wrap_partition_values(false);
        let provider = Arc::new(DeltaScanNext::new(snapshot, config).map_err(PythonError::from)?)
            as Arc<dyn TableProvider>;

        self.ctx
            .register_table(table_name, provider)
            .map_err(PythonError::from)?;

        Ok(())
    }

    /// Execute the given SQL command within the [SessionContext] of this instance
    ///
    /// **NOTE:** Since this function returns a materialized Python list of `RecordBatch`
    /// instances, it may result unexpected memory consumption for queries which return large data
    /// sets.
    pub fn execute(&self, py: Python, sql: &str) -> PyResult<PyRecordBatchReader> {
        let stream = py.detach(|| {
            rt().block_on(async {
                let df = self.ctx.sql(sql).await?;
                df.execute_stream().await
            })
            .map_err(PythonError::from)
        })?;

        let stream = convert_stream_to_reader(stream);
        Ok(PyRecordBatchReader::new(stream))
    }
}
