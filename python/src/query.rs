use std::sync::Arc;

use deltalake::{
    arrow::pyarrow::ToPyArrow,
    datafusion::prelude::SessionContext,
    delta_datafusion::{DeltaScanConfigBuilder, DeltaSessionConfig, DeltaTableProvider},
};
use pyo3::prelude::*;

use crate::{error::PythonError, utils::rt, RawDeltaTable};

/// PyQueryBuilder supports the _experimental_ `QueryBuilder` Pythoh interface which allows users
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
        let config = DeltaSessionConfig::default().into();
        let ctx = SessionContext::new_with_config(config);

        PyQueryBuilder { ctx }
    }

    /// Register the given [RawDeltaTable] into the [SessionContext] using the provided
    /// `table_name`
    ///
    /// Once called, the provided `delta_table` will be referencable in SQL queries so long as
    /// another table of the same name is not registered over it.
    pub fn register(&self, table_name: &str, delta_table: &RawDeltaTable) -> PyResult<()> {
        let snapshot = delta_table._table.snapshot().map_err(PythonError::from)?;
        let log_store = delta_table._table.log_store();

        let scan_config = DeltaScanConfigBuilder::default()
            .build(snapshot)
            .map_err(PythonError::from)?;

        let provider = Arc::new(
            DeltaTableProvider::try_new(snapshot.clone(), log_store, scan_config)
                .map_err(PythonError::from)?,
        );

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
    pub fn execute(&self, py: Python, sql: &str) -> PyResult<PyObject> {
        let batches = py.allow_threads(|| {
            rt().block_on(async {
                let df = self.ctx.sql(sql).await?;
                df.collect().await
            })
            .map_err(PythonError::from)
        })?;

        batches.to_pyarrow(py)
    }
}
