use std::sync::Arc;

use deltalake::{
    arrow::pyarrow::ToPyArrow,
    datafusion::prelude::SessionContext,
    delta_datafusion::{DeltaScanConfigBuilder, DeltaSessionConfig, DeltaTableProvider},
};
use pyo3::prelude::*;

use crate::{error::PythonError, utils::rt, RawDeltaTable};

#[pyclass(module = "deltalake._internal")]
pub(crate) struct PyQueryBuilder {
    _ctx: SessionContext,
}

#[pymethods]
impl PyQueryBuilder {
    #[new]
    pub fn new() -> Self {
        let config = DeltaSessionConfig::default().into();
        let _ctx = SessionContext::new_with_config(config);

        PyQueryBuilder { _ctx }
    }

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

        self._ctx
            .register_table(table_name, provider)
            .map_err(PythonError::from)?;

        Ok(())
    }

    pub fn execute(&self, py: Python, sql: &str) -> PyResult<PyObject> {
        let batches = py.allow_threads(|| {
            rt().block_on(async {
                let df = self._ctx.sql(sql).await?;
                df.collect().await
            })
            .map_err(PythonError::from)
        })?;

        batches.to_pyarrow(py)
    }
}
