#![deny(warnings)]

mod error;
mod filesystem;
mod schema;
mod utils;

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::pyarrow::PyArrowType;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use deltalake::action::{
    self, Action, ColumnCountStat, ColumnValueStat, DeltaOperation, SaveMode, Stats,
};
use deltalake::arrow::compute::concat_batches;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::{self, datatypes::Schema as ArrowSchema};
use deltalake::builder::DeltaTableBuilder;
use deltalake::checkpoints::create_checkpoint;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::errors::DeltaTableError;
use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::operations::transaction::commit;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::partitions::PartitionFilter;
use deltalake::{DeltaOps, Invariant, Schema};
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyFrozenSet, PyType};

use crate::error::DeltaProtocolError;
use crate::error::PythonError;
use crate::filesystem::FsConfig;
use crate::schema::schema_to_pyobject;

#[inline]
fn rt() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

#[derive(FromPyObject)]
enum PartitionFilterValue<'a> {
    Single(&'a str),
    Multiple(Vec<&'a str>),
}

#[pyclass]
struct RawDeltaTable {
    _table: deltalake::DeltaTable,
    // storing the config additionally on the table helps us make pickling work.
    _config: FsConfig,
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
    created_time: Option<i64>,
    #[pyo3(get)]
    configuration: HashMap<String, Option<String>>,
}

#[pymethods]
impl RawDeltaTable {
    #[new]
    #[pyo3(signature = (table_uri, version = None, storage_options = None, without_files = false))]
    fn new(
        table_uri: &str,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
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

        let table = rt()?.block_on(builder.load()).map_err(PythonError::from)?;
        Ok(RawDeltaTable {
            _table: table,
            _config: FsConfig {
                root_url: table_uri.into(),
                options,
            },
        })
    }

    #[classmethod]
    fn get_table_uri_from_data_catalog(
        _cls: &PyType,
        data_catalog: &str,
        database_name: &str,
        table_name: &str,
        data_catalog_id: Option<String>,
    ) -> PyResult<String> {
        let data_catalog =
            deltalake::data_catalog::get_data_catalog(data_catalog).map_err(|_| {
                PyValueError::new_err(format!("Catalog '{}' not available.", data_catalog))
            })?;
        let table_uri = rt()?
            .block_on(data_catalog.get_table_storage_location(
                data_catalog_id,
                database_name,
                table_name,
            ))
            .map_err(|err| PyIOError::new_err(err.to_string()))?;

        Ok(table_uri)
    }

    pub fn table_uri(&self) -> PyResult<String> {
        Ok(self._table.table_uri())
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version())
    }

    pub fn metadata(&self) -> PyResult<RawDeltaTableMetaData> {
        let metadata = self._table.get_metadata().map_err(PythonError::from)?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            partition_columns: metadata.partition_columns.clone(),
            created_time: metadata.created_time,
            configuration: metadata.configuration.clone(),
        })
    }

    pub fn protocol_versions(&self) -> PyResult<(i32, i32)> {
        Ok((
            self._table.get_min_reader_version(),
            self._table.get_min_writer_version(),
        ))
    }

    pub fn load_version(&mut self, version: i64) -> PyResult<()> {
        Ok(rt()?
            .block_on(self._table.load_version(version))
            .map_err(PythonError::from)?)
    }

    pub fn load_with_datetime(&mut self, ds: &str) -> PyResult<()> {
        let datetime =
            DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds).map_err(
                |err| PyValueError::new_err(format!("Failed to parse datetime string: {err}")),
            )?);
        Ok(rt()?
            .block_on(self._table.load_with_datetime(datetime))
            .map_err(PythonError::from)?)
    }

    pub fn files_by_partitions(
        &self,
        partitions_filters: Vec<(&str, &str, PartitionFilterValue)>,
    ) -> PyResult<Vec<String>> {
        let partition_filters: Result<Vec<PartitionFilter<&str>>, DeltaTableError> =
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
                .get_files_by_partitions(&filters)
                .map_err(PythonError::from)?
                .into_iter()
                .map(|p| p.to_string())
                .collect()),
            Err(err) => Err(PythonError::from(err).into()),
        }
    }

    pub fn files(
        &self,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(PythonError::from)?;
            Ok(self
                ._table
                .get_files_by_partitions(&filters)
                .map_err(PythonError::from)?
                .into_iter()
                .map(|p| p.to_string())
                .collect())
        } else {
            Ok(self
                ._table
                .get_files_iter()
                .map(|f| f.to_string())
                .collect())
        }
    }

    pub fn file_uris(
        &self,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(PythonError::from)?;
            Ok(self
                ._table
                .get_file_uris_by_partitions(&filters)
                .map_err(PythonError::from)?)
        } else {
            Ok(self._table.get_file_uris().collect())
        }
    }

    #[getter]
    pub fn schema(&self, py: Python) -> PyResult<PyObject> {
        let schema: &Schema = self._table.get_schema().map_err(PythonError::from)?;
        schema_to_pyobject(schema, py)
    }

    /// Run the Vacuum command on the Delta Table: list and delete files no longer referenced
    /// by the Delta table and are older than the retention threshold.
    #[pyo3(signature = (dry_run, retention_hours = None, enforce_retention_duration = true, max_concurrent_requests = 10))]
    pub fn vacuum(
        &mut self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
        max_concurrent_requests: usize,
    ) -> PyResult<Vec<String>> {
        let mut cmd = VacuumBuilder::new(self._table.object_store(), self._table.state.clone())
            .with_enforce_retention_duration(enforce_retention_duration)
            .with_dry_run(dry_run)
            .with_max_concurrent_requests(max_concurrent_requests);
        if let Some(retention_period) = retention_hours {
            cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
        }
        let (table, metrics) = rt()?
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(metrics.files_deleted)
    }

    /// Run the optimize command on the Delta Table: merge small files into a large file by bin-packing.
    #[pyo3(signature = (partition_filters = None, target_size = None, max_concurrent_tasks = None))]
    pub fn compact_optimize(
        &mut self,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
    ) -> PyResult<String> {
        let mut cmd = OptimizeBuilder::new(self._table.object_store(), self._table.state.clone())
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get));
        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())
            .map_err(PythonError::from)?;
        cmd = cmd.with_filters(&converted_filters);

        let (table, metrics) = rt()?
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run z-order variation of optimize
    #[pyo3(signature = (z_order_columns, partition_filters = None, target_size = None, max_concurrent_tasks = None))]
    pub fn z_order_optimize(
        &mut self,
        z_order_columns: Vec<String>,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
    ) -> PyResult<String> {
        let mut cmd = OptimizeBuilder::new(self._table.object_store(), self._table.state.clone())
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
            .with_type(OptimizeType::ZOrder(z_order_columns));
        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())
            .map_err(PythonError::from)?;
        cmd = cmd.with_filters(&converted_filters);

        let (table, metrics) = rt()?
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run the History command on the Delta Table: Returns provenance information, including the operation, user, and so on, for each write to a table.
    pub fn history(&mut self, limit: Option<usize>) -> PyResult<Vec<String>> {
        let history = rt()?
            .block_on(self._table.history(limit))
            .map_err(PythonError::from)?;
        Ok(history
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect())
    }

    pub fn update_incremental(&mut self) -> PyResult<()> {
        Ok(rt()?
            .block_on(self._table.update_incremental(None))
            .map_err(PythonError::from)?)
    }

    pub fn dataset_partitions<'py>(
        &mut self,
        py: Python<'py>,
        schema: PyArrowType<ArrowSchema>,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<Vec<(String, Option<&'py PyAny>)>> {
        let path_set = match partition_filters {
            Some(filters) => Some(HashSet::<_>::from_iter(
                self.files_by_partitions(filters)?.iter().cloned(),
            )),
            None => None,
        };

        self._table
            .get_files_iter()
            .map(|p| p.to_string())
            .zip(self._table.get_partition_values())
            .zip(self._table.get_stats())
            .filter(|((path, _), _)| match &path_set {
                Some(path_set) => path_set.contains(path),
                None => true,
            })
            .map(|((path, partition_values), stats)| {
                let stats = stats.map_err(PythonError::from)?;
                let expression = filestats_to_expression(py, &schema, partition_values, stats)?;
                Ok((path, expression))
            })
            .collect()
    }

    fn get_active_partitions<'py>(
        &self,
        partitions_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        py: Python<'py>,
    ) -> PyResult<&'py PyFrozenSet> {
        let column_names: HashSet<&str> = self
            ._table
            .schema()
            .ok_or_else(|| DeltaProtocolError::new_err("table does not yet have a schema"))?
            .get_fields()
            .iter()
            .map(|field| field.get_name())
            .collect();
        let partition_columns: HashSet<&str> = self
            ._table
            .get_metadata()
            .map_err(PythonError::from)?
            .partition_columns
            .iter()
            .map(|col| col.as_str())
            .collect();

        if let Some(filters) = &partitions_filters {
            let unknown_columns: Vec<&str> = filters
                .iter()
                .map(|(column_name, _, _)| *column_name)
                .filter(|column_name| !column_names.contains(column_name))
                .collect();
            if !unknown_columns.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "Filters include columns that are not in table schema: {unknown_columns:?}"
                )));
            }

            let non_partition_columns: Vec<&str> = filters
                .iter()
                .map(|(column_name, _, _)| *column_name)
                .filter(|column_name| !partition_columns.contains(column_name))
                .collect();

            if !non_partition_columns.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "Filters include columns that are not partition columns: {non_partition_columns:?}"
                )));
            }
        }

        let converted_filters = convert_partition_filters(partitions_filters.unwrap_or_default())
            .map_err(PythonError::from)?;

        let partition_columns: Vec<&str> = partition_columns.into_iter().collect();

        let active_partitions: HashSet<Vec<(&str, Option<&str>)>> = self
            ._table
            .get_state()
            .get_active_add_actions_by_partitions(&converted_filters)
            .map_err(PythonError::from)?
            .map(|add| {
                partition_columns
                    .iter()
                    .map(|col| (*col, add.partition_values.get(*col).unwrap().as_deref()))
                    .collect()
            })
            .collect();

        let active_partitions: Vec<&'py PyFrozenSet> = active_partitions
            .into_iter()
            .map(|part| PyFrozenSet::new(py, part.iter()))
            .collect::<Result<_, PyErr>>()?;
        PyFrozenSet::new(py, active_partitions.into_iter())
    }

    fn create_write_transaction(
        &mut self,
        add_actions: Vec<PyAddAction>,
        mode: &str,
        partition_by: Vec<String>,
        schema: PyArrowType<ArrowSchema>,
        partitions_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<()> {
        let mode = save_mode_from_str(mode)?;
        let schema: Schema = (&schema.0).try_into().map_err(PythonError::from)?;

        let existing_schema = self._table.get_schema().map_err(PythonError::from)?;

        let mut actions: Vec<action::Action> = add_actions
            .iter()
            .map(|add| Action::add(add.into()))
            .collect();

        match mode {
            SaveMode::Overwrite => {
                let converted_filters =
                    convert_partition_filters(partitions_filters.unwrap_or_default())
                        .map_err(PythonError::from)?;

                let add_actions = self
                    ._table
                    .get_state()
                    .get_active_add_actions_by_partitions(&converted_filters)
                    .map_err(PythonError::from)?;

                for old_add in add_actions {
                    let remove_action = Action::remove(action::Remove {
                        path: old_add.path.clone(),
                        deletion_timestamp: Some(current_timestamp()),
                        data_change: true,
                        extended_file_metadata: Some(old_add.tags.is_some()),
                        partition_values: Some(old_add.partition_values.clone()),
                        size: Some(old_add.size),
                        tags: old_add.tags.clone(),
                    });
                    actions.push(remove_action);
                }

                // Update metadata with new schema
                if &schema != existing_schema {
                    let mut metadata = self
                        ._table
                        .get_metadata()
                        .map_err(PythonError::from)?
                        .clone();
                    metadata.schema = schema;
                    let metadata_action = action::MetaData::try_from(metadata)
                        .map_err(|_| PyValueError::new_err("Failed to reparse metadata"))?;
                    actions.push(Action::metaData(metadata_action));
                }
            }
            _ => {
                // This should be unreachable from Python
                if &schema != existing_schema {
                    DeltaProtocolError::new_err("Cannot change schema except in overwrite.");
                }
            }
        }

        let operation = DeltaOperation::Write {
            mode,
            partition_by: Some(partition_by),
            predicate: None,
        };
        let store = self._table.object_store();

        rt()?
            .block_on(commit(
                &*store,
                &actions,
                operation,
                self._table.get_state(),
                None,
            ))
            .map_err(PythonError::from)?;

        Ok(())
    }

    pub fn get_py_storage_backend(&self) -> PyResult<filesystem::DeltaFileSystemHandler> {
        Ok(filesystem::DeltaFileSystemHandler {
            inner: self._table.object_store(),
            rt: Arc::new(rt()?),
            config: self._config.clone(),
        })
    }

    pub fn create_checkpoint(&self) -> PyResult<()> {
        rt()?
            .block_on(create_checkpoint(&self._table))
            .map_err(PythonError::from)?;

        Ok(())
    }

    pub fn get_add_actions(&self, flatten: bool) -> PyResult<PyArrowType<RecordBatch>> {
        Ok(PyArrowType(
            self._table
                .get_state()
                .add_actions_table(flatten)
                .map_err(PythonError::from)?,
        ))
    }
}

fn convert_partition_filters<'a>(
    partitions_filters: Vec<(&'a str, &'a str, PartitionFilterValue<'a>)>,
) -> Result<Vec<PartitionFilter<&'a str>>, DeltaTableError> {
    partitions_filters
        .into_iter()
        .map(|filter| match filter {
            (key, op, PartitionFilterValue::Single(v)) => PartitionFilter::try_from((key, op, v)),
            (key, op, PartitionFilterValue::Multiple(v)) => PartitionFilter::try_from((key, op, v)),
        })
        .collect()
}

fn json_value_to_py(value: &serde_json::Value, py: Python) -> PyObject {
    match value {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(val) => val.to_object(py),
        serde_json::Value::Number(val) => {
            if val.is_f64() {
                val.as_f64().expect("not an f64").to_object(py)
            } else if val.is_i64() {
                val.as_i64().expect("not an i64").to_object(py)
            } else {
                val.as_u64().expect("not an u64").to_object(py)
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
    schema: &PyArrowType<ArrowSchema>,
    partitions_values: &HashMap<String, Option<String>>,
    stats: Option<Stats>,
) -> PyResult<Option<&'py PyAny>> {
    let ds = PyModule::import(py, "pyarrow.dataset")?;
    let field = ds.getattr("field")?;
    let pa = PyModule::import(py, "pyarrow")?;
    let mut expressions: Vec<PyResult<&PyAny>> = Vec::new();

    let cast_to_type = |column_name: &String, value: PyObject, schema: &ArrowSchema| {
        let column_type = PyArrowType(
            schema
                .field_with_name(column_name)
                .map_err(|_| {
                    PyValueError::new_err(format!("Column not found in schema: {column_name}"))
                })?
                .data_type()
                .clone(),
        )
        .into_py(py);
        pa.call_method1("scalar", (value,))?
            .call_method1("cast", (column_type,))
    };

    for (column, value) in partitions_values.iter() {
        if let Some(value) = value {
            // value is a string, but needs to be parsed into appropriate type
            let converted_value = cast_to_type(column, value.into_py(py), &schema.0)?;
            expressions.push(
                field
                    .call1((column,))?
                    .call_method1("__eq__", (converted_value,)),
            );
        }
    }

    if let Some(stats) = stats {
        for (col_name, minimum) in stats.min_values.iter().filter_map(|(k, v)| match v {
            ColumnValueStat::Value(val) => Some((k.clone(), json_value_to_py(val, py))),
            // TODO(wjones127): Handle nested field statistics.
            // Blocked on https://issues.apache.org/jira/browse/ARROW-11259
            _ => None,
        }) {
            let maybe_minimum = cast_to_type(&col_name, minimum, &schema.0);
            if let Ok(minimum) = maybe_minimum {
                expressions.push(field.call1((col_name,))?.call_method1("__ge__", (minimum,)));
            }
        }

        for (col_name, maximum) in stats.max_values.iter().filter_map(|(k, v)| match v {
            ColumnValueStat::Value(val) => Some((k.clone(), json_value_to_py(val, py))),
            _ => None,
        }) {
            let maybe_maximum = cast_to_type(&col_name, maximum, &schema.0);
            if let Ok(maximum) = maybe_maximum {
                expressions.push(field.call1((col_name,))?.call_method1("__le__", (maximum,)));
            }
        }

        for (col_name, null_count) in stats.null_count.iter().filter_map(|(k, v)| match v {
            ColumnCountStat::Value(val) => Some((k, val)),
            _ => None,
        }) {
            if *null_count == stats.num_records {
                expressions.push(field.call1((col_name.clone(),))?.call_method0("is_null"));
            }

            if *null_count == 0 {
                expressions.push(field.call1((col_name.clone(),))?.call_method0("is_valid"));
            }
        }
    }

    if expressions.is_empty() {
        Ok(None)
    } else {
        expressions
            .into_iter()
            .reduce(|accum, item| accum?.getattr("__and__")?.call1((item?,)))
            .transpose()
    }
}

#[pyfunction]
fn rust_core_version() -> &'static str {
    deltalake::crate_version()
}

#[pyfunction]
fn batch_distinct(batch: PyArrowType<RecordBatch>) -> PyResult<PyArrowType<RecordBatch>> {
    let ctx = SessionContext::new();
    let schema = batch.0.schema();
    ctx.register_batch("batch", batch.0)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    let batches = rt()?
        .block_on(async { ctx.table("batch").await?.distinct()?.collect().await })
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

    Ok(PyArrowType(
        concat_batches(&schema, &batches).map_err(PythonError::from)?,
    ))
}

fn save_mode_from_str(value: &str) -> PyResult<SaveMode> {
    match value {
        "append" => Ok(SaveMode::Append),
        "overwrite" => Ok(SaveMode::Overwrite),
        "error" => Ok(SaveMode::ErrorIfExists),
        "ignore" => Ok(SaveMode::Ignore),
        _ => Err(PyValueError::new_err("Invalid save mode")),
    }
}

fn current_timestamp() -> i64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_millis().try_into().unwrap()
}

#[derive(FromPyObject)]
pub struct PyAddAction {
    path: String,
    size: i64,
    partition_values: HashMap<String, Option<String>>,
    modification_time: i64,
    data_change: bool,
    stats: Option<String>,
}

impl From<&PyAddAction> for action::Add {
    fn from(action: &PyAddAction) -> Self {
        action::Add {
            path: action.path.clone(),
            size: action.size,
            partition_values: action.partition_values.clone(),
            partition_values_parsed: None,
            modification_time: action.modification_time,
            data_change: action.data_change,
            stats: action.stats.clone(),
            stats_parsed: None,
            tags: None,
        }
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn write_new_deltalake(
    table_uri: String,
    schema: PyArrowType<ArrowSchema>,
    add_actions: Vec<PyAddAction>,
    _mode: &str,
    partition_by: Vec<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(storage_options.unwrap_or_default())
        .build()
        .map_err(PythonError::from)?;

    let schema: Schema = (&schema.0).try_into().map_err(PythonError::from)?;

    let mut builder = DeltaOps(table)
        .create()
        .with_columns(schema.get_fields().clone())
        .with_partition_columns(partition_by)
        .with_actions(add_actions.iter().map(|add| Action::add(add.into())));

    if let Some(name) = &name {
        builder = builder.with_table_name(name);
    };

    if let Some(description) = &description {
        builder = builder.with_comment(description);
    };

    if let Some(config) = configuration {
        builder = builder.with_configuration(config);
    };

    rt()?
        .block_on(builder.into_future())
        .map_err(PythonError::from)?;

    Ok(())
}

#[pyclass(name = "DeltaDataChecker", text_signature = "(invariants)")]
struct PyDeltaDataChecker {
    inner: DeltaDataChecker,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl PyDeltaDataChecker {
    #[new]
    fn new(invariants: Vec<(String, String)>) -> Self {
        let invariants: Vec<Invariant> = invariants
            .into_iter()
            .map(|(field_name, invariant_sql)| Invariant {
                field_name,
                invariant_sql,
            })
            .collect();
        Self {
            inner: DeltaDataChecker::new(invariants),
            rt: tokio::runtime::Runtime::new().unwrap(),
        }
    }

    fn check_batch(&self, batch: PyArrowType<RecordBatch>) -> PyResult<()> {
        Ok(self
            .rt
            .block_on(async { self.inner.check_batch(&batch.0).await })
            .map_err(PythonError::from)?)
    }
}

#[pymodule]
// module name need to match project name
fn _internal(py: Python, m: &PyModule) -> PyResult<()> {
    use crate::error::{CommitFailedError, DeltaError, TableNotFoundError};
    m.add("DeltaError", py.get_type::<DeltaError>())?;
    m.add("CommitFailedError", py.get_type::<CommitFailedError>())?;
    m.add("DeltaProtocolError", py.get_type::<DeltaProtocolError>())?;
    m.add("TableNotFoundError", py.get_type::<TableNotFoundError>())?;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(pyo3::wrap_pyfunction!(rust_core_version, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(write_new_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(batch_distinct, m)?)?;
    m.add_class::<RawDeltaTable>()?;
    m.add_class::<RawDeltaTableMetaData>()?;
    m.add_class::<PyDeltaDataChecker>()?;
    // There are issues with submodules, so we will expose them flat for now
    // See also: https://github.com/PyO3/pyo3/issues/759
    m.add_class::<schema::PrimitiveType>()?;
    m.add_class::<schema::ArrayType>()?;
    m.add_class::<schema::MapType>()?;
    m.add_class::<schema::Field>()?;
    m.add_class::<schema::StructType>()?;
    m.add_class::<schema::PySchema>()?;
    m.add_class::<filesystem::DeltaFileSystemHandler>()?;
    m.add_class::<filesystem::ObjectInputFile>()?;
    m.add_class::<filesystem::ObjectOutputStream>()?;
    Ok(())
}
