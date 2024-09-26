mod error;
mod features;
mod filesystem;
mod merge;
mod schema;
mod utils;

use std::collections::{HashMap, HashSet};
use std::future::IntoFuture;
use std::str::FromStr;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::pyarrow::PyArrowType;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::StructField;
use deltalake::arrow::compute::concat_batches;
use deltalake::arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use deltalake::arrow::record_batch::{RecordBatch, RecordBatchIterator};
use deltalake::arrow::{self, datatypes::Schema as ArrowSchema};
use deltalake::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake::datafusion::physical_plan::ExecutionPlan;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::{
    scalars::ScalarExt, Action, Add, Invariant, LogicalFile, Remove, StructType,
};
use deltalake::operations::add_column::AddColumnBuilder;
use deltalake::operations::add_feature::AddTableFeatureBuilder;
use deltalake::operations::collect_sendable_stream;
use deltalake::operations::constraints::ConstraintBuilder;
use deltalake::operations::convert_to_delta::{ConvertToDeltaBuilder, PartitionStrategy};
use deltalake::operations::delete::DeleteBuilder;
use deltalake::operations::drop_constraints::DropConstraintBuilder;
use deltalake::operations::filesystem_check::FileSystemCheckBuilder;
use deltalake::operations::load_cdf::CdfLoadBuilder;
use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::operations::restore::RestoreBuilder;
use deltalake::operations::set_tbl_properties::SetTablePropertiesBuilder;
use deltalake::operations::transaction::{
    CommitBuilder, CommitProperties, TableReference, PROTOCOL,
};
use deltalake::operations::update::UpdateBuilder;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::errors::ParquetError;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::partitions::PartitionFilter;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::storage::IORuntime;
use deltalake::DeltaTableBuilder;
use deltalake::{DeltaOps, DeltaResult};
use error::DeltaError;
use futures::future::join_all;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyFrozenSet};
use serde_json::{Map, Value};

use crate::error::DeltaProtocolError;
use crate::error::PythonError;
use crate::features::TableFeatures;
use crate::filesystem::FsConfig;
use crate::merge::PyMergeBuilder;
use crate::schema::{schema_to_pyobject, Field};
use crate::utils::rt;

#[derive(FromPyObject)]
enum PartitionFilterValue {
    Single(PyBackedStr),
    Multiple(Vec<PyBackedStr>),
}

#[pyclass(module = "deltalake._internal")]
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

type StringVec = Vec<String>;

#[pymethods]
impl RawDeltaTable {
    #[new]
    #[pyo3(signature = (table_uri, version = None, storage_options = None, without_files = false, log_buffer_size = None))]
    fn new(
        py: Python,
        table_uri: &str,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        log_buffer_size: Option<usize>,
    ) -> PyResult<Self> {
        py.allow_threads(|| {
            let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri)
                .with_io_runtime(IORuntime::default());
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
            if let Some(buf_size) = log_buffer_size {
                builder = builder
                    .with_log_buffer_size(buf_size)
                    .map_err(PythonError::from)?;
            }

            let table = rt().block_on(builder.load()).map_err(PythonError::from)?;
            Ok(RawDeltaTable {
                _table: table,
                _config: FsConfig {
                    root_url: table_uri.into(),
                    options,
                },
            })
        })
    }

    #[pyo3(signature = (table_uri, storage_options = None))]
    #[staticmethod]
    pub fn is_deltatable(
        table_uri: &str,
        storage_options: Option<HashMap<String, String>>,
    ) -> PyResult<bool> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri);
        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
        }
        Ok(rt()
            .block_on(async {
                match builder.build() {
                    Ok(table) => table.verify_deltatable_existence().await,
                    Err(err) => Err(err),
                }
            })
            .map_err(PythonError::from)?)
    }

    pub fn table_uri(&self) -> PyResult<String> {
        Ok(self._table.table_uri())
    }

    pub fn version(&self) -> PyResult<i64> {
        Ok(self._table.version())
    }

    pub fn has_files(&self) -> PyResult<bool> {
        Ok(self._table.config.require_files)
    }

    pub fn metadata(&self) -> PyResult<RawDeltaTableMetaData> {
        let metadata = self._table.metadata().map_err(PythonError::from)?;
        Ok(RawDeltaTableMetaData {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            partition_columns: metadata.partition_columns.clone(),
            created_time: metadata.created_time,
            configuration: metadata.configuration.clone(),
        })
    }

    pub fn protocol_versions(&self) -> PyResult<(i32, i32, Option<StringVec>, Option<StringVec>)> {
        let table_protocol = self._table.protocol().map_err(PythonError::from)?;
        Ok((
            table_protocol.min_reader_version,
            table_protocol.min_writer_version,
            table_protocol
                .writer_features
                .as_ref()
                .and_then(|features| {
                    let empty_set = !features.is_empty();
                    empty_set.then(|| {
                        features
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                    })
                }),
            table_protocol
                .reader_features
                .as_ref()
                .and_then(|features| {
                    let empty_set = !features.is_empty();
                    empty_set.then(|| {
                        features
                            .iter()
                            .map(|v| v.to_string())
                            .collect::<Vec<String>>()
                    })
                }),
        ))
    }

    pub fn check_can_write_timestamp_ntz(&self, schema: PyArrowType<ArrowSchema>) -> PyResult<()> {
        let schema: StructType = (&schema.0).try_into().map_err(PythonError::from)?;
        Ok(PROTOCOL
            .check_can_write_timestamp_ntz(
                self._table.snapshot().map_err(PythonError::from)?,
                &schema,
            )
            .map_err(|e| DeltaTableError::Generic(e.to_string()))
            .map_err(PythonError::from)?)
    }

    pub fn load_version(&mut self, py: Python, version: i64) -> PyResult<()> {
        py.allow_threads(|| {
            Ok(rt()
                .block_on(self._table.load_version(version))
                .map_err(PythonError::from)?)
        })
    }

    pub fn get_latest_version(&mut self, py: Python) -> PyResult<i64> {
        py.allow_threads(|| {
            Ok(rt()
                .block_on(self._table.get_latest_version())
                .map_err(PythonError::from)?)
        })
    }

    pub fn get_num_index_cols(&mut self) -> PyResult<i32> {
        Ok(self
            ._table
            .snapshot()
            .map_err(PythonError::from)?
            .config()
            .num_indexed_cols())
    }

    pub fn get_stats_columns(&mut self) -> PyResult<Option<Vec<String>>> {
        Ok(self
            ._table
            .snapshot()
            .map_err(PythonError::from)?
            .config()
            .stats_columns()
            .map(|v| v.iter().map(|v| v.to_string()).collect::<Vec<String>>()))
    }

    pub fn load_with_datetime(&mut self, py: Python, ds: &str) -> PyResult<()> {
        py.allow_threads(|| {
            let datetime =
                DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds).map_err(
                    |err| PyValueError::new_err(format!("Failed to parse datetime string: {err}")),
                )?);
            Ok(rt()
                .block_on(self._table.load_with_datetime(datetime))
                .map_err(PythonError::from)?)
        })
    }

    pub fn files(
        &self,
        py: Python,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }
        py.allow_threads(|| {
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
                    .map_err(PythonError::from)?
                    .map(|f| f.to_string())
                    .collect())
            }
        })
    }

    pub fn file_uris(
        &self,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
    ) -> PyResult<Vec<String>> {
        if !self._table.config.require_files {
            return Err(DeltaError::new_err("Table is initiated without files."));
        }

        if let Some(filters) = partition_filters {
            let filters = convert_partition_filters(filters).map_err(PythonError::from)?;
            Ok(self
                ._table
                .get_file_uris_by_partitions(&filters)
                .map_err(PythonError::from)?)
        } else {
            Ok(self
                ._table
                .get_file_uris()
                .map_err(PythonError::from)?
                .collect())
        }
    }

    #[getter]
    pub fn schema<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let schema: &StructType = self._table.get_schema().map_err(PythonError::from)?;
        schema_to_pyobject(schema.to_owned(), py)
    }

    /// Run the Vacuum command on the Delta Table: list and delete files no longer referenced
    /// by the Delta table and are older than the retention threshold.
    #[pyo3(signature = (dry_run, retention_hours = None, enforce_retention_duration = true, commit_properties=None, post_commithook_properties=None))]
    pub fn vacuum(
        &mut self,
        py: Python,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<Vec<String>> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = VacuumBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            )
            .with_enforce_retention_duration(enforce_retention_duration)
            .with_dry_run(dry_run);
            if let Some(retention_period) = retention_hours {
                cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }
            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(metrics.files_deleted)
    }

    /// Run the UPDATE command on the Delta Table
    #[pyo3(signature = (updates, predicate=None, writer_properties=None, safe_cast = false, commit_properties = None, post_commithook_properties=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn update(
        &mut self,
        py: Python,
        updates: HashMap<String, String>,
        predicate: Option<String>,
        writer_properties: Option<PyWriterProperties>,
        safe_cast: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = UpdateBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            )
            .with_safe_cast(safe_cast);

            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            for (col_name, expression) in updates {
                cmd = cmd.with_update(col_name.clone(), expression.clone());
            }

            if let Some(update_predicate) = predicate {
                cmd = cmd.with_predicate(update_predicate);
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run the optimize command on the Delta Table: merge small files into a large file by bin-packing.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        partition_filters = None,
        target_size = None,
        max_concurrent_tasks = None,
        min_commit_interval = None,
        writer_properties=None,
        commit_properties=None,
        post_commithook_properties=None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn compact_optimize(
        &mut self,
        py: Python,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        min_commit_interval: Option<u64>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = OptimizeBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            )
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get));
            if let Some(size) = target_size {
                cmd = cmd.with_target_size(size);
            }
            if let Some(commit_interval) = min_commit_interval {
                cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
            }

            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            let converted_filters =
                convert_partition_filters(partition_filters.unwrap_or_default())
                    .map_err(PythonError::from)?;
            cmd = cmd.with_filters(&converted_filters);

            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run z-order variation of optimize
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (z_order_columns,
        partition_filters = None,
        target_size = None,
        max_concurrent_tasks = None,
        max_spill_size = 20 * 1024 * 1024 * 1024,
        min_commit_interval = None,
        writer_properties=None,
        commit_properties=None,
        post_commithook_properties=None))]
    pub fn z_order_optimize(
        &mut self,
        py: Python,
        z_order_columns: Vec<String>,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        max_spill_size: usize,
        min_commit_interval: Option<u64>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = OptimizeBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            )
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
            .with_max_spill_size(max_spill_size)
            .with_type(OptimizeType::ZOrder(z_order_columns));
            if let Some(size) = target_size {
                cmd = cmd.with_target_size(size);
            }
            if let Some(commit_interval) = min_commit_interval {
                cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
            }

            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            let converted_filters =
                convert_partition_filters(partition_filters.unwrap_or_default())
                    .map_err(PythonError::from)?;
            cmd = cmd.with_filters(&converted_filters);

            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    #[pyo3(signature = (fields, commit_properties=None, post_commithook_properties=None))]
    pub fn add_columns(
        &mut self,
        py: Python,
        fields: Vec<Field>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = AddColumnBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            );

            let new_fields = fields
                .iter()
                .map(|v| v.inner.clone())
                .collect::<Vec<StructField>>();

            cmd = cmd.with_fields(new_fields);

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }
            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(())
    }

    #[pyo3(signature = (feature, allow_protocol_versions_increase, commit_properties=None, post_commithook_properties=None))]
    pub fn add_feature(
        &mut self,
        py: Python,
        feature: Vec<TableFeatures>,
        allow_protocol_versions_increase: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = AddTableFeatureBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            )
            .with_features(feature)
            .with_allow_protocol_versions_increase(allow_protocol_versions_increase);

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }
            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(())
    }

    #[pyo3(signature = (constraints, commit_properties=None, post_commithook_properties=None))]
    pub fn add_constraints(
        &mut self,
        py: Python,
        constraints: HashMap<String, String>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = ConstraintBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            );

            for (col_name, expression) in constraints {
                cmd = cmd.with_constraint(col_name.clone(), expression.clone());
            }

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(())
    }

    #[pyo3(signature = (name, raise_if_not_exists, commit_properties=None, post_commithook_properties=None))]
    pub fn drop_constraints(
        &mut self,
        py: Python,
        name: String,
        raise_if_not_exists: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        let table = py.allow_threads(|| {
            let mut cmd = DropConstraintBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            )
            .with_constraint(name)
            .with_raise_if_not_exists(raise_if_not_exists);

            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(())
    }

    #[pyo3(signature = (starting_version = 0, ending_version = None, starting_timestamp = None, ending_timestamp = None, columns = None))]
    pub fn load_cdf(
        &mut self,
        py: Python,
        starting_version: i64,
        ending_version: Option<i64>,
        starting_timestamp: Option<String>,
        ending_timestamp: Option<String>,
        columns: Option<Vec<String>>,
    ) -> PyResult<PyArrowType<ArrowArrayStreamReader>> {
        let ctx = SessionContext::new();
        let mut cdf_read = CdfLoadBuilder::new(
            self._table.log_store(),
            self._table.snapshot().map_err(PythonError::from)?.clone(),
        )
        .with_starting_version(starting_version);

        if let Some(ev) = ending_version {
            cdf_read = cdf_read.with_ending_version(ev);
        }
        if let Some(st) = starting_timestamp {
            let starting_ts: DateTime<Utc> = DateTime::<Utc>::from_str(&st)
                .map_err(|pe| PyValueError::new_err(pe.to_string()))?
                .to_utc();
            cdf_read = cdf_read.with_starting_timestamp(starting_ts);
        }
        if let Some(et) = ending_timestamp {
            let ending_ts = DateTime::<Utc>::from_str(&et)
                .map_err(|pe| PyValueError::new_err(pe.to_string()))?
                .to_utc();
            cdf_read = cdf_read.with_starting_timestamp(ending_ts);
        }

        if let Some(columns) = columns {
            cdf_read = cdf_read.with_columns(columns);
        }

        cdf_read = cdf_read.with_session_ctx(ctx.clone());

        let plan = rt().block_on(cdf_read.build()).map_err(PythonError::from)?;

        py.allow_threads(|| {
            let mut tasks = vec![];
            for p in 0..plan.properties().output_partitioning().partition_count() {
                let inner_plan = plan.clone();
                let partition_batch = inner_plan.execute(p, ctx.task_ctx()).unwrap();
                let handle = rt().spawn(collect_sendable_stream(partition_batch));
                tasks.push(handle);
            }

            // This is unfortunate.
            let batches = rt()
                .block_on(join_all(tasks))
                .into_iter()
                .flatten()
                .collect::<Result<Vec<Vec<_>>, _>>()
                .unwrap()
                .into_iter()
                .flatten()
                .map(Ok);
            let batch_iter = RecordBatchIterator::new(batches, plan.schema());
            let ffi_stream = FFI_ArrowArrayStream::new(Box::new(batch_iter));
            let reader = ArrowArrayStreamReader::try_new(ffi_stream).unwrap();
            Ok(PyArrowType(reader))
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        source,
        predicate,
        source_alias = None,
        target_alias = None,
        safe_cast = false,
        writer_properties = None,
        post_commithook_properties = None,
        commit_properties = None,
    ))]
    pub fn create_merge_builder(
        &self,
        py: Python,
        source: PyArrowType<ArrowArrayStreamReader>,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        safe_cast: bool,
        writer_properties: Option<PyWriterProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<PyMergeBuilder> {
        py.allow_threads(|| {
            Ok(PyMergeBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
                source.0,
                predicate,
                source_alias,
                target_alias,
                safe_cast,
                writer_properties,
                post_commithook_properties,
                commit_properties,
            )
            .map_err(PythonError::from)?)
        })
    }

    #[pyo3(signature=(
        merge_builder
    ))]
    pub fn merge_execute(
        &mut self,
        py: Python,
        merge_builder: &mut PyMergeBuilder,
    ) -> PyResult<String> {
        py.allow_threads(|| {
            let (table, metrics) = merge_builder.execute().map_err(PythonError::from)?;
            self._table.state = table.state;
            Ok(metrics)
        })
    }

    // Run the restore command on the Delta Table: restore table to a given version or datetime
    #[pyo3(signature = (target, *, ignore_missing_files = false, protocol_downgrade_allowed = false, commit_properties=None))]
    pub fn restore(
        &mut self,
        target: Option<&Bound<'_, PyAny>>,
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<String> {
        let mut cmd = RestoreBuilder::new(
            self._table.log_store(),
            self._table.snapshot().map_err(PythonError::from)?.clone(),
        );
        if let Some(val) = target {
            if let Ok(version) = val.extract::<i64>() {
                cmd = cmd.with_version_to_restore(version)
            }
            if let Ok(ds) = val.extract::<PyBackedStr>() {
                let datetime = DateTime::<Utc>::from(
                    DateTime::<FixedOffset>::parse_from_rfc3339(ds.as_ref()).map_err(|err| {
                        PyValueError::new_err(format!("Failed to parse datetime string: {err}"))
                    })?,
                );
                cmd = cmd.with_datetime_to_restore(datetime)
            }
        }
        cmd = cmd.with_ignore_missing_files(ignore_missing_files);
        cmd = cmd.with_protocol_downgrade_allowed(protocol_downgrade_allowed);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run the History command on the Delta Table: Returns provenance information, including the operation, user, and so on, for each write to a table.
    pub fn history(&mut self, limit: Option<usize>) -> PyResult<Vec<String>> {
        let history = rt()
            .block_on(self._table.history(limit))
            .map_err(PythonError::from)?;
        Ok(history
            .iter()
            .map(|c| serde_json::to_string(c).unwrap())
            .collect())
    }

    pub fn update_incremental(&mut self) -> PyResult<()> {
        #[allow(deprecated)]
        Ok(rt()
            .block_on(self._table.update_incremental(None))
            .map_err(PythonError::from)?)
    }

    pub fn dataset_partitions<'py>(
        &mut self,
        py: Python<'py>,
        schema: PyArrowType<ArrowSchema>,
        partition_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
    ) -> PyResult<Vec<(String, Option<Bound<'py, PyAny>>)>> {
        let path_set = match partition_filters {
            Some(filters) => Some(HashSet::<_>::from_iter(
                self.files(py, Some(filters))?.iter().cloned(),
            )),
            None => None,
        };
        self._table
            .snapshot()
            .map_err(PythonError::from)?
            .log_data()
            .into_iter()
            .filter_map(|f| {
                let path = f.path().to_string();
                match &path_set {
                    Some(path_set) => path_set.contains(&path).then_some((path, f)),
                    None => Some((path, f)),
                }
            })
            .map(|(path, f)| {
                let expression = filestats_to_expression_next(py, &schema, f)?;
                Ok((path, expression))
            })
            .collect()
    }

    fn get_active_partitions<'py>(
        &self,
        partitions_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyFrozenSet>> {
        let column_names: HashSet<&str> = self
            ._table
            .get_schema()
            .map_err(|_| DeltaProtocolError::new_err("table does not yet have a schema"))?
            .fields()
            .map(|field| field.name().as_str())
            .collect();
        let partition_columns: HashSet<&str> = self
            ._table
            .metadata()
            .map_err(PythonError::from)?
            .partition_columns
            .iter()
            .map(|col| col.as_str())
            .collect();

        if let Some(filters) = &partitions_filters {
            let unknown_columns: Vec<&PyBackedStr> = filters
                .iter()
                .map(|(column_name, _, _)| column_name)
                .filter(|column_name| {
                    let column_name: &'_ str = column_name.as_ref();
                    !column_names.contains(column_name)
                })
                .collect();
            if !unknown_columns.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "Filters include columns that are not in table schema: {unknown_columns:?}"
                )));
            }

            let non_partition_columns: Vec<&PyBackedStr> = filters
                .iter()
                .map(|(column_name, _, _)| column_name)
                .filter(|column_name| {
                    let column_name: &'_ str = column_name.as_ref();
                    !partition_columns.contains(column_name)
                })
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

        let adds = self
            ._table
            .snapshot()
            .map_err(PythonError::from)?
            .get_active_add_actions_by_partitions(&converted_filters)
            .map_err(PythonError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(PythonError::from)?;
        let active_partitions: HashSet<Vec<(&str, Option<String>)>> = adds
            .iter()
            .flat_map(|add| {
                Ok::<_, PythonError>(
                    partition_columns
                        .iter()
                        .flat_map(|col| {
                            Ok::<_, PythonError>((
                                *col,
                                add.partition_values()
                                    .map_err(PythonError::from)?
                                    .get(*col)
                                    .map(|v| v.serialize()),
                            ))
                        })
                        .collect(),
                )
            })
            .collect();

        let active_partitions = active_partitions
            .into_iter()
            .map(|part| PyFrozenSet::new_bound(py, part.iter()))
            .collect::<Result<Vec<Bound<'py, _>>, PyErr>>()?;
        PyFrozenSet::new_bound(py, &active_partitions)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_write_transaction(
        &mut self,
        py: Python,
        add_actions: Vec<PyAddAction>,
        mode: &str,
        partition_by: Vec<String>,
        schema: PyArrowType<ArrowSchema>,
        partitions_filters: Option<Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<()> {
        py.allow_threads(|| {
            let mode = mode.parse().map_err(PythonError::from)?;

            let schema: StructType = (&schema.0).try_into().map_err(PythonError::from)?;

            let existing_schema = self._table.get_schema().map_err(PythonError::from)?;

            let mut actions: Vec<Action> = add_actions
                .iter()
                .map(|add| Action::Add(add.into()))
                .collect();

            match mode {
                SaveMode::Overwrite => {
                    let converted_filters =
                        convert_partition_filters(partitions_filters.unwrap_or_default())
                            .map_err(PythonError::from)?;

                    let add_actions = self
                        ._table
                        .snapshot()
                        .map_err(PythonError::from)?
                        .get_active_add_actions_by_partitions(&converted_filters)
                        .map_err(PythonError::from)?;

                    for old_add in add_actions {
                        let old_add = old_add.map_err(PythonError::from)?;
                        let remove_action = Action::Remove(Remove {
                            path: old_add.path().to_string(),
                            deletion_timestamp: Some(current_timestamp()),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(
                                old_add
                                    .partition_values()
                                    .map_err(PythonError::from)?
                                    .iter()
                                    .map(|(k, v)| {
                                        (
                                            k.to_string(),
                                            if v.is_null() {
                                                None
                                            } else {
                                                Some(v.serialize())
                                            },
                                        )
                                    })
                                    .collect(),
                            ),
                            size: Some(old_add.size()),
                            deletion_vector: None,
                            tags: None,
                            base_row_id: None,
                            default_row_commit_version: None,
                        });
                        actions.push(remove_action);
                    }

                    // Update metadata with new schema
                    if &schema != existing_schema {
                        let mut metadata =
                            self._table.metadata().map_err(PythonError::from)?.clone();
                        metadata.schema_string = serde_json::to_string(&schema)
                            .map_err(DeltaTableError::from)
                            .map_err(PythonError::from)?;
                        actions.push(Action::Metadata(metadata));
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

            let mut properties = CommitProperties::default();
            if let Some(props) = commit_properties {
                if let Some(metadata) = props.custom_metadata {
                    let json_metadata: Map<String, Value> =
                        metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
                    properties = properties.with_metadata(json_metadata);
                };

                if let Some(max_retries) = props.max_commit_retries {
                    properties = properties.with_max_retries(max_retries);
                };
            }

            if let Some(post_commit_hook_props) = post_commithook_properties {
                properties = set_post_commithook_properties(properties, post_commit_hook_props)
            }

            rt().block_on(
                CommitBuilder::from(properties)
                    .with_actions(actions)
                    .build(
                        Some(self._table.snapshot().map_err(PythonError::from)?),
                        self._table.log_store(),
                        operation,
                    )
                    .into_future(),
            )
            .map_err(PythonError::from)?;

            Ok(())
        })
    }

    pub fn get_py_storage_backend(&self) -> PyResult<filesystem::DeltaFileSystemHandler> {
        Ok(filesystem::DeltaFileSystemHandler {
            inner: self._table.object_store(),
            config: self._config.clone(),
            known_sizes: None,
        })
    }

    pub fn create_checkpoint(&self, py: Python) -> PyResult<()> {
        py.allow_threads(|| {
            Ok::<_, pyo3::PyErr>(
                rt().block_on(create_checkpoint(&self._table))
                    .map_err(PythonError::from)?,
            )
        })?;

        Ok(())
    }

    pub fn cleanup_metadata(&self, py: Python) -> PyResult<()> {
        py.allow_threads(|| {
            Ok::<_, pyo3::PyErr>(
                rt().block_on(cleanup_metadata(&self._table))
                    .map_err(PythonError::from)?,
            )
        })?;

        Ok(())
    }

    pub fn get_add_actions(&self, flatten: bool) -> PyResult<PyArrowType<RecordBatch>> {
        if !self.has_files()? {
            return Err(DeltaError::new_err("Table is instantiated without files."));
        }
        Ok(PyArrowType(
            self._table
                .snapshot()
                .map_err(PythonError::from)?
                .add_actions_table(flatten)
                .map_err(PythonError::from)?,
        ))
    }

    pub fn get_add_file_sizes(&self) -> PyResult<HashMap<String, i64>> {
        Ok(self
            ._table
            .snapshot()
            .map_err(PythonError::from)?
            .eager_snapshot()
            .files()
            .map(|f| (f.path().to_string(), f.size()))
            .collect::<HashMap<String, i64>>())
    }
    /// Run the delete command on the delta table: delete records following a predicate and return the delete metrics.
    #[pyo3(signature = (predicate = None, writer_properties=None, commit_properties=None, post_commithook_properties=None))]
    pub fn delete(
        &mut self,
        py: Python,
        predicate: Option<String>,
        writer_properties: Option<PyWriterProperties>,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let (table, metrics) = py.allow_threads(|| {
            let mut cmd = DeleteBuilder::new(
                self._table.log_store(),
                self._table.snapshot().map_err(PythonError::from)?.clone(),
            );
            if let Some(predicate) = predicate {
                cmd = cmd.with_predicate(predicate);
            }
            if let Some(writer_props) = writer_properties {
                cmd = cmd.with_writer_properties(
                    set_writer_properties(writer_props).map_err(PythonError::from)?,
                );
            }
            if let Some(commit_properties) =
                maybe_create_commit_properties(commit_properties, post_commithook_properties)
            {
                cmd = cmd.with_commit_properties(commit_properties);
            }

            rt().block_on(cmd.into_future()).map_err(PythonError::from)
        })?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    #[pyo3(signature = (properties, raise_if_not_exists, commit_properties=None))]
    pub fn set_table_properties(
        &mut self,
        properties: HashMap<String, String>,
        raise_if_not_exists: bool,
        commit_properties: Option<PyCommitProperties>,
    ) -> PyResult<()> {
        let mut cmd = SetTablePropertiesBuilder::new(
            self._table.log_store(),
            self._table.snapshot().map_err(PythonError::from)?.clone(),
        )
        .with_properties(properties)
        .with_raise_if_not_exists(raise_if_not_exists);

        if let Some(commit_properties) = maybe_create_commit_properties(commit_properties, None) {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let table = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(())
    }

    /// Execute the File System Check command (FSCK) on the delta table: removes old reference to files that
    /// have been deleted or are malformed
    #[pyo3(signature = (dry_run = true, commit_properties = None, post_commithook_properties=None))]
    pub fn repair(
        &mut self,
        dry_run: bool,
        commit_properties: Option<PyCommitProperties>,
        post_commithook_properties: Option<PyPostCommitHookProperties>,
    ) -> PyResult<String> {
        let mut cmd = FileSystemCheckBuilder::new(
            self._table.log_store(),
            self._table.snapshot().map_err(PythonError::from)?.clone(),
        )
        .with_dry_run(dry_run);

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            cmd = cmd.with_commit_properties(commit_properties);
        }

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }
}

fn set_post_commithook_properties(
    mut commit_properties: CommitProperties,
    post_commithook_properties: PyPostCommitHookProperties,
) -> CommitProperties {
    commit_properties =
        commit_properties.with_create_checkpoint(post_commithook_properties.create_checkpoint);
    commit_properties = commit_properties
        .with_cleanup_expired_logs(post_commithook_properties.cleanup_expired_logs);
    commit_properties
}

fn set_writer_properties(writer_properties: PyWriterProperties) -> DeltaResult<WriterProperties> {
    let mut properties = WriterProperties::builder();
    let data_page_size_limit = writer_properties.data_page_size_limit;
    let dictionary_page_size_limit = writer_properties.dictionary_page_size_limit;
    let data_page_row_count_limit = writer_properties.data_page_row_count_limit;
    let write_batch_size = writer_properties.write_batch_size;
    let max_row_group_size = writer_properties.max_row_group_size;
    let compression = writer_properties.compression;
    let statistics_truncate_length = writer_properties.statistics_truncate_length;
    let default_column_properties = writer_properties.default_column_properties;
    let column_properties = writer_properties.column_properties;

    if let Some(data_page_size) = data_page_size_limit {
        properties = properties.set_data_page_size_limit(data_page_size);
    }
    if let Some(dictionary_page_size) = dictionary_page_size_limit {
        properties = properties.set_dictionary_page_size_limit(dictionary_page_size);
    }
    if let Some(data_page_row_count) = data_page_row_count_limit {
        properties = properties.set_data_page_row_count_limit(data_page_row_count);
    }
    if let Some(batch_size) = write_batch_size {
        properties = properties.set_write_batch_size(batch_size);
    }
    if let Some(row_group_size) = max_row_group_size {
        properties = properties.set_max_row_group_size(row_group_size);
    }
    properties = properties.set_statistics_truncate_length(statistics_truncate_length);

    if let Some(compression) = compression {
        let compress: Compression = compression
            .parse()
            .map_err(|err: ParquetError| DeltaTableError::Generic(err.to_string()))?;

        properties = properties.set_compression(compress);
    }

    if let Some(default_column_properties) = default_column_properties {
        if let Some(dictionary_enabled) = default_column_properties.dictionary_enabled {
            properties = properties.set_dictionary_enabled(dictionary_enabled);
        }
        if let Some(max_statistics_size) = default_column_properties.max_statistics_size {
            properties = properties.set_max_statistics_size(max_statistics_size);
        }
        if let Some(bloom_filter_properties) = default_column_properties.bloom_filter_properties {
            if let Some(set_bloom_filter_enabled) = bloom_filter_properties.set_bloom_filter_enabled
            {
                properties = properties.set_bloom_filter_enabled(set_bloom_filter_enabled);
            }
            if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
                properties = properties.set_bloom_filter_fpp(bloom_filter_fpp);
            }
            if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
                properties = properties.set_bloom_filter_ndv(bloom_filter_ndv);
            }
        }
    }
    if let Some(column_properties) = column_properties {
        for (column_name, column_prop) in column_properties {
            if let Some(column_prop) = column_prop {
                if let Some(dictionary_enabled) = column_prop.dictionary_enabled {
                    properties = properties.set_column_dictionary_enabled(
                        column_name.clone().into(),
                        dictionary_enabled,
                    );
                }
                if let Some(bloom_filter_properties) = column_prop.bloom_filter_properties {
                    if let Some(set_bloom_filter_enabled) =
                        bloom_filter_properties.set_bloom_filter_enabled
                    {
                        properties = properties.set_column_bloom_filter_enabled(
                            column_name.clone().into(),
                            set_bloom_filter_enabled,
                        );
                    }
                    if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
                        properties = properties.set_column_bloom_filter_fpp(
                            column_name.clone().into(),
                            bloom_filter_fpp,
                        );
                    }
                    if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
                        properties = properties
                            .set_column_bloom_filter_ndv(column_name.into(), bloom_filter_ndv);
                    }
                }
            }
        }
    }
    Ok(properties.build())
}

fn convert_partition_filters(
    partitions_filters: Vec<(PyBackedStr, PyBackedStr, PartitionFilterValue)>,
) -> Result<Vec<PartitionFilter>, DeltaTableError> {
    partitions_filters
        .into_iter()
        .map(|filter| match filter {
            (key, op, PartitionFilterValue::Single(v)) => {
                let key: &'_ str = key.as_ref();
                let op: &'_ str = op.as_ref();
                let v: &'_ str = v.as_ref();
                PartitionFilter::try_from((key, op, v))
            }
            (key, op, PartitionFilterValue::Multiple(v)) => {
                let key: &'_ str = key.as_ref();
                let op: &'_ str = op.as_ref();
                let v: Vec<&'_ str> = v.iter().map(|v| v.as_ref()).collect();
                PartitionFilter::try_from((key, op, v.as_slice()))
            }
        })
        .collect()
}

fn maybe_create_commit_properties(
    maybe_commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> Option<CommitProperties> {
    if maybe_commit_properties.is_none() && post_commithook_properties.is_none() {
        return None;
    }
    let mut commit_properties = CommitProperties::default();

    if let Some(commit_props) = maybe_commit_properties {
        if let Some(metadata) = commit_props.custom_metadata {
            let json_metadata: Map<String, Value> =
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
            commit_properties = commit_properties.with_metadata(json_metadata);
        };

        if let Some(max_retries) = commit_props.max_commit_retries {
            commit_properties = commit_properties.with_max_retries(max_retries);
        };
    }

    if let Some(post_commit_hook_props) = post_commithook_properties {
        commit_properties =
            set_post_commithook_properties(commit_properties, post_commit_hook_props)
    }
    Some(commit_properties)
}

fn scalar_to_py<'py>(value: &Scalar, py_date: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    use Scalar::*;

    let py = py_date.py();
    let val = match value {
        Null(_) => py.None(),
        Boolean(val) => val.to_object(py),
        Binary(val) => val.to_object(py),
        String(val) => val.to_object(py),
        Byte(val) => val.to_object(py),
        Short(val) => val.to_object(py),
        Integer(val) => val.to_object(py),
        Long(val) => val.to_object(py),
        Float(val) => val.to_object(py),
        Double(val) => val.to_object(py),
        Timestamp(_) => {
            // We need to manually append 'Z' add to end so that pyarrow can cast the
            // the scalar value to pa.timestamp("us","UTC")
            let value = value.serialize();
            format!("{}Z", value).to_object(py)
        }
        TimestampNtz(_) => {
            let value = value.serialize();
            value.to_object(py)
        }
        // NOTE: PyArrow 13.0.0 lost the ability to cast from string to date32, so
        // we have to implement that manually.
        Date(_) => {
            let date = py_date.call_method1("fromisoformat", (value.serialize(),))?;
            date.to_object(py)
        }
        Decimal(_, _, _) => value.serialize().to_object(py),
        Struct(data) => {
            let py_struct = PyDict::new_bound(py);
            for (field, value) in data.fields().iter().zip(data.values().iter()) {
                py_struct.set_item(field.name(), scalar_to_py(value, py_date)?)?;
            }
            py_struct.to_object(py)
        }
    };

    Ok(val.into_bound(py))
}

/// Create expression that file statistics guarantee to be true.
///
/// PyArrow uses this expression to determine which Dataset fragments may be
/// skipped during a scan.
///
/// Partition values are translated to equality expressions (if they are valid)
/// or is_null expression otherwise. For example, if the partition is
/// {"date": "2021-01-01", "x": null}, then the expression is:
/// field(date) = "2021-01-01" AND x IS NULL
///
/// Statistics are translated into inequalities. If there are null values, then
/// they must be OR'd with is_null.
fn filestats_to_expression_next<'py>(
    py: Python<'py>,
    schema: &PyArrowType<ArrowSchema>,
    file_info: LogicalFile<'_>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let ds = PyModule::import_bound(py, "pyarrow.dataset")?;
    let py_field = ds.getattr("field")?;
    let pa = PyModule::import_bound(py, "pyarrow")?;
    let py_date = Python::import_bound(py, "datetime")?.getattr("date")?;
    let mut expressions = Vec::new();

    let cast_to_type = |column_name: &String, value: &Bound<'py, PyAny>, schema: &ArrowSchema| {
        let column_type = schema
            .field_with_name(column_name)
            .map_err(|_| {
                PyValueError::new_err(format!("Column not found in schema: {column_name}"))
            })?
            .data_type()
            .clone();
        let column_type = PyArrowType(column_type).into_py(py);
        pa.call_method1("scalar", (value,))?
            .call_method1("cast", (column_type,))
    };

    if let Ok(partitions_values) = file_info.partition_values() {
        for (column, value) in partitions_values.iter() {
            let column = column.to_string();
            if !value.is_null() {
                // value is a string, but needs to be parsed into appropriate type
                let converted_value =
                    cast_to_type(&column, &scalar_to_py(value, &py_date)?, &schema.0)?;
                expressions.push(
                    py_field
                        .call1((&column,))?
                        .call_method1("__eq__", (converted_value,)),
                );
            } else {
                expressions.push(py_field.call1((column,))?.call_method0("is_null"));
            }
        }
    }

    let mut has_nulls_set: HashSet<String> = HashSet::new();

    // NOTE: null_counts should always return a struct scalar.
    if let Some(Scalar::Struct(data)) = file_info.null_counts() {
        for (field, value) in data.fields().iter().zip(data.values().iter()) {
            if let Scalar::Long(val) = value {
                if *val == 0 {
                    expressions.push(py_field.call1((field.name(),))?.call_method0("is_valid"));
                } else if Some(*val as usize) == file_info.num_records() {
                    expressions.push(py_field.call1((field.name(),))?.call_method0("is_null"));
                } else {
                    has_nulls_set.insert(field.name().to_string());
                }
            }
        }
    }

    // NOTE: min_values should always return a struct scalar.
    if let Some(Scalar::Struct(data)) = file_info.min_values() {
        for (field, value) in data.fields().iter().zip(data.values().iter()) {
            match value {
                // TODO: Handle nested field statistics.
                Scalar::Struct(_) => {}
                _ => {
                    let maybe_minimum =
                        cast_to_type(field.name(), &scalar_to_py(value, &py_date)?, &schema.0);
                    if let Ok(minimum) = maybe_minimum {
                        let field_expr = py_field.call1((field.name(),))?;
                        let expr = field_expr.call_method1("__ge__", (minimum,));
                        let expr = if has_nulls_set.contains(field.name()) {
                            // col >= min_value OR col is null
                            let is_null_expr = field_expr.call_method0("is_null");
                            expr?.call_method1("__or__", (is_null_expr?,))
                        } else {
                            // col >= min_value
                            expr
                        };
                        expressions.push(expr);
                    }
                }
            }
        }
    }

    // NOTE: max_values should always return a struct scalar.
    if let Some(Scalar::Struct(data)) = file_info.max_values() {
        for (field, value) in data.fields().iter().zip(data.values().iter()) {
            match value {
                // TODO: Handle nested field statistics.
                Scalar::Struct(_) => {}
                _ => {
                    let maybe_maximum =
                        cast_to_type(field.name(), &scalar_to_py(value, &py_date)?, &schema.0);
                    if let Ok(maximum) = maybe_maximum {
                        let field_expr = py_field.call1((field.name(),))?;
                        let expr = field_expr.call_method1("__le__", (maximum,));
                        let expr = if has_nulls_set.contains(field.name()) {
                            // col <= max_value OR col is null
                            let is_null_expr = field_expr.call_method0("is_null");
                            expr?.call_method1("__or__", (is_null_expr?,))
                        } else {
                            // col <= max_value
                            expr
                        };
                        expressions.push(expr);
                    }
                }
            }
        }
    }

    if expressions.is_empty() {
        Ok(None)
    } else {
        expressions
            .into_iter()
            .reduce(|accum, item| accum?.call_method1("__and__", (item?,)))
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
    let batches = rt()
        .block_on(async { ctx.table("batch").await?.distinct()?.collect().await })
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

    Ok(PyArrowType(
        concat_batches(&schema, &batches).map_err(PythonError::from)?,
    ))
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

impl From<&PyAddAction> for Add {
    fn from(action: &PyAddAction) -> Self {
        Add {
            path: action.path.clone(),
            size: action.size,
            partition_values: action.partition_values.clone(),
            modification_time: action.modification_time,
            data_change: action.data_change,
            stats: action.stats.clone(),
            stats_parsed: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        }
    }
}

#[derive(FromPyObject)]
pub struct BloomFilterProperties {
    pub set_bloom_filter_enabled: Option<bool>,
    pub fpp: Option<f64>,
    pub ndv: Option<u64>,
}

#[derive(FromPyObject)]
pub struct ColumnProperties {
    pub dictionary_enabled: Option<bool>,
    pub max_statistics_size: Option<usize>,
    pub bloom_filter_properties: Option<BloomFilterProperties>,
}

#[derive(FromPyObject)]
pub struct PyWriterProperties {
    data_page_size_limit: Option<usize>,
    dictionary_page_size_limit: Option<usize>,
    data_page_row_count_limit: Option<usize>,
    write_batch_size: Option<usize>,
    max_row_group_size: Option<usize>,
    statistics_truncate_length: Option<usize>,
    compression: Option<String>,
    default_column_properties: Option<ColumnProperties>,
    column_properties: Option<HashMap<String, Option<ColumnProperties>>>,
}

#[derive(FromPyObject)]
pub struct PyPostCommitHookProperties {
    create_checkpoint: bool,
    cleanup_expired_logs: Option<bool>,
}

#[derive(FromPyObject)]
pub struct PyCommitProperties {
    custom_metadata: Option<HashMap<String, String>>,
    max_commit_retries: Option<usize>,
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn write_to_deltalake(
    py: Python,
    table_uri: String,
    data: PyArrowType<ArrowArrayStreamReader>,
    mode: String,
    table: Option<&RawDeltaTable>,
    schema_mode: Option<String>,
    partition_by: Option<Vec<String>>,
    predicate: Option<String>,
    target_file_size: Option<usize>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    writer_properties: Option<PyWriterProperties>,
    commit_properties: Option<PyCommitProperties>,
    post_commithook_properties: Option<PyPostCommitHookProperties>,
) -> PyResult<()> {
    py.allow_threads(|| {
        let batches = data.0.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let save_mode = mode.parse().map_err(PythonError::from)?;

        let options = storage_options.clone().unwrap_or_default();
        let table = if let Some(table) = table {
            DeltaOps(table._table.clone())
        } else {
            rt().block_on(DeltaOps::try_from_uri_with_storage_options(
                &table_uri, options,
            ))
            .map_err(PythonError::from)?
        };

        let mut builder = table.write(batches).with_save_mode(save_mode);
        if let Some(schema_mode) = schema_mode {
            builder = builder.with_schema_mode(schema_mode.parse().map_err(PythonError::from)?);
        }
        if let Some(partition_columns) = partition_by {
            builder = builder.with_partition_columns(partition_columns);
        }

        if let Some(writer_props) = writer_properties {
            builder = builder.with_writer_properties(
                set_writer_properties(writer_props).map_err(PythonError::from)?,
            );
        }

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        };

        if let Some(description) = &description {
            builder = builder.with_description(description);
        };

        if let Some(predicate) = predicate {
            builder = builder.with_replace_where(predicate);
        };

        if let Some(target_file_size) = target_file_size {
            builder = builder.with_target_file_size(target_file_size)
        };

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(commit_properties) =
            maybe_create_commit_properties(commit_properties, post_commithook_properties)
        {
            builder = builder.with_commit_properties(commit_properties);
        };

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;

        Ok(())
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn create_deltalake(
    py: Python,
    table_uri: String,
    schema: PyArrowType<ArrowSchema>,
    partition_by: Vec<String>,
    mode: String,
    raise_if_key_not_exists: bool,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    custom_metadata: Option<HashMap<String, String>>,
) -> PyResult<()> {
    py.allow_threads(|| {
        let table = DeltaTableBuilder::from_uri(table_uri)
            .with_storage_options(storage_options.unwrap_or_default())
            .build()
            .map_err(PythonError::from)?;

        let mode = mode.parse().map_err(PythonError::from)?;
        let schema: StructType = (&schema.0).try_into().map_err(PythonError::from)?;

        let mut builder = DeltaOps(table)
            .create()
            .with_columns(schema.fields().cloned())
            .with_save_mode(mode)
            .with_raise_if_key_not_exists(raise_if_key_not_exists)
            .with_partition_columns(partition_by);

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        };

        if let Some(description) = &description {
            builder = builder.with_comment(description);
        };

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(metadata) = custom_metadata {
            let json_metadata: Map<String, Value> =
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
            builder = builder.with_metadata(json_metadata);
        };

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;

        Ok(())
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn write_new_deltalake(
    py: Python,
    table_uri: String,
    schema: PyArrowType<ArrowSchema>,
    add_actions: Vec<PyAddAction>,
    _mode: &str,
    partition_by: Vec<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    custom_metadata: Option<HashMap<String, String>>,
) -> PyResult<()> {
    py.allow_threads(|| {
        let table = DeltaTableBuilder::from_uri(table_uri)
            .with_storage_options(storage_options.unwrap_or_default())
            .build()
            .map_err(PythonError::from)?;

        let schema: StructType = (&schema.0).try_into().map_err(PythonError::from)?;

        let mut builder = DeltaOps(table)
            .create()
            .with_columns(schema.fields().cloned())
            .with_partition_columns(partition_by)
            .with_actions(add_actions.iter().map(|add| Action::Add(add.into())));

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        };

        if let Some(description) = &description {
            builder = builder.with_comment(description);
        };

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(metadata) = custom_metadata {
            let json_metadata: Map<String, Value> =
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
            builder = builder.with_metadata(json_metadata);
        };

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;

        Ok(())
    })
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn convert_to_deltalake(
    py: Python,
    uri: String,
    partition_schema: Option<PyArrowType<ArrowSchema>>,
    partition_strategy: Option<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    custom_metadata: Option<HashMap<String, String>>,
) -> PyResult<()> {
    py.allow_threads(|| {
        let mut builder = ConvertToDeltaBuilder::new().with_location(uri);

        if let Some(part_schema) = partition_schema {
            let schema: StructType = (&part_schema.0).try_into().map_err(PythonError::from)?;
            builder = builder.with_partition_schema(schema.fields().cloned());
        }

        if let Some(partition_strategy) = &partition_strategy {
            let strategy: PartitionStrategy =
                partition_strategy.parse().map_err(PythonError::from)?;
            builder = builder.with_partition_strategy(strategy);
        }

        if let Some(name) = &name {
            builder = builder.with_table_name(name);
        }

        if let Some(description) = &description {
            builder = builder.with_comment(description);
        }

        if let Some(config) = configuration {
            builder = builder.with_configuration(config);
        };

        if let Some(strg_options) = storage_options {
            builder = builder.with_storage_options(strg_options);
        };

        if let Some(metadata) = custom_metadata {
            let json_metadata: Map<String, Value> =
                metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
            builder = builder.with_metadata(json_metadata);
        };

        rt().block_on(builder.into_future())
            .map_err(PythonError::from)?;
        Ok(())
    })
}

#[pyfunction]
fn get_num_idx_cols_and_stats_columns(
    table: Option<&RawDeltaTable>,
    configuration: Option<HashMap<String, Option<String>>>,
) -> PyResult<(i32, Option<Vec<String>>)> {
    let config = table
        .as_ref()
        .map(|table| table._table.snapshot())
        .transpose()
        .map_err(PythonError::from)?
        .map(|snapshot| snapshot.table_config());

    Ok(deltalake::operations::get_num_idx_cols_and_stats_columns(
        config,
        configuration.unwrap_or_default(),
    ))
}

#[pyclass(name = "DeltaDataChecker", module = "deltalake._internal")]
struct PyDeltaDataChecker {
    inner: DeltaDataChecker,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl PyDeltaDataChecker {
    #[new]
    #[pyo3(signature = (invariants))]
    fn new(invariants: Vec<(String, String)>) -> Self {
        let invariants: Vec<Invariant> = invariants
            .into_iter()
            .map(|(field_name, invariant_sql)| Invariant {
                field_name,
                invariant_sql,
            })
            .collect();
        Self {
            inner: DeltaDataChecker::new_with_invariants(invariants),
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
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    use crate::error::{CommitFailedError, DeltaError, SchemaMismatchError, TableNotFoundError};
    deltalake::aws::register_handlers(None);
    deltalake::azure::register_handlers(None);
    deltalake::gcp::register_handlers(None);
    deltalake::hdfs::register_handlers(None);
    deltalake_mount::register_handlers(None);

    let py = m.py();
    m.add("DeltaError", py.get_type_bound::<DeltaError>())?;
    m.add(
        "CommitFailedError",
        py.get_type_bound::<CommitFailedError>(),
    )?;
    m.add(
        "DeltaProtocolError",
        py.get_type_bound::<DeltaProtocolError>(),
    )?;
    m.add(
        "TableNotFoundError",
        py.get_type_bound::<TableNotFoundError>(),
    )?;
    m.add(
        "SchemaMismatchError",
        py.get_type_bound::<SchemaMismatchError>(),
    )?;

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(pyo3::wrap_pyfunction_bound!(rust_core_version, m)?)?;
    m.add_function(pyo3::wrap_pyfunction_bound!(create_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction_bound!(write_new_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction_bound!(write_to_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction_bound!(convert_to_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction_bound!(batch_distinct, m)?)?;
    m.add_function(pyo3::wrap_pyfunction_bound!(
        get_num_idx_cols_and_stats_columns,
        m
    )?)?;
    m.add_class::<RawDeltaTable>()?;
    m.add_class::<PyMergeBuilder>()?;
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
    m.add_class::<features::TableFeatures>()?;
    Ok(())
}
