#![deny(warnings)]

mod error;
mod filesystem;
mod schema;
mod utils;
extern crate pyo3_asyncio;

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::pyarrow::PyArrowType;
use arrow_schema::DataType;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use deltalake::arrow::compute::concat_batches;
use deltalake::arrow::ffi_stream::ArrowArrayStreamReader;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::arrow::record_batch::RecordBatchReader;
use deltalake::arrow::{self, datatypes::Schema as ArrowSchema};
use deltalake::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake::datafusion::datasource::memory::MemTable;
use deltalake::datafusion::datasource::provider::TableProvider;
use deltalake::datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaDataChecker;
use deltalake::errors::DeltaTableError;
use deltalake::kernel::{Action, Add, Invariant, Remove, StructType};
use deltalake::operations::convert_to_delta::{ConvertToDeltaBuilder, PartitionStrategy};
use deltalake::operations::delete::DeleteBuilder;
use deltalake::operations::filesystem_check::FileSystemCheckBuilder;
use deltalake::operations::merge::MergeBuilder;
use deltalake::operations::optimize::{OptimizeBuilder, OptimizeType};
use deltalake::operations::restore::RestoreBuilder;
use deltalake::operations::transaction::commit;
use deltalake::operations::update::UpdateBuilder;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::partitions::PartitionFilter;
use deltalake::protocol::{ColumnCountStat, ColumnValueStat, DeltaOperation, SaveMode, Stats};
use deltalake::DeltaOps;
use deltalake::DeltaTableBuilder;
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyFrozenSet, PyType};
use serde_json::{Map, Value};

use crate::error::DeltaProtocolError;
use crate::error::PythonError;
use crate::filesystem::FsConfig;
use crate::schema::schema_to_pyobject;

#[inline]
fn rt_pyo3() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|err| PyRuntimeError::new_err(err.to_string()))
}

#[inline]
fn rt() -> &'static tokio::runtime::Runtime {
    pyo3_asyncio::tokio::get_runtime()
}

#[derive(FromPyObject)]
enum PartitionFilterValue<'a> {
    Single(&'a str),
    Multiple(Vec<&'a str>),
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

#[pymethods]
impl RawDeltaTable {
    #[new]
    #[pyo3(signature = (table_uri, version = None, storage_options = None, without_files = false, log_buffer_size = None))]
    fn new(
        table_uri: &str,
        version: Option<i64>,
        storage_options: Option<HashMap<String, String>>,
        without_files: bool,
        log_buffer_size: Option<usize>,
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
    }

    #[classmethod]
    #[pyo3(signature = (data_catalog, database_name, table_name, data_catalog_id, catalog_options = None))]
    fn get_table_uri_from_data_catalog(
        _cls: &PyType,
        data_catalog: &str,
        database_name: &str,
        table_name: &str,
        data_catalog_id: Option<String>,
        catalog_options: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let data_catalog = deltalake::data_catalog::get_data_catalog(data_catalog, catalog_options)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))?;
        let table_uri = rt()
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

    pub fn protocol_versions(&self) -> PyResult<(i32, i32)> {
        Ok((
            self._table.protocol().min_reader_version,
            self._table.protocol().min_writer_version,
        ))
    }

    pub fn load_version(&mut self, version: i64) -> PyResult<()> {
        Ok(rt()
            .block_on(self._table.load_version(version))
            .map_err(PythonError::from)?)
    }

    pub fn get_latest_version(&mut self) -> PyResult<i64> {
        Ok(rt()
            .block_on(self._table.get_latest_version())
            .map_err(PythonError::from)?)
    }

    pub fn load_with_datetime(&mut self, ds: &str) -> PyResult<()> {
        let datetime =
            DateTime::<Utc>::from(DateTime::<FixedOffset>::parse_from_rfc3339(ds).map_err(
                |err| PyValueError::new_err(format!("Failed to parse datetime string: {err}")),
            )?);
        Ok(rt()
            .block_on(self._table.load_with_datetime(datetime))
            .map_err(PythonError::from)?)
    }

    pub fn files_by_partitions(
        &self,
        partitions_filters: Vec<(&str, &str, PartitionFilterValue)>,
    ) -> PyResult<Vec<String>> {
        let partition_filters: Result<Vec<PartitionFilter>, DeltaTableError> = partitions_filters
            .into_iter()
            .map(|filter| match filter {
                (key, op, PartitionFilterValue::Single(v)) => {
                    PartitionFilter::try_from((key, op, v))
                }
                (key, op, PartitionFilterValue::Multiple(v)) => {
                    PartitionFilter::try_from((key, op, v.as_slice()))
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
        let schema: &StructType = self._table.get_schema().map_err(PythonError::from)?;
        schema_to_pyobject(schema, py)
    }

    /// Run the Vacuum command on the Delta Table: list and delete files no longer referenced
    /// by the Delta table and are older than the retention threshold.
    #[pyo3(signature = (dry_run, retention_hours = None, enforce_retention_duration = true))]
    pub fn vacuum(
        &mut self,
        dry_run: bool,
        retention_hours: Option<u64>,
        enforce_retention_duration: bool,
    ) -> PyResult<Vec<String>> {
        let mut cmd = VacuumBuilder::new(self._table.log_store(), self._table.state.clone())
            .with_enforce_retention_duration(enforce_retention_duration)
            .with_dry_run(dry_run);
        if let Some(retention_period) = retention_hours {
            cmd = cmd.with_retention_period(Duration::hours(retention_period as i64));
        }
        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(metrics.files_deleted)
    }

    /// Run the UPDATE command on the Delta Table
    #[pyo3(signature = (updates, predicate=None, writer_properties=None, safe_cast = false))]
    pub fn update(
        &mut self,
        updates: HashMap<String, String>,
        predicate: Option<String>,
        writer_properties: Option<HashMap<String, usize>>,
        safe_cast: bool,
    ) -> PyResult<String> {
        let mut cmd = UpdateBuilder::new(self._table.log_store(), self._table.state.clone())
            .with_safe_cast(safe_cast);

        if let Some(writer_props) = writer_properties {
            let mut properties = WriterProperties::builder();
            let data_page_size_limit = writer_props.get("data_page_size_limit");
            let dictionary_page_size_limit = writer_props.get("dictionary_page_size_limit");
            let data_page_row_count_limit = writer_props.get("data_page_row_count_limit");
            let write_batch_size = writer_props.get("write_batch_size");
            let max_row_group_size = writer_props.get("max_row_group_size");

            if let Some(data_page_size) = data_page_size_limit {
                properties = properties.set_data_page_size_limit(*data_page_size);
            }
            if let Some(dictionary_page_size) = dictionary_page_size_limit {
                properties = properties.set_dictionary_page_size_limit(*dictionary_page_size);
            }
            if let Some(data_page_row_count) = data_page_row_count_limit {
                properties = properties.set_data_page_row_count_limit(*data_page_row_count);
            }
            if let Some(batch_size) = write_batch_size {
                properties = properties.set_write_batch_size(*batch_size);
            }
            if let Some(row_group_size) = max_row_group_size {
                properties = properties.set_max_row_group_size(*row_group_size);
            }
            cmd = cmd.with_writer_properties(properties.build());
        }

        for (col_name, expression) in updates {
            cmd = cmd.with_update(col_name.clone(), expression.clone());
        }

        if let Some(update_predicate) = predicate {
            cmd = cmd.with_predicate(update_predicate);
        }

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run the optimize command on the Delta Table: merge small files into a large file by bin-packing.
    #[pyo3(signature = (partition_filters = None, target_size = None, max_concurrent_tasks = None, min_commit_interval = None))]
    pub fn compact_optimize(
        &mut self,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        min_commit_interval: Option<u64>,
    ) -> PyResult<String> {
        let mut cmd = OptimizeBuilder::new(self._table.log_store(), self._table.state.clone())
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get));
        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        if let Some(commit_interval) = min_commit_interval {
            cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
        }
        let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())
            .map_err(PythonError::from)?;
        cmd = cmd.with_filters(&converted_filters);

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Run z-order variation of optimize
    #[pyo3(signature = (z_order_columns, partition_filters = None, target_size = None, max_concurrent_tasks = None, max_spill_size = 20 * 1024 * 1024 * 1024, min_commit_interval = None))]
    pub fn z_order_optimize(
        &mut self,
        z_order_columns: Vec<String>,
        partition_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
        target_size: Option<i64>,
        max_concurrent_tasks: Option<usize>,
        max_spill_size: usize,
        min_commit_interval: Option<u64>,
    ) -> PyResult<String> {
        let mut cmd = OptimizeBuilder::new(self._table.log_store(), self._table.state.clone())
            .with_max_concurrent_tasks(max_concurrent_tasks.unwrap_or_else(num_cpus::get))
            .with_max_spill_size(max_spill_size)
            .with_type(OptimizeType::ZOrder(z_order_columns));
        if let Some(size) = target_size {
            cmd = cmd.with_target_size(size);
        }
        if let Some(commit_interval) = min_commit_interval {
            cmd = cmd.with_min_commit_interval(time::Duration::from_secs(commit_interval));
        }

        let converted_filters = convert_partition_filters(partition_filters.unwrap_or_default())
            .map_err(PythonError::from)?;
        cmd = cmd.with_filters(&converted_filters);

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (source,
        predicate,
        source_alias = None,
        target_alias = None,
        safe_cast = false,
        writer_properties = None,
        matched_update_updates = None,
        matched_update_predicate = None,
        matched_delete_predicate = None,
        matched_delete_all = None,
        not_matched_insert_updates = None,
        not_matched_insert_predicate = None,
        not_matched_by_source_update_updates = None,
        not_matched_by_source_update_predicate = None,
        not_matched_by_source_delete_predicate = None,
        not_matched_by_source_delete_all = None,
    ))]
    pub fn merge_execute(
        &mut self,
        source: PyArrowType<ArrowArrayStreamReader>,
        predicate: String,
        source_alias: Option<String>,
        target_alias: Option<String>,
        safe_cast: bool,
        writer_properties: Option<HashMap<String, usize>>,
        matched_update_updates: Option<Vec<HashMap<String, String>>>,
        matched_update_predicate: Option<Vec<Option<String>>>,
        matched_delete_predicate: Option<Vec<String>>,
        matched_delete_all: Option<bool>,
        not_matched_insert_updates: Option<Vec<HashMap<String, String>>>,
        not_matched_insert_predicate: Option<Vec<Option<String>>>,
        not_matched_by_source_update_updates: Option<Vec<HashMap<String, String>>>,
        not_matched_by_source_update_predicate: Option<Vec<Option<String>>>,
        not_matched_by_source_delete_predicate: Option<Vec<String>>,
        not_matched_by_source_delete_all: Option<bool>,
    ) -> PyResult<String> {
        let ctx = SessionContext::new();
        let schema = source.0.schema();
        let batches = vec![source.0.map(|batch| batch.unwrap()).collect::<Vec<_>>()];
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, batches).unwrap());
        let source_df = ctx.read_table(table_provider).unwrap();

        let mut cmd = MergeBuilder::new(
            self._table.log_store(),
            self._table.state.clone(),
            predicate,
            source_df,
        )
        .with_safe_cast(safe_cast);

        if let Some(src_alias) = source_alias {
            cmd = cmd.with_source_alias(src_alias);
        }

        if let Some(trgt_alias) = target_alias {
            cmd = cmd.with_target_alias(trgt_alias);
        }

        if let Some(writer_props) = writer_properties {
            let mut properties = WriterProperties::builder();
            let data_page_size_limit = writer_props.get("data_page_size_limit");
            let dictionary_page_size_limit = writer_props.get("dictionary_page_size_limit");
            let data_page_row_count_limit = writer_props.get("data_page_row_count_limit");
            let write_batch_size = writer_props.get("write_batch_size");
            let max_row_group_size = writer_props.get("max_row_group_size");

            if let Some(data_page_size) = data_page_size_limit {
                properties = properties.set_data_page_size_limit(*data_page_size);
            }
            if let Some(dictionary_page_size) = dictionary_page_size_limit {
                properties = properties.set_dictionary_page_size_limit(*dictionary_page_size);
            }
            if let Some(data_page_row_count) = data_page_row_count_limit {
                properties = properties.set_data_page_row_count_limit(*data_page_row_count);
            }
            if let Some(batch_size) = write_batch_size {
                properties = properties.set_write_batch_size(*batch_size);
            }
            if let Some(row_group_size) = max_row_group_size {
                properties = properties.set_max_row_group_size(*row_group_size);
            }
            cmd = cmd.with_writer_properties(properties.build());
        }

        if let Some(mu_updates) = matched_update_updates {
            if let Some(mu_predicate) = matched_update_predicate {
                for it in mu_updates.iter().zip(mu_predicate.iter()) {
                    let (update_values, predicate_value) = it;

                    if let Some(pred) = predicate_value {
                        cmd = cmd
                            .when_matched_update(|mut update| {
                                for (col_name, expression) in update_values {
                                    update = update.update(col_name.clone(), expression.clone());
                                }
                                update.predicate(pred.clone())
                            })
                            .map_err(PythonError::from)?;
                    } else {
                        cmd = cmd
                            .when_matched_update(|mut update| {
                                for (col_name, expression) in update_values {
                                    update = update.update(col_name.clone(), expression.clone());
                                }
                                update
                            })
                            .map_err(PythonError::from)?;
                    }
                }
            }
        }

        if let Some(_md_delete_all) = matched_delete_all {
            cmd = cmd
                .when_matched_delete(|delete| delete)
                .map_err(PythonError::from)?;
        } else if let Some(md_predicate) = matched_delete_predicate {
            for pred in md_predicate.iter() {
                cmd = cmd
                    .when_matched_delete(|delete| delete.predicate(pred.clone()))
                    .map_err(PythonError::from)?;
            }
        }

        if let Some(nmi_updates) = not_matched_insert_updates {
            if let Some(nmi_predicate) = not_matched_insert_predicate {
                for it in nmi_updates.iter().zip(nmi_predicate.iter()) {
                    let (update_values, predicate_value) = it;
                    if let Some(pred) = predicate_value {
                        cmd = cmd
                            .when_not_matched_insert(|mut insert| {
                                for (col_name, expression) in update_values {
                                    insert = insert.set(col_name.clone(), expression.clone());
                                }
                                insert.predicate(pred.clone())
                            })
                            .map_err(PythonError::from)?;
                    } else {
                        cmd = cmd
                            .when_not_matched_insert(|mut insert| {
                                for (col_name, expression) in update_values {
                                    insert = insert.set(col_name.clone(), expression.clone());
                                }
                                insert
                            })
                            .map_err(PythonError::from)?;
                    }
                }
            }
        }

        if let Some(nmbsu_updates) = not_matched_by_source_update_updates {
            if let Some(nmbsu_predicate) = not_matched_by_source_update_predicate {
                for it in nmbsu_updates.iter().zip(nmbsu_predicate.iter()) {
                    let (update_values, predicate_value) = it;
                    if let Some(pred) = predicate_value {
                        cmd = cmd
                            .when_not_matched_by_source_update(|mut update| {
                                for (col_name, expression) in update_values {
                                    update = update.update(col_name.clone(), expression.clone());
                                }
                                update.predicate(pred.clone())
                            })
                            .map_err(PythonError::from)?;
                    } else {
                        cmd = cmd
                            .when_not_matched_by_source_update(|mut update| {
                                for (col_name, expression) in update_values {
                                    update = update.update(col_name.clone(), expression.clone());
                                }
                                update
                            })
                            .map_err(PythonError::from)?;
                    }
                }
            }
        }

        if let Some(_nmbs_delete_all) = not_matched_by_source_delete_all {
            cmd = cmd
                .when_not_matched_by_source_delete(|delete| delete)
                .map_err(PythonError::from)?;
        } else if let Some(nmbs_predicate) = not_matched_by_source_delete_predicate {
            for pred in nmbs_predicate.iter() {
                cmd = cmd
                    .when_not_matched_by_source_delete(|delete| delete.predicate(pred.clone()))
                    .map_err(PythonError::from)?;
            }
        }

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    // Run the restore command on the Delta Table: restore table to a given version or datetime
    #[pyo3(signature = (target, *, ignore_missing_files = false, protocol_downgrade_allowed = false))]
    pub fn restore(
        &mut self,
        target: Option<&PyAny>,
        ignore_missing_files: bool,
        protocol_downgrade_allowed: bool,
    ) -> PyResult<String> {
        let mut cmd = RestoreBuilder::new(self._table.log_store(), self._table.state.clone());
        if let Some(val) = target {
            if let Ok(version) = val.extract::<i64>() {
                cmd = cmd.with_version_to_restore(version)
            }
            if let Ok(ds) = val.extract::<&str>() {
                let datetime = DateTime::<Utc>::from(
                    DateTime::<FixedOffset>::parse_from_rfc3339(ds).map_err(|err| {
                        PyValueError::new_err(format!("Failed to parse datetime string: {err}"))
                    })?,
                );
                cmd = cmd.with_datetime_to_restore(datetime)
            }
        }
        cmd = cmd.with_ignore_missing_files(ignore_missing_files);
        cmd = cmd.with_protocol_downgrade_allowed(protocol_downgrade_allowed);
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
        Ok(rt()
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
            .get_schema()
            .map_err(|_| DeltaProtocolError::new_err("table does not yet have a schema"))?
            .fields()
            .iter()
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
        PyFrozenSet::new(py, active_partitions)
    }

    fn create_write_transaction(
        &mut self,
        add_actions: Vec<PyAddAction>,
        mode: &str,
        partition_by: Vec<String>,
        schema: PyArrowType<ArrowSchema>,
        partitions_filters: Option<Vec<(&str, &str, PartitionFilterValue)>>,
    ) -> PyResult<()> {
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
                    .get_state()
                    .get_active_add_actions_by_partitions(&converted_filters)
                    .map_err(PythonError::from)?;

                for old_add in add_actions {
                    let remove_action = Action::Remove(Remove {
                        path: old_add.path.clone(),
                        deletion_timestamp: Some(current_timestamp()),
                        data_change: true,
                        extended_file_metadata: Some(old_add.tags.is_some()),
                        partition_values: Some(old_add.partition_values.clone()),
                        size: Some(old_add.size),
                        deletion_vector: old_add.deletion_vector.clone(),
                        tags: old_add.tags.clone(),
                        base_row_id: old_add.base_row_id,
                        default_row_commit_version: old_add.default_row_commit_version,
                    });
                    actions.push(remove_action);
                }

                // Update metadata with new schema
                if &schema != existing_schema {
                    let mut metadata = self._table.metadata().map_err(PythonError::from)?.clone();
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
        let store = self._table.log_store();

        rt().block_on(commit(
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
            rt: Arc::new(rt_pyo3()?),
            config: self._config.clone(),
            known_sizes: None,
        })
    }

    pub fn create_checkpoint(&self) -> PyResult<()> {
        rt().block_on(create_checkpoint(&self._table))
            .map_err(PythonError::from)?;

        Ok(())
    }

    pub fn cleanup_metadata(&self) -> PyResult<()> {
        rt().block_on(cleanup_metadata(&self._table))
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

    /// Run the delete command on the delta table: delete records following a predicate and return the delete metrics.
    #[pyo3(signature = (predicate = None))]
    pub fn delete(&mut self, predicate: Option<String>) -> PyResult<String> {
        let mut cmd = DeleteBuilder::new(self._table.log_store(), self._table.state.clone());
        if let Some(predicate) = predicate {
            cmd = cmd.with_predicate(predicate);
        }
        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }

    /// Execute the File System Check command (FSCK) on the delta table: removes old reference to files that
    /// have been deleted or are malformed
    #[pyo3(signature = (dry_run = true))]
    pub fn repair(&mut self, dry_run: bool) -> PyResult<String> {
        let cmd = FileSystemCheckBuilder::new(self._table.log_store(), self._table.state.clone())
            .with_dry_run(dry_run);

        let (table, metrics) = rt()
            .block_on(cmd.into_future())
            .map_err(PythonError::from)?;
        self._table.state = table.state;
        Ok(serde_json::to_string(&metrics).unwrap())
    }
}

fn convert_partition_filters<'a>(
    partitions_filters: Vec<(&'a str, &'a str, PartitionFilterValue)>,
) -> Result<Vec<PartitionFilter>, DeltaTableError> {
    partitions_filters
        .into_iter()
        .map(|filter| match filter {
            (key, op, PartitionFilterValue::Single(v)) => PartitionFilter::try_from((key, op, v)),
            (key, op, PartitionFilterValue::Multiple(v)) => {
                PartitionFilter::try_from((key, op, v.as_slice()))
            }
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
///
/// Partition values are translated to equality expressions (if they are valid)
/// or is_null expression otherwise. For example, if the partition is
/// {"date": "2021-01-01", "x": null}, then the expression is:
/// field(date) = "2021-01-01" AND x IS NULL
///
/// Statistics are translated into inequalities. If there are null values, then
/// they must be OR'd with is_null.
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
        let column_type = schema
            .field_with_name(column_name)
            .map_err(|_| {
                PyValueError::new_err(format!("Column not found in schema: {column_name}"))
            })?
            .data_type()
            .clone();

        let value = match column_type {
            // Since PyArrow 13.0.0, casting string -> timestamp fails if it ends with "Z"
            // and the target type is timezone naive.
            DataType::Timestamp(_, _) if value.extract::<String>(py).is_ok() => {
                value.call_method1(py, "rstrip", ("Z",))?
            }
            // PyArrow 13.0.0 lost the ability to cast from string to date32, so
            // we have to implement that manually.
            DataType::Date32 if value.extract::<String>(py).is_ok() => {
                let date = Python::import(py, "datetime")?.getattr("date")?;
                let date = date.call_method1("fromisoformat", (value,))?;
                date.to_object(py)
            }
            _ => value,
        };

        let column_type = PyArrowType(column_type).into_py(py);
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
        } else {
            expressions.push(field.call1((column,))?.call_method0("is_null"));
        }
    }

    if let Some(stats) = stats {
        let mut has_nulls_set: HashSet<String> = HashSet::new();

        for (col_name, null_count) in stats.null_count.iter().filter_map(|(k, v)| match v {
            ColumnCountStat::Value(val) => Some((k, val)),
            _ => None,
        }) {
            if *null_count == 0 {
                expressions.push(field.call1((col_name,))?.call_method0("is_valid"));
            } else if *null_count == stats.num_records {
                expressions.push(field.call1((col_name,))?.call_method0("is_null"));
            } else {
                has_nulls_set.insert(col_name.clone());
            }
        }

        for (col_name, minimum) in stats.min_values.iter().filter_map(|(k, v)| match v {
            ColumnValueStat::Value(val) => Some((k.clone(), json_value_to_py(val, py))),
            // TODO(wjones127): Handle nested field statistics.
            // Blocked on https://issues.apache.org/jira/browse/ARROW-11259
            _ => None,
        }) {
            let maybe_minimum = cast_to_type(&col_name, minimum, &schema.0);
            if let Ok(minimum) = maybe_minimum {
                let field_expr = field.call1((&col_name,))?;
                let expr = field_expr.call_method1("__ge__", (minimum,));
                let expr = if has_nulls_set.contains(&col_name) {
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

        for (col_name, maximum) in stats.max_values.iter().filter_map(|(k, v)| match v {
            ColumnValueStat::Value(val) => Some((k.clone(), json_value_to_py(val, py))),
            _ => None,
        }) {
            let maybe_maximum = cast_to_type(&col_name, maximum, &schema.0);
            if let Ok(maximum) = maybe_maximum {
                let field_expr = field.call1((&col_name,))?;
                let expr = field_expr.call_method1("__le__", (maximum,));
                let expr = if has_nulls_set.contains(&col_name) {
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
            partition_values_parsed: None,
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

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn write_to_deltalake(
    table_uri: String,
    data: PyArrowType<ArrowArrayStreamReader>,
    mode: String,
    max_rows_per_group: i64,
    overwrite_schema: bool,
    partition_by: Option<Vec<String>>,
    predicate: Option<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let batches = data.0.map(|batch| batch.unwrap()).collect::<Vec<_>>();
    let save_mode = mode.parse().map_err(PythonError::from)?;

    let options = storage_options.clone().unwrap_or_default();
    let table = rt()
        .block_on(DeltaOps::try_from_uri_with_storage_options(
            &table_uri, options,
        ))
        .map_err(PythonError::from)?;

    let mut builder = table
        .write(batches)
        .with_save_mode(save_mode)
        .with_overwrite_schema(overwrite_schema)
        .with_write_batch_size(max_rows_per_group as usize);

    if let Some(partition_columns) = partition_by {
        builder = builder.with_partition_columns(partition_columns);
    }

    if let Some(name) = &name {
        builder = builder.with_table_name(name);
    };

    if let Some(description) = &description {
        builder = builder.with_description(description);
    };

    if let Some(predicate) = &predicate {
        builder = builder.with_replace_where(predicate);
    };

    if let Some(config) = configuration {
        builder = builder.with_configuration(config);
    };

    rt().block_on(builder.into_future())
        .map_err(PythonError::from)?;

    Ok(())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn create_deltalake(
    table_uri: String,
    schema: PyArrowType<ArrowSchema>,
    partition_by: Vec<String>,
    mode: String,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(storage_options.unwrap_or_default())
        .build()
        .map_err(PythonError::from)?;

    let mode = mode.parse().map_err(PythonError::from)?;
    let schema: StructType = (&schema.0).try_into().map_err(PythonError::from)?;

    let mut builder = DeltaOps(table)
        .create()
        .with_columns(schema.fields().clone())
        .with_save_mode(mode)
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

    rt().block_on(builder.into_future())
        .map_err(PythonError::from)?;

    Ok(())
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

    let schema: StructType = (&schema.0).try_into().map_err(PythonError::from)?;

    let mut builder = DeltaOps(table)
        .create()
        .with_columns(schema.fields().clone())
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

    rt().block_on(builder.into_future())
        .map_err(PythonError::from)?;

    Ok(())
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
fn convert_to_deltalake(
    uri: String,
    partition_schema: Option<PyArrowType<ArrowSchema>>,
    partition_strategy: Option<String>,
    name: Option<String>,
    description: Option<String>,
    configuration: Option<HashMap<String, Option<String>>>,
    storage_options: Option<HashMap<String, String>>,
    custom_metadata: Option<HashMap<String, String>>,
) -> PyResult<()> {
    let mut builder = ConvertToDeltaBuilder::new().with_location(uri);

    if let Some(part_schema) = partition_schema {
        let schema: StructType = (&part_schema.0).try_into().map_err(PythonError::from)?;
        builder = builder.with_partition_schema(schema.fields().clone());
    }

    if let Some(partition_strategy) = &partition_strategy {
        let strategy: PartitionStrategy = partition_strategy.parse().map_err(PythonError::from)?;
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
    m.add_function(pyo3::wrap_pyfunction!(create_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(write_new_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(write_to_deltalake, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(convert_to_deltalake, m)?)?;
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
