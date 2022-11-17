//! Datafusion integration for Delta Table
//!
//! Example:
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::execution::context::SessionContext;
//!
//! async {
//!   let mut ctx = SessionContext::new();
//!   let table = deltalake::open_table("./tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").await.unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::datasource::datasource::TableProviderFactory;
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::{listing::PartitionedFile, MemTable, TableProvider, TableType};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::{
    ColumnStatistics, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::{Column, DataFusionError, Result as DataFusionResult};
use datafusion_expr::{Expr, Extension, LogicalPlan};
use datafusion_proto::logical_plan::{LogicalExtensionCodec, PhysicalExtensionCodec};
use object_store::{path::Path, ObjectMeta};
use url::Url;

use crate::Invariant;
use crate::{action, open_table};
use crate::{schema, DeltaTableBuilder};
use crate::{DeltaTable, DeltaTableError};

impl From<DeltaTableError> for DataFusionError {
    fn from(err: DeltaTableError) -> Self {
        match err {
            DeltaTableError::Arrow { source } => DataFusionError::ArrowError(source),
            DeltaTableError::Io { source } => DataFusionError::IoError(source),
            DeltaTableError::ObjectStore { source } => DataFusionError::ObjectStore(source),
            DeltaTableError::Parquet { source } => DataFusionError::ParquetError(source),
            _ => DataFusionError::External(Box::new(err)),
        }
    }
}

impl From<DataFusionError> for DeltaTableError {
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::ArrowError(source) => DeltaTableError::Arrow { source },
            DataFusionError::IoError(source) => DeltaTableError::Io { source },
            DataFusionError::ObjectStore(source) => DeltaTableError::ObjectStore { source },
            DataFusionError::ParquetError(source) => DeltaTableError::Parquet { source },
            _ => DeltaTableError::Generic(err.to_string()),
        }
    }
}

impl DeltaTable {
    /// Return statistics for Datafusion Table
    pub fn datafusion_table_statistics(&self) -> Statistics {
        let stats = self
            .get_state()
            .files()
            .iter()
            .fold(
                Some(Statistics {
                    num_rows: Some(0),
                    total_byte_size: Some(0),
                    column_statistics: Some(vec![
                        ColumnStatistics {
                            null_count: Some(0),
                            max_value: None,
                            min_value: None,
                            distinct_count: None
                        };
                        self.schema().unwrap().get_fields().len()
                    ]),
                    is_exact: true,
                }),
                |acc, action| {
                    let acc = acc?;
                    let new_stats = action
                        .get_stats()
                        .unwrap_or_else(|_| Some(action::Stats::default()))?;
                    Some(Statistics {
                        num_rows: acc
                            .num_rows
                            .map(|rows| rows + new_stats.num_records as usize),
                        total_byte_size: acc
                            .total_byte_size
                            .map(|total_size| total_size + action.size as usize),
                        column_statistics: acc.column_statistics.map(|col_stats| {
                            self.schema()
                                .unwrap()
                                .get_fields()
                                .iter()
                                .zip(col_stats)
                                .map(|(field, stats)| {
                                    let null_count = new_stats
                                        .null_count
                                        .get(field.get_name())
                                        .and_then(|x| {
                                            let null_count_acc = stats.null_count?;
                                            let null_count = x.as_value()? as usize;
                                            Some(null_count_acc + null_count)
                                        })
                                        .or(stats.null_count);

                                    let max_value = new_stats
                                        .max_values
                                        .get(field.get_name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let max_value = to_scalar_value(x.as_value()?);

                                            match (max_value, old_stats.max_value) {
                                                (Some(max_value), Some(old_max_value)) => {
                                                    if left_larger_than_right(
                                                        old_max_value.clone(),
                                                        max_value.clone(),
                                                    )? {
                                                        Some(old_max_value)
                                                    } else {
                                                        Some(max_value)
                                                    }
                                                }
                                                (Some(max_value), None) => Some(max_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.max_value.clone());

                                    let min_value = new_stats
                                        .min_values
                                        .get(field.get_name())
                                        .and_then(|x| {
                                            let old_stats = stats.clone();
                                            let min_value = to_scalar_value(x.as_value()?);

                                            match (min_value, old_stats.min_value) {
                                                (Some(min_value), Some(old_min_value)) => {
                                                    if left_larger_than_right(
                                                        min_value.clone(),
                                                        old_min_value.clone(),
                                                    )? {
                                                        Some(old_min_value)
                                                    } else {
                                                        Some(min_value)
                                                    }
                                                }
                                                (Some(min_value), None) => Some(min_value),
                                                (None, old) => old,
                                            }
                                        })
                                        .or_else(|| stats.min_value.clone());

                                    ColumnStatistics {
                                        null_count,
                                        max_value,
                                        min_value,
                                        distinct_count: None, // TODO: distinct
                                    }
                                })
                                .collect()
                        }),
                        is_exact: true,
                    })
                },
            )
            .unwrap_or_default();

        // Convert column max/min scalar values to correct types based on arrow types.
        Statistics {
            is_exact: true,
            num_rows: stats.num_rows,
            total_byte_size: stats.total_byte_size,
            column_statistics: stats.column_statistics.map(|col_stats| {
                let fields = self.schema().unwrap().get_fields();
                col_stats
                    .iter()
                    .zip(fields)
                    .map(|(col_states, field)| {
                        let dt = (self as &dyn TableProvider)
                            .schema()
                            .field_with_name(field.get_name())
                            .unwrap()
                            .data_type()
                            .clone();
                        ColumnStatistics {
                            null_count: col_states.null_count,
                            max_value: col_states
                                .max_value
                                .as_ref()
                                .and_then(|scalar| correct_scalar_value_type(scalar.clone(), &dt)),
                            min_value: col_states
                                .min_value
                                .as_ref()
                                .and_then(|scalar| correct_scalar_value_type(scalar.clone(), &dt)),
                            distinct_count: col_states.distinct_count,
                        }
                    })
                    .collect()
            }),
        }
    }
}

impl PruningStatistics for DeltaTable {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let field = self
            .get_schema()
            .ok()
            .map(|s| s.get_field_with_name(&column.name).ok())??;
        let data_type = field.get_type().try_into().ok()?;
        let values = self.get_state().files().iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .min_values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                    .unwrap_or(ScalarValue::Null)
            } else {
                ScalarValue::Null
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let field = self
            .get_schema()
            .ok()
            .map(|s| s.get_field_with_name(&column.name).ok())??;
        let data_type = field.get_type().try_into().ok()?;
        let values = self.get_state().files().iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .max_values
                    .get(&column.name)
                    .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                    .unwrap_or(ScalarValue::Null)
            } else {
                ScalarValue::Null
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }

    /// return the number of containers (e.g. row groups) being
    /// pruned with these statistics
    fn num_containers(&self) -> usize {
        self.get_state().files().len()
    }

    /// return the number of null values for the named column as an
    /// `Option<UInt64Array>`.
    ///
    /// Note: the returned array must contain `num_containers()` rows.
    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let values = self.get_state().files().iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                statistics
                    .null_count
                    .get(&column.name)
                    .map(|f| ScalarValue::UInt64(f.as_value().map(|val| val as u64)))
                    .unwrap_or(ScalarValue::UInt64(None))
            } else {
                ScalarValue::UInt64(None)
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }
}

// each delta table must register a specific object store, since paths are internally
// handled relative to the table root.
fn register_store(table: &DeltaTable, env: Arc<RuntimeEnv>) {
    let object_store_url = table.storage.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(
        url.scheme(),
        url.host_str().unwrap_or_default(),
        table.object_store(),
    );
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(DeltaTable::schema(self).unwrap())
                .unwrap(),
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(<ArrowSchema as TryFrom<&schema::Schema>>::try_from(
            DeltaTable::schema(self).unwrap(),
        )?);

        register_store(self, session.runtime_env.clone());

        // TODO we group files together by their partition values. If the table is partitioned
        // and partitions are somewhat evenly distributed, probably not the worst choice ...
        // However we may want to do some additional balancing in case we are far off from the above.
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
        if let Some(Some(predicate)) =
            (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
        {
            let pruning_predicate = PruningPredicate::try_new(predicate, schema.clone())?;
            let files_to_prune = pruning_predicate.prune(self)?;
            self.get_state()
                .files()
                .iter()
                .zip(files_to_prune.into_iter())
                .for_each(|(action, prune_file)| {
                    if !prune_file {
                        let part = partitioned_file_from_action(action, &schema);
                        file_groups
                            .entry(part.partition_values.clone())
                            .or_default()
                            .push(part);
                    };
                });
        } else {
            self.get_state().files().iter().for_each(|action| {
                let part = partitioned_file_from_action(action, &schema);
                file_groups
                    .entry(part.partition_values.clone())
                    .or_default()
                    .push(part);
            });
        };

        let table_partition_cols = self.get_metadata()?.partition_columns.clone();
        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect(),
        ));
        let parquet_scan = ParquetFormat::default()
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: self.storage.object_store_url(),
                    file_schema,
                    file_groups: file_groups.into_values().collect(),
                    statistics: self.datafusion_table_statistics(),
                    projection: projection.clone(),
                    limit,
                    table_partition_cols,
                    config_options: Default::default(),
                },
                filters,
            )
            .await?;
        let mut url = self.table_uri();
        if url.ends_with(':') {
            url += "//"; // table_uri() trims slashes from `memory://` so add them back
        }
        let delta_scan = DeltaScan { url, parquet_scan };

        Ok(Arc::new(delta_scan))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// TODO: this will likely also need to perform column mapping later when we support reader protocol v2
/// A wrapper for parquet scans that installs the required ObjectStore
#[derive(Debug)]
pub struct DeltaScan {
    /// The URL of the ObjectStore root
    pub url: String,
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for DeltaScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.parquet_scan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.parquet_scan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.parquet_scan.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.parquet_scan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        ExecutionPlan::with_new_children(self.parquet_scan.clone(), children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let url = self.url.as_str();
        let url = ListingTableUrl::parse(url)?;
        let storage = context.runtime_env().object_store_registry.get_by_url(url);
        let mut table = DeltaTableBuilder::from_uri(self.url.clone());
        if let Ok(storage) = storage {
            // When running in ballista, the store will be deserialized and re-created
            // When testing with a MemoryStore, it will already be present and we should re-use it
            let path = &Path::parse("")?;
            table = table.with_storage_backend(storage, path);
        }
        let table = table.build()?;
        register_store(&table, context.runtime_env());
        self.parquet_scan.execute(partition, context)
    }

    fn statistics(&self) -> Statistics {
        self.parquet_scan.statistics()
    }
}

fn partitioned_file_from_action(action: &action::Add, schema: &ArrowSchema) -> PartitionedFile {
    let partition_values = schema
        .fields()
        .iter()
        .filter_map(|f| {
            action.partition_values.get(f.name()).map(|val| match val {
                Some(value) => to_correct_scalar_value(
                    &serde_json::Value::String(value.to_string()),
                    f.data_type(),
                )
                .unwrap_or(ScalarValue::Null),
                None => ScalarValue::Null,
            })
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified = DateTime::<Utc>::from_utc(
        NaiveDateTime::from_timestamp_opt(ts_secs, ts_ns as u32).unwrap(),
        Utc,
    );
    PartitionedFile {
        object_meta: ObjectMeta {
            location: Path::from(action.path.clone()),
            last_modified,
            size: action.size as usize,
        },
        partition_values,
        range: None,
        extensions: None,
    }
}

fn to_scalar_value(stat_val: &serde_json::Value) -> Option<ScalarValue> {
    match stat_val {
        serde_json::Value::Bool(val) => Some(ScalarValue::from(*val)),
        serde_json::Value::Number(num) => {
            if let Some(val) = num.as_i64() {
                Some(ScalarValue::from(val))
            } else if let Some(val) = num.as_u64() {
                Some(ScalarValue::from(val))
            } else {
                num.as_f64().map(ScalarValue::from)
            }
        }
        serde_json::Value::String(s) => Some(ScalarValue::from(s.as_str())),
        serde_json::Value::Array(_) => None,
        serde_json::Value::Object(_) => None,
        serde_json::Value::Null => None,
    }
}

fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> Option<ScalarValue> {
    match stat_val {
        serde_json::Value::Array(_) => None,
        serde_json::Value::Object(_) => None,
        serde_json::Value::Null => None,
        serde_json::Value::String(string_val) => match field_dt {
            ArrowDataType::Timestamp(_, _) => {
                let time_nanos = ScalarValue::try_from_string(
                    string_val.to_owned(),
                    &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                )
                .ok()?;
                let cast_arr = cast_with_options(
                    &time_nanos.to_array(),
                    field_dt,
                    &CastOptions { safe: false },
                )
                .ok()?;
                Some(ScalarValue::try_from_array(&cast_arr, 0).ok()?)
            }
            _ => Some(ScalarValue::try_from_string(string_val.to_owned(), field_dt).ok()?),
        },
        other => match field_dt {
            ArrowDataType::Timestamp(_, _) => {
                let time_nanos = ScalarValue::try_from_string(
                    other.to_string(),
                    &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                )
                .ok()?;
                let cast_arr = cast_with_options(
                    &time_nanos.to_array(),
                    field_dt,
                    &CastOptions { safe: false },
                )
                .ok()?;
                Some(ScalarValue::try_from_array(&cast_arr, 0).ok()?)
            }
            _ => Some(ScalarValue::try_from_string(other.to_string(), field_dt).ok()?),
        },
    }
}

fn correct_scalar_value_type(value: ScalarValue, field_dt: &ArrowDataType) -> Option<ScalarValue> {
    match field_dt {
        ArrowDataType::Int64 => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int32 => {
            let raw_value = i64::try_from(value).ok()? as i32;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int16 => {
            let raw_value = i64::try_from(value).ok()? as i16;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Int8 => {
            let raw_value = i64::try_from(value).ok()? as i8;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Float32 => {
            let raw_value = f64::try_from(value).ok()? as f32;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Float64 => {
            let raw_value = f64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Utf8 => match value {
            ScalarValue::Utf8(val) => Some(ScalarValue::Utf8(val)),
            _ => None,
        },
        ArrowDataType::LargeUtf8 => match value {
            ScalarValue::Utf8(val) => Some(ScalarValue::LargeUtf8(val)),
            _ => None,
        },
        ArrowDataType::Boolean => {
            let raw_value = bool::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Decimal128(_, _) => {
            let raw_value = f64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Decimal256(_, _) => {
            let raw_value = f64::try_from(value).ok()?;
            Some(ScalarValue::from(raw_value))
        }
        ArrowDataType::Date32 => {
            let raw_value = i64::try_from(value).ok()? as i32;
            Some(ScalarValue::Date32(Some(raw_value)))
        }
        ArrowDataType::Date64 => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::Date64(Some(raw_value)))
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::TimestampNanosecond(Some(raw_value), None))
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::TimestampMicrosecond(Some(raw_value), None))
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, None) => {
            let raw_value = i64::try_from(value).ok()?;
            Some(ScalarValue::TimestampMillisecond(Some(raw_value), None))
        }
        _ => {
            log::error!(
                "Scalar value of arrow type unimplemented for {:?} and {:?}",
                value,
                field_dt
            );
            None
        }
    }
}

fn left_larger_than_right(left: ScalarValue, right: ScalarValue) -> Option<bool> {
    match left {
        ScalarValue::Float64(Some(v)) => {
            let f_right = f64::try_from(right).ok()?;
            Some(v > f_right)
        }
        ScalarValue::Float32(Some(v)) => {
            let f_right = f32::try_from(right).ok()?;
            Some(v > f_right)
        }
        ScalarValue::Int8(Some(v)) => {
            let i_right = i8::try_from(right).ok()?;
            Some(v > i_right)
        }
        ScalarValue::Int16(Some(v)) => {
            let i_right = i16::try_from(right).ok()?;
            Some(v > i_right)
        }
        ScalarValue::Int32(Some(v)) => {
            let i_right = i32::try_from(right).ok()?;
            Some(v > i_right)
        }
        ScalarValue::Int64(Some(v)) => {
            let i_right = i64::try_from(right).ok()?;
            Some(v > i_right)
        }
        ScalarValue::Boolean(Some(v)) => {
            let b_right = bool::try_from(right).ok()?;
            Some(v & !b_right)
        }
        ScalarValue::Utf8(Some(v)) => match right {
            ScalarValue::Utf8(Some(s_right)) => Some(v > s_right),
            _ => None,
        },
        _ => {
            log::error!(
                "Scalar value comparison unimplemented for {:?} and {:?}",
                left,
                right
            );
            None
        }
    }
}

/// Responsible for checking batches of data conform to table's invariants.
pub struct DeltaDataChecker {
    invariants: Vec<Invariant>,
    ctx: SessionContext,
}

impl DeltaDataChecker {
    /// Create a new DeltaDataChecker
    pub fn new(invariants: Vec<Invariant>) -> Self {
        Self {
            invariants,
            ctx: SessionContext::new(),
        }
    }

    /// Check that a record batch conforms to table's invariants.
    ///
    /// If it does not, it will return [DeltaTableError::InvalidData] with a list
    /// of values that violated each invariant.
    pub async fn check_batch(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.enforce_invariants(record_batch).await
        // TODO: for support for Protocol V3, check constraints
    }

    async fn enforce_invariants(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError> {
        // Invariants are deprecated, so let's not pay the overhead for any of this
        // if we can avoid it.
        if self.invariants.is_empty() {
            return Ok(());
        }

        let table = MemTable::try_new(record_batch.schema(), vec![vec![record_batch.clone()]])?;
        self.ctx.register_table("data", Arc::new(table))?;

        let mut violations: Vec<String> = Vec::new();

        for invariant in self.invariants.iter() {
            if invariant.field_name.contains('.') {
                return Err(DeltaTableError::Generic(
                    "Support for column invariants on nested columns is not supported.".to_string(),
                ));
            }

            let sql = format!(
                "SELECT {} FROM data WHERE not ({}) LIMIT 1",
                invariant.field_name, invariant.invariant_sql
            );

            let dfs: Vec<RecordBatch> = self.ctx.sql(&sql).await?.collect().await?;
            if !dfs.is_empty() && dfs[0].num_rows() > 0 {
                let value = format!("{:?}", dfs[0].column(0));
                let msg = format!(
                    "Invariant ({}) violated by value {}",
                    invariant.invariant_sql, value
                );
                violations.push(msg);
            }
        }

        if !violations.is_empty() {
            Err(DeltaTableError::InvalidData { violations })
        } else {
            Ok(())
        }
    }
}

/// A codec for deltalake physical plans
#[derive(Debug)]
pub struct DeltaPhysicalCodec {}

impl PhysicalExtensionCodec for DeltaPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let url: String = serde_json::from_reader(buf)
            .map_err(|_| DataFusionError::Internal("Unable to decode DeltaScan".to_string()))?;
        let delta_scan = DeltaScan {
            url,
            parquet_scan: (*inputs)[0].clone(),
        };
        Ok(Arc::new(delta_scan))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let delta_scan = node
            .as_any()
            .downcast_ref::<DeltaScan>()
            .ok_or_else(|| DataFusionError::Internal("Not a delta scan!".to_string()))?;
        serde_json::to_writer(buf, delta_scan.url.as_str())
            .map_err(|_| DataFusionError::Internal("Unable to encode delta scan!".to_string()))?;
        Ok(())
    }
}

/// Does serde on DeltaTables
#[derive(Debug)]
pub struct DeltaLogicalCodec {}

impl LogicalExtensionCodec for DeltaLogicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension, DataFusionError> {
        todo!("DeltaLogicalCodec")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        todo!("DeltaLogicalCodec")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let provider: DeltaTable = serde_json::from_slice(buf)
            .map_err(|_| DataFusionError::Internal("Error encoding delta table".to_string()))?;
        Ok(Arc::new(provider))
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn TableProvider>,
        mut buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let table = node
            .as_ref()
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or_else(|| {
                DataFusionError::Internal("Can't encode non-delta tables".to_string())
            })?;
        serde_json::to_writer(&mut buf, table)
            .map_err(|_| DataFusionError::Internal("Error encoding delta table".to_string()))
    }
}

/// Responsible for creating deltatables
pub struct DeltaTableFactory {}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(&self, url: &str) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let provider = open_table(url).await.unwrap();
        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StructArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::{TimeZone, Utc};
    use datafusion::from_slice::FromSlice;
    use serde_json::json;

    use super::*;

    // test deserialization of serialized partition values.
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    #[test]
    fn test_parse_scalar_value() {
        let reference_pairs = &[
            (
                json!("2015"),
                ArrowDataType::Int16,
                ScalarValue::Int16(Some(2015)),
            ),
            (
                json!("2015"),
                ArrowDataType::Int32,
                ScalarValue::Int32(Some(2015)),
            ),
            (
                json!("2015"),
                ArrowDataType::Int64,
                ScalarValue::Int64(Some(2015)),
            ),
            (
                json!("2015"),
                ArrowDataType::Float32,
                ScalarValue::Float32(Some(2015_f32)),
            ),
            (
                json!("2015"),
                ArrowDataType::Float64,
                ScalarValue::Float64(Some(2015_f64)),
            ),
            (
                json!(2015),
                ArrowDataType::Float64,
                ScalarValue::Float64(Some(2015_f64)),
            ),
            (
                json!("2015-01-01"),
                ArrowDataType::Date32,
                ScalarValue::Date32(Some(16436)),
            ),
            // (
            //     json!("2015-01-01"),
            //     ArrowDataType::Date64,
            //     ScalarValue::Date64(Some(16436)),
            // ),
            // TODO(roeap) there seem to be differences in how precisions are handled locally and in CI, need to investigate
            // (
            //     json!("2020-09-08 13:42:29"),
            //     ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            //     ScalarValue::TimestampNanosecond(Some(1599565349000000000), None),
            // ),
            // (
            //     json!("2020-09-08 13:42:29"),
            //     ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            //     ScalarValue::TimestampMicrosecond(Some(1599565349000000), None),
            // ),
            // (
            //     json!("2020-09-08 13:42:29"),
            //     ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            //     ScalarValue::TimestampMillisecond(Some(1599565349000), None),
            // ),
            (
                json!(true),
                ArrowDataType::Boolean,
                ScalarValue::Boolean(Some(true)),
            ),
        ];

        for (raw, data_type, ref_scalar) in reference_pairs {
            let scalar = to_correct_scalar_value(raw, data_type).unwrap();
            assert_eq!(*ref_scalar, scalar)
        }
    }

    #[test]
    fn test_partitioned_file_from_action() {
        let mut partition_values = std::collections::HashMap::new();
        partition_values.insert("month".to_string(), Some("1".to_string()));
        partition_values.insert("year".to_string(), Some("2015".to_string()));
        let action = action::Add {
            path: "year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string(),
            size: 10644,
            partition_values,
            modification_time: 1660497727833,
            partition_values_parsed: None,
            data_change: true,
            stats: None,
            stats_parsed: None,
            tags: None,
        };
        let schema = ArrowSchema::new(vec![
            Field::new("year", ArrowDataType::Int64, true),
            Field::new("month", ArrowDataType::Int64, true),
        ]);

        let file = partitioned_file_from_action(&action, &schema);
        let ref_file = PartitionedFile {
            object_meta: object_store::ObjectMeta {
                location: Path::from("year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string()), 
                last_modified: Utc.timestamp_millis_opt(1660497727833).unwrap(),
                size: 10644,
            },
            partition_values: [ScalarValue::Int64(Some(2015)), ScalarValue::Int64(Some(1))].to_vec(),
            range: None,
            extensions: None,
        };
        assert_eq!(file.partition_values, ref_file.partition_values)
    }

    #[tokio::test]
    async fn test_enforce_invariants() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(&["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from_slice(&[1, 10, 10, 100])),
            ],
        )
        .unwrap();
        // Empty invariants is okay
        let invariants: Vec<Invariant> = vec![];
        assert!(DeltaDataChecker::new(invariants)
            .check_batch(&batch)
            .await
            .is_ok());

        // Valid invariants return Ok(())
        let invariants = vec![
            Invariant::new("a", "a is not null"),
            Invariant::new("b", "b < 1000"),
        ];
        assert!(DeltaDataChecker::new(invariants)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let invariants = vec![
            Invariant::new("a", "a is null"),
            Invariant::new("b", "b < 100"),
        ];
        let result = DeltaDataChecker::new(invariants).check_batch(&batch).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant invariants return a different error
        let invariants = vec![Invariant::new("c", "c > 2000")];
        let result = DeltaDataChecker::new(invariants).check_batch(&batch).await;
        assert!(result.is_err());

        // Nested invariants are unsupported
        let struct_fields = schema.fields().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(struct_fields),
            false,
        )]));
        let inner = Arc::new(StructArray::from(batch));
        let batch = RecordBatch::try_new(schema, vec![inner]).unwrap();

        let invariants = vec![Invariant::new("x.b", "x.b < 1000")];
        let result = DeltaDataChecker::new(invariants).check_batch(&batch).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::Generic { .. })));
    }
}
