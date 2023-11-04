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
//!   let table = deltalake_core::open_table("./tests/data/simple_table")
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
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType;
use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_array::types::UInt16Type;
use arrow_array::{DictionaryArray, StringArray};
use arrow_schema::Field;
use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone, Utc};
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::physical_plan::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileScanConfig,
};
use datafusion::datasource::provider::TableProviderFactory;
use datafusion::datasource::{listing::PartitionedFile, MemTable, TableProvider, TableType};
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::FunctionRegistry;
use datafusion::optimizer::utils::conjunction;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_optimizer::pruning::{PruningPredicate, PruningStatistics};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::scalar::ScalarValue;
use datafusion_common::tree_node::{TreeNode, TreeNodeVisitor, VisitRecursion};
use datafusion_common::{Column, DataFusionError, Result as DataFusionResult, ToDFSchema};
use datafusion_expr::expr::{ScalarFunction, ScalarUDF};
use datafusion_expr::logical_plan::CreateExternalTable;
use datafusion_expr::{col, Expr, Extension, LogicalPlan, TableProviderFilterPushDown, Volatility};
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use object_store::ObjectMeta;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::protocol::{self, Add};
use crate::storage::ObjectStoreRef;
use crate::table::builder::ensure_table_uri;
use crate::table::state::DeltaTableState;
use crate::{open_table, open_table_with_storage_options, DeltaTable, Invariant, SchemaDataType};

const PATH_COLUMN: &str = "__delta_rs_path";

pub mod expr;

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

impl DeltaTableState {
    /// Return statistics for Datafusion Table
    pub fn datafusion_table_statistics(&self) -> Statistics {
        let stats = self
            .files()
            .iter()
            .try_fold(
                Statistics {
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
                },
                |acc, action| {
                    let new_stats = action
                        .get_stats()
                        .unwrap_or_else(|_| Some(protocol::Stats::default()))?;
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
                        let dt = self
                            .arrow_schema()
                            .unwrap()
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

// TODO: Collapse with operations/transaction/state.rs method of same name
fn get_prune_stats(table: &DeltaTable, column: &Column, get_max: bool) -> Option<ArrayRef> {
    let field = table
        .get_schema()
        .ok()
        .map(|s| s.get_field_with_name(&column.name).ok())??;

    // See issue 1214. Binary type does not support natural order which is required for Datafusion to prune
    if let SchemaDataType::primitive(t) = &field.get_type() {
        if t == "binary" {
            return None;
        }
    }

    let data_type = field.get_type().try_into().ok()?;
    let partition_columns = &table.get_metadata().ok()?.partition_columns;

    let values = table.get_state().files().iter().map(|add| {
        if partition_columns.contains(&column.name) {
            let value = add.partition_values.get(&column.name).unwrap();
            let value = match value {
                Some(v) => serde_json::Value::String(v.to_string()),
                None => serde_json::Value::Null,
            };
            to_correct_scalar_value(&value, &data_type).unwrap_or(
                get_null_of_arrow_type(&data_type).expect("Could not determine null type"),
            )
        } else if let Ok(Some(statistics)) = add.get_stats() {
            let values = if get_max {
                statistics.max_values
            } else {
                statistics.min_values
            };

            values
                .get(&column.name)
                .and_then(|f| to_correct_scalar_value(f.as_value()?, &data_type))
                .unwrap_or(
                    get_null_of_arrow_type(&data_type).expect("Could not determine null type"),
                )
        } else {
            // No statistics available
            get_null_of_arrow_type(&data_type).expect("Could not determine null type")
        }
    });
    ScalarValue::iter_to_array(values).ok()
}

impl PruningStatistics for DeltaTable {
    /// return the minimum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        get_prune_stats(self, column, false)
    }

    /// return the maximum values for the named column, if known.
    /// Note: the returned array must contain `num_containers()` rows.
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        get_prune_stats(self, column, true)
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
        let partition_columns = &self.get_metadata().ok()?.partition_columns;

        let values = self.get_state().files().iter().map(|add| {
            if let Ok(Some(statistics)) = add.get_stats() {
                if partition_columns.contains(&column.name) {
                    let value = add.partition_values.get(&column.name).unwrap();
                    match value {
                        Some(_) => ScalarValue::UInt64(Some(0)),
                        None => ScalarValue::UInt64(Some(statistics.num_records as u64)),
                    }
                } else {
                    statistics
                        .null_count
                        .get(&column.name)
                        .map(|f| ScalarValue::UInt64(f.as_value().map(|val| val as u64)))
                        .unwrap_or(ScalarValue::UInt64(None))
                }
            } else if partition_columns.contains(&column.name) {
                let value = add.partition_values.get(&column.name).unwrap();
                match value {
                    Some(_) => ScalarValue::UInt64(Some(0)),
                    None => ScalarValue::UInt64(None),
                }
            } else {
                ScalarValue::UInt64(None)
            }
        });
        ScalarValue::iter_to_array(values).ok()
    }
}

// each delta table must register a specific object store, since paths are internally
// handled relative to the table root.
pub(crate) fn register_store(store: ObjectStoreRef, env: Arc<RuntimeEnv>) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store);
}

pub(crate) fn logical_schema(
    snapshot: &DeltaTableState,
    scan_config: &DeltaScanConfig,
) -> DeltaResult<SchemaRef> {
    let input_schema = snapshot.input_schema()?;
    let mut fields = Vec::new();
    for field in input_schema.fields.iter() {
        fields.push(field.to_owned());
    }

    if let Some(file_column_name) = &scan_config.file_column_name {
        fields.push(Arc::new(Field::new(
            file_column_name,
            arrow_schema::DataType::Utf8,
            true,
        )));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
}

#[derive(Debug, Clone, Default)]
/// Used to specify if additonal metadata columns are exposed to the user
pub struct DeltaScanConfigBuilder {
    /// Include the source path for each record. The name of this column is determine by `file_column_name`
    include_file_column: bool,
    /// Column name that contains the source path.
    ///
    /// If include_file_column is true and the name is None then it will be auto-generated
    /// Otherwise the user provided name will be used
    file_column_name: Option<String>,
}

impl DeltaScanConfigBuilder {
    /// Construct a new instance of `DeltaScanConfigBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Indicate that a column containing a records file path is included.
    /// Column name is generated and can be determined once this Config is built
    pub fn with_file_column(mut self, include: bool) -> Self {
        self.include_file_column = include;
        self.file_column_name = None;
        self
    }

    /// Indicate that a column containing a records file path is included and column name is user defined.
    pub fn with_file_column_name<S: ToString>(mut self, name: &S) -> Self {
        self.file_column_name = Some(name.to_string());
        self.include_file_column = true;
        self
    }

    /// Build a DeltaScanConfig and ensure no column name conflicts occur during downstream processing
    pub fn build(&self, snapshot: &DeltaTableState) -> DeltaResult<DeltaScanConfig> {
        let input_schema = snapshot.input_schema()?;
        let mut file_column_name = None;
        let mut column_names: HashSet<&String> = HashSet::new();
        for field in input_schema.fields.iter() {
            column_names.insert(field.name());
        }

        if self.include_file_column {
            match &self.file_column_name {
                Some(name) => {
                    if column_names.contains(name) {
                        return Err(DeltaTableError::Generic(format!(
                            "Unable to add file path column since column with name {} exits",
                            name
                        )));
                    }

                    file_column_name = Some(name.to_owned())
                }
                None => {
                    let prefix = PATH_COLUMN;
                    let mut idx = 0;
                    let mut name = prefix.to_owned();

                    while column_names.contains(&name) {
                        idx += 1;
                        name = format!("{}_{}", prefix, idx);
                    }

                    file_column_name = Some(name);
                }
            }
        }

        Ok(DeltaScanConfig { file_column_name })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
/// Include additonal metadata columns during a [`DeltaScan`]
pub struct DeltaScanConfig {
    /// Include the source path for each record
    pub file_column_name: Option<String>,
}

#[derive(Debug)]
pub(crate) struct DeltaScanBuilder<'a> {
    snapshot: &'a DeltaTableState,
    object_store: ObjectStoreRef,
    filter: Option<Expr>,
    state: &'a SessionState,
    projection: Option<&'a Vec<usize>>,
    limit: Option<usize>,
    files: Option<&'a [Add]>,
    config: DeltaScanConfig,
    schema: Option<SchemaRef>,
}

impl<'a> DeltaScanBuilder<'a> {
    pub fn new(
        snapshot: &'a DeltaTableState,
        object_store: ObjectStoreRef,
        state: &'a SessionState,
    ) -> Self {
        DeltaScanBuilder {
            snapshot,
            object_store,
            filter: None,
            state,
            files: None,
            projection: None,
            limit: None,
            config: DeltaScanConfig::default(),
            schema: None,
        }
    }

    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    pub fn with_files(mut self, files: &'a [Add]) -> Self {
        self.files = Some(files);
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_scan_config(mut self, config: DeltaScanConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    pub async fn build(self) -> DeltaResult<DeltaScan> {
        let config = self.config;
        let schema = match self.schema {
            Some(schema) => schema,
            None => {
                self.snapshot
                    .physical_arrow_schema(self.object_store.clone())
                    .await?
            }
        };
        let logical_schema = logical_schema(self.snapshot, &config)?;

        let logical_schema = if let Some(used_columns) = self.projection {
            let mut fields = vec![];
            for idx in used_columns {
                fields.push(logical_schema.field(*idx).to_owned());
            }
            Arc::new(ArrowSchema::new(fields))
        } else {
            logical_schema
        };

        let logical_filter = self
            .filter
            .map(|expr| logical_expr_to_physical_expr(&expr, &logical_schema));

        // Perform Pruning of files to scan
        let files = match self.files {
            Some(files) => files.to_owned(),
            None => {
                if let Some(predicate) = &logical_filter {
                    let pruning_predicate =
                        PruningPredicate::try_new(predicate.clone(), logical_schema.clone())?;
                    let files_to_prune = pruning_predicate.prune(self.snapshot)?;
                    self.snapshot
                        .files()
                        .iter()
                        .zip(files_to_prune.into_iter())
                        .filter_map(
                            |(action, keep)| {
                                if keep {
                                    Some(action.to_owned())
                                } else {
                                    None
                                }
                            },
                        )
                        .collect()
                } else {
                    self.snapshot.files().to_owned()
                }
            }
        };

        // TODO we group files together by their partition values. If the table is partitioned
        // and partitions are somewhat evenly distributed, probably not the worst choice ...
        // However we may want to do some additional balancing in case we are far off from the above.
        let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();

        let table_partition_cols = &self
            .snapshot
            .current_metadata()
            .ok_or(DeltaTableError::NoMetadata)?
            .partition_columns;

        for action in files.iter() {
            let mut part = partitioned_file_from_action(action, table_partition_cols, &schema);

            if config.file_column_name.is_some() {
                part.partition_values
                    .push(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(
                        action.path.clone(),
                    ))));
            }

            file_groups
                .entry(part.partition_values.clone())
                .or_default()
                .push(part);
        }

        let file_schema = Arc::new(ArrowSchema::new(
            schema
                .fields()
                .iter()
                .filter(|f| !table_partition_cols.contains(f.name()))
                .cloned()
                .collect::<Vec<arrow::datatypes::FieldRef>>(),
        ));

        let mut table_partition_cols = table_partition_cols
            .iter()
            .map(|c| Ok((c.to_owned(), schema.field_with_name(c)?.data_type().clone())))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        if let Some(file_column_name) = &config.file_column_name {
            table_partition_cols.push((
                file_column_name.clone(),
                wrap_partition_type_in_dict(DataType::Utf8),
            ));
        }

        let scan = ParquetFormat::new()
            .create_physical_plan(
                self.state,
                FileScanConfig {
                    object_store_url: self.object_store.object_store_url(),
                    file_schema,
                    file_groups: file_groups.into_values().collect(),
                    statistics: self.snapshot.datafusion_table_statistics(),
                    projection: self.projection.cloned(),
                    limit: self.limit,
                    table_partition_cols,
                    output_ordering: vec![],
                    infinite_source: false,
                },
                logical_filter.as_ref(),
            )
            .await?;

        Ok(DeltaScan {
            table_uri: ensure_table_uri(self.object_store.root_uri())?
                .as_str()
                .into(),
            parquet_scan: scan,
            config,
            logical_schema,
        })
    }
}

#[async_trait]
impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.state.arrow_schema().unwrap()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        register_store(self.object_store(), session.runtime_env().clone());
        let filter_expr = conjunction(filters.iter().cloned());

        let scan = DeltaScanBuilder::new(&self.state, self.object_store(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .build()
            .await?;

        Ok(Arc::new(scan))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.state.datafusion_table_statistics())
    }
}

/// A Delta table provider that enables additonal metadata columns to be included during the scan
pub struct DeltaTableProvider {
    snapshot: DeltaTableState,
    store: ObjectStoreRef,
    config: DeltaScanConfig,
    schema: Arc<ArrowSchema>,
}

impl DeltaTableProvider {
    /// Build a DeltaTableProvider
    pub fn try_new(
        snapshot: DeltaTableState,
        store: ObjectStoreRef,
        config: DeltaScanConfig,
    ) -> DeltaResult<Self> {
        Ok(DeltaTableProvider {
            schema: logical_schema(&snapshot, &config)?,
            snapshot,
            store,
            config,
        })
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    async fn scan(
        &self,
        session: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        register_store(self.store.clone(), session.runtime_env().clone());
        let filter_expr = conjunction(filters.iter().cloned());

        let scan = DeltaScanBuilder::new(&self.snapshot, self.store.clone(), session)
            .with_projection(projection)
            .with_limit(limit)
            .with_filter(filter_expr)
            .with_scan_config(self.config.clone())
            .build()
            .await?;

        Ok(Arc::new(scan))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.snapshot.datafusion_table_statistics())
    }
}

// TODO: this will likely also need to perform column mapping later when we support reader protocol v2
/// A wrapper for parquet scans
#[derive(Debug)]
pub struct DeltaScan {
    /// The URL of the ObjectStore root
    pub table_uri: String,
    /// Column that contains an index that maps to the original metadata Add
    pub config: DeltaScanConfig,
    /// The parquet scan to wrap
    pub parquet_scan: Arc<dyn ExecutionPlan>,
    /// The schema of the table to be used when evaluating expressions
    pub logical_schema: Arc<ArrowSchema>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DeltaScanWire {
    pub table_uri: String,
    pub config: DeltaScanConfig,
    pub logical_schema: Arc<ArrowSchema>,
}

impl DisplayAs for DeltaScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaScan")
    }
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
        self.parquet_scan.execute(partition, context)
    }

    fn statistics(&self) -> Statistics {
        self.parquet_scan.statistics()
    }
}

pub(crate) fn get_null_of_arrow_type(t: &ArrowDataType) -> DeltaResult<ScalarValue> {
    match t {
        ArrowDataType::Null => Ok(ScalarValue::Null),
        ArrowDataType::Boolean => Ok(ScalarValue::Boolean(None)),
        ArrowDataType::Int8 => Ok(ScalarValue::Int8(None)),
        ArrowDataType::Int16 => Ok(ScalarValue::Int16(None)),
        ArrowDataType::Int32 => Ok(ScalarValue::Int32(None)),
        ArrowDataType::Int64 => Ok(ScalarValue::Int64(None)),
        ArrowDataType::UInt8 => Ok(ScalarValue::UInt8(None)),
        ArrowDataType::UInt16 => Ok(ScalarValue::UInt16(None)),
        ArrowDataType::UInt32 => Ok(ScalarValue::UInt32(None)),
        ArrowDataType::UInt64 => Ok(ScalarValue::UInt64(None)),
        ArrowDataType::Float32 => Ok(ScalarValue::Float32(None)),
        ArrowDataType::Float64 => Ok(ScalarValue::Float64(None)),
        ArrowDataType::Date32 => Ok(ScalarValue::Date32(None)),
        ArrowDataType::Date64 => Ok(ScalarValue::Date64(None)),
        ArrowDataType::Binary => Ok(ScalarValue::Binary(None)),
        ArrowDataType::FixedSizeBinary(size) => {
            Ok(ScalarValue::FixedSizeBinary(size.to_owned(), None))
        }
        ArrowDataType::LargeBinary => Ok(ScalarValue::LargeBinary(None)),
        ArrowDataType::Utf8 => Ok(ScalarValue::Utf8(None)),
        ArrowDataType::LargeUtf8 => Ok(ScalarValue::LargeUtf8(None)),
        ArrowDataType::Decimal128(precision, scale) => Ok(ScalarValue::Decimal128(
            None,
            precision.to_owned(),
            scale.to_owned(),
        )),
        ArrowDataType::Timestamp(unit, tz) => {
            let tz = tz.to_owned();
            Ok(match unit {
                TimeUnit::Second => ScalarValue::TimestampSecond(None, tz),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(None, tz),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(None, tz),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(None, tz),
            })
        }
        ArrowDataType::Dictionary(k, v) => Ok(ScalarValue::Dictionary(
            k.clone(),
            Box::new(get_null_of_arrow_type(v).unwrap()),
        )),
        //Unsupported types...
        ArrowDataType::Float16
        | ArrowDataType::Decimal256(_, _)
        | ArrowDataType::Union(_, _)
        | ArrowDataType::LargeList(_)
        | ArrowDataType::Struct(_)
        | ArrowDataType::List(_)
        | ArrowDataType::FixedSizeList(_, _)
        | ArrowDataType::Time32(_)
        | ArrowDataType::Time64(_)
        | ArrowDataType::Duration(_)
        | ArrowDataType::Interval(_)
        | ArrowDataType::RunEndEncoded(_, _)
        | ArrowDataType::Map(_, _) => Err(DeltaTableError::Generic(format!(
            "Unsupported data type for Delta Lake {}",
            t
        ))),
    }
}

pub(crate) fn partitioned_file_from_action(
    action: &protocol::Add,
    partition_columns: &[String],
    schema: &ArrowSchema,
) -> PartitionedFile {
    let partition_values = partition_columns
        .iter()
        .map(|part| {
            action
                .partition_values
                .get(part)
                .map(|val| {
                    schema
                        .field_with_name(part)
                        .map(|field| match val {
                            Some(value) => to_correct_scalar_value(
                                &serde_json::Value::String(value.to_string()),
                                field.data_type(),
                            )
                            .unwrap_or(ScalarValue::Null),
                            None => get_null_of_arrow_type(field.data_type())
                                .unwrap_or(ScalarValue::Null),
                        })
                        .unwrap_or(ScalarValue::Null)
                })
                .unwrap_or(ScalarValue::Null)
        })
        .collect::<Vec<_>>();

    let ts_secs = action.modification_time / 1000;
    let ts_ns = (action.modification_time % 1000) * 1_000_000;
    let last_modified =
        Utc.from_utc_datetime(&NaiveDateTime::from_timestamp_opt(ts_secs, ts_ns as u32).unwrap());
    PartitionedFile {
        object_meta: ObjectMeta {
            last_modified,
            ..action.try_into().unwrap()
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

pub(crate) fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> Option<ScalarValue> {
    match stat_val {
        serde_json::Value::Array(_) => None,
        serde_json::Value::Object(_) => None,
        serde_json::Value::Null => get_null_of_arrow_type(field_dt).ok(),
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
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
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
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
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
    match (&left, &right) {
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => Some(l > r),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => Some(l > r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => Some(l > r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => Some(l > r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => Some(l > r),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => Some(l > r),
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r))) => Some(l > r),
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => Some(l & !r),
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

pub(crate) fn logical_expr_to_physical_expr(
    expr: &Expr,
    schema: &ArrowSchema,
) -> Arc<dyn PhysicalExpr> {
    let df_schema = schema.clone().to_dfschema().unwrap();
    let execution_props = ExecutionProps::new();
    create_physical_expr(expr, &df_schema, schema, &execution_props).unwrap()
}

/// Responsible for checking batches of data conform to table's invariants.
#[derive(Clone)]
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
        let wire: DeltaScanWire = serde_json::from_reader(buf)
            .map_err(|_| DataFusionError::Internal("Unable to decode DeltaScan".to_string()))?;
        let delta_scan = DeltaScan {
            table_uri: wire.table_uri,
            parquet_scan: (*inputs)[0].clone(),
            config: wire.config,
            logical_schema: wire.logical_schema,
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

        let wire = DeltaScanWire {
            table_uri: delta_scan.table_uri.to_owned(),
            config: delta_scan.config.clone(),
            logical_schema: delta_scan.logical_schema.clone(),
        };
        serde_json::to_writer(buf, &wire)
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
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        let table = node
            .as_ref()
            .as_any()
            .downcast_ref::<DeltaTable>()
            .ok_or_else(|| {
                DataFusionError::Internal("Can't encode non-delta tables".to_string())
            })?;
        serde_json::to_writer(buf, table)
            .map_err(|_| DataFusionError::Internal("Error encoding delta table".to_string()))
    }
}

/// Responsible for creating deltatables
pub struct DeltaTableFactory {}

#[async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let provider = if cmd.options.is_empty() {
            open_table(cmd.to_owned().location).await?
        } else {
            open_table_with_storage_options(cmd.to_owned().location, cmd.to_owned().options).await?
        };
        Ok(Arc::new(provider))
    }
}

pub(crate) struct FindFilesExprProperties {
    pub partition_columns: Vec<String>,

    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor for FindFilesExprProperties {
    type N = Expr;

    fn pre_visit(&mut self, expr: &Self::N) -> datafusion_common::Result<VisitRecursion> {
        // TODO: We can likely relax the volatility to STABLE. Would require further
        // research to confirm the same value is generated during the scan and
        // rewrite phases.

        match expr {
            Expr::Column(c) => {
                if !self.partition_columns.contains(&c.name) {
                    self.partition_only = false;
                }
            }
            Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::Alias(_)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::InList { .. }
            | Expr::GetIndexedField(_)
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_) => (),
            Expr::ScalarFunction(ScalarFunction { fun, .. }) => {
                let v = fun.volatility();
                if v > Volatility::Immutable {
                    self.result = Err(DeltaTableError::Generic(format!(
                        "Find files predicate contains nondeterministic function {}",
                        fun
                    )));
                    return Ok(VisitRecursion::Stop);
                }
            }
            Expr::ScalarUDF(ScalarUDF { fun, .. }) => {
                let v = fun.signature.volatility;
                if v > Volatility::Immutable {
                    self.result = Err(DeltaTableError::Generic(format!(
                        "Find files predicate contains nondeterministic function {}",
                        fun.name
                    )));
                    return Ok(VisitRecursion::Stop);
                }
            }
            _ => {
                self.result = Err(DeltaTableError::Generic(format!(
                    "Find files predicate contains unsupported expression {}",
                    expr
                )));
                return Ok(VisitRecursion::Stop);
            }
        }

        Ok(VisitRecursion::Continue)
    }
}

/// Representing the result of the [find_files] function.
pub struct FindFiles {
    /// A list of `Add` objects that match the given predicate
    pub candidates: Vec<Add>,
    /// Was a physical read to the datastore required to determine the candidates
    pub partition_scan: bool,
}

fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
) -> DeltaResult<Vec<Add>> {
    // Given RecordBatches that contains `__delta_rs_path` perform a hash join
    // with actions to obtain original add actions

    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let array = batch.column_by_name(path_column).ok_or_else(|| {
            DeltaTableError::Generic(format!("Unable to find column {}", path_column))
        })?;

        let iter: Box<dyn Iterator<Item = Option<&str>>> =
            if dict_array {
                let array = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt16Type>>()
                    .ok_or(DeltaTableError::Generic(format!(
                        "Unable to downcast column {}",
                        path_column
                    )))?
                    .downcast_dict::<StringArray>()
                    .ok_or(DeltaTableError::Generic(format!(
                        "Unable to downcast column {}",
                        path_column
                    )))?;
                Box::new(array.into_iter())
            } else {
                let array = array.as_any().downcast_ref::<StringArray>().ok_or(
                    DeltaTableError::Generic(format!("Unable to downcast column {}", path_column)),
                )?;
                Box::new(array.into_iter())
            };

        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{} cannot be null",
                path_column
            )))?;

            match actions.remove(path) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ))
                }
            }
        }
    }
    Ok(files)
}

/// Determine which files contain a record that statisfies the predicate
pub(crate) async fn find_files_scan<'a>(
    snapshot: &DeltaTableState,
    store: ObjectStoreRef,
    state: &SessionState,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    let candidate_map: HashMap<String, Add> = snapshot
        .files()
        .iter()
        .map(|add| (add.path.clone(), add.to_owned()))
        .collect();

    let scan_config = DeltaScanConfigBuilder {
        include_file_column: true,
        ..Default::default()
    }
    .build(snapshot)?;

    let logical_schema = logical_schema(snapshot, &scan_config)?;

    // Identify which columns we need to project
    let mut used_columns = expression
        .to_columns()?
        .into_iter()
        .map(|column| logical_schema.index_of(&column.name))
        .collect::<Result<Vec<usize>, ArrowError>>()?;
    // Add path column
    used_columns.push(logical_schema.index_of(scan_config.file_column_name.as_ref().unwrap())?);

    let scan = DeltaScanBuilder::new(snapshot, store.clone(), state)
        .with_filter(Some(expression.clone()))
        .with_projection(Some(&used_columns))
        .with_scan_config(scan_config)
        .build()
        .await?;
    let scan = Arc::new(scan);

    let config = &scan.config;
    let input_schema = scan.logical_schema.as_ref().to_owned();
    let input_dfschema = input_schema.clone().try_into()?;

    let predicate_expr = create_physical_expr(
        &Expr::IsTrue(Box::new(expression.clone())),
        &input_dfschema,
        &input_schema,
        state.execution_props(),
    )?;

    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));

    let task_ctx = Arc::new(TaskContext::from(state));
    let path_batches = datafusion::physical_plan::collect(limit, task_ctx).await?;

    join_batches_with_add_actions(
        path_batches,
        candidate_map,
        config.file_column_name.as_ref().unwrap(),
        true,
    )
}

pub(crate) async fn scan_memory_table(
    snapshot: &DeltaTableState,
    predicate: &Expr,
) -> DeltaResult<Vec<Add>> {
    let actions = snapshot.files().to_owned();

    let batch = snapshot.add_actions_table(true)?;
    let mut arrays = Vec::new();
    let mut fields = Vec::new();

    let schema = batch.schema();

    arrays.push(
        batch
            .column_by_name("path")
            .ok_or(DeltaTableError::Generic(
                "Column with name `path` does not exist".to_owned(),
            ))?
            .to_owned(),
    );
    fields.push(Field::new(PATH_COLUMN, DataType::Utf8, false));

    for field in schema.fields() {
        if field.name().starts_with("partition.") {
            let name = field.name().strip_prefix("partition.").unwrap();

            arrays.push(batch.column_by_name(field.name()).unwrap().to_owned());
            fields.push(Field::new(
                name,
                field.data_type().to_owned(),
                field.is_nullable(),
            ));
        }
    }

    let schema = Arc::new(ArrowSchema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    let mut df = ctx.read_table(Arc::new(mem_table))?;
    df = df
        .filter(predicate.to_owned())?
        .select(vec![col(PATH_COLUMN)])?;
    let batches = df.collect().await?;

    let map = actions
        .into_iter()
        .map(|action| (action.path.clone(), action))
        .collect::<HashMap<String, Add>>();

    join_batches_with_add_actions(batches, map, PATH_COLUMN, false)
}

/// Finds files in a snapshot that match the provided predicate.
pub async fn find_files<'a>(
    snapshot: &DeltaTableState,
    object_store: ObjectStoreRef,
    state: &SessionState,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    let current_metadata = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?;

    match &predicate {
        Some(predicate) => {
            // Validate the Predicate and determine if it only contains partition columns
            let mut expr_properties = FindFilesExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns.clone(),
                result: Ok(()),
            };

            TreeNode::visit(predicate, &mut expr_properties)?;
            expr_properties.result?;

            if expr_properties.partition_only {
                let candidates = scan_memory_table(snapshot, predicate).await?;
                Ok(FindFiles {
                    candidates,
                    partition_scan: true,
                })
            } else {
                let candidates =
                    find_files_scan(snapshot, object_store.clone(), state, predicate.to_owned())
                        .await?;

                Ok(FindFiles {
                    candidates,
                    partition_scan: false,
                })
            }
        }
        None => Ok(FindFiles {
            candidates: snapshot.files().to_owned(),
            partition_scan: true,
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::writer::test_utils::get_delta_schema;
    use arrow::array::StructArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::{TimeZone, Utc};
    use datafusion::assert_batches_sorted_eq;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf;
    use object_store::path::Path;
    use serde_json::json;
    use std::ops::Deref;

    use super::*;

    // test deserialization of serialized partition values.
    // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#partition-value-serialization
    #[test]
    fn test_to_correct_scalar_value() {
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
    fn test_to_scalar_value() {
        let reference_pairs = &[
            (
                json!("val"),
                Some(ScalarValue::Utf8(Some(String::from("val")))),
            ),
            (json!("2"), Some(ScalarValue::Utf8(Some(String::from("2"))))),
            (json!(true), Some(ScalarValue::Boolean(Some(true)))),
            (json!(false), Some(ScalarValue::Boolean(Some(false)))),
            (json!(2), Some(ScalarValue::Int64(Some(2)))),
            (json!(-2), Some(ScalarValue::Int64(Some(-2)))),
            (json!(2.0), Some(ScalarValue::Float64(Some(2.0)))),
            (json!(["1", "2"]), None),
            (json!({"key": "val"}), None),
            (json!(null), None),
        ];
        for (stat_val, scalar_val) in reference_pairs {
            assert_eq!(to_scalar_value(stat_val), *scalar_val)
        }
    }

    #[test]
    fn test_left_larger_than_right() {
        let correct_reference_pairs = vec![
            (
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float64(Some(2.0)),
            ),
            (
                ScalarValue::Float32(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
            ),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(2))),
            (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(2))),
            (ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))),
            (ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))),
            (
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                ScalarValue::Utf8(Some(String::from("1"))),
                ScalarValue::Utf8(Some(String::from("2"))),
            ),
        ];
        for (smaller_val, larger_val) in correct_reference_pairs {
            assert_eq!(
                left_larger_than_right(smaller_val.clone(), larger_val.clone()),
                Some(false)
            );
            assert_eq!(left_larger_than_right(larger_val, smaller_val), Some(true));
        }

        let incorrect_reference_pairs = vec![
            (
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
            ),
            (ScalarValue::Int32(Some(1)), ScalarValue::Float32(Some(2.0))),
            (
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Float32(Some(2.0)),
            ),
        ];
        for (left, right) in incorrect_reference_pairs {
            assert_eq!(left_larger_than_right(left, right), None);
        }
    }

    #[test]
    fn test_partitioned_file_from_action() {
        let mut partition_values = std::collections::HashMap::new();
        partition_values.insert("month".to_string(), Some("1".to_string()));
        partition_values.insert("year".to_string(), Some("2015".to_string()));
        let action = protocol::Add {
            path: "year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string(),
            size: 10644,
            partition_values,
            modification_time: 1660497727833,
            partition_values_parsed: None,
            data_change: true,
            stats: None,
            deletion_vector: None,
            stats_parsed: None,
            tags: None,
        };
        let schema = ArrowSchema::new(vec![
            Field::new("year", ArrowDataType::Int64, true),
            Field::new("month", ArrowDataType::Int64, true),
        ]);

        let part_columns = vec!["year".to_string(), "month".to_string()];
        let file = partitioned_file_from_action(&action, &part_columns, &schema);
        let ref_file = PartitionedFile {
            object_meta: object_store::ObjectMeta {
                location: Path::from("year=2015/month=1/part-00000-4dcb50d3-d017-450c-9df7-a7257dbd3c5d-c000.snappy.parquet".to_string()), 
                last_modified: Utc.timestamp_millis_opt(1660497727833).unwrap(),
                size: 10644,
                e_tag: None
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
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
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

    #[test]
    fn roundtrip_test_delta_exec_plan() {
        let ctx = SessionContext::new();
        let codec = DeltaPhysicalCodec {};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let exec_plan = Arc::from(DeltaScan {
            table_uri: "s3://my_bucket/this/is/some/path".to_string(),
            parquet_scan: Arc::from(EmptyExec::new(false, schema.clone())),
            config: DeltaScanConfig::default(),
            logical_schema: schema.clone(),
        });
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
                .expect("to proto");

        let runtime = ctx.runtime_env();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&ctx, runtime.deref(), &codec)
            .expect("from proto");
        assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
    }

    #[tokio::test]
    async fn delta_table_provider_with_config() {
        let table = crate::open_table("tests/data/delta-2.2.0-partitioned-types")
            .await
            .unwrap();
        let config = DeltaScanConfigBuilder::new()
            .with_file_column_name(&"file_source")
            .build(&table.state)
            .unwrap();

        let provider = DeltaTableProvider::try_new(table.state, table.storage, config).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("select * from test").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec! [
                "+----+----+----+-------------------------------------------------------------------------------+",
                "| c3 | c1 | c2 | file_source                                                                   |",
                "+----+----+----+-------------------------------------------------------------------------------+",
                "| 4  | 6  | a  | c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet |",
                "| 5  | 4  | c  | c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet |",
                "| 6  | 5  | b  | c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet |",
                "+----+----+----+-------------------------------------------------------------------------------+",
            ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn delta_scan_mixed_partition_order() {
        // Tests issue (1787) where partition columns were incorrect when they
        // have a different order in the metadata and table schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("modified", DataType::Utf8, true),
            Field::new("id", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ]));

        let table = crate::DeltaOps::new_in_memory()
            .create()
            .with_columns(get_delta_schema().get_fields().clone())
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), 0);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-02",
                ])),
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 20, 100])),
            ],
        )
        .unwrap();
        // write some data
        let table = crate::DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new().build(&table.state).unwrap();

        let provider = DeltaTableProvider::try_new(table.state, table.storage, config).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("select * from test").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+-------+------------+----+",
            "| value | modified   | id |",
            "+-------+------------+----+",
            "| 1     | 2021-02-01 | A  |",
            "| 10    | 2021-02-01 | B  |",
            "| 100   | 2021-02-02 | D  |",
            "| 20    | 2021-02-02 | C  |",
            "+-------+------------+----+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }
}
