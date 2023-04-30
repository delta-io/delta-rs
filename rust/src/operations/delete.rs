//! Delete records from a Delta Table that statisfy a predicate
//!
//! When a predicate is not provided then all records are deleted from the Delta
//! Table. Otherwise a scan of the Delta table is performed to mark any files
//! that contain records that satisfy the predicate. Once files are determined
//! they are rewritten without the records.
//!
//!
//! Predicates MUST be deterministic otherwise undefined behaviour may occur during the
//! scanning and rewriting phase.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = DeleteBuilder::new(table.object_store(), table.state)
//!     .with_predicate(col("col1").eq(lit(1)))
//!     .await?;
//! ````

use crate::action::DeltaOperation;
use crate::delta::DeltaResult;
use crate::delta_datafusion::logical_expr_to_physical_expr;
use crate::delta_datafusion::parquet_scan_from_actions;
use crate::delta_datafusion::partitioned_file_from_action;
use crate::delta_datafusion::register_store;
use crate::operations::transaction::commit;
use crate::operations::write::write_execution_plan;
use crate::storage::DeltaObjectStore;
use crate::storage::ObjectStoreRef;
use crate::table_state::DeltaTableState;
use crate::DeltaDataTypeVersion;
use crate::DeltaTable;
use crate::DeltaTableError;

use crate::action::{Action, Add, Remove};
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;

use arrow::array::BooleanBuilder;
use arrow::array::Float32Builder;
use arrow::array::Float64Builder;
use arrow::array::Int16Builder;
use arrow::array::Int32Builder;
use arrow::array::Int64Builder;
use arrow::array::Int8Builder;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::array::UInt16Builder;
use arrow::array::UInt32Builder;
use arrow::array::UInt64Builder;
use arrow::array::UInt8Builder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::{parquet::ParquetFormat, FileFormat};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::file_format::{wrap_partition_type_in_dict, FileScanConfig};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::prelude::Expr;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::tree_node::TreeNodeVisitor;
use datafusion_common::tree_node::VisitRecursion;
use datafusion_common::DFSchema;
use datafusion_expr::col;
use datafusion_expr::Volatility;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use parquet::file::properties::WriterProperties;
use serde_json::Map;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const PATH_COLUMN: &str = "__delta_rs_path";

/// Delete Records from the Delta Table.
/// See this module's documentaiton for more information
pub struct DeleteBuilder {
    /// Which records to delete
    predicate: Option<Expr>,
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    store: Arc<DeltaObjectStore>,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<Map<String, serde_json::Value>>,
}

#[derive(Default, Debug)]
/// Metrics for the Delete Operation
pub struct DeleteMetrics {
    /// Number of files added
    pub num_added_files: usize,
    /// Number of files removed
    pub num_removed_files: usize,
    /// Number of rows removed
    pub num_deleted_rows: Option<usize>,
    /// Number of rows copied in the process of deleting files
    pub num_copied_rows: Option<usize>,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u128,
    /// Time taken to scan the file for matches
    pub scan_time_ms: u128,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u128,
}

/// Determine which files contain a record that statisfies the predicate
async fn find_files<'a>(
    snapshot: &DeltaTableState,
    store: ObjectStoreRef,
    schema: &ArrowSchema,
    file_schema: Arc<ArrowSchema>,
    candidates: Vec<&'a Add>,
    state: &SessionState,
    expression: &Expr,
) -> DeltaResult<Vec<&'a Add>> {
    let mut files = Vec::new();
    let mut candidate_map: HashMap<String, &'a Add> = HashMap::new();

    let table_partition_cols = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)?
        .partition_columns
        .clone();

    let mut file_groups: HashMap<Vec<ScalarValue>, Vec<PartitionedFile>> = HashMap::new();
    for action in candidates {
        let mut part = partitioned_file_from_action(action, schema);
        part.partition_values
            .push(ScalarValue::Utf8(Some(action.path.clone())));

        file_groups
            .entry(part.partition_values.clone())
            .or_default()
            .push(part);

        candidate_map.insert(action.path.to_owned(), action);
    }

    let mut table_partition_cols = table_partition_cols
        .iter()
        .map(|c| {
            Ok((
                c.to_owned(),
                wrap_partition_type_in_dict(schema.field_with_name(c)?.data_type().clone()),
            ))
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    // Append a column called __delta_rs_path to track the file path
    table_partition_cols.push((PATH_COLUMN.to_owned(), DataType::Utf8));

    let physical_schema = file_schema.clone();
    let logical_schema: DFSchema = file_schema.as_ref().clone().try_into()?;
    let execution_props = ExecutionProps::new();

    let predicate_expr = create_physical_expr(
        expression,
        &logical_schema,
        &physical_schema,
        &execution_props,
    )?;
    let parquet_scan = ParquetFormat::new()
        .create_physical_plan(
            state,
            FileScanConfig {
                object_store_url: store.object_store_url(),
                file_schema,
                file_groups: file_groups.into_values().collect(),
                statistics: snapshot.datafusion_table_statistics(),
                projection: None,
                limit: None,
                table_partition_cols,
                infinite_source: false,
                output_ordering: None,
            },
            None,
        )
        .await?;

    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, parquet_scan.clone())?);
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));

    let task_ctx = Arc::new(TaskContext::from(state));
    let partitions = limit.output_partitioning().partition_count();
    let mut tasks = Vec::with_capacity(partitions);

    for i in 0..partitions {
        let stream = limit.execute(i, task_ctx.clone())?;
        tasks.push(handle_stream(stream));
    }

    for res in futures::future::join_all(tasks).await.into_iter() {
        let path = res?;
        if let Some(path) = path {
            match candidate_map.remove(&path) {
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

async fn handle_stream(
    mut stream: Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Option<String>, DeltaTableError> {
    if let Some(maybe_batch) = stream.next().await {
        let batch: RecordBatch = maybe_batch?;
        if batch.num_rows() > 1 {
            return Err(DeltaTableError::Generic(
                "Find files returned multiple records for batch".to_owned(),
            ));
        }
        let array = batch
            .column_by_name(PATH_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::Generic(format!(
                "Unable to downcast column {}",
                PATH_COLUMN
            )))?;

        let path = array
            .into_iter()
            .next()
            .unwrap()
            .ok_or(DeltaTableError::Generic(format!(
                "{} cannot be null",
                PATH_COLUMN
            )))?;
        return Ok(Some(path.to_string()));
    }

    Ok(None)
}

struct ExprProperties {
    partition_columns: Vec<String>,

    partition_only: bool,
    volaility: Volatility,
    result: DeltaResult<()>,
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor for ExprProperties {
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
            | Expr::Alias(_, _)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::ILike(_)
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
            Expr::ScalarFunction { fun, .. } => {
                self.volaility = self.volaility.max(fun.volatility());
                if self.volaility > Volatility::Immutable {
                    self.result = Err(DeltaTableError::Generic(format!(
                        "Delete predicate contains nondeterministic function {}",
                        fun
                    )));
                    return Ok(VisitRecursion::Stop);
                }
            }
            Expr::ScalarUDF { fun, .. } => {
                self.volaility = self.volaility.max(fun.signature.volatility);
                if self.volaility > Volatility::Immutable {
                    self.result = Err(DeltaTableError::Generic(format!(
                        "Delete predicate contains nondeterministic function {}",
                        fun.name
                    )));
                    return Ok(VisitRecursion::Stop);
                }
            }
            _ => {
                self.result = Err(DeltaTableError::Generic(format!(
                    "Delete predicate contains unsupported expression {}",
                    expr
                )));
                return Ok(VisitRecursion::Stop);
            }
        }

        Ok(VisitRecursion::Continue)
    }
}

impl DeleteBuilder {
    /// Create a new [`DeleteBuilder`]
    pub fn new(object_store: ObjectStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            predicate: None,
            snapshot,
            store: object_store,
            state: None,
            app_metadata: None,
            writer_properties: None,
        }
    }

    /// A predicate that determines if a record is deleted
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Parse the provided query into a Datafusion expression
    pub fn with_str_predicate(
        mut self,
        predicate: impl AsRef<str>,
    ) -> Result<Self, DeltaTableError> {
        let expr = self.snapshot.parse_predicate_expression(predicate)?;
        self.predicate = Some(expr);

        Ok(self)
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = Some(Map::from_iter(metadata));
        self
    }

    /// Writer properties passed to parquet writer for when fiiles are rewritten
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }
}

async fn excute_non_empty_expr(
    snapshot: &DeltaTableState,
    object_store: ObjectStoreRef,
    state: &SessionState,
    expression: &Expr,
    metrics: &mut DeleteMetrics,
    writer_properties: Option<WriterProperties>,
) -> DeltaResult<(Vec<Add>, Vec<Add>)> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.

    let schema = snapshot.arrow_schema().unwrap();
    let scan_start = Instant::now();

    let table_partition_cols = snapshot
        .current_metadata()
        .ok_or(DeltaTableError::NoMetadata)
        .unwrap()
        .partition_columns
        .clone();
    let file_schema = Arc::new(ArrowSchema::new(
        schema
            .fields()
            .iter()
            .filter(|f| !table_partition_cols.contains(f.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ));
    let expr = logical_expr_to_physical_expr(expression, &schema);

    let pruning_predicate = PruningPredicate::try_new(expr, schema.clone())?;
    let files_to_prune = pruning_predicate.prune(snapshot).unwrap();
    let files: Vec<&Add> = snapshot
        .files()
        .iter()
        .zip(files_to_prune.into_iter())
        .filter_map(|(action, keep)| if keep { Some(action) } else { None })
        .collect();

    // Create a new delta scan plan with only files that have a record
    let rewrite = find_files(
        snapshot,
        object_store.clone(),
        &schema,
        file_schema.clone(),
        files,
        state,
        expression,
    )
    .await?;

    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_millis();

    let rewrite: Vec<Add> = rewrite.into_iter().map(|s| s.to_owned()).collect();
    let parquet_scan = parquet_scan_from_actions(
        snapshot,
        object_store.clone(),
        &rewrite,
        &schema,
        None,
        state,
        None,
        None,
    )
    .await?;

    // Apply the negation of the filter and rewrite files
    let negated_expression = Expr::Not(Box::new(expression.clone()));
    let physical_schema = file_schema.clone();
    let logical_schema: DFSchema = file_schema.as_ref().clone().try_into()?;
    let execution_props = ExecutionProps::new();

    let predicate_expr = create_physical_expr(
        &negated_expression,
        &logical_schema,
        &physical_schema,
        &execution_props,
    )?;
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, parquet_scan.clone())?);

    let write_start = Instant::now();

    let add_actions = write_execution_plan(
        snapshot,
        state.clone(),
        filter.clone(),
        table_partition_cols.clone(),
        object_store.clone(),
        None,
        None,
        writer_properties,
    )
    .await?;
    metrics.rewrite_time_ms = Instant::now().duration_since(write_start).as_millis();

    let read_records = parquet_scan.metrics().unwrap().output_rows().unwrap();
    let filter_records = filter.metrics().unwrap().output_rows().unwrap();
    metrics.num_copied_rows = Some(filter_records);
    metrics.num_deleted_rows = Some(read_records - filter_records);

    Ok((add_actions, rewrite))
}

async fn execute(
    predicate: Option<Expr>,
    object_store: ObjectStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<Map<String, Value>>,
) -> DeltaResult<((Vec<Action>, DeltaDataTypeVersion), DeleteMetrics)> {
    let mut metrics = DeleteMetrics::default();
    let exec_start = Instant::now();

    let (add_actions, to_delete) = match &predicate {
        Some(expr) => {
            let current_metadata = snapshot
                .current_metadata()
                .ok_or(DeltaTableError::NoMetadata)?;

            let mut expr_properties = ExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns.clone(),
                volaility: Volatility::Immutable,
                result: Ok(()),
            };

            TreeNode::visit(expr, &mut expr_properties)?;
            expr_properties.result?;

            if expr_properties.partition_only {
                let scan_start = Instant::now();
                let remove = scan_memory_table(snapshot, expr).await?;
                metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_micros();
                (Vec::new(), remove)
            } else {
                excute_non_empty_expr(
                    snapshot,
                    object_store.clone(),
                    &state,
                    expr,
                    &mut metrics,
                    writer_properties,
                )
                .await?
            }
        }
        None => (Vec::<Add>::new(), snapshot.files().to_owned()),
    };

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut actions: Vec<Action> = add_actions.into_iter().map(Action::add).collect();
    let mut version = snapshot.version();
    metrics.num_removed_files = to_delete.len();
    metrics.num_added_files = actions.len();

    for action in to_delete {
        actions.push(Action::remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            tags: None,
        }))
    }

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_micros();

    // Do not make a commit when there are zero updates to the state
    if !actions.is_empty() {
        let operation = DeltaOperation::Delete {
            predicate: Some(predicate.canonical_name()),
        };
        version = commit(
            object_store.as_ref(),
            &actions,
            operation,
            snapshot,
            app_metadata,
        )
        .await?;
    }

    Ok(((actions, version), metrics))
}

async fn scan_memory_table(snapshot: &DeltaTableState, predicate: &Expr) -> DeltaResult<Vec<Add>> {
    let actions = snapshot.files().to_owned();

    let batch = create_partition_record_batch(snapshot, &actions)?;
    let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;

    let ctx = SessionContext::new();
    let mut df = ctx.read_table(Arc::new(mem_table))?;
    df = df
        .filter(predicate.to_owned())?
        .select(vec![col(PATH_COLUMN)])?;
    let batches = df.collect().await?;

    let mut map = HashMap::new();
    for action in actions {
        map.insert(action.path.clone(), action);
    }
    let mut files = Vec::new();

    for batch in batches {
        let array = batch
            .column_by_name(PATH_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DeltaTableError::Generic(format!(
                "Unable to downcast column {}",
                PATH_COLUMN
            )))?;
        for path in array {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{} cannot be null",
                PATH_COLUMN
            )))?;
            let value = map.remove(path).unwrap();
            files.push(value);
        }
    }

    Ok(files)
}

macro_rules! append_builder {
    ($builder:expr, $value:expr, $ARROW_BUILDER_TYPE:ident, $RUST_TYPE:ident) => {{
        let array = $builder
            .as_any_mut()
            .downcast_mut::<$ARROW_BUILDER_TYPE>()
            .unwrap();
        let value = match $value {
            Some(s) => {
                Some(
                    $RUST_TYPE::from_str(&s).map_err(|e| DeltaTableError::GenericError {
                        source: Box::new(e),
                    })?,
                )
            }
            None => None,
        };
        array.append_option(value);
    }};
}

fn append_option_str(
    builder: &mut Box<dyn ArrayBuilder>,
    value: Option<String>,
    t: DataType,
) -> DeltaResult<()> {
    match t {
        DataType::Boolean => append_builder!(builder, value, BooleanBuilder, bool),
        DataType::Int8 => append_builder!(builder, value, Int8Builder, i8),
        DataType::Int16 => append_builder!(builder, value, Int16Builder, i16),
        DataType::Int32 => append_builder!(builder, value, Int32Builder, i32),
        DataType::Int64 => append_builder!(builder, value, Int64Builder, i64),
        DataType::UInt8 => append_builder!(builder, value, UInt8Builder, u8),
        DataType::UInt16 => append_builder!(builder, value, UInt16Builder, u16),
        DataType::UInt32 => append_builder!(builder, value, UInt32Builder, u32),
        DataType::UInt64 => append_builder!(builder, value, UInt64Builder, u64),
        DataType::Float32 => append_builder!(builder, value, Float32Builder, f32),
        DataType::Float64 => append_builder!(builder, value, Float64Builder, f64),
        DataType::Utf8 => {
            let array = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            array.append_option(value);
        }
        _ => {
            return Err(DeltaTableError::Generic(format!(
                "Unable to convert value to arrow type {}",
                t
            )))
        }
    }

    Ok(())
}

fn new_array_builder(t: DataType, capacity: usize) -> DeltaResult<Box<dyn ArrayBuilder>> {
    Ok(match t {
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 1024)),
        _ => {
            return Err(DeltaTableError::Generic(format!(
                "Unable to create builder for arrow type {}",
                t
            )))
        }
    })
}

fn get_inner_type(t: DataType) -> DeltaResult<DataType> {
    match t {
        DataType::Dictionary(_, value) => Ok(value.as_ref().to_owned()),
        _ => Err(DeltaTableError::Generic(format!(
            "Unable to create builder for arrow type {}",
            t
        ))),
    }
}

// Create a record batch that contains the partition columns plus the path of the file
fn create_partition_record_batch(
    snapshot: &DeltaTableState,
    actions: &Vec<Add>,
) -> DeltaResult<RecordBatch> {
    let partition_columns = &snapshot.current_metadata().unwrap().partition_columns;
    let arrow_schema = snapshot.arrow_schema().unwrap();
    let cap = actions.len();
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
    for partition in partition_columns {
        let t = arrow_schema
            .field_with_name(partition)
            .unwrap()
            .data_type()
            .to_owned();
        let t = get_inner_type(t)?;
        let builder = new_array_builder(t, cap)?;
        builders.push(builder);
    }
    let mut path_builder = StringBuilder::with_capacity(cap, 1024);

    for action in actions {
        let partition_values = &action.partition_values;
        for (i, column) in partition_columns.iter().enumerate() {
            let value = partition_values.get(column);
            let builder = builders.get_mut(i).unwrap();
            let t = arrow_schema
                .field_with_name(column)
                .unwrap()
                .data_type()
                .to_owned();

            // Partitions values are wrapped in a dictionary

            let value = match value {
                Some(v) => v.to_owned(),
                None => None,
            };
            let t = get_inner_type(t)?;
            append_option_str(builder, value, t)?;
        }
        path_builder.append_value(action.path.clone());
    }

    let mut arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut array| Arc::new(array.finish()) as ArrayRef)
        .collect();
    arrays.push(Arc::new(path_builder.finish()));

    let mut fields = Vec::new();
    for partition in partition_columns {
        let column = arrow_schema.field_with_name(partition).unwrap();

        let t = column.data_type().to_owned();
        let t = get_inner_type(t)?;
        let field = Field::new(partition, t, column.is_nullable());
        fields.push(field);
    }
    fields.push(Field::new(PATH_COLUMN, DataType::Utf8, false));

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        arrays,
    )?)
}

impl std::future::IntoFuture for DeleteBuilder {
    type Output = DeltaResult<(DeltaTable, DeleteMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            let state = this.state.unwrap_or_else(|| {
                let session = SessionContext::new();

                // If a user provides their own their DF state then they must register the store themselves
                register_store(this.store.clone(), session.runtime_env());

                session.state()
            });

            let ((actions, version), metrics) = execute(
                this.predicate,
                this.store.clone(),
                &this.snapshot,
                state,
                this.writer_properties,
                this.app_metadata,
            )
            .await?;

            this.snapshot
                .merge(DeltaTableState::from_actions(actions, version)?, true, true);
            let table = DeltaTable::new_with_state(this.store, this.snapshot);

            Ok((table, metrics))
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::action::*;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::{get_arrow_schema, get_delta_schema};
    use crate::DeltaTable;
    use arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::*;
    use std::sync::Arc;

    async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.get_fields().clone())
            .with_partition_columns(partitions.unwrap_or_default())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        table
    }

    async fn get_data(table: DeltaTable) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();
        ctx.sql("select * from test")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_delete_default() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();
        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);

        let (table, metrics) = DeltaOps(table).delete().await.unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);

        // Scan and rewrite is not required
        assert_eq!(metrics.scan_time_ms, 0);
        assert_eq!(metrics.rewrite_time_ms, 0);

        // Deletes with no changes to state must not commit
        let (table, metrics) = DeltaOps(table).delete().await.unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);
    }

    #[tokio::test]
    async fn test_delete_on_nonpartiton_column() {
        // Delete based on a nonpartition column
        // Only rewrite files that match the predicate
        // Test data designed to force a scan of the underlying data

        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 1);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(lit(1)))
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_file_uris().count(), 2);

        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert!(metrics.scan_time_ms > 0);
        assert_eq!(metrics.num_deleted_rows, Some(1));
        assert_eq!(metrics.num_copied_rows, Some(3));

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| B  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_on_partition_column() {
        // Perform a delete where the predicate only contains partition columns

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from_slice(["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from_slice([0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from_slice([
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_file_uris().count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_file_uris().count(), 1);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);
        assert!(metrics.scan_time_ms > 0);
        assert_eq!(metrics.rewrite_time_ms, 0);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_failure_nondeterministic_query() {
        // Deletion requires a deterministic predicate

        let table = setup_table(None).await;

        let res = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(cast(
                random() * lit(20.0),
                arrow::datatypes::DataType::Int32,
            )))
            .await;
        assert!(res.is_err());
    }
}
