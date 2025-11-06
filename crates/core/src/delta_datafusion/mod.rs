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
//!   let table = deltalake_core::open_table_with_storage_options(
//!       url::Url::parse("memory://").unwrap(),
//!       std::collections::HashMap::new()
//!   )
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

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::types::UInt16Type;
use arrow::array::{Array, DictionaryArray, RecordBatch, StringArray, TypedDictionaryArray};
use arrow_cast::display::array_value_to_string;
use arrow_cast::{cast_with_options, CastOptions};
use arrow_schema::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
    SchemaRef as ArrowSchemaRef, TimeUnit,
};
use datafusion::catalog::{Session, TableProviderFactory};
use datafusion::common::scalar::ScalarValue;
use datafusion::common::{
    Column, DFSchema, DataFusionError, Result as DataFusionResult, TableReference, ToDFSchema,
};
use datafusion::datasource::physical_plan::wrap_partition_type_in_dict;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::logical_plan::CreateExternalTable;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use either::Either;
use itertools::Itertools;
use url::Url;

use crate::delta_datafusion::expr::parse_predicate_expression;
use crate::delta_datafusion::table_provider::DeltaScanWire;
use crate::ensure_table_uri;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{
    Add, DataCheck, EagerSnapshot, Invariant, LogDataHandler, Snapshot, StructTypeExt,
};
use crate::logstore::{LogStore, LogStoreRef};
use crate::table::config::TablePropertiesExt as _;
use crate::table::state::DeltaTableState;
use crate::table::{Constraint, GeneratedColumn};
use crate::{open_table, open_table_with_storage_options, DeltaTable};

pub use self::session::*;
pub(crate) use find_files::*;

pub(crate) const PATH_COLUMN: &str = "__delta_rs_path";

pub mod cdf;
pub mod engine;
pub mod expr;
mod find_files;
pub mod logical;
pub mod physical;
pub mod planner;
mod schema_adapter;
mod session;
mod table_provider;

pub use cdf::scan::DeltaCdfTableProvider;
pub(crate) use table_provider::DeltaScanBuilder;
pub use table_provider::{DeltaScan, DeltaScanConfig, DeltaScanConfigBuilder, DeltaTableProvider};

impl From<DeltaTableError> for DataFusionError {
    fn from(err: DeltaTableError) -> Self {
        match err {
            DeltaTableError::Arrow { source } => DataFusionError::from(source),
            DeltaTableError::Io { source } => DataFusionError::IoError(source),
            DeltaTableError::ObjectStore { source } => DataFusionError::from(source),
            DeltaTableError::Parquet { source } => DataFusionError::from(source),
            _ => DataFusionError::External(Box::new(err)),
        }
    }
}

impl From<DataFusionError> for DeltaTableError {
    fn from(err: DataFusionError) -> Self {
        match err {
            DataFusionError::ArrowError(source, _) => DeltaTableError::from(*source),
            DataFusionError::IoError(source) => DeltaTableError::Io { source },
            DataFusionError::ObjectStore(source) => DeltaTableError::from(*source),
            DataFusionError::ParquetError(source) => DeltaTableError::from(*source),
            _ => DeltaTableError::Generic(err.to_string()),
        }
    }
}

/// Convenience trait for calling common methods on snapshot hierarchies
pub trait DataFusionMixins {
    /// The physical datafusion schema of a table
    fn read_schema(&self) -> ArrowSchemaRef;

    /// Get the table schema as an [`ArrowSchemaRef`]
    fn input_schema(&self) -> ArrowSchemaRef;

    /// Parse an expression string into a datafusion [`Expr`]
    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        session: &dyn Session,
    ) -> DeltaResult<Expr>;
}

impl DataFusionMixins for Snapshot {
    fn read_schema(&self) -> ArrowSchemaRef {
        _arrow_schema(
            self.arrow_schema(),
            self.metadata().partition_columns(),
            true,
        )
    }

    fn input_schema(&self) -> ArrowSchemaRef {
        _arrow_schema(
            self.arrow_schema(),
            self.metadata().partition_columns(),
            false,
        )
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        session: &dyn Session,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.read_schema().as_ref().to_owned())?;
        parse_predicate_expression(&schema, expr, session)
    }
}

impl DataFusionMixins for LogDataHandler<'_> {
    fn read_schema(&self) -> ArrowSchemaRef {
        _arrow_schema(
            Arc::new(
                self.table_configuration()
                    .schema()
                    .as_ref()
                    .try_into_arrow()
                    .unwrap(),
            ),
            self.table_configuration().metadata().partition_columns(),
            true,
        )
    }

    fn input_schema(&self) -> ArrowSchemaRef {
        _arrow_schema(
            Arc::new(
                self.table_configuration()
                    .schema()
                    .as_ref()
                    .try_into_arrow()
                    .unwrap(),
            ),
            self.table_configuration().metadata().partition_columns(),
            false,
        )
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        session: &dyn Session,
    ) -> DeltaResult<Expr> {
        let schema = DFSchema::try_from(self.read_schema().as_ref().to_owned())?;
        parse_predicate_expression(&schema, expr, session)
    }
}

impl DataFusionMixins for EagerSnapshot {
    fn read_schema(&self) -> ArrowSchemaRef {
        self.snapshot().read_schema()
    }

    fn input_schema(&self) -> ArrowSchemaRef {
        self.snapshot().input_schema()
    }

    fn parse_predicate_expression(
        &self,
        expr: impl AsRef<str>,
        session: &dyn Session,
    ) -> DeltaResult<Expr> {
        self.snapshot().parse_predicate_expression(expr, session)
    }
}

fn _arrow_schema(
    schema: SchemaRef,
    partition_columns: &[String],
    wrap_partitions: bool,
) -> ArrowSchemaRef {
    let fields = schema
        .fields()
        .into_iter()
        .filter(|f| !partition_columns.contains(&f.name().to_string()))
        .cloned()
        .chain(
            // We need stable order between logical and physical schemas, but the order of
            // partitioning columns is not always the same in the json schema and the array
            partition_columns.iter().map(|partition_col| {
                let field = schema.field_with_name(partition_col).unwrap();
                let corrected = if wrap_partitions {
                    match field.data_type() {
                        // Only dictionary-encode types that may be large
                        // // https://github.com/apache/arrow-datafusion/pull/5545
                        ArrowDataType::Utf8
                        | ArrowDataType::LargeUtf8
                        | ArrowDataType::Binary
                        | ArrowDataType::LargeBinary => {
                            wrap_partition_type_in_dict(field.data_type().clone())
                        }
                        _ => field.data_type().clone(),
                    }
                } else {
                    field.data_type().clone()
                };
                Arc::new(field.clone().with_data_type(corrected))
            }),
        )
        .collect::<Vec<_>>();
    Arc::new(ArrowSchema::new(fields))
}

pub(crate) fn files_matching_predicate<'a>(
    log_data: LogDataHandler<'a>,
    filters: &[Expr],
) -> DeltaResult<impl Iterator<Item = Add> + 'a> {
    if let Some(Some(predicate)) =
        (!filters.is_empty()).then_some(conjunction(filters.iter().cloned()))
    {
        let expr = SessionContext::new()
            .create_physical_expr(predicate, &log_data.read_schema().to_dfschema()?)?;
        let pruning_predicate = PruningPredicate::try_new(expr, log_data.read_schema())?;
        let mask = pruning_predicate.prune(&log_data)?;

        Ok(Either::Left(log_data.into_iter().zip(mask).filter_map(
            |(file, keep_file)| {
                if keep_file {
                    Some(file.add_action())
                } else {
                    None
                }
            },
        )))
    } else {
        Ok(Either::Right(
            log_data.into_iter().map(|file| file.add_action()),
        ))
    }
}

pub(crate) fn get_path_column<'a>(
    batch: &'a RecordBatch,
    path_column: &str,
) -> DeltaResult<TypedDictionaryArray<'a, UInt16Type, StringArray>> {
    let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());
    batch
        .column_by_name(path_column)
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<UInt16Type>>()
        .ok_or_else(err)?
        .downcast_dict::<StringArray>()
        .ok_or_else(err)
}

impl DeltaTableState {
    /// Provide table level statistics to Datafusion
    pub fn datafusion_table_statistics(&self) -> Option<Statistics> {
        self.snapshot.log_data().statistics()
    }
}

// each delta table must register a specific object store, since paths are internally
// handled relative to the table root.
pub(crate) fn register_store(store: LogStoreRef, env: &RuntimeEnv) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store.object_store(None));
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
pub(crate) fn df_logical_schema(
    snapshot: &EagerSnapshot,
    file_column_name: &Option<String>,
    schema: Option<ArrowSchemaRef>,
) -> DeltaResult<SchemaRef> {
    let input_schema = match schema {
        Some(schema) => schema,
        None => snapshot.input_schema(),
    };
    let table_partition_cols = snapshot.metadata().partition_columns();

    let mut fields: Vec<Arc<Field>> = input_schema
        .fields()
        .iter()
        .filter(|f| !table_partition_cols.contains(f.name()))
        .cloned()
        .collect();

    for partition_col in table_partition_cols.iter() {
        fields.push(Arc::new(
            input_schema
                .field_with_name(partition_col)
                .unwrap()
                .to_owned(),
        ));
    }

    if let Some(file_column_name) = file_column_name {
        fields.push(Arc::new(Field::new(
            file_column_name,
            ArrowDataType::Utf8,
            true,
        )));
    }

    Ok(Arc::new(ArrowSchema::new(fields)))
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
            Box::new(get_null_of_arrow_type(v)?),
        )),
        //Unsupported types...
        ArrowDataType::Float16
        | ArrowDataType::Decimal32(_, _)
        | ArrowDataType::Decimal64(_, _)
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
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View
        | ArrowDataType::LargeListView(_)
        | ArrowDataType::ListView(_)
        | ArrowDataType::Map(_, _) => Err(DeltaTableError::Generic(format!(
            "Unsupported data type for Delta Lake {t}"
        ))),
    }
}

fn parse_date(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(string, &ArrowDataType::Date32)?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

fn parse_timestamp(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<ScalarValue> {
    let string = match stat_val {
        serde_json::Value::String(s) => s.to_owned(),
        _ => stat_val.to_string(),
    };

    let time_micro = ScalarValue::try_from_string(
        string,
        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    let cast_arr = cast_with_options(
        &time_micro.to_array()?,
        field_dt,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    ScalarValue::try_from_array(&cast_arr, 0)
}

pub(crate) fn to_correct_scalar_value(
    stat_val: &serde_json::Value,
    field_dt: &ArrowDataType,
) -> DataFusionResult<Option<ScalarValue>> {
    match stat_val {
        serde_json::Value::Array(_) => Ok(None),
        serde_json::Value::Object(_) => Ok(None),
        serde_json::Value::Null => Ok(Some(get_null_of_arrow_type(field_dt)?)),
        serde_json::Value::String(string_val) => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                string_val.to_owned(),
                field_dt,
            )?)),
        },
        other => match field_dt {
            ArrowDataType::Timestamp(_, _) => Ok(Some(parse_timestamp(stat_val, field_dt)?)),
            ArrowDataType::Date32 => Ok(Some(parse_date(stat_val, field_dt)?)),
            _ => Ok(Some(ScalarValue::try_from_string(
                other.to_string(),
                field_dt,
            )?)),
        },
    }
}

/// Responsible for checking batches of data conform to table's invariants, constraints and nullability.
#[derive(Clone, Default)]
pub struct DeltaDataChecker {
    constraints: Vec<Constraint>,
    invariants: Vec<Invariant>,
    generated_columns: Vec<GeneratedColumn>,
    non_nullable_columns: Vec<String>,
    ctx: SessionContext,
}

impl DeltaDataChecker {
    /// Create a new DeltaDataChecker with no invariants or constraints
    pub fn empty() -> Self {
        Self {
            invariants: vec![],
            constraints: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new DeltaDataChecker with a specified set of invariants
    pub fn new_with_invariants(invariants: Vec<Invariant>) -> Self {
        Self {
            invariants,
            constraints: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new DeltaDataChecker with a specified set of constraints
    pub fn new_with_constraints(constraints: Vec<Constraint>) -> Self {
        Self {
            constraints,
            invariants: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new DeltaDataChecker with a specified set of generated columns
    pub fn new_with_generated_columns(generated_columns: Vec<GeneratedColumn>) -> Self {
        Self {
            constraints: vec![],
            invariants: vec![],
            generated_columns,
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Specify the Datafusion context
    pub fn with_session_context(mut self, context: SessionContext) -> Self {
        self.ctx = context;
        self
    }

    /// Add the specified set of constraints to the current DeltaDataChecker's constraints
    pub fn with_extra_constraints(mut self, constraints: Vec<Constraint>) -> Self {
        self.constraints.extend(constraints);
        self
    }

    /// Create a new DeltaDataChecker
    pub fn new(snapshot: &EagerSnapshot) -> Self {
        let invariants = snapshot.schema().get_invariants().unwrap_or_default();
        let generated_columns = snapshot
            .schema()
            .get_generated_columns()
            .unwrap_or_default();
        let constraints = snapshot.table_properties().get_constraints();
        let non_nullable_columns = snapshot
            .schema()
            .fields()
            .filter_map(|f| {
                if !f.is_nullable() {
                    Some(f.name().clone())
                } else {
                    None
                }
            })
            .collect_vec();
        Self {
            invariants,
            constraints,
            generated_columns,
            non_nullable_columns,
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Check that a record batch conforms to table's invariants.
    ///
    /// If it does not, it will return [DeltaTableError::InvalidData] with a list
    /// of values that violated each invariant.
    pub async fn check_batch(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.check_nullability(record_batch)?;
        self.enforce_checks(record_batch, &self.invariants).await?;
        self.enforce_checks(record_batch, &self.constraints).await?;
        self.enforce_checks(record_batch, &self.generated_columns)
            .await
    }

    /// Return true if all the nullability checks are valid
    fn check_nullability(&self, record_batch: &RecordBatch) -> Result<bool, DeltaTableError> {
        let mut violations = Vec::with_capacity(self.non_nullable_columns.len());
        for col in self.non_nullable_columns.iter() {
            if let Some(arr) = record_batch.column_by_name(col) {
                if arr.null_count() > 0 {
                    violations.push(format!(
                        "Non-nullable column violation for {col}, found {} null values",
                        arr.null_count()
                    ));
                }
            } else {
                violations.push(format!(
                    "Non-nullable column violation for {col}, not found in batch!"
                ));
            }
        }
        if !violations.is_empty() {
            Err(DeltaTableError::InvalidData { violations })
        } else {
            Ok(true)
        }
    }

    async fn enforce_checks<C: DataCheck>(
        &self,
        record_batch: &RecordBatch,
        checks: &[C],
    ) -> Result<(), DeltaTableError> {
        if checks.is_empty() {
            return Ok(());
        }
        let table = MemTable::try_new(record_batch.schema(), vec![vec![record_batch.clone()]])?;
        table.schema();
        // Use a random table name to avoid clashes when running multiple parallel tasks, e.g. when using a partitioned table
        let table_name: String = uuid::Uuid::new_v4().to_string();
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let mut violations: Vec<String> = Vec::with_capacity(checks.len());

        for check in checks {
            if check.get_name().contains('.') {
                return Err(DeltaTableError::Generic(
                    "delta constraints for nested columns are not supported at the moment."
                        .to_string(),
                ));
            }

            let field_to_select = if check.as_any().is::<Constraint>() {
                "*"
            } else {
                check.get_name()
            };
            let sql = format!(
                "SELECT {} FROM `{table_name}` WHERE NOT ({}) LIMIT 1",
                field_to_select,
                check.get_expression()
            );

            let dfs: Vec<RecordBatch> = self.ctx.sql(&sql).await?.collect().await?;
            if !dfs.is_empty() && dfs[0].num_rows() > 0 {
                let value: String = dfs[0]
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c, 0).unwrap_or(String::from("null")))
                    .join(", ");

                let msg = format!(
                    "Check or Invariant ({}) violated by value in row: [{value}]",
                    check.get_expression(),
                );
                violations.push(msg);
            }
        }

        self.ctx.deregister_table(&table_name)?;
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
        _registry: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let wire: DeltaScanWire = serde_json::from_reader(buf)
            .map_err(|_| DataFusionError::Internal("Unable to decode DeltaScan".to_string()))?;
        let delta_scan = DeltaScan {
            table_uri: wire.table_uri,
            parquet_scan: (*inputs)[0].clone(),
            config: wire.config,
            logical_schema: wire.logical_schema,
            metrics: ExecutionPlanMetricsSet::new(),
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
        _ctx: &TaskContext,
    ) -> Result<Extension, DataFusionError> {
        todo!("DeltaLogicalCodec")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        todo!("DeltaLogicalCodec")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let provider: DeltaTable = serde_json::from_slice(buf)
            .map_err(|_| DataFusionError::Internal("Error encoding delta table".to_string()))?;
        Ok(Arc::new(provider))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
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
#[derive(Debug)]
pub struct DeltaTableFactory {}

#[async_trait::async_trait]
impl TableProviderFactory for DeltaTableFactory {
    async fn create(
        &self,
        _ctx: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let provider = if cmd.options.is_empty() {
            let table_url = ensure_table_uri(&cmd.to_owned().location)?;
            open_table(table_url).await?
        } else {
            let table_url = ensure_table_uri(&cmd.to_owned().location)?;
            open_table_with_storage_options(table_url, cmd.to_owned().options).await?
        };
        Ok(Arc::new(provider))
    }
}

/// A wrapper for Deltafusion's Column to preserve case-sensitivity during string conversion
pub struct DeltaColumn {
    inner: Column,
}

impl From<&str> for DeltaColumn {
    fn from(c: &str) -> Self {
        DeltaColumn {
            inner: Column::from_qualified_name_ignore_case(c),
        }
    }
}

/// Create a column, cloning the string
impl From<&String> for DeltaColumn {
    fn from(c: &String) -> Self {
        DeltaColumn {
            inner: Column::from_qualified_name_ignore_case(c),
        }
    }
}

/// Create a column, reusing the existing string
impl From<String> for DeltaColumn {
    fn from(c: String) -> Self {
        DeltaColumn {
            inner: Column::from_qualified_name_ignore_case(c),
        }
    }
}

impl From<DeltaColumn> for Column {
    fn from(value: DeltaColumn) -> Self {
        value.inner
    }
}

/// Create a column, reusing the existing datafusion column
impl From<Column> for DeltaColumn {
    fn from(c: Column) -> Self {
        DeltaColumn { inner: c }
    }
}

#[cfg(test)]
mod tests {
    use crate::logstore::default_logstore::DefaultLogStore;
    use crate::logstore::ObjectStoreRef;
    use crate::operations::write::SchemaMode;
    use crate::writer::test_utils::get_delta_schema;
    use arrow::array::StructArray;
    use arrow::datatypes::{Field, Schema};
    use arrow_array::cast::AsArray;
    use bytes::Bytes;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::config::TableParquetOptions;
    use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::logical_expr::lit;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{visit_execution_plan, ExecutionPlanVisitor, PhysicalExpr};
    use datafusion::prelude::{col, SessionConfig};
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf;
    use delta_kernel::path::{LogPathFileType, ParsedLogPath};
    use delta_kernel::schema::ArrayType;
    use futures::{stream::BoxStream, StreamExt};
    use object_store::ObjectMeta;
    use object_store::{
        path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use serde_json::json;
    use std::fmt::{self, Debug, Display, Formatter};
    use std::ops::{Deref, Range};
    use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
            let scalar = to_correct_scalar_value(raw, data_type).unwrap().unwrap();
            assert_eq!(*ref_scalar, scalar)
        }
    }

    #[tokio::test]
    async fn test_enforce_invariants() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b", ArrowDataType::Int32, false),
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
        assert!(DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await
            .is_ok());

        // Valid invariants return Ok(())
        let invariants = vec![
            Invariant::new("a", "a is not null"),
            Invariant::new("b", "b < 1000"),
        ];
        assert!(DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let invariants = vec![
            Invariant::new("a", "a is null"),
            Invariant::new("b", "b < 100"),
        ];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant invariants return a different error
        let invariants = vec![Invariant::new("c", "c > 2000")];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());

        // Nested invariants are unsupported
        let struct_fields = schema.fields().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            ArrowDataType::Struct(struct_fields),
            false,
        )]));
        let inner = Arc::new(StructArray::from(batch));
        let batch = RecordBatch::try_new(schema, vec![inner]).unwrap();

        let invariants = vec![Invariant::new("x.b", "x.b < 1000")];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::Generic { .. })));
    }

    #[tokio::test]
    async fn test_enforce_constraints() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )
        .unwrap();
        // Empty constraints is okay
        let constraints: Vec<Constraint> = vec![];
        assert!(DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await
            .is_ok());

        // Valid invariants return Ok(())
        let constraints = vec![
            Constraint::new("custom_a", "a is not null"),
            Constraint::new("custom_b", "b < 1000"),
        ];
        assert!(DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let constraints = vec![
            Constraint::new("custom_a", "a is null"),
            Constraint::new("custom_B", "b < 100"),
        ];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant constraints return a different error
        let constraints = vec![Constraint::new("custom_c", "c > 2000")];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
    }

    /// Ensure that constraints when there are spaces in the field name still work
    ///
    /// See <https://github.com/delta-io/delta-rs/pull/3374>
    #[tokio::test]
    async fn test_constraints_with_spacey_fields() -> DeltaResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b bop", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b bop", "c", "d",
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        // Valid invariants return Ok(())
        let constraints = vec![
            Constraint::new("custom a", "a is not null"),
            Constraint::new("custom_b", "`b bop` < 1000"),
        ];
        assert!(DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await
            .is_ok());

        // Violated invariants returns an error with list of violations
        let constraints = vec![
            Constraint::new("custom_a", "a is null"),
            Constraint::new("custom_B", "\"b bop\" < 100"),
        ];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant constraints return a different error
        let constraints = vec![Constraint::new("custom_c", "c > 2000")];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn roundtrip_test_delta_exec_plan() {
        let ctx = SessionContext::new();
        let codec = DeltaPhysicalCodec {};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", ArrowDataType::Utf8, false),
            Field::new("b", ArrowDataType::Int32, false),
        ]));
        let exec_plan = Arc::from(DeltaScan {
            table_uri: "s3://my_bucket/this/is/some/path".to_string(),
            parquet_scan: Arc::from(EmptyExec::new(schema.clone())),
            config: DeltaScanConfig::default(),
            logical_schema: schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        });
        let proto: protobuf::PhysicalPlanNode =
            protobuf::PhysicalPlanNode::try_from_physical_plan(exec_plan.clone(), &codec)
                .expect("to proto");

        let task_ctx = ctx.task_ctx();
        let result_exec_plan: Arc<dyn ExecutionPlan> = proto
            .try_into_physical_plan(&task_ctx, &codec)
            .expect("from proto");
        assert_eq!(format!("{exec_plan:?}"), format!("{result_exec_plan:?}"));
    }

    #[tokio::test]
    async fn delta_table_provider_with_config() {
        let table_path = std::path::Path::new("../test/tests/data/delta-2.2.0-partitioned-types")
            .canonicalize()
            .unwrap();
        let table_url = url::Url::from_directory_path(table_path).unwrap();
        let table = crate::open_table(table_url).await.unwrap();
        let config = DeltaScanConfigBuilder::new()
            .with_file_column_name(&"file_source")
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();

        let log_store = table.log_store();
        let provider = DeltaTableProvider::try_new(
            table.snapshot().unwrap().snapshot().clone(),
            log_store,
            config,
        )
        .unwrap();
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
            Field::new("modified", ArrowDataType::Utf8, true),
            Field::new("id", ArrowDataType::Utf8, true),
            Field::new("value", ArrowDataType::Int32, true),
        ]));

        let table = crate::DeltaOps::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified", "id"])
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));

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

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();

        let log_store = table.log_store();
        let provider = DeltaTableProvider::try_new(
            table.snapshot().unwrap().snapshot().clone(),
            log_store,
            config,
        )
        .unwrap();
        let logical_schema = provider.schema();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let expected_logical_order = vec!["value", "modified", "id"];
        let actual_order: Vec<String> = logical_schema
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();

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
        assert_eq!(expected_logical_order, actual_order);
    }

    #[tokio::test]
    async fn delta_scan_case_sensitive() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("moDified", ArrowDataType::Utf8, true),
            Field::new("ID", ArrowDataType::Utf8, true),
            Field::new("vaLue", ArrowDataType::Int32, true),
        ]));

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
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().snapshot().clone(), log, config)
                .unwrap();
        let ctx: SessionContext = DeltaSessionContext::default().into();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("select ID, moDified, vaLue from test")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+----+------------+-------+",
            "| ID | moDified   | vaLue |",
            "+----+------------+-------+",
            "| A  | 2021-02-01 | 1     |",
            "| B  | 2021-02-01 | 10    |",
            "| C  | 2021-02-02 | 20    |",
            "| D  | 2021-02-02 | 100   |",
            "+----+------------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);

        /* TODO: Datafusion doesn't have any options to prevent case-sensitivity with the col func */
        /*
        let df = ctx
            .table("test")
            .await
            .unwrap()
            .select(vec![col("ID"), col("moDified"), col("vaLue")])
            .unwrap();
        let actual = df.collect().await.unwrap();
        assert_batches_sorted_eq!(&expected, &actual);
        */
    }

    #[tokio::test]
    async fn delta_scan_supports_missing_columns() {
        let schema1 = Arc::new(ArrowSchema::new(vec![Field::new(
            "col_1",
            ArrowDataType::Utf8,
            true,
        )]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("A"),
                Some("B"),
            ]))],
        )
        .unwrap();

        let schema2 = Arc::new(ArrowSchema::new(vec![
            Field::new("col_1", ArrowDataType::Utf8, true),
            Field::new("col_2", ArrowDataType::Utf8, true),
        ]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("E"),
                    Some("F"),
                    Some("G"),
                ])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("E2"),
                    Some("F2"),
                    Some("G2"),
                ])),
            ],
        )
        .unwrap();

        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch2])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let table = crate::DeltaOps(table)
            .write(vec![batch1])
            .with_schema_mode(SchemaMode::Merge)
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().snapshot().clone(), log, config)
                .unwrap();
        let ctx: SessionContext = DeltaSessionContext::default().into();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx.sql("select col_1, col_2 from test").await.unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| A     |       |",
            "| B     |       |",
            "| E     | E2    |",
            "| F     | F2    |",
            "| G     | G2    |",
            "+-------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn delta_scan_supports_pushdown() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("col_1", ArrowDataType::Utf8, false),
            Field::new("col_2", ArrowDataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("A"),
                    Some("B"),
                    Some("C"),
                ])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("A2"),
                    Some("B2"),
                    Some("C2"),
                ])),
            ],
        )
        .unwrap();

        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().snapshot().clone(), log, config)
                .unwrap();

        let mut cfg = SessionConfig::default();
        cfg.options_mut().execution.parquet.pushdown_filters = true;
        let ctx = SessionContext::new_with_config(cfg);
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("select col_1, col_2 from test WHERE col_1 = 'A'")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+-------+-------+",
            "| col_1 | col_2 |",
            "+-------+-------+",
            "| A     | A2    |",
            "+-------+-------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn delta_scan_supports_nested_missing_columns() {
        let column1_schema1: arrow::datatypes::Fields =
            vec![Field::new("col_1a", ArrowDataType::Utf8, true)].into();
        let schema1 = Arc::new(ArrowSchema::new(vec![Field::new(
            "col_1",
            ArrowDataType::Struct(column1_schema1.clone()),
            true,
        )]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![Arc::new(StructArray::new(
                column1_schema1,
                vec![Arc::new(arrow::array::StringArray::from(vec![
                    Some("A"),
                    Some("B"),
                ]))],
                None,
            ))],
        )
        .unwrap();

        let column1_schema2: arrow_schema::Fields = vec![
            Field::new("col_1a", ArrowDataType::Utf8, true),
            Field::new("col_1b", ArrowDataType::Utf8, true),
        ]
        .into();
        let schema2 = Arc::new(ArrowSchema::new(vec![Field::new(
            "col_1",
            ArrowDataType::Struct(column1_schema2.clone()),
            true,
        )]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![Arc::new(StructArray::new(
                column1_schema2,
                vec![
                    Arc::new(arrow::array::StringArray::from(vec![
                        Some("E"),
                        Some("F"),
                        Some("G"),
                    ])),
                    Arc::new(arrow::array::StringArray::from(vec![
                        Some("E2"),
                        Some("F2"),
                        Some("G2"),
                    ])),
                ],
                None,
            ))],
        )
        .unwrap();

        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch1])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let table = crate::DeltaOps(table)
            .write(vec![batch2])
            .with_schema_mode(SchemaMode::Merge)
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();
        let log = table.log_store();

        let provider =
            DeltaTableProvider::try_new(table.snapshot().unwrap().snapshot().clone(), log, config)
                .unwrap();
        let ctx: SessionContext = DeltaSessionContext::default().into();
        ctx.register_table("test", Arc::new(provider)).unwrap();

        let df = ctx
            .sql("select col_1.col_1a, col_1.col_1b from test")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();
        let expected = vec![
            "+--------------------+--------------------+",
            "| test.col_1[col_1a] | test.col_1[col_1b] |",
            "+--------------------+--------------------+",
            "| A                  |                    |",
            "| B                  |                    |",
            "| E                  | E2                 |",
            "| F                  | F2                 |",
            "| G                  | G2                 |",
            "+--------------------+--------------------+",
        ];
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_multiple_predicate_pushdown() {
        use crate::datafusion::prelude::SessionContext;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("moDified", ArrowDataType::Utf8, true),
            Field::new("id", ArrowDataType::Utf8, true),
            Field::new("vaLue", ArrowDataType::Int32, true),
        ]));

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
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch.clone()])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let datafusion = SessionContext::new();
        let table = Arc::new(table);

        datafusion.register_table("snapshot", table).unwrap();

        let df = datafusion
            .sql("select * from snapshot where id > 10000 and id < 20000")
            .await
            .unwrap();

        df.collect().await.unwrap();
    }

    #[tokio::test]
    async fn test_delta_scan_builder_no_scan_config() {
        let arr: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", arr, false)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let state = ctx.state();
        let scan = DeltaScanBuilder::new(
            table.snapshot().unwrap().snapshot(),
            table.log_store(),
            &state,
        )
        .with_filter(Some(col("a").eq(lit("s"))))
        .build()
        .await
        .unwrap();

        let mut visitor = ParquetVisitor::default();
        visit_execution_plan(&scan, &mut visitor).unwrap();

        assert_eq!(visitor.predicate.unwrap().to_string(), "a@0 = s");
    }

    #[tokio::test]
    async fn test_delta_scan_builder_scan_config_disable_pushdown() {
        let arr: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", arr, false)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let snapshot = table.snapshot().unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();
        let scan = DeltaScanBuilder::new(snapshot.snapshot(), table.log_store(), &state)
            .with_filter(Some(col("a").eq(lit("s"))))
            .with_scan_config(
                DeltaScanConfigBuilder::new()
                    .with_parquet_pushdown(false)
                    .build(snapshot.snapshot())
                    .unwrap(),
            )
            .build()
            .await
            .unwrap();

        let mut visitor = ParquetVisitor::default();
        visit_execution_plan(&scan, &mut visitor).unwrap();

        assert!(visitor.predicate.is_none());
    }

    #[tokio::test]
    async fn test_delta_scan_applies_parquet_options() {
        let arr: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter_with_nullable(vec![("a", arr, false)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let snapshot = table.snapshot().unwrap();

        let mut config = SessionConfig::default();
        config.options_mut().execution.parquet.pushdown_filters = true;
        let ctx = SessionContext::new_with_config(config);
        let state = ctx.state();

        let scan = DeltaScanBuilder::new(snapshot.snapshot(), table.log_store(), &state)
            .build()
            .await
            .unwrap();

        let mut visitor = ParquetVisitor::default();
        visit_execution_plan(&scan, &mut visitor).unwrap();

        assert_eq!(ctx.copied_table_options().parquet, visitor.options.unwrap());
    }

    /// Extracts fields from the parquet scan
    #[derive(Default)]
    struct ParquetVisitor {
        predicate: Option<Arc<dyn PhysicalExpr>>,
        options: Option<TableParquetOptions>,
    }

    impl ExecutionPlanVisitor for ParquetVisitor {
        type Error = DataFusionError;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            let Some(datasource_exec) = plan.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(true);
            };

            let Some(scan_config) = datasource_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(true);
            };

            if let Some(parquet_source) = scan_config
                .file_source
                .as_any()
                .downcast_ref::<ParquetSource>()
            {
                self.options = Some(parquet_source.table_parquet_options().clone());
                self.predicate = parquet_source.predicate().cloned();
            }

            Ok(true)
        }
    }

    #[tokio::test]
    async fn passes_sanity_checker_when_all_files_filtered() {
        // Run a query that filters out all files and sorts.
        // Verify that it returns an empty set of rows without panicking.
        //
        // Historically, we had a bug that caused us to emit a query plan with 0 partitions, which
        // datafusion rejected.
        let table_path = std::path::Path::new("../test/tests/data/delta-2.2.0-partitioned-types")
            .canonicalize()
            .unwrap();
        let table_url = url::Url::from_directory_path(table_path).unwrap();
        let table = crate::open_table(table_url).await.unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(table)).unwrap();

        let df = ctx
            .sql("select * from test where c3 = 100 ORDER BY c1 ASC")
            .await
            .unwrap();
        let actual = df.collect().await.unwrap();

        assert_eq!(actual.len(), 0);
    }

    #[tokio::test]
    async fn test_check_nullability() -> DeltaResult<()> {
        use arrow::array::StringArray;

        let data_checker = DeltaDataChecker {
            non_nullable_columns: vec!["zed".to_string(), "yap".to_string()],
            ..Default::default()
        };

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let nulls: Arc<dyn Array> = Arc::new(StringArray::new_null(1));
        let batch = RecordBatch::try_from_iter(vec![("a", arr), ("zed", nulls)]).unwrap();

        let result = data_checker.check_nullability(&batch);
        assert!(
            result.is_err(),
            "The result should have errored! {result:?}"
        );

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter(vec![("zed", arr)]).unwrap();
        let result = data_checker.check_nullability(&batch);
        assert!(
            result.is_err(),
            "The result should have errored! {result:?}"
        );

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter(vec![("zed", arr.clone()), ("yap", arr)]).unwrap();
        let _ = data_checker.check_nullability(&batch)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_uses_parquet_column_pruning() {
        let small: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["a"]));
        let large: Arc<dyn Array> = Arc::new(arrow::array::StringArray::from(vec!["b"
            .repeat(1024)
            .as_str()]));
        let batch = RecordBatch::try_from_iter(vec![("small", small), ("large", large)]).unwrap();
        let table = crate::DeltaOps::new_in_memory()
            .write(vec![batch])
            .with_save_mode(crate::protocol::SaveMode::Append)
            .await
            .unwrap();

        let config = DeltaScanConfigBuilder::new()
            .build(table.snapshot().unwrap().snapshot())
            .unwrap();

        let (object_store, mut operations) =
            RecordingObjectStore::new(table.log_store().object_store(None));
        // this uses an in memory store pointing at root...
        let both_store = Arc::new(object_store);
        let log_store = DefaultLogStore::new(
            both_store.clone(),
            both_store,
            table.log_store().config().clone(),
        );
        let provider = DeltaTableProvider::try_new(
            table.snapshot().unwrap().snapshot().clone(),
            Arc::new(log_store),
            config,
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("test", Arc::new(provider)).unwrap();
        let state = ctx.state();

        let df = ctx.sql("select small from test").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();

        let mut stream = plan.execute(0, state.task_ctx()).unwrap();
        let Some(Ok(batch)) = stream.next().await else {
            panic!()
        };
        assert!(stream.next().await.is_none());
        assert_eq!(1, batch.num_columns());
        assert_eq!(1, batch.num_rows());
        let small = batch.column_by_name("small").unwrap().as_string::<i32>();
        assert_eq!("a", small.iter().next().unwrap().unwrap());

        let expected = vec![
            ObjectStoreOperation::GetRange(LocationType::Data, 957..965),
            ObjectStoreOperation::GetRange(LocationType::Data, 326..957),
            #[expect(clippy::single_range_in_vec_init)]
            ObjectStoreOperation::GetRanges(LocationType::Data, vec![4..46]),
        ];
        let mut actual = Vec::new();
        operations.recv_many(&mut actual, 3).await;
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_push_down_filter_panic_2602() -> DeltaResult<()> {
        use crate::kernel::schema::{DataType, PrimitiveType};
        let ctx = SessionContext::new();
        let table = crate::DeltaOps::new_in_memory()
            .create()
            .with_column("id", DataType::Primitive(PrimitiveType::Long), true, None)
            .with_column(
                "name",
                DataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .with_column("b", DataType::Primitive(PrimitiveType::Boolean), true, None)
            .with_column(
                "ts",
                DataType::Primitive(PrimitiveType::Timestamp),
                true,
                None,
            )
            .with_column("dt", DataType::Primitive(PrimitiveType::Date), true, None)
            .with_column(
                "zap",
                DataType::Array(Box::new(ArrayType::new(
                    DataType::Primitive(PrimitiveType::Boolean),
                    true,
                ))),
                true,
                None,
            )
            .await?;

        ctx.register_table("snapshot", Arc::new(table)).unwrap();

        let df = ctx
            .sql("select * from snapshot where id > 10000 and id < 20000")
            .await
            .unwrap();

        let _ = df.collect().await?;
        Ok(())
    }

    /// Records operations made by the inner object store on a channel obtained at construction
    struct RecordingObjectStore {
        inner: ObjectStoreRef,
        operations: UnboundedSender<ObjectStoreOperation>,
    }

    impl RecordingObjectStore {
        /// Returns an object store and a channel recording all operations made by the inner object store
        fn new(inner: ObjectStoreRef) -> (Self, UnboundedReceiver<ObjectStoreOperation>) {
            let (operations, operations_receiver) = unbounded_channel();
            (Self { inner, operations }, operations_receiver)
        }
    }

    impl Display for RecordingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            Display::fmt(&self.inner, f)
        }
    }

    impl Debug for RecordingObjectStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            Debug::fmt(&self.inner, f)
        }
    }

    #[derive(Debug, PartialEq)]
    enum ObjectStoreOperation {
        GetRanges(LocationType, Vec<Range<u64>>),
        GetRange(LocationType, Range<u64>),
        GetOpts(LocationType),
        Get(LocationType),
    }

    #[derive(Debug, PartialEq)]
    enum LocationType {
        Data,
        Commit,
    }

    impl From<&Path> for LocationType {
        fn from(value: &Path) -> Self {
            let dummy_url = Url::parse("dummy:///").unwrap();
            let parsed = ParsedLogPath::try_from(dummy_url.join(value.as_ref()).unwrap()).unwrap();
            if let Some(parsed) = parsed {
                if matches!(parsed.file_type, LogPathFileType::Commit) {
                    return LocationType::Commit;
                }
            }
            if value.to_string().starts_with("part-") {
                LocationType::Data
            } else {
                panic!("Unknown location type: {value:?}")
            }
        }
    }

    // Currently only read operations are recorded. Extend as necessary.
    #[async_trait::async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put(
            &self,
            location: &Path,
            payload: PutPayload,
        ) -> object_store::Result<PutResult> {
            self.inner.put(location, payload).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart(
            &self,
            location: &Path,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart(location).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
            self.operations
                .send(ObjectStoreOperation::Get(location.into()))
                .unwrap();
            self.inner.get(location).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.operations
                .send(ObjectStoreOperation::GetOpts(location.into()))
                .unwrap();
            self.inner.get_opts(location, options).await
        }

        async fn get_range(
            &self,
            location: &Path,
            range: Range<u64>,
        ) -> object_store::Result<Bytes> {
            self.operations
                .send(ObjectStoreOperation::GetRange(
                    location.into(),
                    range.clone(),
                ))
                .unwrap();
            self.inner.get_range(location, range).await
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[Range<u64>],
        ) -> object_store::Result<Vec<Bytes>> {
            self.operations
                .send(ObjectStoreOperation::GetRanges(
                    location.into(),
                    ranges.to_vec(),
                ))
                .unwrap();
            self.inner.get_ranges(location, ranges).await
        }

        async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn delete_stream<'a>(
            &'a self,
            locations: BoxStream<'a, object_store::Result<Path>>,
        ) -> BoxStream<'a, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.rename(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }

        async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.rename_if_not_exists(from, to).await
        }
    }
}
