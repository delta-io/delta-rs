//! Runs a workload on one engine and reduces its result to a value two engines can compare.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Schema as ArrowSchema, SchemaRef, TimeUnit,
};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::planner::TypePlanner;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::DataType as SqlDataType;
use delta_kernel::actions::{Metadata, Protocol};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use delta_kernel::engine::arrow_data::EngineDataArrowExt as _;
use delta_kernel::engine::arrow_expression::evaluate_expression::evaluate_predicate;
use delta_kernel::expressions::Predicate;
use delta_kernel::history_manager::{HistoryCommitType, latest_version_as_of};
use delta_kernel::schema::Schema;
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_changes::TableChanges;
use delta_kernel::{
    Engine, EngineData, KernelError, KernelResult as DeltaResult, SnapshotRef, Version,
};
use deltalake_dat::{CdfSpec, ReadSpec, Spec, TimeTravel, Workload};
use deltalake_kernel_executor::expressions::to_delta_predicate;
use url::Url;

/// What a workload produced.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// The rows of a scan, each formatted as one line and sorted, with the schema of its batches.
    Rows {
        schema: Option<SchemaRef>,
        rows: Vec<String>,
    },
    Snapshot {
        version: Version,
        protocol: Box<Protocol>,
        metadata: Box<Metadata>,
    },
}

/// Runs `workload` on `engine`.
pub fn run(workload: &Workload, engine: Arc<dyn Engine>) -> DeltaResult<Outcome> {
    let root = &workload.table_root;
    match &workload.spec {
        Spec::Read(spec) => read(root, spec, engine),
        Spec::Snapshot(spec) => snapshot(root, &spec.at, engine.as_ref()),
        Spec::Cdf(spec) => cdf(root, spec, engine),
        _ => unreachable!("the test skips workloads that write"),
    }
}

fn read(root: &Url, spec: &ReadSpec, engine: Arc<dyn Engine>) -> DeltaResult<Outcome> {
    let snapshot = build_snapshot(root, &spec.at, engine.as_ref())?;
    let schema = snapshot.schema();
    let predicate = parse(spec.predicate.as_deref(), &schema)?;
    let mut scan = snapshot.scan_builder();
    if let Some(columns) = &spec.columns {
        scan = scan.with_schema(schema.project(columns)?);
    }
    let scan = scan.with_predicate(predicate.clone()).build()?;
    rows(scan.execute(engine)?, predicate.as_deref())
}

fn snapshot(root: &Url, at: &TimeTravel, engine: &dyn Engine) -> DeltaResult<Outcome> {
    let snapshot = build_snapshot(root, at, engine)?;
    let config = snapshot.table_configuration();
    Ok(Outcome::Snapshot {
        version: snapshot.version(),
        protocol: Box::new(config.protocol().clone()),
        metadata: Box::new(config.metadata().clone()),
    })
}

fn cdf(root: &Url, spec: &CdfSpec, engine: Arc<dyn Engine>) -> DeltaResult<Outcome> {
    let changes = TableChanges::try_new(
        root.clone(),
        engine.as_ref(),
        spec.start_version,
        spec.end_version,
    )?;
    let predicate = parse(spec.predicate.as_deref(), changes.schema())?;
    let scan = Arc::new(changes)
        .scan_builder()
        .with_predicate(predicate.clone())
        .build()?;
    rows(scan.execute(engine)?, predicate.as_deref())
}

fn build_snapshot(root: &Url, at: &TimeTravel, engine: &dyn Engine) -> DeltaResult<SnapshotRef> {
    let version = match (&at.version, &at.timestamp) {
        (Some(version), _) => Some(
            Version::try_from(*version)
                .map_err(|_| KernelError::generic(format!("negative version {version}")))?,
        ),
        (None, Some(timestamp)) => {
            let millis = string_to_timestamp_nanos(timestamp)? / 1_000_000;
            let latest = Snapshot::builder_for(root.clone()).build(engine)?;
            let commit =
                latest_version_as_of(&latest, engine, millis, HistoryCommitType::Recreatable)?;
            Some(commit.version)
        }
        (None, None) => None,
    };
    let mut builder = Snapshot::builder_for(root.clone());
    if let Some(version) = version {
        builder = builder.at_version(version);
    }
    builder.build(engine)
}

fn parse(predicate: Option<&str>, schema: &Schema) -> DeltaResult<Option<Arc<Predicate>>> {
    predicate
        .map(|sql| parse_sql(sql, schema).map(Arc::new))
        .transpose()
        .map_err(|err| KernelError::generic(format!("predicate: {err}")))
}

/// The Spark SQL predicate `sql` as a kernel predicate: DataFusion parses it, types it against
/// `schema`, and folds its constants.
fn parse_sql(sql: &str, schema: &Schema) -> DFResult<Predicate> {
    let schema: ArrowSchema = schema.try_into_arrow()?;
    let schema = Arc::new(DFSchema::try_from(schema)?);
    // Spark resolves column names case-insensitively; keep the names as written.
    let config =
        SessionConfig::new().set_bool("datafusion.sql_parser.enable_ident_normalization", false);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_type_planner(Arc::new(SparkTypePlanner))
        .build();
    let expr = SessionContext::new_with_state(state).parse_sql_expr(sql, &schema)?;
    let simplifier = ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(schema.clone())
            .build(),
    );
    let expr = simplifier.simplify(simplifier.coerce(expr, &schema)?)?;
    to_delta_predicate(&expr)
}

/// Plans the Spark type names that DataFusion's SQL does not know.
#[derive(Debug)]
struct SparkTypePlanner;

impl TypePlanner for SparkTypePlanner {
    fn plan_type_field(&self, sql_type: &SqlDataType) -> DFResult<Option<FieldRef>> {
        let data_type = match sql_type {
            SqlDataType::TimestampNtz(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
            SqlDataType::Custom(name, modifiers) if modifiers.is_empty() => {
                match name.to_string().to_ascii_uppercase().as_str() {
                    "BYTE" => DataType::Int8,
                    "SHORT" => DataType::Int16,
                    _ => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(Arc::new(Field::new("", data_type, true))))
    }
}

/// The rows of `batches` that `predicate` keeps. A scan only skips files, and the default engine
/// also skips row groups, so the rows are filtered here as a reader would.
fn rows(
    batches: impl Iterator<Item = DeltaResult<Box<dyn EngineData>>>,
    predicate: Option<&Predicate>,
) -> DeltaResult<Outcome> {
    let mut schema = None;
    let mut rows = Vec::new();
    for batch in batches {
        let mut batch: RecordBatch = batch?.try_into_record_batch()?;
        if let Some(predicate) = predicate {
            batch = filter_record_batch(&batch, &evaluate_predicate(predicate, &batch, false)?)?;
        }
        if batch.num_rows() == 0 {
            continue;
        }
        match &schema {
            None => schema = Some(batch.schema()),
            Some(schema) if schema != &batch.schema() => {
                return Err(KernelError::generic(format!(
                    "batches disagree on the schema: {schema:?} and {:?}",
                    batch.schema()
                )));
            }
            Some(_) => {}
        }
        rows.extend(format_rows(&batch)?);
    }
    rows.sort();
    Ok(Outcome::Rows { schema, rows })
}

/// Each row of `batch` as `value | value | ...`.
fn format_rows(batch: &RecordBatch) -> DeltaResult<Vec<String>> {
    let options = FormatOptions::default().with_null("NULL");
    let columns = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((0..batch.num_rows())
        .map(|row| {
            columns
                .iter()
                .map(|column| column.value(row).to_string())
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect())
}
