use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::StringViewType;
use arrow_array::{Array, GenericByteViewArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{HashSet, Result};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Volatility, col};
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::{ExecutionPlan, collect_partitioned};
use datafusion::prelude::{cast, lit};
use delta_kernel::Predicate;
use futures::TryStreamExt as _;
use itertools::Itertools;
use percent_encoding::percent_decode_str;
use tracing::*;

use crate::delta_datafusion::engine::to_delta_predicate;
use crate::delta_datafusion::logical::LogicalPlanBuilderExt as _;
use crate::delta_datafusion::table_provider::next::{FileSelection, MissingFilePolicy};
use crate::delta_datafusion::{
    DataFusionMixins as _, DeltaScanBuilder, DeltaScanConfigBuilder, DeltaScanNext,
    FILE_ID_COLUMN_DEFAULT, PATH_COLUMN, get_path_column,
};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, EagerSnapshot};
use crate::logstore::LogStoreRef;

#[derive(Debug, Hash, Eq, PartialEq)]
/// Representing the result of the [find_files] function.
pub(crate) struct FindFiles {
    /// A list of `Add` objects that match the given predicate
    pub candidates: Vec<Add>,
    /// Was a physical read to the datastore required to determine the candidates
    pub partition_scan: bool,
}

/// Finds files in a snapshot that match the provided predicate.
#[instrument(
    skip_all,
    fields(
        version = snapshot.version(),
        has_predicate = predicate.is_some(),
        partition_scan = field::Empty,
        candidate_count = field::Empty
    )
)]
pub(crate) async fn find_files(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    predicate: Option<Expr>,
) -> DeltaResult<FindFiles> {
    let current_metadata = snapshot.metadata();

    match &predicate {
        Some(predicate) => {
            // Validate the Predicate and determine if it only contains partition columns
            let mut expr_properties = FindFilesExprProperties {
                partition_only: true,
                partition_columns: current_metadata.partition_columns().clone(),
                result: Ok(()),
            };

            TreeNode::visit(predicate, &mut expr_properties)?;
            expr_properties.result?;

            if expr_properties.partition_only {
                let candidates = scan_memory_table(snapshot, predicate).await?;
                let result = FindFiles {
                    candidates,
                    partition_scan: true,
                };
                Span::current().record("partition_scan", result.partition_scan);
                Span::current().record("candidate_count", result.candidates.len());
                Ok(result)
            } else {
                let candidates =
                    find_files_scan(snapshot, log_store, session, predicate.to_owned()).await?;

                let result = FindFiles {
                    candidates,
                    partition_scan: false,
                };
                Span::current().record("partition_scan", result.partition_scan);
                Span::current().record("candidate_count", result.candidates.len());
                Ok(result)
            }
        }
        None => {
            let result = FindFiles {
                candidates: snapshot
                    .file_views(&log_store, None)
                    .map_ok(|f| f.add_action())
                    .try_collect()
                    .await?,
                partition_scan: true,
            };
            Span::current().record("partition_scan", result.partition_scan);
            Span::current().record("candidate_count", result.candidates.len());
            Ok(result)
        }
    }
}

pub(crate) struct FindFilesExprProperties {
    pub partition_columns: Vec<String>,

    pub partition_only: bool,
    pub result: DeltaResult<()>,
}

impl Default for FindFilesExprProperties {
    fn default() -> Self {
        Self {
            partition_columns: Vec::new(),
            partition_only: true,
            result: Ok(()),
        }
    }
}

/// Ensure only expressions that make sense are accepted, check for
/// non-deterministic functions, and determine if the expression only contains
/// partition columns
impl TreeNodeVisitor<'_> for FindFilesExprProperties {
    type Node = Expr;

    fn f_down(&mut self, expr: &Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
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
            | Expr::Literal(_, _)
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
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_) => (),
            Expr::ScalarFunction(scalar_function) => {
                match scalar_function.func.signature().volatility {
                    Volatility::Immutable => (),
                    _ => {
                        self.result = Err(DeltaTableError::Generic(format!(
                            "Find files predicate contains nondeterministic function {}",
                            scalar_function.func.name()
                        )));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            _ => {
                self.result = Err(DeltaTableError::Generic(format!(
                    "Find files predicate contains unsupported expression {expr}"
                )));
                return Ok(TreeNodeRecursion::Stop);
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

fn join_batches_with_add_actions(
    batches: Vec<RecordBatch>,
    mut actions: HashMap<String, Add>,
    path_column: &str,
    dict_array: bool,
    decode_paths: bool,
) -> DeltaResult<Vec<Add>> {
    // Given RecordBatches that contains `__delta_rs_path` perform a hash join
    // with actions to obtain original add actions

    let mut files = Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
    for batch in batches {
        let err = || DeltaTableError::Generic("Unable to obtain Delta-rs path column".to_string());

        let iter: Box<dyn Iterator<Item = Option<&str>>> = if dict_array {
            let array = get_path_column(&batch, path_column)?;
            Box::new(array.into_iter())
        } else {
            let array = batch
                .column_by_name(path_column)
                .ok_or_else(err)?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(err)?;
            Box::new(array.into_iter())
        };

        for path in iter {
            let path = path.ok_or(DeltaTableError::Generic(format!(
                "{path_column} cannot be null"
            )))?;

            let key = if decode_paths {
                percent_decode_str(path).decode_utf8_lossy()
            } else {
                std::borrow::Cow::Borrowed(path)
            };

            match actions.remove(key.as_ref()) {
                Some(action) => files.push(action),
                None => {
                    return Err(DeltaTableError::Generic(
                        "Unable to map __delta_rs_path to action.".to_owned(),
                    ));
                }
            }
        }
    }
    Ok(files)
}

/// Determine which files contain a record that satisfies the predicate
#[instrument(
    skip_all,
    fields(
        version = snapshot.version(),
        total_files = field::Empty,
        matching_files = field::Empty
    )
)]
async fn find_files_scan(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    // let kernel_predicate = to_predicate(&expression).ok().map(Arc::new);
    let candidate_map: HashMap<_, _> = snapshot
        .file_views(&log_store, None)
        .map_ok(|f| {
            let add = f.add_action();
            (add.path.clone(), add)
        })
        .try_collect()
        .await?;

    Span::current().record("total_files", candidate_map.len());

    let scan_config = DeltaScanConfigBuilder::default()
        .with_file_column(true)
        .build(snapshot)?;
    let file_column_name = scan_config
        .file_column_name
        .as_ref()
        .ok_or(DeltaTableError::Generic(
            "File column name must be set in scan config".to_string(),
        ))?
        .clone();

    let logical_schema = df_logical_schema(snapshot, &file_column_name)?;

    // Identify which columns we need to project
    let mut used_columns: Vec<_> = expression
        .column_refs()
        .into_iter()
        .map(|column| logical_schema.index_of(&column.name))
        .try_collect()?;
    // Add path column
    used_columns.push(logical_schema.index_of(&file_column_name)?);

    let scan = DeltaScanBuilder::new(snapshot, log_store, session)
        .with_filter(Some(expression.clone()))
        .with_projection(Some(&used_columns))
        .with_scan_config(scan_config)
        .build()
        .await?;
    let scan = Arc::new(scan);

    let input_dfschema = scan.logical_schema.as_ref().to_owned().try_into()?;
    let predicate_expr = session
        .create_physical_expr(Expr::IsTrue(Box::new(expression.clone())), &input_dfschema)?;

    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);
    let limit: Arc<dyn ExecutionPlan> = Arc::new(LocalLimitExec::new(filter, 1));

    let path_batches = datafusion::physical_plan::collect(limit, session.task_ctx()).await?;

    let result =
        join_batches_with_add_actions(path_batches, candidate_map, &file_column_name, true, false)?;

    Span::current().record("matching_files", result.len());
    Ok(result)
}

async fn scan_memory_table(snapshot: &EagerSnapshot, predicate: &Expr) -> DeltaResult<Vec<Add>> {
    let actions = snapshot
        .log_data()
        .iter()
        .map(|f| f.add_action())
        .collect_vec();

    let batch = snapshot.add_actions_table(true)?;
    let schema = batch.schema();
    let mut arrays = Vec::with_capacity(schema.fields().len());
    let mut fields = Vec::with_capacity(schema.fields().len());

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
        if let Some(name) = field.name().strip_prefix("partition.") {
            arrays.push(batch.column_by_name(field.name()).unwrap().to_owned());
            fields.push(field.as_ref().clone().with_name(name));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let mem_table = MemTable::try_new(
        schema.clone(),
        vec![vec![RecordBatch::try_new(schema, arrays)?]],
    )?;

    let ctx = SessionContext::new();
    let mut df = ctx.read_table(Arc::new(mem_table))?;
    df = df
        .filter(predicate.to_owned())?
        .select(vec![col(PATH_COLUMN)])?;
    let batches = df.collect().await?;

    let map = actions
        .into_iter()
        .map(|action| (action.path.clone(), action))
        .collect::<HashMap<_, _>>();

    join_batches_with_add_actions(batches, map, PATH_COLUMN, false, true)
}

/// The logical schema for a Deltatable is different from the protocol level schema since partition
/// columns must appear at the end of the schema. This is to align with how partition are handled
/// at the physical level
fn df_logical_schema(
    snapshot: &EagerSnapshot,
    file_column_name: &String,
) -> DeltaResult<SchemaRef> {
    let input_schema = snapshot.input_schema();
    let table_partition_cols = snapshot.metadata().partition_columns();

    let mut fields: Vec<_> = input_schema
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

    fields.push(Arc::new(Field::new(file_column_name, DataType::Utf8, true)));

    Ok(Arc::new(Schema::new(fields)))
}

pub(crate) struct MatchedFilesScan {
    /// A logical plan to perform a scan over all matched filed
    plan: LogicalPlan,
    /// Arrays containing the matched file names.
    valid_files: Vec<GenericByteViewArray<StringViewType>>,
    /// The optimized predicate used to find files with some maatching data
    pub(crate) predicate: Expr,
    /// A kernel version of the predicate.
    ///
    /// This predicate may be a subset of the optimized predicate, since
    /// we do not (yet) have full coverage to translate datafusion to
    /// kernel predicates.
    pub(crate) delta_predicate: Arc<Predicate>,
    /// The predicate contains only partition column references
    ///
    /// This implies that for each matched file all data matches.
    pub(crate) partition_only: bool,
}

impl MatchedFilesScan {
    pub(crate) fn scan(&self) -> &LogicalPlan {
        &self.plan
    }

    /// Create a HashSet containing all files included in this scan.
    ///
    /// Files are referenced using fully qualified URLs.
    pub(crate) fn files_set(&self) -> HashSet<String> {
        self.valid_files
            .iter()
            .flat_map(|arr| arr.iter().flatten().map(|v| v.to_string()))
            .collect()
    }
}

/// Create a table scan plan for reading all data from
/// all files which contain any data matching a predicate.
///
/// This is useful for DML where we need to re-write files by
/// updating/removing some records so we require all records
/// but only from matching files.
///
/// ## Returns
///
/// If no files contain matching data, `None` is returned.
///
/// Otherwise:
/// - The logical plan for reading data from matched files only.
/// - The set of matched file URLs.
/// - The potentially simplified predicate applied when matching data.
/// - A kernel predicate (best effort) which can be used to filter log replays.
pub(crate) async fn scan_files_where_matches(
    session: &dyn Session,
    snapshot: &EagerSnapshot,
    predicate: Expr,
) -> Result<Option<MatchedFilesScan>> {
    let skipping_pred = simplify_predicates(split_conjunction_owned(predicate))?;

    let partition_columns = snapshot
        .table_configuration()
        .metadata()
        .partition_columns();
    // validate that the expressions contain no illegal variants
    // that are not eligible for file skipping, e.g. volatile functions.
    let mut visitor = FindFilesExprProperties {
        partition_columns: partition_columns.clone(),
        partition_only: true,
        result: Ok(()),
    };
    for term in &skipping_pred {
        visitor.result = Ok(());
        term.visit(&mut visitor)?;
        visitor.result?
    }

    // convert to a delta predicate that can be applied to kernel scans.
    // This is a best effort predicate and downstream code needs to also
    // apply the explicit file selection so we can ignore errors in the
    // conversion.
    let delta_predicate = Arc::new(Predicate::and_from(
        skipping_pred
            .iter()
            .flat_map(|p| to_delta_predicate(p).ok()),
    ));

    let predicate = conjunction(skipping_pred.clone()).unwrap_or(lit(true));

    // Scan the delta table with a dedicated predicate applied for file skipping
    // and with the source file path exposed as column.
    let table_source = provider_as_source(
        DeltaScanNext::builder()
            .with_eager_snapshot(snapshot.clone())
            .with_file_skipping_predicates(skipping_pred.clone())
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .await?,
    );

    // the kernel scan only provides a best effort file skipping, in this case
    // we want to determine the file we certainly need to rewrite. For this
    // we perform an initial aggregate scan to see if we can quickly find
    // at least one matching record in the files.
    let files_plan = LogicalPlanBuilder::scan("files_scan", table_source.clone(), None)?
        .filter(predicate.clone())?
        .project([
            cast(col(FILE_ID_COLUMN_DEFAULT), DataType::Utf8View).alias(FILE_ID_COLUMN_DEFAULT)
        ])?
        .distinct()?
        .build()?;

    let files_exec = session.create_physical_plan(&files_plan).await?;
    let files_data = collect_partitioned(files_exec, session.task_ctx()).await?;
    let files_count = files_data
        .iter()
        .flat_map(|batches| batches.iter().map(|b| b.num_rows()))
        .sum::<usize>();
    if files_count == 0 {
        return Ok(None);
    }

    let valid_files = files_data
        .iter()
        .flat_map(|batches| batches.iter().map(|b| b.column(0).as_string_view().clone()))
        .collect_vec();

    // Create a table scan limited to the matched files by forwarding an explicit
    // file selection into the table provider.
    let table_root = snapshot
        .snapshot()
        .scan_builder()
        .build()?
        .table_root()
        .clone();
    let file_selection = FileSelection::from_paths(
        valid_files
            .iter()
            .flat_map(|arr| arr.iter().flatten().map(|v| v.to_string())),
        &table_root,
    )?
    .with_missing_file_policy(MissingFilePolicy::Ignore);
    let selected_provider = DeltaScanNext::builder()
        .with_eager_snapshot(snapshot.clone())
        .with_file_skipping_predicates(skipping_pred)
        .with_file_column(FILE_ID_COLUMN_DEFAULT)
        .build()
        .await?
        .with_file_selection(file_selection);
    let selected_table_source = provider_as_source(Arc::new(selected_provider));

    let plan = LogicalPlanBuilder::scan("source", selected_table_source, None)?
        .drop_columns([FILE_ID_COLUMN_DEFAULT])?
        .build()?;

    Ok(Some(MatchedFilesScan {
        plan,
        valid_files,
        predicate,
        delta_predicate,
        partition_only: visitor.partition_only,
    }))
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{col, lit};

    use crate::{
        delta_datafusion::create_session,
        test_utils::{TestResult, open_fs_path},
    };

    use super::*;

    #[tokio::test]
    async fn test_scan_files_where_matches_plan_can_be_planned() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/simple_table");
        table.load().await?;

        let ctx = create_session().into_inner();
        let session = ctx.state();
        table.update_datafusion_session(&session)?;

        let snapshot = table.snapshot()?.snapshot().clone();
        let predicate = col("id").gt(lit(-1i64));
        let Some(scan) = scan_files_where_matches(&session, &snapshot, predicate).await? else {
            panic!("Expected at least one matching file");
        };

        let plan = session.create_physical_plan(scan.scan()).await?;
        let _data = collect(plan, session.task_ctx()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_files_where_matches_plan_has_no_file_id_in_list_filter() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/simple_table");
        table.load().await?;

        let ctx = create_session().into_inner();
        let session = ctx.state();
        table.update_datafusion_session(&session)?;

        let snapshot = table.snapshot()?.snapshot().clone();
        let predicate = col("id").gt(lit(-1i64));
        let Some(scan) = scan_files_where_matches(&session, &snapshot, predicate).await? else {
            panic!("Expected at least one matching file");
        };

        let plan_debug = format!("{:?}", scan.scan());
        assert!(
            !(plan_debug.contains("InList(") && plan_debug.contains(FILE_ID_COLUMN_DEFAULT)),
            "unexpected plan with file-id IN filter: {plan_debug}"
        );

        Ok(())
    }
}
