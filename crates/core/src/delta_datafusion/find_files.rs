use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::StringViewType;
use arrow_array::{Array, GenericByteViewArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{HashSet, Result};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Volatility, col};
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::physical_plan::collect_partitioned;
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
    DataFusionMixins as _, DeltaScanNext, FILE_ID_COLUMN_DEFAULT, PATH_COLUMN, get_path_column,
    normalize_path_as_file_id, resolve_file_column_name,
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

/// Abstraction over sources that can provide partition metadata add-action batches.
pub(crate) trait PartitionAddActionsProvider {
    fn add_actions_partition_batches(&self) -> DeltaResult<Vec<RecordBatch>>;
}

impl PartitionAddActionsProvider for EagerSnapshot {
    fn add_actions_partition_batches(&self) -> DeltaResult<Vec<RecordBatch>> {
        EagerSnapshot::add_actions_partition_batches(self)
    }
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
                partition_columns: current_metadata.partition_columns().to_vec(),
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
                    .map_ok(|f| f.to_add())
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

struct MatchingFilesScanSeed {
    valid_files: Vec<GenericByteViewArray<StringViewType>>,
    file_skipping_predicates: Vec<Expr>,
    predicate: Expr,
    delta_predicate: Arc<Predicate>,
}

async fn collect_matching_files(
    session: &dyn Session,
    snapshot: &EagerSnapshot,
    log_store: Option<&LogStoreRef>,
    predicate: Expr,
    file_column_name: &str,
) -> Result<Option<MatchingFilesScanSeed>> {
    let skipping_pred = simplify_predicates(split_conjunction_owned(predicate))?;

    let partition_columns = snapshot
        .table_configuration()
        .metadata()
        .partition_columns();
    let mut visitor = FindFilesExprProperties {
        partition_columns: partition_columns.to_vec(),
        partition_only: true,
        result: Ok(()),
    };
    for term in &skipping_pred {
        term.visit(&mut visitor)?;
        std::mem::replace(&mut visitor.result, Ok(()))?;
    }

    let delta_predicate = Arc::new(Predicate::and_from(
        skipping_pred
            .iter()
            .flat_map(|term| to_delta_predicate(term).ok()),
    ));
    let predicate = conjunction(skipping_pred.clone()).unwrap_or(lit(true));

    let mut builder = DeltaScanNext::builder()
        .with_snapshot(snapshot.snapshot().clone())
        .with_file_skipping_predicates(skipping_pred.clone())
        .with_file_column(file_column_name);
    if let Some(log_store) = log_store {
        builder = builder.with_log_store(log_store.clone());
    }
    let table_source = provider_as_source(builder.await?);

    let files_plan = LogicalPlanBuilder::scan("files_scan", table_source, None)?
        .filter(predicate.clone())?
        .project([cast(col(file_column_name), DataType::Utf8View).alias(file_column_name)])?
        .distinct()?
        .build()?;

    let files_exec = session.create_physical_plan(&files_plan).await?;
    let files_data = collect_partitioned(files_exec, session.task_ctx()).await?;
    let files_count = files_data
        .iter()
        .flat_map(|batches| batches.iter().map(|batch| batch.num_rows()))
        .sum::<usize>();
    if files_count == 0 {
        return Ok(None);
    }

    let valid_files = files_data
        .iter()
        .flat_map(|batches| {
            batches
                .iter()
                .map(|batch| batch.column(0).as_string_view().clone())
        })
        .collect_vec();

    Ok(Some(MatchingFilesScanSeed {
        valid_files,
        file_skipping_predicates: skipping_pred,
        predicate,
        delta_predicate,
    }))
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
pub(in crate::delta_datafusion) async fn find_files_scan(
    snapshot: &EagerSnapshot,
    log_store: LogStoreRef,
    session: &dyn Session,
    expression: Expr,
) -> DeltaResult<Vec<Add>> {
    let file_column_name = resolve_file_column_name(snapshot.input_schema().as_ref(), None)?;
    let table_root = snapshot
        .snapshot()
        .scan_builder()
        .build()?
        .table_root()
        .clone();
    let mut candidate_map: HashMap<_, _> = snapshot
        .file_views(log_store.as_ref(), None)
        .try_fold(HashMap::new(), |mut candidate_map, view| {
            // The next-provider file-id column is derived from the raw log path representation.
            // Key candidate lookup with that same representation, but keep decoded Add actions
            // as values for downstream DML consumers.
            let file_id =
                normalize_path_as_file_id(view.path_raw(), &table_root, "find_files candidate");
            let add = view.to_add();

            futures::future::ready(match file_id {
                Ok(file_id) => {
                    candidate_map.insert(file_id, add);
                    Ok(candidate_map)
                }
                Err(err) => Err(err),
            })
        })
        .await?;

    Span::current().record("total_files", candidate_map.len());

    let Some(matches) = collect_matching_files(
        session,
        snapshot,
        Some(&log_store),
        expression,
        &file_column_name,
    )
    .await?
    else {
        Span::current().record("matching_files", 0);
        return Ok(vec![]);
    };

    let mut result = Vec::new();
    for file_id in matches
        .valid_files
        .iter()
        .flat_map(|arr| arr.iter().flatten().map(str::to_owned))
    {
        let add = candidate_map.remove(&file_id).ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "Unable to map matched file id back to Add action: {file_id}"
            ))
        })?;
        result.push(add);
    }

    Span::current().record("matching_files", result.len());
    Ok(result)
}

/// Build a partition-metadata MemTable from snapshot add actions.
///
/// Projects only the `path` column and `partition.*` columns for evaluating
/// partition predicates without materializing full add-action metadata.
/// Returns `None` when the snapshot contains no add actions.
pub(crate) fn add_actions_partition_mem_table(
    snapshot: &(impl PartitionAddActionsProvider + ?Sized),
) -> DeltaResult<Option<MemTable>> {
    let add_action_batches = snapshot.add_actions_partition_batches()?;
    if add_action_batches.is_empty() {
        return Ok(None);
    }

    let first_schema = add_action_batches[0].schema();
    let mut projected_columns = Vec::with_capacity(first_schema.fields().len());
    projected_columns.push((
        "path".to_string(),
        Field::new(PATH_COLUMN, DataType::Utf8, false),
    ));
    for field in first_schema.fields() {
        if let Some(name) = field.name().strip_prefix("partition.") {
            projected_columns.push((
                field.name().to_string(),
                field.as_ref().clone().with_name(name),
            ));
        }
    }

    let projected_schema = Arc::new(Schema::new(
        projected_columns
            .iter()
            .map(|(_, field)| field.clone())
            .collect::<Vec<_>>(),
    ));

    let mut projected_batches = Vec::with_capacity(add_action_batches.len());
    for batch in add_action_batches {
        let projected_arrays = projected_columns
            .iter()
            .map(|(source_column_name, _)| {
                batch
                    .column_by_name(source_column_name)
                    .cloned()
                    .ok_or_else(|| {
                        DeltaTableError::Generic(format!(
                            "Column with name `{source_column_name}` does not exist"
                        ))
                    })
            })
            .collect::<DeltaResult<Vec<_>>>()?;
        projected_batches.push(RecordBatch::try_new(
            projected_schema.clone(),
            projected_arrays,
        )?);
    }

    Ok(Some(MemTable::try_new(
        projected_schema,
        vec![projected_batches],
    )?))
}

async fn scan_memory_table(snapshot: &EagerSnapshot, predicate: &Expr) -> DeltaResult<Vec<Add>> {
    let actions = snapshot.log_data().iter().map(|f| f.to_add()).collect_vec();

    let Some(mem_table) = add_actions_partition_mem_table(snapshot)? else {
        return Ok(vec![]);
    };

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
    log_store: LogStoreRef,
    predicate: Expr,
) -> Result<Option<MatchedFilesScan>> {
    let Some(matches) = collect_matching_files(
        session,
        snapshot,
        Some(&log_store),
        predicate,
        FILE_ID_COLUMN_DEFAULT,
    )
    .await?
    else {
        return Ok(None);
    };
    let MatchingFilesScanSeed {
        valid_files,
        file_skipping_predicates,
        predicate,
        delta_predicate,
    } = matches;

    // Create a table scan limited to the matched files by forwarding an explicit
    // file selection into the table provider.
    let table_root = snapshot
        .snapshot()
        .scan_builder()
        .build()?
        .table_root()
        .clone();
    let file_selection = FileSelection::from_file_paths(
        valid_files
            .iter()
            .flat_map(|arr| arr.iter().flatten().map(|v| v.to_string())),
        &table_root,
    )?
    .with_missing_file_policy(MissingFilePolicy::Ignore);
    let selected_provider = DeltaScanNext::builder()
        .with_snapshot(snapshot.snapshot().clone())
        .with_file_skipping_predicates(file_skipping_predicates)
        .with_file_column(FILE_ID_COLUMN_DEFAULT)
        .with_log_store(log_store)
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
    }))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{col, lit};
    use delta_kernel::schema::{DataType, PrimitiveType, StructField};

    use crate::{
        DeltaTable, DeltaTableBuilder,
        delta_datafusion::{
            DataFusionMixins as _, DeltaSessionExt as _, create_session, resolve_file_column_name,
        },
        protocol::SaveMode,
        test_utils::{TestResult, multibatch_add_actions_for_partition, open_fs_path},
        writer::test_utils::{get_delta_schema, get_record_batch},
    };

    use super::*;

    #[tokio::test]
    async fn test_scan_files_where_matches_plan_can_be_planned() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/simple_table");
        table.load().await?;

        let ctx = create_session().into_inner();
        let session = ctx.state();
        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();
        let predicate = col("id").gt(lit(-1i64));
        let Some(scan) =
            scan_files_where_matches(&session, &snapshot, log_store, predicate).await?
        else {
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
        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();
        let predicate = col("id").gt(lit(-1i64));
        let Some(scan) =
            scan_files_where_matches(&session, &snapshot, log_store, predicate).await?
        else {
            panic!("Expected at least one matching file");
        };

        let plan_debug = format!("{:?}", scan.scan());
        assert!(
            !(plan_debug.contains("InList(") && plan_debug.contains(FILE_ID_COLUMN_DEFAULT)),
            "unexpected plan with file-id IN filter: {plan_debug}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_files_scan_honors_data_and_file_column_predicate() -> TestResult {
        let mut table = open_fs_path("../test/tests/data/simple_table");
        table.load().await?;

        let ctx = create_session().into_inner();
        let session = ctx.state();
        table.update_datafusion_session(&session)?;

        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();
        session.ensure_log_store_registered(log_store.as_ref())?;
        let file_column_name = resolve_file_column_name(snapshot.input_schema().as_ref(), None)?;

        let by_id = find_files_scan(
            &snapshot,
            log_store.clone(),
            &session,
            col("id").eq(lit(7i64)),
        )
        .await?;
        assert!(!by_id.is_empty());
        let expected_path = by_id[0].path.clone();
        let table_root = snapshot
            .snapshot()
            .scan_builder()
            .build()?
            .table_root()
            .clone();
        let expected_file_id = crate::delta_datafusion::normalize_path_as_file_id(
            &expected_path,
            &table_root,
            "find_files test path",
        )?;
        let matched_paths = by_id.iter().map(|add| add.path.as_str()).collect_vec();

        let matches = find_files_scan(
            &snapshot,
            log_store.clone(),
            &session,
            col("id")
                .eq(lit(7i64))
                .and(col(file_column_name.clone()).eq(lit(expected_file_id))),
        )
        .await?;
        assert_eq!(
            matches.iter().map(|add| add.path.as_str()).collect_vec(),
            vec![expected_path.as_str()]
        );

        let other_path = snapshot
            .log_data()
            .iter()
            .map(|add| add.path().to_string())
            .find(|path| !matched_paths.contains(&path.as_str()))
            .expect("expected a non-matching file path");
        let other_file_id = crate::delta_datafusion::normalize_path_as_file_id(
            &other_path,
            &table_root,
            "find_files test path",
        )?;
        let no_matches = find_files_scan(
            &snapshot,
            log_store,
            &session,
            col("id")
                .eq(lit(7i64))
                .and(col(file_column_name).eq(lit(other_file_id))),
        )
        .await?;
        assert!(no_matches.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_find_files_scan_prepares_fresh_session_and_snapshot_without_files() -> TestResult
    {
        let mut base = open_fs_path("../test/tests/data/simple_table");
        base.load().await?;

        let table = DeltaTableBuilder::from_url(base.table_url().clone())?
            .without_files()
            .load()
            .await?;
        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();

        let ctx = create_session().into_inner();
        let session = ctx.state();

        let matches =
            find_files_scan(&snapshot, log_store, &session, col("id").eq(lit(7i64))).await?;

        assert_eq!(matches.len(), 1);
        assert!(matches[0].path.ends_with(".parquet"));

        Ok(())
    }

    #[tokio::test]
    async fn test_find_files_scan_matches_encoded_partition_paths_for_data_predicates() -> TestResult
    {
        let tmp_dir = tempfile::tempdir()?;
        let table_path = std::fs::canonicalize(tmp_dir.path())?;
        let table_url = url::Url::from_directory_path(&table_path)
            .map_err(|_| DeltaTableError::InvalidTableLocation(table_path.display().to_string()))?;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Utf8, true),
            Field::new("price", ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("1 2")])),
                Arc::new(Int64Array::from(vec![Some(10)])),
            ],
        )?;

        let table = DeltaTable::try_from_url(table_url)
            .await?
            .write(vec![batch])
            .with_partition_columns(["id"])
            .await?;

        let ctx = create_session().into_inner();
        let session = ctx.state();

        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();

        let matches =
            find_files_scan(&snapshot, log_store, &session, col("price").eq(lit(10i64))).await?;

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].partition_values.get("id"),
            Some(&Some("1 2".to_string()))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_files_scan_uses_collision_safe_file_column_namespace() -> TestResult {
        let delta_schema = vec![
            StructField::new(
                "id".to_string(),
                DataType::Primitive(PrimitiveType::String),
                false,
            ),
            StructField::new(
                "value".to_string(),
                DataType::Primitive(PrimitiveType::Integer),
                true,
            ),
            StructField::new(
                "modified".to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                PATH_COLUMN.to_string(),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
            StructField::new(
                format!("{PATH_COLUMN}_1"),
                DataType::Primitive(PrimitiveType::String),
                true,
            ),
        ];

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema)
            .await?;
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", ArrowDataType::Utf8, false),
            Field::new("value", ArrowDataType::Int32, true),
            Field::new("modified", ArrowDataType::Utf8, true),
            Field::new(PATH_COLUMN, ArrowDataType::Utf8, true),
            Field::new(format!("{PATH_COLUMN}_1"), ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B"])),
                Arc::new(arrow::array::Int32Array::from(vec![20])),
                Arc::new(arrow::array::StringArray::from(vec!["2021-03-01"])),
                Arc::new(arrow::array::StringArray::from(vec!["beta-src"])),
                Arc::new(arrow::array::StringArray::from(vec!["beta-src-1"])),
            ],
        )?;
        let table = table
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await?;

        let ctx = create_session().into_inner();
        let session = ctx.state();
        table.update_datafusion_session(&session)?;

        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();
        session.ensure_log_store_registered(log_store.as_ref())?;

        let file_column_name = resolve_file_column_name(snapshot.input_schema().as_ref(), None)?;
        assert_eq!(file_column_name, format!("{PATH_COLUMN}_2"));

        let matches = find_files_scan(
            &snapshot,
            log_store,
            &session,
            col("id")
                .eq(lit("B"))
                .and(col(file_column_name).is_not_null()),
        )
        .await?;

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches[0].path,
            snapshot.log_data().iter().next().unwrap().path()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_files_where_matches_prepares_fresh_session_and_snapshot_without_files()
    -> TestResult {
        let mut base = open_fs_path("../test/tests/data/simple_table");
        base.load().await?;

        let table = DeltaTableBuilder::from_url(base.table_url().clone())?
            .without_files()
            .load()
            .await?;
        let snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();

        let ctx = create_session().into_inner();
        let session = ctx.state();

        let Some(scan) =
            scan_files_where_matches(&session, &snapshot, log_store, col("id").eq(lit(7i64)))
                .await?
        else {
            panic!("Expected at least one matching file");
        };

        let plan = session.create_physical_plan(scan.scan()).await?;
        let data = collect(plan, session.task_ctx()).await?;
        assert!(!data.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_memory_table_returns_empty_for_empty_table() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .await?;
        let predicate = col("modified").eq(lit("2021-02-02"));
        let matches = scan_memory_table(table.snapshot()?.snapshot(), &predicate).await?;
        assert!(matches.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_memory_table_filters_partition_values() -> TestResult {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .await?;
        let table = table
            .write(vec![get_record_batch(None, false)])
            .with_save_mode(SaveMode::Append)
            .await?;

        let snapshot = table.snapshot()?.snapshot();
        let total_actions = snapshot.log_data().iter().count();
        let predicate = col("modified").eq(lit("2021-02-02"));
        let matches = scan_memory_table(snapshot, &predicate).await?;

        assert!(!matches.is_empty());
        assert!(matches.len() < total_actions);
        Ok(())
    }

    #[tokio::test]
    async fn test_scan_memory_table_multibatch_stress_partition_filtering() -> TestResult {
        use std::collections::HashSet;

        let action_count = 9000usize;
        let expected_matches = action_count / 2;
        let actions = multibatch_add_actions_for_partition(
            action_count,
            "modified",
            "2021-02-02",
            "2021-02-03",
        );

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(get_delta_schema().fields().cloned())
            .with_partition_columns(["modified"])
            .with_actions(actions)
            .await?;

        let snapshot = table.snapshot()?.snapshot();
        let add_action_batches = snapshot.add_actions_partition_batches()?;
        assert!(
            add_action_batches.len() > 1,
            "expected multi-batch partition metadata fixture"
        );

        let predicate = col("modified").eq(lit("2021-02-02"));
        let matches = scan_memory_table(snapshot, &predicate).await?;

        assert_eq!(matches.len(), expected_matches);

        let match_paths = matches
            .iter()
            .map(|add| add.path.clone())
            .collect::<HashSet<_>>();
        let expected_paths = (0..action_count)
            .filter(|idx| idx % 2 == 0)
            .map(|idx| format!("modified=2021-02-02/file-{idx:05}.parquet"))
            .collect::<HashSet<_>>();
        assert_eq!(match_paths, expected_paths);
        Ok(())
    }

    struct MockPartitionProvider {
        batches: Vec<RecordBatch>,
    }

    impl PartitionAddActionsProvider for MockPartitionProvider {
        fn add_actions_partition_batches(&self) -> DeltaResult<Vec<RecordBatch>> {
            Ok(self.batches.clone())
        }
    }

    #[test]
    fn test_add_actions_partition_mem_table_accepts_generic_provider() -> DeltaResult<()> {
        let provider = MockPartitionProvider {
            batches: Vec::new(),
        };
        let mem_table = add_actions_partition_mem_table(&provider)?;
        assert!(mem_table.is_none());
        Ok(())
    }
}
