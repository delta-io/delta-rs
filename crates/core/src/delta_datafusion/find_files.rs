use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::StringViewType;
use arrow_array::GenericByteViewArray;
use arrow_schema::DataType;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{HashSet, Result, exec_datafusion_err, plan_err};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Volatility, col};
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::physical_plan::collect_partitioned;
use datafusion::prelude::{cast, lit};
use datafusion::scalar::ScalarValue;
use datafusion_datasource::file_scan_config::wrap_partition_value_in_dict;
use delta_kernel::Predicate;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use itertools::Itertools as _;
use url::Url;

use crate::delta_datafusion::engine::to_delta_predicate;
use crate::delta_datafusion::logical::LogicalPlanBuilderExt as _;
use crate::delta_datafusion::{DeltaScanNext, FILE_ID_COLUMN_DEFAULT};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Action, EagerSnapshot};
use crate::logstore::LogStore;

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

    pub(crate) async fn remove_actions(
        &self,
        log_store: &dyn LogStore,
        snapshot: &EagerSnapshot,
    ) -> Result<Vec<Action>> {
        let root_url = Arc::new(snapshot.table_configuration().table_root().clone());
        Ok(snapshot
            .file_views(log_store, Some(self.delta_predicate.clone()))
            .zip(stream::iter(std::iter::repeat((
                root_url,
                Arc::new(self.files_set()),
            ))))
            .map(|(f, u)| f.map(|f| (f, u)))
            .try_filter_map(|(f, (root, valid))| async move {
                let url = path_to_url(root.as_ref(), f.path_raw())
                    .map_err(|e| exec_datafusion_err!("{e}"))?;
                Ok(valid.contains(&url).then(|| f.remove_action(true).into()))
            })
            .try_collect()
            .await?)
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
/// If no files contaon matching data, `None` is returned.
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
    // and with the source file path exosed as column.
    let table_source = provider_as_source(
        DeltaScanNext::builder()
            .with_eager_snapshot(snapshot.clone())
            .with_file_skipping_predicates(skipping_pred.clone())
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .await?,
    );

    // the kernel scan only provides a best effort file skipping, in this case
    // we want to determine the file we certainly need to rewrite. For this
    // we perform an initial aggreagte scan to see if we can quickly find
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

    // Crate a table scan limiting the data to that originating from valid files.
    let file_list = valid_files
        .iter()
        .flat_map(|arr| arr.iter().flatten().map(|v| v.to_string()))
        .map(|v| lit(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(v)))))
        .collect_vec();
    let plan = LogicalPlanBuilder::scan("source", table_source, None)?
        .filter(col(FILE_ID_COLUMN_DEFAULT).in_list(file_list, false))?
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

pub(crate) async fn scan_file_list(
    snapshot: &EagerSnapshot,
    file_list: impl IntoIterator<Item = impl ToString>,
) -> Result<LogicalPlan> {
    let table_source = provider_as_source(
        DeltaScanNext::builder()
            .with_eager_snapshot(snapshot.clone())
            .with_file_column(FILE_ID_COLUMN_DEFAULT)
            .await?,
    );

    // Crate a table scan limiting the data to that originating from valid files.
    let root_url = snapshot.table_configuration().table_root();
    let file_list_expr = file_list
        .into_iter()
        .map(|p| path_to_url(root_url, p.to_string()))
        .map_ok(|v| lit(wrap_partition_value_in_dict(ScalarValue::Utf8(Some(v)))))
        .try_collect()?;
    let plan = LogicalPlanBuilder::scan("source", table_source, None)?
        .filter(col(FILE_ID_COLUMN_DEFAULT).in_list(file_list_expr, false))?
        .drop_columns([FILE_ID_COLUMN_DEFAULT])?
        .build()?;

    Ok(plan)
}

fn path_to_url(root: &Url, path: impl AsRef<str>) -> Result<String> {
    if let Ok(url) = root.join(path.as_ref()) {
        return Ok(url.to_string());
    }
    if let Ok(url) = Url::parse(path.as_ref()) {
        return Ok(url.to_string());
    }
    plan_err!(
        "Failed to construct URL from root '{root}' and path '{}'",
        path.as_ref()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_url() {
        let root = Url::parse("s3://my-bucket/delta-table/").unwrap();
        let path = "part-00001-abcde.parquet";
        let url = path_to_url(&root, path).unwrap();
        assert_eq!(url, "s3://my-bucket/delta-table/part-00001-abcde.parquet");

        let path = "s3://other-bucket/other-table/part-00002-fghij.parquet";
        let url = path_to_url(&root, path).unwrap();
        assert_eq!(
            url,
            "s3://other-bucket/other-table/part-00002-fghij.parquet"
        );
    }
}
