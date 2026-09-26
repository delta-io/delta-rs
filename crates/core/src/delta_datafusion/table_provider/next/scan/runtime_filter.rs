//! Whole-file pruning that is decided while a query runs.
//!
//! Some predicates are only known during execution. For example, a MERGE executed in streaming
//! fashion learns the key range of its source only after it has read the whole source.
//! A [`RuntimeFileFilter`] passes this information as predicates to a scan
//! that is already planned, which allows it to apply additional predicates to the file scan.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue, internal_datafusion_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr, lit};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Gauge;
use datafusion::prelude::Expr;
use delta_kernel::Engine;
use tokio::sync::OnceCell;
use tracing::warn;

use super::plan::process_filters;
use super::replay::ScanFileContext;
use crate::DeltaResult;
use crate::delta_datafusion::DeltaScanConfig;
use crate::kernel::{FileStatsMaterialization, Snapshot, StatsProjection};

/// Predicates that skip whole files of planned Delta scans, set once during execution.
///
/// A scan reads the filter once, when it is first polled, and does not wait for a value. When
/// the filter has predicates, the scan runs kernel file skipping with them.
/// It then sets the kept files in a dynamic filter on the file id column of its Parquet input,
/// so the Parquet scan skips a file before it reads the footer. Terms that kernel file skipping
/// cannot use are ignored, so the scan keeps every file that may match.
///
/// Predicates that are set after a scan started have no effect on it. So they skip files only
/// in scans that are polled later, for example the probe side of a hash join that builds on the
/// input that the predicates come from.
///
/// The predicates only remove whole files, never rows. This makes them safe for scans that must
/// read every row of a file that has a match, such as the target scan of a MERGE.
pub(crate) type RuntimeFileFilter = Arc<OnceLock<Vec<Expr>>>;

/// Prunes the planned files of one scan with the predicates of a [`RuntimeFileFilter`], when the
/// scan starts.
pub(super) struct RuntimeScanFilePruner {
    predicates: RuntimeFileFilter,
    snapshot: Snapshot,
    config: DeltaScanConfig,
    engine: Arc<dyn Engine>,
    /// Index of each planned file, keyed by file URL
    file_indexes: HashMap<String, usize>,
    /// The `count_files_scanned` metric of the scan
    files_scanned: Gauge,
    /// The file id column of the Parquet input
    file_id: Arc<dyn PhysicalExpr>,
    /// Predicate of the Parquet input. It keeps all files until the scan starts, then only the
    /// kept files.
    filter: Arc<DynamicFilterPhysicalExpr>,
    /// Set when the first partition has decided the kept files
    started: OnceCell<()>,
}

impl fmt::Debug for RuntimeScanFilePruner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeScanFilePruner")
            .field("files", &self.file_indexes.len())
            .field("filter", &self.filter)
            .finish_non_exhaustive()
    }
}

impl RuntimeScanFilePruner {
    /// `file_id` is the file id column of the Parquet input.
    pub(super) fn new(
        predicates: RuntimeFileFilter,
        snapshot: Snapshot,
        config: DeltaScanConfig,
        engine: Arc<dyn Engine>,
        files: &[ScanFileContext],
        files_scanned: Gauge,
        file_id: Column,
    ) -> Self {
        let file_indexes = files
            .iter()
            .enumerate()
            .map(|(file_index, file)| (file.file_url.to_string(), file_index))
            .collect();
        let file_id: Arc<dyn PhysicalExpr> = Arc::new(file_id);
        let filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&file_id)],
            lit(true),
        ));
        Self {
            predicates,
            snapshot,
            config,
            engine,
            file_indexes,
            files_scanned,
            file_id,
            filter,
            started: OnceCell::new(),
        }
    }

    /// The predicate to set on the Parquet input of the scan.
    pub(super) fn predicate(&self) -> Arc<dyn PhysicalExpr> {
        Arc::clone(&self.filter) as _
    }

    /// Decide the kept files, and set them in the predicate of the Parquet input.
    ///
    /// Each partition of the scan calls this when it is first polled, before it executes its
    /// part of the input. The first call decides, and later calls wait for it. All partitions
    /// execute the same input, so they all see the same kept files.
    pub(super) async fn start(&self) -> Result<()> {
        self.started
            .get_or_try_init(|| async {
                if let Some(keep) = self.select_files().await {
                    self.filter.update(Arc::new(KeptFilesExpr {
                        file_id: Arc::clone(&self.file_id),
                        keep: Arc::new(keep),
                    }))?;
                }
                // No later update: the Parquet scan does not need to check the filter again.
                self.filter.mark_complete();
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// Whether to keep each planned file, for the predicates in the filter. Returns `None`
    /// when no file is skipped.
    async fn select_files(&self) -> Option<Vec<bool>> {
        // Never wait: when this scan is the build side of a join, the probe side sets the
        // predicates only after this scan ends.
        let predicates = self.predicates.get()?;
        let keep = match self.matching_files(predicates).await {
            Ok(keep) => keep,
            Err(err) => {
                warn!("Could not skip files with runtime predicates, reading all files: {err}");
                return None;
            }
        };

        let skipped = keep.iter().filter(|keep| !**keep).count();
        if skipped == 0 {
            return None;
        }
        self.files_scanned.sub(skipped);
        Some(keep)
    }

    /// Whether kernel file skipping keeps each planned file for `predicates`, by file index.
    ///
    /// Keeps all files when no term of `predicates` can be used for file skipping.
    async fn matching_files(&self, predicates: &[Expr]) -> DeltaResult<Vec<bool>> {
        let snapshot = &self.snapshot;
        let (predicate, _) =
            process_filters(predicates, snapshot.table_configuration(), &self.config)?;
        let Some(predicate) = predicate else {
            return Ok(vec![true; self.file_indexes.len()]);
        };
        // Only the file paths are read
        let scan = snapshot
            .scan_builder()
            .with_predicate(predicate)
            // we don't parse stats beyond what file skipping needs
            .with_stats_materialization(FileStatsMaterialization::query(StatsProjection::none()))
            .build()?;
        let stream =
            scan.scan_metadata_seeded(Arc::clone(&self.engine), snapshot.materialized_files());

        let mut keep = vec![false; self.file_indexes.len()];
        super::for_each_selected_file(scan.table_root(), stream, |file_url| {
            // This scan applies only the runtime predicates, so it can also return files that
            // planning skipped. Therefore `keep` refers to the intersection of the two:
            // kept files = 'planned files' ∩ 'matching runtime files'.
            if let Some(&file_index) = self.file_indexes.get(file_url.as_str()) {
                keep[file_index] = true;
            }
            true
        })
        .await?;
        Ok(keep)
    }
}

/// `true` for the rows of the kept files, by the file id column of the Parquet input.
///
/// The Parquet scan replaces the file id column with the id of the file that it opens, so the
/// expression is constant for each file. The scan then skips a whole file, and row-level uses of
/// the predicate, such as a row filter, never remove single rows.
///
/// This is not an `IN` list, because the Parquet scan rewrites the predicate for each file, and
/// an `IN` list copies all its values in each rewrite.
#[derive(Debug, Eq)]
struct KeptFilesExpr {
    file_id: Arc<dyn PhysicalExpr>,
    /// Whether to keep each planned file, by file index
    keep: Arc<Vec<bool>>,
}

impl PartialEq for KeptFilesExpr {
    fn eq(&self, other: &Self) -> bool {
        self.file_id.as_ref() == other.file_id.as_ref() && self.keep == other.keep
    }
}

impl Hash for KeptFilesExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.file_id.as_ref().hash(state);
        self.keep.hash(state);
    }
}

impl KeptFilesExpr {
    fn keeps(&self, file_id: &ScalarValue) -> bool {
        // Keep a file with an unknown id, because it may match
        file_id
            .try_as_str()
            .flatten()
            .and_then(super::internal_file_index)
            .is_none_or(|file_index| self.keep.get(file_index).copied().unwrap_or(true))
    }
}

impl fmt::Display for KeptFilesExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} IN kept files", self.file_id)
    }
}

impl PhysicalExpr for KeptFilesExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(match self.file_id.evaluate(batch)? {
            ColumnarValue::Scalar(file_id) => {
                ColumnarValue::Scalar(ScalarValue::from(self.keeps(&file_id)))
            }
            ColumnarValue::Array(file_ids) => ColumnarValue::Array(Arc::new(
                (0..file_ids.len())
                    .map(|row| {
                        ScalarValue::try_from_array(&file_ids, row)
                            .map(|file_id| Some(self.keeps(&file_id)))
                    })
                    .collect::<Result<BooleanArray>>()?,
            )),
        })
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.file_id]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let file_id = children
            .into_iter()
            .next()
            .ok_or_else(|| internal_datafusion_err!("KeptFilesExpr expects one child"))?;
        Ok(Arc::new(Self {
            file_id,
            keep: Arc::clone(&self.keep),
        }))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray, DictionaryArray};
    use arrow::datatypes::UInt16Type;
    use rstest::rstest;

    use super::*;
    use crate::delta_datafusion::file_id::{file_id_field, wrap_file_id_value};

    /// Keeps file 0 and skips file 1. An unknown file id is not
    /// filtered out
    #[rstest]
    #[case::kept("0", true)]
    #[case::skipped("1", false)]
    #[case::out_of_range("7", true)]
    #[case::not_a_file_index("x", true)]
    fn kept_files_expr_keeps_file(#[case] file_id: &str, #[case] expected: bool) -> Result<()> {
        let field = file_id_field(None);
        let expr: Arc<dyn PhysicalExpr> = Arc::new(KeptFilesExpr {
            file_id: Arc::new(Column::new(field.name(), 0)),
            keep: Arc::new(vec![true, false]),
        });

        let literal =
            Arc::clone(&expr).with_new_children(vec![lit(wrap_file_id_value(file_id))])?;

        let file_ids: DictionaryArray<UInt16Type> = [file_id].into_iter().collect();
        let batch =
            RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(file_ids)])?;

        // The column gives an array evaluation, the literal a scalar eval
        for expr in [expr, literal] {
            let keeps = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(keeps.as_boolean(), &BooleanArray::from(vec![expected]));
        }
        Ok(())
    }
}
