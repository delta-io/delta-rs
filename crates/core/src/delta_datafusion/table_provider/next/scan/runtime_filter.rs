//! Whole-file pruning that is decided while a query runs.
//!
//! Some predicates are only known during execution. For example, a MERGE executed in streaming
//! fashion learns the key range of its source only after it has read the whole source.
//! A [`RuntimeFileFilter`] passes this information as predicates to a scan
//! that is already planned, which allows it to apply additional predicates to the file scan.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::Gauge;
use datafusion::prelude::Expr;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use delta_kernel::Engine;
use parking_lot::Mutex;
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
/// It then reads a copy of its Parquet input that has only the kept files, so a skipped file
/// is never opened. Terms that kernel file skipping cannot use are ignored, so the scan keeps
/// every file that may match.
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
    /// Whether to keep each planned file, by file index. `None` keeps all files.
    keep: OnceCell<Option<Vec<bool>>>,
    /// The last input that the scan started with, and its copy
    input_copy: Mutex<Option<InputCopy>>,
}

/// A scan input, and its copy with only the kept files.
struct InputCopy {
    input: Arc<dyn ExecutionPlan>,
    copy: Arc<dyn ExecutionPlan>,
}

impl fmt::Debug for RuntimeScanFilePruner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeScanFilePruner")
            .field("files", &self.file_indexes.len())
            .field("keep", &self.keep)
            .finish_non_exhaustive()
    }
}

impl RuntimeScanFilePruner {
    pub(super) fn new(
        predicates: RuntimeFileFilter,
        snapshot: Snapshot,
        config: DeltaScanConfig,
        engine: Arc<dyn Engine>,
        files: &[ScanFileContext],
        files_scanned: Gauge,
    ) -> Self {
        let file_indexes = files
            .iter()
            .enumerate()
            .map(|(file_index, file)| (file.file_url.to_string(), file_index))
            .collect();
        Self {
            predicates,
            snapshot,
            config,
            engine,
            file_indexes,
            files_scanned,
            keep: OnceCell::new(),
            input_copy: Mutex::new(None),
        }
    }

    /// Return the scan plan to read: `input` with only the kept files.
    ///
    /// Each partition of the scan calls this when it is first polled, before it executes its
    /// part of the input. The partitions start at different times, and the predicates can be set
    /// between two starts. So all calls must return the same plan:
    /// - The first call decides if and which files need to be kept, and later calls reuse
    ///   that decision. This needs to be set once so all partitions are in sync.
    /// - Calls with the same `input` return the same copy. A Parquet `DataSourceExec` keeps its
    ///   queue of files in the node itself, and all partitions that execute that node take files
    ///   from it. A new copy is a new node with a new, full queue, so a copy for each partition
    ///   would read each kept file once per partition.
    /// - A new `input`, for example after the plan is reset for a second execution, gets a new
    ///   copy with the same kept files, because the queue of the old copy is empty.
    pub(super) async fn maybe_prune_input(
        &self,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(keep) = self.keep.get_or_init(|| self.select_files()).await else {
            return Ok(input);
        };
        // Check and build under one lock, so that two partitions cannot both build a copy.
        let mut input_copy = self.input_copy.lock();

        // The same node object: another partition of this run. Share its copy, and its queue.
        if let Some(last) = input_copy.as_ref()
            && Arc::ptr_eq(&last.input, &input)
        {
            return Ok(Arc::clone(&last.copy));
        }

        // The first call of a run, or a new `input` after a reset: build a copy with the same kept
        // files. The old copy has used up its queue.
        let copy = rewrite_parquet_nodes_by_pruning(Arc::clone(&input), keep)?;
        *input_copy = Some(InputCopy {
            input,
            copy: Arc::clone(&copy),
        });
        Ok(copy)
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
            if let Some(&file_index) = self.file_indexes.get(file_url.as_str()) {
                keep[file_index] = true;
            }
            true
        })
        .await?;
        Ok(keep)
    }
}

/// `plan` with only the files in `keep` in its Parquet scans.
///
/// Empty file groups stay, so the partitions of `plan` do not change.
fn rewrite_parquet_nodes_by_pruning(
    plan: Arc<dyn ExecutionPlan>,
    keep: &[bool],
) -> Result<Arc<dyn ExecutionPlan>> {
    // The only partition column of the Parquet scan is the file id, checked below.
    let keeps = |file: &&PartitionedFile| {
        file.partition_values
            .first()
            .and_then(|value| value.try_as_str().flatten())
            .and_then(super::internal_file_index)
            .is_none_or(|file_index| keep.get(file_index).copied().unwrap_or(true))
    };
    plan.transform_up(|node| {
        let Some(config) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|source| source.data_source().downcast_ref::<FileScanConfig>())
        else {
            return Ok(Transformed::no(node));
        };
        let partition_columns = config.table_partition_cols().len();
        if partition_columns != 1 {
            return Err(internal_datafusion_err!(
                "DeltaScanExec runtime file pruning requires the file id as the only partition column, got {partition_columns}"
            ));
        }
        let file_groups = config
            .file_groups
            .iter()
            .map(|group| group.iter().filter(keeps).cloned().collect::<FileGroup>())
            .collect();
        let config = FileScanConfigBuilder::from(config.clone())
            .with_file_groups(file_groups)
            .build();
        Ok(Transformed::yes(
            DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
        ))
    })
    .map(|plan| plan.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_datafusion::file_id::{file_id_field, wrap_file_id_value};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::ParquetSource;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion_datasource::TableSchema;
    use rstest::rstest;

    fn planned_file(file_index: usize) -> PartitionedFile {
        let mut file = PartitionedFile::new(format!("file-{file_index}.parquet"), 10);
        file.partition_values = vec![wrap_file_id_value(super::super::compact_internal_file_id(
            file_index,
        ))];
        file
    }

    /// The planned file groups are `[file-0, file-1]` and `[file-2]`.
    #[rstest]
    #[case::first_file(&[true, false, false], vec![vec!["file-0.parquet"], vec![]])]
    #[case::first_group(&[true, true, false], vec![vec!["file-0.parquet", "file-1.parquet"], vec![]])]
    #[case::second_file(&[false, true, false], vec![vec!["file-1.parquet"], vec![]])]
    #[case::second_group(&[false, false, true], vec![vec![], vec!["file-2.parquet"]])]
    #[case::one_file_per_group(&[true, false, true], vec![vec!["file-0.parquet"], vec!["file-2.parquet"]])]
    fn rewrite_parquet_nodes_by_pruning_removes_skipped_files_and_keeps_partitions(
        #[case] keep: &[bool],
        #[case] expected_groups: Vec<Vec<&str>>,
    ) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let table_schema = TableSchema::builder(schema)
            .with_table_partition_cols(vec![file_id_field(None)])
            .build();
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(table_schema)),
        )
        .with_file_groups(vec![
            FileGroup::new(vec![planned_file(0), planned_file(1)]),
            FileGroup::new(vec![planned_file(2)]),
        ])
        .build();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(
            DataSourceExec::from_data_source(config),
        ));

        let plan = rewrite_parquet_nodes_by_pruning(plan, keep).unwrap();

        let source = plan.children()[0].downcast_ref::<DataSourceExec>().unwrap();
        let config = source
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .unwrap();
        let files: Vec<Vec<String>> = config
            .file_groups
            .iter()
            .map(|group| {
                group
                    .iter()
                    .map(|file| file.object_meta.location.to_string())
                    .collect()
            })
            .collect();
        assert_eq!(files, expected_groups);
        assert_eq!(
            plan.children()[0].output_partitioning().partition_count(),
            2
        );
    }
}
