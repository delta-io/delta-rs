//! File metadata replay and statistics extraction.
//!
//! This module processes Delta Kernel's scan metadata stream, extracting file information,
//! loading deletion vectors, and computing statistics for query planning. It bridges the
//! gap between kernel-level file metadata and DataFusion's execution requirements.

use std::{
    collections::HashSet,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{array::BooleanArray, compute::filter_record_batch};
use arrow_array::RecordBatch;
use datafusion::{
    common::{
        ColumnStatistics, HashMap, Statistics, error::DataFusionErrorBuilder, stats::Precision,
    },
    error::DataFusionError,
    scalar::ScalarValue,
};
use delta_kernel::{
    Engine, ExpressionRef,
    engine::{arrow_conversion::TryIntoArrow, arrow_data::ArrowEngineData},
    engine_data::FilteredEngineData,
    expressions::{Scalar, StructData},
    scan::{
        Scan as KernelScan, ScanMetadata,
        state::{DvInfo, ScanFile},
    },
};
use futures::Stream;
use itertools::Itertools;
use pin_project_lite::pin_project;
use url::Url;

use crate::{
    DeltaResult, DeltaTableError,
    delta_datafusion::{DeltaScanConfig, engine::to_datafusion_scalar},
    kernel::{
        LogicalFileView, ReceiverStreamBuilder, Scan, StatsProjection, StructDataExt,
        parse_stats_column_with_schema,
    },
};

#[derive(Debug)]
pub(crate) struct ReplayStats {
    pub(crate) num_scanned: usize,
}

impl ReplayStats {
    fn new() -> Self {
        Self { num_scanned: 0 }
    }
}

pin_project! {
    /// Stream that processes kernel scan metadata into file contexts with statistics.
    ///
    /// This stream consumes [`ScanMetadata`] from Delta Kernel's scan and produces
    /// [`ScanFileContext`] entries enriched with:
    ///
    /// - **File statistics**: Row counts, min/max values, null counts
    /// - **Deletion vectors**: Asynchronously loaded and cached
    /// - **Partition values**: Extracted from file metadata
    /// - **Transforms**: Column mapping expressions for physical-to-logical translation
    pub(crate) struct ScanFileStream<'a, S> {
        pub(crate) metrics: ReplayStats,

        engine: Arc<dyn Engine>,

        table_root: Url,

        kernel_scan: Arc<KernelScan>,

        scan_config: DeltaScanConfig,

        file_selection: Option<&'a HashSet<String>>,

        pub(crate) dv_stream: ReceiverStreamBuilder<(Url, Option<Vec<bool>>, Option<u64>)>,

        #[pin]
        stream: S,
    }
}

impl<'a, S> ScanFileStream<'a, S> {
    pub(crate) fn new(
        engine: Arc<dyn Engine>,
        scan: &Arc<Scan>,
        scan_config: DeltaScanConfig,
        file_selection: Option<&'a HashSet<String>>,
        stream: S,
    ) -> Self {
        Self {
            metrics: ReplayStats::new(),
            dv_stream: ReceiverStreamBuilder::<(Url, Option<Vec<bool>>, Option<u64>)>::new(100),
            engine,
            table_root: scan.table_root().clone(),
            kernel_scan: scan.inner().clone(),
            stream,
            scan_config,
            file_selection,
        }
    }
}

impl<'a, S> Stream for ScanFileStream<'a, S>
where
    S: Stream<Item = DeltaResult<ScanMetadata>>,
{
    type Item = DeltaResult<Vec<ScanFileContext>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let physical_arrow = this
            .kernel_scan
            .physical_schema()
            .as_ref()
            .try_into_arrow()
            .map_err(DeltaTableError::from);
        let physical_arrow = match physical_arrow {
            Ok(schema) => schema,
            Err(err) => return Poll::Ready(Some(Err(err))),
        };
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(scan_data))) => {
                let scan_data = if let Some(selection) = this.file_selection {
                    match apply_file_selection(scan_data, this.table_root, selection) {
                        Ok(scan_data) => scan_data,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    }
                } else {
                    scan_data
                };

                let ctx = match scan_data
                    .visit_scan_files(ScanContext::new(this.table_root.clone()), visit_scan_file)
                    .map_err(|err| DataFusionError::from(DeltaTableError::from(err)))
                    .and_then(ScanContext::error_or)
                {
                    Ok(ctx) => ctx,
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                };

                // Spawn tasks to read the deletion vectors from disk.
                for file in &ctx.files {
                    if file.dv_info.has_vector() {
                        let engine = this.engine.clone();
                        let dv_info = file.dv_info.clone();
                        let file_url = file.file_url.clone();
                        let num_records = file.num_records;
                        let table_root = this.table_root.clone();
                        let tx = this.dv_stream.tx();

                        let load_dv = move || {
                            let dv = dv_info.get_selection_vector(engine.as_ref(), &table_root)?;
                            let _ = tx.blocking_send(Ok((file_url, dv, num_records)));
                            Ok(())
                        };
                        this.dv_stream.spawn_blocking(load_dv);
                    }
                }

                this.metrics.num_scanned += ctx.count;

                let (data, selection_vector) = scan_data.scan_files.into_parts();
                let batch = ArrowEngineData::try_from_engine_data(data)?.into();
                let scan_files =
                    filter_record_batch(&batch, &BooleanArray::from(selection_vector))?;

                let stats_projection = match StatsProjection::for_scan(this.kernel_scan.as_ref()) {
                    Ok(projection) => projection,
                    Err(err) => return Poll::Ready(Some(Err(err))),
                };
                let snapshot = this.kernel_scan.snapshot();
                let stats_schema = match stats_projection.stats_schema(snapshot) {
                    Ok(schema) => schema,
                    Err(err) => return Poll::Ready(Some(Err(err))),
                };

                // Parse statistics (will skip parsing for unreferenced columns)
                let parsed_stats =
                    parse_stats_column_with_schema(snapshot.as_ref(), &scan_files, stats_schema)?;

                let mut file_statistics = extract_file_statistics(
                    this.kernel_scan,
                    this.scan_config,
                    parsed_stats,
                    &stats_projection,
                );

                Poll::Ready(Some(Ok(ctx
                    .files
                    .into_iter()
                    .map(|ctx| {
                        let (stats, partitions) = file_statistics
                            .remove(&ctx.file_url)
                            .unwrap_or_else(|| (Statistics::new_unknown(&physical_arrow), None));
                        ScanFileContext::new(ctx, stats, partitions)
                    })
                    .collect_vec())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Extracts DataFusion statistics from parsed file metadata.
///
/// Convert Delta Kernel file statistics into DataFusion [`Statistics`].
///
/// Only statistics for predicate columns are extracted.
///
/// The scan provides schema and predicate information. `parsed_stats` contains parsed
/// statistics for all files. `stats_projection` names the stats fields materialized
/// for this scan.
///
/// # Returns
///
/// A map from file URL to DataFusion statistics and optional partition values.
fn extract_file_statistics(
    scan: &KernelScan,
    scan_config: &DeltaScanConfig,
    parsed_stats: RecordBatch,
    stats_projection: &StatsProjection,
) -> HashMap<Url, (Statistics, Option<StructData>)> {
    (0..parsed_stats.num_rows())
        .map(move |idx| LogicalFileView::new(parsed_stats.clone(), idx))
        .filter_map(|view| {
            let num_rows = view
                .num_records()
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent);
            let total_byte_size = Precision::Exact(view.size() as usize);

            let null_counts = extract_struct(view.null_counts());
            let max_values = extract_struct(view.max_values());
            let min_values = extract_struct(view.min_values());

            let column_statistics = scan
                .physical_schema()
                .fields()
                .map(|f| {
                    let should_extract_stats =
                        stats_projection.emits_top_level_column_stats(f.name());

                    if !should_extract_stats {
                        // Return unknown statistics for non-predicate columns
                        return ColumnStatistics {
                            null_count: Precision::Absent,
                            max_value: Precision::Absent,
                            min_value: Precision::Absent,
                            sum_value: Precision::Absent,
                            distinct_count: Precision::Absent,
                            byte_size: Precision::Absent,
                        };
                    }

                    // Extract statistics for predicate columns
                    let null_count = if let Some(field_index) =
                        null_counts.as_ref().and_then(|v| v.index_of(f.name()))
                    {
                        null_counts
                            .as_ref()
                            .map(|v| match v.values()[field_index] {
                                Scalar::Integer(int_val) => Precision::Exact(int_val as usize),
                                Scalar::Long(long_val) => Precision::Exact(long_val as usize),
                                _ => Precision::Absent,
                            })
                            .unwrap_or_default()
                    } else {
                        Precision::Absent
                    };

                    let max_value =
                        physical_precision(extract_precision(&max_values, f.name()), scan_config);
                    let min_value =
                        physical_precision(extract_precision(&min_values, f.name()), scan_config);

                    ColumnStatistics {
                        null_count,
                        max_value,
                        min_value,
                        sum_value: Precision::Absent,
                        distinct_count: Precision::Absent,
                        byte_size: Precision::Absent,
                    }
                })
                .collect_vec();

            Some((
                parse_path(scan.snapshot().table_root(), view.path_raw()).ok()?,
                (
                    Statistics {
                        num_rows,
                        total_byte_size,
                        column_statistics,
                    },
                    view.partition_values(),
                ),
            ))
        })
        .collect()
}

#[inline]
fn physical_precision(
    precision: Precision<ScalarValue>,
    scan_config: &DeltaScanConfig,
) -> Precision<ScalarValue> {
    match precision {
        Precision::Exact(v) => Precision::Exact(scan_config.map_scalar_value(v)),
        Precision::Inexact(v) => Precision::Inexact(scan_config.map_scalar_value(v)),
        Precision::Absent => Precision::Absent,
    }
}

fn extract_precision(data: &Option<StructData>, name: impl AsRef<str>) -> Precision<ScalarValue> {
    if let Some(field_index) = data.as_ref().and_then(|v| v.index_of(name.as_ref())) {
        data.as_ref()
            .map(|v| match to_datafusion_scalar(&v.values()[field_index]) {
                Ok(df) => Precision::Exact(df),
                _ => Precision::Absent,
            })
            .unwrap_or_default()
    } else {
        Precision::Absent
    }
}

fn extract_struct(scalar: Option<Scalar>) -> Option<StructData> {
    match scalar {
        Some(Scalar::Struct(data)) => Some(data),
        _ => None,
    }
}

/// Enriched metadata for a single data file in a table scan.
///
/// Contains all information needed by DataFusion to read and process a Parquet file
/// as part of a Delta table scan. This includes the file location, size, statistics,
/// and any transformations required by the Delta protocol.
///
/// # Fields
///
/// - [`file_url`](Self::file_url): Complete URL for reading the file from object storage
/// - [`size`](Self::size): File size in bytes for I/O planning
/// - [`transform`](Self::transform): Expression to apply partition values and column mapping
/// - [`stats`](Self::stats): File-level statistics for predicate pushdown and pruning
/// - [`partitions`](Self::partitions): Partition column values to materialize
///
/// These contexts are created during the scan replay phase and consumed by execution plans.
#[derive(Debug)]
pub(crate) struct ScanFileContext {
    /// Fully qualified URL of the file.
    pub file_url: Url,
    /// Size of the file on disk.
    pub size: u64,
    /// Transformations to apply to the data in the file.
    pub transform: Option<ExpressionRef>,
    /// Statistics about the data in the file.
    ///
    /// The query engine may choose to use these statistics to further optimize the scan.
    pub stats: Statistics,
    /// Partition values for the file.
    pub partitions: Option<StructData>,
}

impl ScanFileContext {
    /// Create a new `ScanFileContext` with the given file URL, size, and statistics.
    fn new(inner: ScanFileContextInner, stats: Statistics, partitions: Option<StructData>) -> Self {
        Self {
            file_url: inner.file_url,
            size: inner.size,
            transform: inner.transform,
            stats,
            partitions,
        }
    }
}

/// Metadata to read a data file from object storage.
struct ScanFileContextInner {
    /// Fully qualified URL of the file.
    pub file_url: Url,
    /// Size of the file on disk.
    pub size: u64,
    /// Transformations to apply to the data in the file.
    pub transform: Option<ExpressionRef>,
    /// Number of records in the file from Add-file stats.
    pub num_records: Option<u64>,

    pub dv_info: DvInfo,
}

struct ScanContext {
    /// Table root URL
    table_root: Url,
    /// Files to be scanned.
    files: Vec<ScanFileContextInner>,
    /// Errors encountered during the scan.
    errs: DataFusionErrorBuilder,
    count: usize,
}

impl ScanContext {
    fn new(table_root: Url) -> Self {
        Self {
            table_root,
            files: Vec::new(),
            errs: DataFusionErrorBuilder::new(),
            count: 0,
        }
    }

    fn parse_path(&self, path: &str) -> DeltaResult<Url, DataFusionError> {
        parse_path(&self.table_root, path)
    }

    fn error_or(self) -> DeltaResult<Self, DataFusionError> {
        let ScanContext {
            table_root,
            files,
            errs,
            count,
        } = self;
        errs.error_or(())?;
        Ok(ScanContext {
            table_root,
            files,
            errs: DataFusionErrorBuilder::new(),
            count,
        })
    }
}

fn parse_path(url: &Url, path: &str) -> DeltaResult<Url, DataFusionError> {
    Ok(match Url::parse(path) {
        Ok(url) => url,
        Err(_) => url
            .join(path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    })
}

fn apply_file_selection(
    mut scan_data: ScanMetadata,
    table_root: &Url,
    file_selection: &HashSet<String>,
) -> DeltaResult<ScanMetadata> {
    let (data, mut selection_vector) = scan_data.scan_files.into_parts();
    let batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();

    // Kernel allows a shorter selection vector; missing entries are implicitly true.
    selection_vector.resize(batch.num_rows(), true);

    for (idx, select) in selection_vector.iter_mut().enumerate() {
        if *select {
            let file_url = parse_path(
                table_root,
                LogicalFileView::new(batch.clone(), idx).path_raw(),
            )?;
            *select = file_selection.contains(file_url.as_str());
        }
    }

    scan_data.scan_files =
        FilteredEngineData::try_new(Box::new(ArrowEngineData::new(batch)), selection_vector)?;
    Ok(scan_data)
}

fn visit_scan_file(ctx: &mut ScanContext, scan_file: ScanFile) {
    let file_url = match ctx.parse_path(&scan_file.path) {
        Ok(v) => v,
        Err(e) => {
            ctx.errs.add_error(e);
            return;
        }
    };

    ctx.files.push(ScanFileContextInner {
        dv_info: scan_file.dv_info,
        transform: scan_file.transform,
        file_url,
        size: scan_file.size as u64,
        num_records: scan_file.stats.map(|stats| stats.num_records),
    });
    ctx.count += 1;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use delta_kernel::scan::state::{DvInfo, ScanFile};
    use url::Url;

    use super::{ScanContext, visit_scan_file};

    fn scan_file(path: impl Into<String>) -> ScanFile {
        ScanFile {
            path: path.into(),
            size: 1,
            modification_time: 0,
            stats: None,
            dv_info: DvInfo::default(),
            transform: None,
            partition_values: HashMap::new(),
        }
    }

    #[test]
    fn test_scan_context_error_or_returns_error_for_invalid_path() {
        let mut ctx = ScanContext::new(Url::parse("mailto:delta@example.com").unwrap());
        visit_scan_file(&mut ctx, scan_file("part-000.parquet"));
        assert!(ctx.error_or().is_err());
    }

    #[test]
    fn test_scan_context_error_or_keeps_valid_path() {
        let mut ctx = ScanContext::new(Url::parse("file:///tmp/delta/").unwrap());
        visit_scan_file(&mut ctx, scan_file("part-000.parquet"));

        let ctx = ctx.error_or().unwrap();
        assert_eq!(ctx.files.len(), 1);
        assert_eq!(
            ctx.files[0].file_url.as_str(),
            "file:///tmp/delta/part-000.parquet"
        );
    }
}
