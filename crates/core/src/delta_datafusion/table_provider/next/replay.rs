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
    expressions::{Scalar, StructData},
    scan::{
        Scan as KernelScan, ScanMetadata,
        state::{DvInfo, ScanFile},
    },
    schema::{DataType, Schema, StructField},
};
use futures::Stream;
use itertools::Itertools;
use pin_project_lite::pin_project;
use url::Url;

use crate::{
    DeltaResult,
    delta_datafusion::engine::to_datafusion_scalar,
    kernel::{
        LogicalFileView, ReceiverStreamBuilder, Scan, StructDataExt,
        arrow::engine_ext::stats_schema, parse_stats_column_with_schema,
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

/// Extract column names referenced in the physical predicate
fn extract_predicate_columns(scan: &KernelScan) -> Option<HashSet<String>> {
    scan.physical_predicate().map(|predicate| {
        predicate
            .references()
            .into_iter()
            .map(|col_name| col_name.to_string().trim_matches('`').to_string())
            .collect()
    })
}

/// Create a stats schema containing only columns needed for predicate evaluation
///
/// Returns:
/// - If no predicate: Schema with just `numRecords`
/// - If predicate exists: Schema with `numRecords` + stats for referenced columns only
fn create_minimal_stats_schema(
    scan: &KernelScan,
    predicate_columns: Option<&HashSet<String>>,
) -> Arc<Schema> {
    match predicate_columns {
        None => {
            // No predicate - only need numRecords for file statistics
            Arc::new(
                Schema::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
            )
        }
        Some(cols) if cols.is_empty() => {
            // Empty predicate columns - minimal schema
            Arc::new(
                Schema::try_new(vec![StructField::nullable("numRecords", DataType::LONG)]).unwrap(),
            )
        }
        Some(cols) => {
            // Filter physical schema to only referenced columns
            let physical_schema = scan.physical_schema();
            let filtered_fields: Vec<_> = physical_schema
                .fields()
                .filter(|field| cols.contains(field.name()))
                .cloned()
                .collect();

            if filtered_fields.is_empty() {
                // Predicate references only partition columns - minimal schema
                Arc::new(
                    Schema::try_new(vec![StructField::nullable("numRecords", DataType::LONG)])
                        .unwrap(),
                )
            } else {
                // Create stats schema for filtered fields
                let filtered_schema = Schema::try_new(filtered_fields).unwrap();
                Arc::new(stats_schema(
                    &filtered_schema,
                    scan.snapshot().table_properties(),
                ))
            }
        }
    }
}

pin_project! {
    /// Stream to read scan file contexts from a scan metadata stream.
    pub(crate) struct ScanFileStream<S> {
        pub(crate) metrics: ReplayStats,

        engine: Arc<dyn Engine>,

        table_root: Url,

        kernel_scan: Arc<KernelScan>,

        pub(crate) dv_stream: ReceiverStreamBuilder<(Url, Option<Vec<bool>>)>,

        #[pin]
        stream: S,
    }
}

impl<S> ScanFileStream<S> {
    pub(crate) fn new(engine: Arc<dyn Engine>, scan: &Arc<Scan>, stream: S) -> Self {
        Self {
            metrics: ReplayStats::new(),
            dv_stream: ReceiverStreamBuilder::<(Url, Option<Vec<bool>>)>::new(100),
            engine,
            table_root: scan.table_root().clone(),
            kernel_scan: scan.inner.clone(),
            stream,
        }
    }
}

impl<S> Stream for ScanFileStream<S>
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
            .unwrap();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(scan_data))) => {
                let mut ctx = ScanContext::new(this.table_root.clone());
                ctx = match scan_data.visit_scan_files(ctx, visit_scan_file) {
                    Ok(ctx) => ctx,
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                };

                // Spawn tasks to read the deletion vectors from disk.
                for file in &ctx.files {
                    if file.dv_info.has_vector() {
                        let engine = this.engine.clone();
                        let dv_info = file.dv_info.clone();
                        let file_url = file.file_url.clone();
                        let table_root = this.table_root.clone();
                        let tx = this.dv_stream.tx();

                        let load_dv = move || {
                            let dv = dv_info.get_selection_vector(engine.as_ref(), &table_root)?;
                            let _ = tx.blocking_send(Ok((file_url, dv)));
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

                // Extract columns referenced in predicate (if any)
                let predicate_columns = extract_predicate_columns(this.kernel_scan.as_ref());

                // Create minimal stats schema based on predicate columns
                let stats_schema = create_minimal_stats_schema(
                    this.kernel_scan.as_ref(),
                    predicate_columns.as_ref(),
                );

                // Parse statistics (will skip parsing for unreferenced columns)
                let parsed_stats = parse_stats_column_with_schema(
                    this.kernel_scan.snapshot().as_ref(),
                    &scan_files,
                    stats_schema,
                )?;

                // TODO: do we need to mnake the stats inexact if deletion vectors are present?
                let mut file_statistics = extract_file_statistics(
                    this.kernel_scan,
                    parsed_stats,
                    predicate_columns.as_ref(),
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

fn extract_file_statistics(
    scan: &KernelScan,
    parsed_stats: RecordBatch,
    predicate_columns: Option<&HashSet<String>>,
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
                    // Check if we should extract stats for this column
                    let should_extract_stats = predicate_columns
                        .map(|cols| cols.contains(f.name()))
                        .unwrap_or(false); // No predicate = no stats needed for pruning

                    if !should_extract_stats {
                        // Return unknown statistics for non-predicate columns
                        return ColumnStatistics {
                            null_count: Precision::Absent,
                            max_value: Precision::Absent,
                            min_value: Precision::Absent,
                            sum_value: Precision::Absent,
                            distinct_count: Precision::Absent,
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

                    let max_value = extract_precision(&max_values, f.name());
                    let min_value = extract_precision(&min_values, f.name());

                    ColumnStatistics {
                        null_count,
                        max_value,
                        min_value,
                        sum_value: Precision::Absent,
                        distinct_count: Precision::Absent,
                    }
                })
                .collect_vec();

            Some((
                parse_path(scan.snapshot().table_root(), view.path_raw().as_ref()).ok()?,
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

#[derive(Debug)]
pub(crate) struct ScanFileContext {
    /// Fully qualified URL of the file.
    pub file_url: Url,
    /// Size of the file on disk.
    pub size: u64,
    /// Selection vector to filter the data in the file.
    // pub selection_vector: Option<Vec<bool>>,
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
    /// Selection vector to filter the data in the file.
    // pub selection_vector: Option<Vec<bool>>,
    /// Transformations to apply to the data in the file.
    pub transform: Option<ExpressionRef>,

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
}

fn parse_path(url: &Url, path: &str) -> DeltaResult<Url, DataFusionError> {
    Ok(match Url::parse(path) {
        Ok(url) => url,
        Err(_) => url
            .join(path)
            .map_err(|e| DataFusionError::External(Box::new(e)))?,
    })
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
    });
    ctx.count += 1;
}
