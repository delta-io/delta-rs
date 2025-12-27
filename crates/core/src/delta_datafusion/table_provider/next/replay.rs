use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{array::BooleanArray, compute::filter_record_batch};
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
    pub(crate) num_skipped: usize,
    pub(crate) num_scanned: usize,
}

impl ReplayStats {
    fn new() -> Self {
        Self {
            num_skipped: 0,
            num_scanned: 0,
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
                this.metrics.num_skipped += scan_data
                    .scan_files
                    .selection_vector()
                    .len()
                    .saturating_sub(ctx.count);

                let (data, selection_vector) = scan_data.scan_files.into_parts();
                let batch = ArrowEngineData::try_from_engine_data(data)?.into();
                let scan_files =
                    filter_record_batch(&batch, &BooleanArray::from(selection_vector))?;

                let stats_schema = Arc::new(stats_schema(
                    this.kernel_scan.physical_schema(),
                    this.kernel_scan.snapshot().table_properties(),
                ));
                let parsed_stats = parse_stats_column_with_schema(
                    this.kernel_scan.snapshot().as_ref(),
                    &scan_files,
                    stats_schema,
                )?;

                let mut file_statistics = extract_file_statistics(this.kernel_scan, parsed_stats);

                Poll::Ready(Some(Ok(ctx
                    .files
                    .into_iter()
                    .map(|ctx| {
                        let stats = file_statistics
                            .remove(&ctx.file_url)
                            .unwrap_or_else(|| Statistics::new_unknown(&physical_arrow));
                        ScanFileContext::new(ctx, stats)
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
    parsed_stats: arrow_array::RecordBatch,
) -> HashMap<Url, Statistics> {
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
                parse_path(scan.snapshot().table_root(), view.path().as_ref()).ok()?,
                Statistics {
                    num_rows,
                    total_byte_size,
                    column_statistics,
                },
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
pub struct ScanFileContext {
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
}

impl ScanFileContext {
    /// Create a new `ScanFileContext` with the given file URL, size, and statistics.
    fn new(inner: ScanFileContextInner, stats: Statistics) -> Self {
        Self {
            file_url: inner.file_url,
            size: inner.size,
            transform: inner.transform,
            stats,
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
