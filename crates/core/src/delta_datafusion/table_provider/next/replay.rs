use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::compute::filter_record_batch;
use arrow_array::BooleanArray;
use datafusion::{common::error::DataFusionErrorBuilder, error::DataFusionError};
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::{
    engine::arrow_data::ArrowEngineData,
    scan::{
        state::{DvInfo, Stats},
        ScanMetadata,
    },
    Engine, ExpressionRef,
};
use futures::Stream;
use itertools::Itertools;
use pin_project_lite::pin_project;
use url::Url;

use crate::kernel::Scan;
use crate::{kernel::ReceiverStreamBuilder, DeltaResult};

#[derive(Debug)]
pub(crate) struct ReplayStats {
    num_skipped: usize,
    num_scanned: usize,
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
    pub(crate) struct ScanFileStream<S> {
        pub(crate) metrics: ReplayStats,

        engine: Arc<dyn Engine>,

        table_root: Url,

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
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(scan_data))) => {
                let mut ctx = ScanContext::new(this.table_root.clone());
                ctx = match scan_data.visit_scan_files(ctx, visit_scan_file) {
                    Ok(ctx) => ctx,
                    Err(err) => return Poll::Ready(Some(Err(err.into()))),
                };

                // Spawn tasks to read the deletion vectors from disk.
                for file in &ctx.files {
                    let engine = this.engine.clone();
                    let dv_info = file.dv_info.clone();
                    let file_url = file.file_url.clone();
                    let table_root = this.table_root.clone();
                    let tx = this.dv_stream.tx();
                    let load_dv = move || {
                        let dv_res = dv_info.get_selection_vector(engine.as_ref(), &table_root)?;
                        let _ = tx.blocking_send(Ok((file_url, dv_res)));
                        Ok(())
                    };
                    this.dv_stream.spawn_blocking(load_dv);
                }

                this.metrics.num_scanned += ctx.count;
                this.metrics.num_skipped += scan_data
                    .scan_files
                    .selection_vector
                    .len()
                    .saturating_sub(ctx.count);

                Poll::Ready(Some(Ok(ctx
                    .files
                    .into_iter()
                    .map(Into::into)
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

fn kernel_to_arrow(metadata: ScanMetadata) -> DeltaResult<ScanMetadata> {
    let selected = metadata.scan_files.selection_vector.iter();
    let scan_file_transforms = selected
        .zip(metadata.scan_file_transforms)
        .filter_map(|(selected, v)| selected.then_some(v))
        .collect();
    let batch = ArrowEngineData::try_from_engine_data(metadata.scan_files.data)?.into();
    let scan_files = filter_record_batch(
        &batch,
        &BooleanArray::from(metadata.scan_files.selection_vector),
    )?;
    Ok(ScanMetadata {
        scan_files: FilteredEngineData {
            selection_vector: vec![true; scan_files.num_rows()],
            data: Box::new(ArrowEngineData::new(scan_files)),
        },
        scan_file_transforms,
    })
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
    pub stats: Option<Stats>,
}

impl From<ScanFileContextInner> for ScanFileContext {
    fn from(inner: ScanFileContextInner) -> Self {
        Self {
            file_url: inner.file_url,
            size: inner.size,
            transform: inner.transform,
            stats: inner.stats,
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
    /// Statistics about the data in the file.
    ///
    /// The query engine may choose to use these statistics to further optimize the scan.
    pub stats: Option<Stats>,
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
        Ok(match Url::parse(path) {
            Ok(url) => url,
            Err(_) => self
                .table_root
                .join(path)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        })
    }
}
fn visit_scan_file(
    ctx: &mut ScanContext,
    path: &str,
    size: i64,
    stats: Option<Stats>,
    dv_info: DvInfo,
    transform: Option<ExpressionRef>,
    // NB: partition values are passed for backwards compatibility
    // all required transformations are now part of the transform field
    _: std::collections::HashMap<String, String>,
) {
    let file_url = match ctx.parse_path(path) {
        Ok(v) => v,
        Err(e) => {
            ctx.errs.add_error(e);
            return;
        }
    };

    ctx.files.push(ScanFileContextInner {
        dv_info,
        transform,
        stats,
        file_url,
        size: size as u64,
    });
    ctx.count += 1;
}
