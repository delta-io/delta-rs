use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use arrow_arith::boolean::{is_not_null, or};
use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray};
use arrow_select::filter::filter_record_batch;
use futures::Stream;
use hashbrown::HashSet;
use pin_project_lite::pin_project;
use tracing::debug;

use super::extract::{
    extract_and_cast, extract_and_cast_opt, read_primitive_opt, read_str, ProvidesColumnByName,
};
use crate::{DeltaResult, DeltaTableError};

pin_project! {
    pub struct ReplayStream<S> {
        scanner: LogReplayScanner,

        #[pin]
        stream: S,

        #[pin]
        checkpoint: S,
    }
}

impl<S> ReplayStream<S> {
    pub(super) fn new(stream: S, checkpoint: S) -> Self {
        Self {
            stream,
            scanner: LogReplayScanner::new(),
            checkpoint,
        }
    }
}

impl<S> Stream for ReplayStream<S>
where
    S: Stream<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = this.stream.poll_next(cx).map(|b| match b {
            Some(Ok(batch)) => match this.scanner.process_files_batch(&batch, true) {
                Ok(filtered) => Some(Ok(filtered)),
                Err(e) => Some(Err(e)),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        });
        if matches!(res, Poll::Ready(None)) {
            this.checkpoint.poll_next(cx).map(|b| match b {
                Some(Ok(batch)) => match this.scanner.process_files_batch(&batch, false) {
                    Ok(filtered) => Some(Ok(filtered)),
                    Err(e) => Some(Err(e)),
                },
                Some(Err(e)) => Some(Err(e)),
                None => None,
            })
        } else {
            res
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[derive(Debug)]
pub(super) struct FileInfo<'a> {
    pub path: &'a str,
    pub dv: Option<DVInfo<'a>>,
}

#[derive(Debug)]
pub(super) struct DVInfo<'a> {
    pub storage_type: &'a str,
    pub path_or_inline_dv: &'a str,
    pub offset: Option<i32>,
    // pub size_in_bytes: i32,
    // pub cardinality: i64,
}

fn seen_key(info: &FileInfo<'_>) -> String {
    if let Some(dv) = &info.dv {
        if let Some(offset) = dv.offset {
            format!(
                "{}::{}{}@{offset}",
                info.path, dv.storage_type, dv.path_or_inline_dv
            )
        } else {
            format!("{}::{}{}", info.path, dv.storage_type, dv.path_or_inline_dv)
        }
    } else {
        info.path.to_string()
    }
}

pub(super) struct LogReplayScanner {
    // filter: Option<DataSkippingFilter>,
    /// A set of (data file path, dv_unique_id) pairs that have been seen thus
    /// far in the log. This is used to filter out files with Remove actions as
    /// well as duplicate entries in the log.
    seen: HashSet<String>,
}

impl LogReplayScanner {
    /// Creates a new [`LogReplayScanner`] instance.
    pub fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }

    /// Takes a record batch of add and protentially remove actions and returns a
    /// filtered batch of actions that contains only active rows.
    pub(super) fn process_files_batch(
        &mut self,
        batch: &RecordBatch,
        is_log_batch: bool,
    ) -> DeltaResult<RecordBatch> {
        let add_col = extract_and_cast::<StructArray>(batch, "add")?;
        let maybe_remove_col = extract_and_cast_opt::<StructArray>(batch, "remove");
        let filter = if let Some(remove_col) = maybe_remove_col {
            or(&is_not_null(add_col)?, &is_not_null(remove_col)?)?
        } else {
            is_not_null(add_col)?
        };

        let filtered = filter_record_batch(batch, &filter)?;
        let add_col = extract_and_cast::<StructArray>(&filtered, "add")?;
        let maybe_remove_col = extract_and_cast_opt::<StructArray>(&filtered, "remove");
        let add_actions = read_file_info(add_col)?;

        let mut keep = Vec::with_capacity(filtered.num_rows());
        if let Some(remove_col) = maybe_remove_col {
            let remove_actions = read_file_info(remove_col)?;
            for (a, r) in add_actions.into_iter().zip(remove_actions.into_iter()) {
                match (a, r) {
                    (Some(a), None) => {
                        let file_id = seen_key(&a);
                        if !self.seen.contains(&file_id) {
                            is_log_batch.then(|| self.seen.insert(file_id));
                            keep.push(true);
                        } else {
                            keep.push(false);
                        }
                    }
                    (None, Some(r)) => {
                        self.seen.insert(seen_key(&r));
                        keep.push(false);
                    }
                    // NOTE: there sould always be only one action per row.
                    (None, None) => debug!("WARNING: no action found for row"),
                    (Some(a), Some(r)) => {
                        debug!(
                            "WARNING: both add and remove actions found for row: {:?} {:?}",
                            a, r
                        )
                    }
                }
            }
        } else {
            for a in add_actions.into_iter() {
                if let Some(a) = a {
                    let file_id = seen_key(&a);
                    if !self.seen.contains(&file_id) {
                        is_log_batch.then(|| self.seen.insert(file_id));
                        keep.push(true);
                    } else {
                        keep.push(false);
                    }
                }
            }
        };

        let projection = filtered
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| (field.name() == "add").then_some(idx))
            .collect::<Vec<_>>();
        let filtered = filtered.project(&projection)?;

        Ok(filter_record_batch(&filtered, &BooleanArray::from(keep))?)
    }
}

fn read_file_info(arr: &dyn ProvidesColumnByName) -> DeltaResult<Vec<Option<FileInfo<'_>>>> {
    let path = extract_and_cast::<StringArray>(arr, "path")?;
    let mut adds = Vec::with_capacity(path.len());

    let base = extract_and_cast_opt::<StructArray>(arr, "deletionVector");

    if let Some(base) = base {
        let storage_type = extract_and_cast::<StringArray>(base, "storageType")?;
        let path_or_inline_dv = extract_and_cast::<StringArray>(base, "pathOrInlineDv")?;
        let offset = extract_and_cast::<Int32Array>(base, "offset")?;
        // let size_in_bytes = extract_and_cast::<Int32Array>(base, "sizeInBytes")?;
        // let cardinality = extract_and_cast::<Int64Array>(base, "cardinality")?;

        for idx in 0..path.len() {
            let value = path
                .is_valid(idx)
                .then(|| {
                    let dv = if read_str(storage_type, idx).is_ok() {
                        Some(DVInfo {
                            storage_type: read_str(storage_type, idx)?,
                            path_or_inline_dv: read_str(path_or_inline_dv, idx)?,
                            offset: read_primitive_opt(offset, idx),
                            // size_in_bytes: read_primitive(size_in_bytes, idx)?,
                            // cardinality: read_primitive(cardinality, idx)?,
                        })
                    } else {
                        None
                    };
                    Ok::<_, DeltaTableError>(FileInfo {
                        path: read_str(path, idx)?,
                        dv,
                    })
                })
                .transpose()?;
            adds.push(value);
        }
    } else {
        for idx in 0..path.len() {
            let value = path
                .is_valid(idx)
                .then(|| {
                    Ok::<_, DeltaTableError>(FileInfo {
                        path: read_str(path, idx)?,
                        dv: None,
                    })
                })
                .transpose()?;
            adds.push(value);
        }
    }

    Ok(adds)
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use arrow_select::concat::concat_batches;
    use deltalake_test::utils::*;
    use futures::TryStreamExt;
    use object_store::path::Path;

    use super::super::log_segment::LogSegment;
    use super::*;
    use crate::kernel::{actions::ActionType, StructType};

    pub(crate) async fn test_log_replay(context: &IntegrationContext) -> TestResult {
        let log_schema = Arc::new(StructType::new(vec![
            ActionType::Add.schema_field().clone(),
            ActionType::Remove.schema_field().clone(),
        ]));
        let commit_schema = Arc::new(StructType::new(vec![ActionType::Add
            .schema_field()
            .clone()]));

        let store = context
            .table_builder(TestTables::Checkpoints)
            .build_storage()?
            .object_store();

        let segment = LogSegment::try_new(&Path::default(), Some(9), store.as_ref()).await?;
        let mut scanner = LogReplayScanner::new();

        let batches = segment
            .commit_stream(store.clone(), &log_schema, &Default::default())?
            .try_collect::<Vec<_>>()
            .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        assert_eq!(batch.schema().fields().len(), 2);
        let filtered = scanner.process_files_batch(&batch, true)?;
        assert_eq!(filtered.schema().fields().len(), 1);

        let batches = segment
            .checkpoint_stream(store, &commit_schema, &Default::default())
            .try_collect::<Vec<_>>()
            .await?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;
        // TODO enable once we do selection pushdown in parquet read
        // assert_eq!(batch.schema().fields().len(), 1);
        let filtered = scanner.process_files_batch(&batch, true)?;
        assert_eq!(filtered.schema().fields().len(), 1);

        let store = context
            .table_builder(TestTables::Simple)
            .build_storage()?
            .object_store();
        let segment = LogSegment::try_new(&Path::default(), None, store.as_ref()).await?;
        let batches = segment
            .commit_stream(store.clone(), &log_schema, &Default::default())?
            .try_collect::<Vec<_>>()
            .await?;

        let batch = concat_batches(&batches[0].schema(), &batches)?;
        let arr_add = batch.column_by_name("add").unwrap();
        let add_count = arr_add.len() - arr_add.null_count();
        let arr_rm = batch.column_by_name("remove").unwrap();
        let rm_count = arr_rm.len() - arr_rm.null_count();

        let filtered = scanner.process_files_batch(&batch, true)?;
        let arr_add = filtered.column_by_name("add").unwrap();
        let add_count_after = arr_add.len() - arr_add.null_count();
        assert_eq!(arr_add.null_count(), 0);
        assert!(add_count_after < add_count);
        assert_eq!(add_count_after, add_count - rm_count);

        Ok(())
    }
}