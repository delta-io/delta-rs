use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow_arith::boolean::{is_not_null, or};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use arrow_select::filter::filter_record_batch;
use futures::Stream;
use hashbrown::HashSet;
use itertools::Itertools;
use percent_encoding::percent_decode_str;
use pin_project_lite::pin_project;
use tracing::debug;

use crate::kernel::arrow::extract::{self as ex, ProvidesColumnByName};
use crate::kernel::arrow::json;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

use super::Snapshot;

pin_project! {
    pub struct ReplayStream<S> {
        scanner: LogReplayScanner,

        mapper: Arc<LogMapper>,

        #[pin]
        commits: S,

        #[pin]
        checkpoint: S,
    }
}

impl<S> ReplayStream<S> {
    pub(super) fn try_new(commits: S, checkpoint: S, snapshot: &Snapshot) -> DeltaResult<Self> {
        let stats_schema = Arc::new((&snapshot.stats_schema()?).try_into()?);
        let mapper = Arc::new(LogMapper {
            stats_schema,
            config: snapshot.config.clone(),
        });
        Ok(Self {
            commits,
            checkpoint,
            mapper,
            scanner: LogReplayScanner::new(),
        })
    }
}

pub(super) struct LogMapper {
    stats_schema: ArrowSchemaRef,
    config: DeltaTableConfig,
}

impl LogMapper {
    pub(super) fn try_new(snapshot: &Snapshot) -> DeltaResult<Self> {
        Ok(Self {
            stats_schema: Arc::new((&snapshot.stats_schema()?).try_into()?),
            config: snapshot.config.clone(),
        })
    }

    pub fn map_batch(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        map_batch(batch, self.stats_schema.clone(), &self.config)
    }
}

fn map_batch(
    batch: RecordBatch,
    stats_schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<RecordBatch> {
    let stats_col = ex::extract_and_cast_opt::<StringArray>(&batch, "add.stats");
    let stats_parsed_col = ex::extract_and_cast_opt::<StringArray>(&batch, "add.stats_parsed");
    if stats_parsed_col.is_some() {
        return Ok(batch);
    }
    if let Some(stats) = stats_col {
        let stats: Arc<StructArray> =
            Arc::new(json::parse_json(stats, stats_schema.clone(), config)?.into());
        let schema = batch.schema();
        let add_col = ex::extract_and_cast::<StructArray>(&batch, "add")?;
        let (add_idx, _) = schema.column_with_name("add").unwrap();
        let add_type = add_col
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(ArrowField::new(
                "stats_parsed",
                ArrowDataType::Struct(stats_schema.fields().clone()),
                true,
            ))))
            .collect_vec();
        let new_add = Arc::new(StructArray::try_new(
            add_type.clone().into(),
            add_col
                .columns()
                .iter()
                .cloned()
                .chain(std::iter::once(stats as ArrayRef))
                .collect(),
            add_col.nulls().cloned(),
        )?);
        let new_add_field = Arc::new(ArrowField::new(
            "add",
            ArrowDataType::Struct(add_type.into()),
            true,
        ));
        let mut fields = schema.fields().to_vec();
        let _ = std::mem::replace(&mut fields[add_idx], new_add_field);
        let mut columns = batch.columns().to_vec();
        let _ = std::mem::replace(&mut columns[add_idx], new_add);
        return Ok(RecordBatch::try_new(
            Arc::new(ArrowSchema::new(fields)),
            columns,
        )?);
    }

    Ok(batch)
}

impl<S> Stream for ReplayStream<S>
where
    S: Stream<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = this.commits.poll_next(cx).map(|b| match b {
            Some(Ok(batch)) => match this.scanner.process_files_batch(&batch, true) {
                Ok(filtered) => Some(this.mapper.map_batch(filtered)),
                Err(e) => Some(Err(e)),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        });
        if matches!(res, Poll::Ready(None)) {
            this.checkpoint.poll_next(cx).map(|b| match b {
                Some(Ok(batch)) => match this.scanner.process_files_batch(&batch, false) {
                    Ok(filtered) => Some(this.mapper.map_batch(filtered)),
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
        let (l_com, u_com) = self.commits.size_hint();
        let (l_cp, u_cp) = self.checkpoint.size_hint();
        (
            l_com + l_cp,
            u_com.and_then(|u_com| u_cp.map(|u_cp| u_com + u_cp)),
        )
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
    let path = percent_decode_str(info.path).decode_utf8_lossy();
    if let Some(dv) = &info.dv {
        if let Some(offset) = &dv.offset {
            format!(
                "{}::{}{}@{offset}",
                path, dv.storage_type, dv.path_or_inline_dv
            )
        } else {
            format!("{}::{}{}", path, dv.storage_type, dv.path_or_inline_dv)
        }
    } else {
        path.to_string()
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
        let add_col = ex::extract_and_cast::<StructArray>(batch, "add")?;
        let maybe_remove_col = ex::extract_and_cast_opt::<StructArray>(batch, "remove");
        let filter = if let Some(remove_col) = maybe_remove_col {
            or(&is_not_null(add_col)?, &is_not_null(remove_col)?)?
        } else {
            is_not_null(add_col)?
        };

        let filtered = filter_record_batch(batch, &filter)?;
        let add_col = ex::extract_and_cast::<StructArray>(&filtered, "add")?;
        let maybe_remove_col = ex::extract_and_cast_opt::<StructArray>(&filtered, "remove");
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
            for a in add_actions.into_iter().flatten() {
                let file_id = seen_key(&a);
                if !self.seen.contains(&file_id) {
                    is_log_batch.then(|| self.seen.insert(file_id));
                    keep.push(true);
                } else {
                    keep.push(false);
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

fn read_file_info<'a>(arr: &'a dyn ProvidesColumnByName) -> DeltaResult<Vec<Option<FileInfo<'a>>>> {
    let path = ex::extract_and_cast::<StringArray>(arr, "path")?;
    let dv = ex::extract_and_cast_opt::<StructArray>(arr, "deletionVector");

    let get_dv: Box<dyn Fn(usize) -> DeltaResult<Option<DVInfo<'a>>>> = if let Some(d) = dv {
        let storage_type = ex::extract_and_cast::<StringArray>(d, "storageType")?;
        let path_or_inline_dv = ex::extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
        let offset = ex::extract_and_cast::<Int32Array>(d, "offset")?;

        Box::new(|idx: usize| {
            if ex::read_str(storage_type, idx).is_ok() {
                Ok(Some(DVInfo {
                    storage_type: ex::read_str(storage_type, idx)?,
                    path_or_inline_dv: ex::read_str(path_or_inline_dv, idx)?,
                    offset: ex::read_primitive_opt(offset, idx),
                }))
            } else {
                Ok(None)
            }
        })
    } else {
        Box::new(|_| Ok(None))
    };

    let mut adds = Vec::with_capacity(path.len());
    for idx in 0..path.len() {
        let value = path
            .is_valid(idx)
            .then(|| {
                Ok::<_, DeltaTableError>(FileInfo {
                    path: ex::read_str(path, idx)?,
                    dv: get_dv(idx)?,
                })
            })
            .transpose()?;
        adds.push(value);
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
    use crate::kernel::{models::ActionType, StructType};

    pub(crate) async fn test_log_replay(context: &IntegrationContext) -> TestResult {
        let log_schema = Arc::new(StructType::new(vec![
            ActionType::Add.schema_field().clone(),
            ActionType::Remove.schema_field().clone(),
        ]));

        let store = context
            .table_builder(TestTables::SimpleWithCheckpoint)
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
