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
use pin_project_lite::pin_project;
use tracing::debug;

use super::extract::{
    extract_and_cast, extract_and_cast_opt, read_primitive_opt, read_str, ProvidesColumnByName,
};
use super::parse::parse_json;
use crate::kernel::{DataType, Schema, StructField, StructType};
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

pin_project! {
    pub struct ReplayStream<S> {
        scanner: LogReplayScanner,

        stats_schema: ArrowSchemaRef,

        config: DeltaTableConfig,

        #[pin]
        commits: S,

        #[pin]
        checkpoint: S,
    }
}

impl<S> ReplayStream<S> {
    pub(super) fn try_new(
        commits: S,
        checkpoint: S,
        table_schema: &Schema,
        config: DeltaTableConfig,
    ) -> DeltaResult<Self> {
        let data_fields: Vec<_> = table_schema
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, f)| {
                if idx < 32 && f.data_type() != &DataType::BINARY {
                    Some(f.clone())
                } else {
                    None
                }
            })
            .collect();
        let stats_schema = StructType::new(vec![
            StructField::new("numRecords", DataType::LONG, true),
            StructField::new("minValues", StructType::new(data_fields.clone()), true),
            StructField::new("maxValues", StructType::new(data_fields.clone()), true),
            StructField::new(
                "nullCounts",
                StructType::new(
                    data_fields
                        .into_iter()
                        .map(|f| StructField::new(f.name(), DataType::LONG, true))
                        .collect(),
                ),
                true,
            ),
        ]);
        let stats_schema = std::sync::Arc::new((&stats_schema).try_into()?);
        Ok(Self {
            commits,
            checkpoint,
            stats_schema,
            config,
            scanner: LogReplayScanner::new(),
        })
    }
}

fn map_batch(
    batch: RecordBatch,
    stats_schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<RecordBatch> {
    let stats_col = extract_and_cast_opt::<StringArray>(&batch, "add.stats");
    let stats_parsed_col = extract_and_cast_opt::<StringArray>(&batch, "add.stats_parsed");
    if stats_parsed_col.is_some() {
        return Ok(batch);
    }
    if let Some(stats) = stats_col {
        let stats: Arc<StructArray> =
            Arc::new(parse_json(stats, stats_schema.clone(), config)?.into());
        let schema = batch.schema();
        let add_col = extract_and_cast::<StructArray>(&batch, "add")?;
        let add_idx = schema.column_with_name("add").unwrap();
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
        let _ = std::mem::replace(&mut fields[add_idx.0], new_add_field);
        let mut columns = batch.columns().to_vec();
        let _ = std::mem::replace(&mut columns[add_idx.0], new_add);
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
                Ok(filtered) => Some(map_batch(filtered, this.stats_schema.clone(), this.config)),
                Err(e) => Some(Err(e)),
            },
            Some(Err(e)) => Some(Err(e)),
            None => None,
        });
        if matches!(res, Poll::Ready(None)) {
            this.checkpoint.poll_next(cx).map(|b| match b {
                Some(Ok(batch)) => match this.scanner.process_files_batch(&batch, false) {
                    Ok(filtered) => {
                        Some(map_batch(filtered, this.stats_schema.clone(), this.config))
                    }
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
    if let Some(dv) = &info.dv {
        if let Some(offset) = &dv.offset {
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

fn read_file_info<'a>(arr: &'a dyn ProvidesColumnByName) -> DeltaResult<Vec<Option<FileInfo<'a>>>> {
    let path = extract_and_cast::<StringArray>(arr, "path")?;
    let dv = extract_and_cast_opt::<StructArray>(arr, "deletionVector");

    let get_dv: Box<dyn Fn(usize) -> DeltaResult<Option<DVInfo<'a>>>> = if let Some(d) = dv {
        let storage_type = extract_and_cast::<StringArray>(d, "storageType")?;
        let path_or_inline_dv = extract_and_cast::<StringArray>(d, "pathOrInlineDv")?;
        let offset = extract_and_cast::<Int32Array>(d, "offset")?;

        Box::new(|idx: usize| {
            if read_str(storage_type, idx).is_ok() {
                Ok(Some(DVInfo {
                    storage_type: read_str(storage_type, idx)?,
                    path_or_inline_dv: read_str(path_or_inline_dv, idx)?,
                    offset: read_primitive_opt(offset, idx),
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
                    path: read_str(path, idx)?,
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
