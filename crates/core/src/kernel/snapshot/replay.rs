use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow_arith::boolean::{is_not_null, or};
use arrow_array::MapArray;
use arrow_array::*;
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Fields, Schema as ArrowSchema,
    SchemaRef as ArrowSchemaRef,
};
use arrow_select::filter::filter_record_batch;
use delta_kernel::expressions::Scalar;
use delta_kernel::schema::DataType;
use delta_kernel::schema::PrimitiveType;
use futures::Stream;
use hashbrown::HashSet;
use itertools::Itertools;
use percent_encoding::percent_decode_str;
use pin_project_lite::pin_project;
use tracing::debug;

use super::parse::collect_map;
use super::ReplayVisitor;
use super::Snapshot;
use crate::kernel::arrow::extract::{self as ex, ProvidesColumnByName};
use crate::kernel::arrow::json;
use crate::kernel::StructType;
use crate::{DeltaResult, DeltaTableConfig, DeltaTableError};

pin_project! {
    pub struct ReplayStream<'a, S> {
        scanner: LogReplayScanner,

        mapper: Arc<LogMapper>,

        visitors: &'a mut Vec<Box<dyn ReplayVisitor>>,

        #[pin]
        commits: S,

        #[pin]
        checkpoint: S,
    }
}

impl<'a, S> ReplayStream<'a, S> {
    pub(super) fn try_new(
        commits: S,
        checkpoint: S,
        snapshot: &Snapshot,
        visitors: &'a mut Vec<Box<dyn ReplayVisitor>>,
    ) -> DeltaResult<Self> {
        let stats_schema = Arc::new((&snapshot.stats_schema(None)?).try_into()?);
        let partitions_schema = snapshot.partitions_schema(None)?.map(|s| Arc::new(s));
        let mapper = Arc::new(LogMapper {
            stats_schema,
            partitions_schema,
            config: snapshot.config.clone(),
        });
        Ok(Self {
            commits,
            checkpoint,
            mapper,
            visitors,
            scanner: LogReplayScanner::new(),
        })
    }
}

pub(super) struct LogMapper {
    stats_schema: ArrowSchemaRef,
    partitions_schema: Option<Arc<StructType>>,
    config: DeltaTableConfig,
}

impl LogMapper {
    pub(super) fn try_new(
        snapshot: &Snapshot,
        table_schema: Option<&StructType>,
    ) -> DeltaResult<Self> {
        Ok(Self {
            stats_schema: Arc::new((&snapshot.stats_schema(table_schema)?).try_into()?),
            partitions_schema: snapshot
                .partitions_schema(table_schema)?
                .map(|s| Arc::new(s)),
            config: snapshot.config.clone(),
        })
    }

    pub fn map_batch(&self, batch: RecordBatch) -> DeltaResult<RecordBatch> {
        map_batch(
            batch,
            self.stats_schema.clone(),
            self.partitions_schema.clone(),
            &self.config,
        )
    }
}

fn map_batch(
    batch: RecordBatch,
    stats_schema: ArrowSchemaRef,
    partition_schema: Option<Arc<StructType>>,
    config: &DeltaTableConfig,
) -> DeltaResult<RecordBatch> {
    let mut new_batch = batch.clone();

    let stats = ex::extract_and_cast_opt::<StringArray>(&batch, "add.stats");
    let stats_parsed_col = ex::extract_and_cast_opt::<StructArray>(&batch, "add.stats_parsed");
    if stats_parsed_col.is_none() && stats.is_some() {
        new_batch = parse_stats(new_batch, stats_schema, config)?;
    }

    if let Some(partitions_schema) = partition_schema {
        let partitions_parsed_col =
            ex::extract_and_cast_opt::<StructArray>(&batch, "add.partitionValues_parsed");
        if partitions_parsed_col.is_none() {
            new_batch = parse_partitions(new_batch, partitions_schema.as_ref())?;
        }
    }

    Ok(new_batch)
}

/// parse the serialized stats in the  `add.stats` column in the files batch
/// and add a new column `stats_parsed` containing the the parsed stats.
fn parse_stats(
    batch: RecordBatch,
    stats_schema: ArrowSchemaRef,
    config: &DeltaTableConfig,
) -> DeltaResult<RecordBatch> {
    let stats = ex::extract_and_cast_opt::<StringArray>(&batch, "add.stats").ok_or(
        DeltaTableError::generic("No stats column found in files batch. This is unexpected."),
    )?;
    let stats: StructArray = json::parse_json(stats, stats_schema.clone(), config)?.into();
    insert_field(batch, stats, "stats_parsed")
}

fn parse_partitions(batch: RecordBatch, partition_schema: &StructType) -> DeltaResult<RecordBatch> {
    let partitions = ex::extract_and_cast_opt::<MapArray>(&batch, "add.partitionValues").ok_or(
        DeltaTableError::generic(
            "No partitionValues column found in files batch. This is unexpected.",
        ),
    )?;

    let mut values = partition_schema
        .fields()
        .map(|f| {
            (
                f.name().to_string(),
                Vec::<Scalar>::with_capacity(partitions.len()),
            )
        })
        .collect::<HashMap<_, _>>();

    for i in 0..partitions.len() {
        if partitions.is_null(i) {
            return Err(DeltaTableError::generic(
                "Expected potentially empty partition values map, but found a null value.",
            ));
        }
        let data: HashMap<_, _> = collect_map(&partitions.value(i))
            .ok_or(DeltaTableError::generic(
                "Failed to collect partition values from map array.",
            ))?
            .map(|(k, v)| {
                let field = partition_schema
                    .field(k.as_str())
                    .ok_or(DeltaTableError::generic(format!(
                        "Partition column {} not found in schema.",
                        k
                    )))?;
                let field_type = match field.data_type() {
                    DataType::Primitive(p) => Ok(p),
                    _ => Err(DeltaTableError::generic(
                        "nested partitioning values are not supported",
                    )),
                }?;
                Ok::<_, DeltaTableError>((
                    k,
                    v.map(|vv| field_type.parse_scalar(vv.as_str()))
                        .transpose()?
                        .unwrap_or(Scalar::Null(field.data_type().clone())),
                ))
            })
            .collect::<Result<_, _>>()?;

        partition_schema.fields().for_each(|f| {
            let value = data
                .get(f.name())
                .cloned()
                .unwrap_or(Scalar::Null(f.data_type().clone()));
            values.get_mut(f.name()).unwrap().push(value);
        });
    }

    let columns = partition_schema
        .fields()
        .map(|f| {
            let values = values.get(f.name()).unwrap();
            match f.data_type() {
                DataType::Primitive(p) => {
                    // Safety: we created the Scalars above using the parsing function of the same PrimitiveType
                    // should this fail, it's a bug in our code, and we should panic
                    let arr = match p {
                        PrimitiveType::String => {
                            Arc::new(StringArray::from_iter(values.iter().map(|v| match v {
                                Scalar::String(s) => Some(s.clone()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Long => {
                            Arc::new(Int64Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Long(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Integer => {
                            Arc::new(Int32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Integer(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Short => {
                            Arc::new(Int16Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Short(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Byte => {
                            Arc::new(Int8Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Byte(i) => Some(*i),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Float => {
                            Arc::new(Float32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Float(f) => Some(*f),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Double => {
                            Arc::new(Float64Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Double(f) => Some(*f),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Boolean => {
                            Arc::new(BooleanArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Boolean(b) => Some(*b),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Binary => {
                            Arc::new(BinaryArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Binary(b) => Some(b.clone()),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }
                        PrimitiveType::Date => {
                            Arc::new(Date32Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Date(d) => Some(*d),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))) as ArrayRef
                        }

                        PrimitiveType::Timestamp => Arc::new(
                            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::Timestamp(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_timezone("UTC"),
                        ) as ArrayRef,
                        PrimitiveType::TimestampNtz => Arc::new(
                            TimestampMicrosecondArray::from_iter(values.iter().map(|v| match v {
                                Scalar::TimestampNtz(t) => Some(*t),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            })),
                        ) as ArrayRef,
                        PrimitiveType::Decimal(p, s) => Arc::new(
                            Decimal128Array::from_iter(values.iter().map(|v| match v {
                                Scalar::Decimal(d, _, _) => Some(*d),
                                Scalar::Null(_) => None,
                                _ => panic!("unexpected scalar type"),
                            }))
                            .with_precision_and_scale(*p, *s as i8)?,
                        ) as ArrayRef,
                    };
                    Ok(arr)
                }
                _ => Err(DeltaTableError::generic(
                    "complex partitioning values are not supported",
                )),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    insert_field(
        batch,
        StructArray::try_new(
            Fields::from(
                partition_schema
                    .fields()
                    .map(|f| f.try_into())
                    .collect::<Result<Vec<ArrowField>, _>>()?,
            ),
            columns,
            None,
        )?,
        "partitionValues_parsed",
    )
}

fn insert_field(batch: RecordBatch, array: StructArray, name: &str) -> DeltaResult<RecordBatch> {
    let schema = batch.schema();
    let add_col = ex::extract_and_cast::<StructArray>(&batch, "add")?;
    let (add_idx, _) = schema.column_with_name("add").unwrap();

    let add_type = add_col
        .fields()
        .iter()
        .cloned()
        .chain(std::iter::once(Arc::new(ArrowField::new(
            name,
            array.data_type().clone(),
            true,
        ))))
        .collect_vec();
    let new_add = Arc::new(StructArray::try_new(
        add_type.clone().into(),
        add_col
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(array) as ArrayRef))
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

    Ok(RecordBatch::try_new(
        Arc::new(ArrowSchema::new(fields)),
        columns,
    )?)
}

impl<'a, S> Stream for ReplayStream<'a, S>
where
    S: Stream<Item = DeltaResult<RecordBatch>>,
{
    type Item = DeltaResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = this.commits.poll_next(cx).map(|b| match b {
            Some(Ok(batch)) => {
                for visitor in this.visitors.iter_mut() {
                    if let Err(e) = visitor.visit_batch(&batch) {
                        return Some(Err(e));
                    }
                }
                match this.scanner.process_files_batch(&batch, true) {
                    Ok(filtered) => Some(this.mapper.map_batch(filtered)),
                    err => Some(err),
                }
            }
            Some(e) => Some(e),
            None => None,
        });
        if matches!(res, Poll::Ready(None)) {
            this.checkpoint.poll_next(cx).map(|b| match b {
                Some(Ok(batch)) => {
                    for visitor in this.visitors.iter_mut() {
                        if let Err(e) = visitor.visit_batch(&batch) {
                            return Some(Err(e));
                        }
                    }
                    match this.scanner.process_files_batch(&batch, false) {
                        Ok(filtered) => Some(this.mapper.map_batch(filtered)),
                        err => Some(err),
                    }
                }
                Some(e) => Some(e),
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_select::concat::concat_batches;
    use delta_kernel::schema::DataType;
    use deltalake_test::utils::*;
    use futures::TryStreamExt;
    use object_store::path::Path;

    use super::super::{log_segment::LogSegment, partitions_schema, stats_schema};
    use super::*;
    use crate::kernel::{models::ActionType, StructType};
    use crate::operations::transaction::CommitData;
    use crate::protocol::DeltaOperation;
    use crate::table::config::TableConfig;
    use crate::test_utils::{ActionFactory, TestResult, TestSchemas};

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

    #[test]
    fn test_parse_stats() -> TestResult {
        let schema = TestSchemas::simple();
        let config_map = HashMap::new();
        let table_config = TableConfig(&config_map);
        let config = DeltaTableConfig::default();

        let commit_data = CommitData {
            actions: vec![ActionFactory::add(schema, HashMap::new(), Vec::new(), true).into()],
            operation: DeltaOperation::Write {
                mode: crate::protocol::SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
            app_metadata: Default::default(),
            app_transactions: Default::default(),
        };
        let (_, maybe_batches) = LogSegment::new_test(&[commit_data])?;

        let batches = maybe_batches.into_iter().collect::<Result<Vec<_>, _>>()?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;

        assert!(ex::extract_and_cast_opt::<StringArray>(&batch, "add.stats").is_some());
        assert!(ex::extract_and_cast_opt::<StructArray>(&batch, "add.stats_parsed").is_none());

        let stats_schema = stats_schema(&schema, table_config)?;
        let new_batch = parse_stats(batch, Arc::new((&stats_schema).try_into()?), &config)?;

        assert!(ex::extract_and_cast_opt::<StructArray>(&new_batch, "add.stats_parsed").is_some());
        let parsed_col = ex::extract_and_cast::<StructArray>(&new_batch, "add.stats_parsed")?;
        let delta_type: DataType = parsed_col.data_type().try_into()?;

        match delta_type {
            DataType::Struct(fields) => {
                assert_eq!(fields.as_ref(), &stats_schema);
            }
            _ => panic!("unexpected data type"),
        }

        // let expression = Expression::column("add.stats");
        // let evaluator = ARROW_HANDLER.get_evaluator(
        //     Arc::new(batch.schema_ref().as_ref().try_into()?),
        //     expression,
        //     DataType::Primitive(PrimitiveType::String),
        // );
        // let engine_data = ArrowEngineData::new(batch);
        // let result = evaluator
        //     .evaluate(&engine_data)?
        //     .as_any()
        //     .downcast_ref::<ArrowEngineData>()
        //     .ok_or(DeltaTableError::generic(
        //         "failed to downcast evaluator result to ArrowEngineData.",
        //     ))?
        //     .record_batch()
        //     .clone();

        Ok(())
    }

    #[test]
    fn test_parse_partition_values() -> TestResult {
        let schema = TestSchemas::simple();
        let partition_columns = vec![schema.field("modified").unwrap().name().to_string()];

        let commit_data = CommitData {
            actions: vec![ActionFactory::add(
                schema,
                HashMap::new(),
                partition_columns.clone(),
                true,
            )
            .into()],
            operation: DeltaOperation::Write {
                mode: crate::protocol::SaveMode::Append,
                partition_by: Some(partition_columns.clone()),
                predicate: None,
            },
            app_metadata: Default::default(),
            app_transactions: Default::default(),
        };
        let (_, maybe_batches) = LogSegment::new_test(&[commit_data])?;

        let batches = maybe_batches.into_iter().collect::<Result<Vec<_>, _>>()?;
        let batch = concat_batches(&batches[0].schema(), &batches)?;

        assert!(ex::extract_and_cast_opt::<MapArray>(&batch, "add.partitionValues").is_some());
        assert!(
            ex::extract_and_cast_opt::<StructArray>(&batch, "add.partitionValues_parsed").is_none()
        );

        let partitions_schema = partitions_schema(&schema, &partition_columns)?.unwrap();
        let new_batch = parse_partitions(batch, &partitions_schema)?;

        assert!(
            ex::extract_and_cast_opt::<StructArray>(&new_batch, "add.partitionValues_parsed")
                .is_some()
        );
        let parsed_col =
            ex::extract_and_cast::<StructArray>(&new_batch, "add.partitionValues_parsed")?;
        let delta_type: DataType = parsed_col.data_type().try_into()?;

        match delta_type {
            DataType::Struct(fields) => {
                assert_eq!(fields.as_ref(), &partitions_schema);
            }
            _ => panic!("unexpected data type"),
        }

        Ok(())
    }
}
