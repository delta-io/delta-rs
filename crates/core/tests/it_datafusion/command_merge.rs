#![allow(dead_code)]

#[allow(unused_imports)]
use crate::fs_common;

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_array::{RecordBatch, StringViewArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use chrono::Duration;
use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_kernel::actions::deletion_vector_writer::{
    KernelDeletionVector, StreamingDeletionVectorWriter,
};
use deltalake_core::kernel::transaction::{CommitBuilder, TransactionError};
use deltalake_core::kernel::{
    Action, DataType as DeltaDataType, PrimitiveType, StructField, StructType,
};
use deltalake_core::operations::merge::MergeMetrics;
use deltalake_core::operations::vacuum::{Clock, VacuumMode};
use deltalake_core::protocol::DeltaOperation;
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError, TableProperty, open_table};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use parquet::file::properties::WriterProperties;
use rstest::rstest;
use std::{collections::HashSet, sync::Arc};
use url::Url;

const SCD_TS_V1_US: i64 = 1_747_133_263_000_000;
const SCD_TS_V2_US: i64 = 1_747_133_498_000_000;
const SCD_OPEN_END_US: i64 = 9_214_646_399_000_000;

async fn create_table(table_uri: &str, partition: Option<Vec<&str>>) -> DeltaTable {
    let table_schema = get_delta_schema();
    let table_url = url::Url::from_directory_path(table_uri).unwrap();
    let ops = DeltaTable::try_from_url(table_url).await.unwrap();
    let table = ops
        .create()
        .with_columns(table_schema.fields().cloned())
        .with_partition_columns(partition.unwrap_or_default())
        .await
        .expect("Failed to create table");

    let schema = get_arrow_schema();
    write_data(table, &schema).await
}

fn get_delta_schema() -> StructType {
    StructType::try_new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
        ),
        StructField::new(
            "event_date".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
    ])
    .unwrap()
}

fn get_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
        Field::new("event_date", DataType::Utf8, true),
    ]))
}

async fn write_data(table: DeltaTable, schema: &Arc<ArrowSchema>) -> DeltaTable {
    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-01",
                "2021-02-01",
                "2021-02-02",
                "2021-02-02",
            ])),
        ],
    )
    .unwrap();
    // write some data
    table
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap()
}

fn create_test_data() -> (DataFrame, DataFrame) {
    let schema = get_arrow_schema();
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["C", "D"])),
            Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-02",
                "2021-02-02",
            ])),
        ],
    )
    .unwrap();
    let df1 = ctx.read_batch(batch).unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["E", "F"])),
            Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
            Arc::new(arrow::array::StringArray::from(vec![
                "2021-02-03",
                "2021-02-03",
            ])),
        ],
    )
    .unwrap();
    let df2 = ctx.read_batch(batch).unwrap();
    (df1, df2)
}

fn id_value_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn id_value_delta_schema() -> StructType {
    StructType::try_new(vec![
        StructField::new(
            "id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
        StructField::new(
            "value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Long),
            false,
        ),
    ])
    .unwrap()
}

fn id_value_batch(ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        id_value_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn int64_value(batch: &RecordBatch, column: usize, row: usize) -> i64 {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(row)
}

async fn latest_dv_merge_actions(table: &DeltaTable) -> Vec<Action> {
    let version = table.version().unwrap();
    let bytes = table
        .log_store()
        .read_commit_entry(version)
        .await
        .unwrap()
        .unwrap();
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[derive(Debug)]
struct DvVacuumClock(i64);

impl Clock for DvVacuumClock {
    fn current_timestamp_millis(&self) -> i64 {
        self.0
    }
}

#[tokio::test]
async fn test_dv_merge_conflict_orphans_are_reclaimed_by_full_vacuum() {
    let directory = tempfile::tempdir().unwrap();
    let table = DeltaTable::try_from_url(Url::from_directory_path(directory.path()).unwrap())
        .await
        .unwrap()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2], vec![10, 20])])
        .await
        .unwrap();
    let stale = table.clone();
    let ctx = SessionContext::new();
    let (table, _) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![1], vec![100])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    let store = table.log_store().object_store();
    let before: HashSet<_> = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .map(|file| file.location)
        .collect();
    let version = table.version().unwrap();
    let error = stale
        .merge(
            ctx.read_batch(id_value_batch(vec![1], vec![999])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_)
        }
    ));
    assert!(
        table
            .log_store()
            .read_commit_entry(version + 1)
            .await
            .unwrap()
            .is_none()
    );
    let orphans: Vec<_> = store
        .list(None)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .into_iter()
        .filter(|file| {
            !before.contains(&file.location)
                && file.location.as_ref().starts_with("deletion_vector_")
        })
        .collect();
    assert_eq!(orphans.len(), 1);
    let orphan = &orphans[0];
    let retention = Duration::hours(24);
    for delta in [-1, 1] {
        let (_, result) = table
            .clone()
            .vacuum()
            .with_mode(VacuumMode::Full)
            .with_retention_period(retention)
            .with_enforce_retention_duration(false)
            .with_dry_run(true)
            .with_clock(Arc::new(DvVacuumClock(
                orphan.last_modified.timestamp_millis() + retention.num_milliseconds() + delta,
            )))
            .await
            .unwrap();
        assert_eq!(
            result.files_deleted.contains(&orphan.location.to_string()),
            delta > 0
        );
    }
    let (_, result) = table
        .clone()
        .vacuum()
        .with_mode(VacuumMode::Full)
        .with_retention_period(retention)
        .with_enforce_retention_duration(false)
        .with_dry_run(false)
        .with_clock(Arc::new(DvVacuumClock(
            orphan.last_modified.timestamp_millis() + retention.num_milliseconds() + 1,
        )))
        .await
        .unwrap();
    assert!(result.files_deleted.contains(&orphan.location.to_string()));
    assert!(matches!(
        store.get(&orphan.location).await,
        Err(object_store::Error::NotFound { .. })
    ));
    ctx.register_table("committed", table.table_provider().await.unwrap())
        .unwrap();
    let rows = ctx
        .sql("SELECT id, value FROM committed ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 1  | 100   |",
            "| 2  | 20    |",
            "+----+-------+",
        ],
        &rows
    );
}

#[tokio::test]
async fn test_dv_merge_absolute_add_without_row_count() {
    let directory = tempfile::tempdir().unwrap();
    let root = Url::from_directory_path(directory.path()).unwrap();
    let mut table = DeltaTable::try_from_url(root.clone())
        .await
        .unwrap()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2, 3], vec![10, 20, 30])])
        .await
        .unwrap();
    let actions = latest_dv_merge_actions(&table).await;
    let mut add = actions
        .into_iter()
        .find_map(|action| match action {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .unwrap();
    let files = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let remove = files[0].remove_action(true);
    let store = table.log_store().object_store();
    let bytes = store
        .get(&files[0].object_store_path())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let data_path = Path::parse("data dir%/part %name.parquet").unwrap();
    store.put(&data_path, bytes.clone().into()).await.unwrap();
    let root = table
        .snapshot()
        .unwrap()
        .snapshot()
        .table_configuration()
        .table_root()
        .clone();
    let absolute_path = root
        .join("data%20dir%25/part%20%25name.parquet")
        .unwrap()
        .to_string();
    let absolute_path = percent_encoding::percent_decode_str(&absolute_path)
        .decode_utf8()
        .unwrap()
        .into_owned();
    add.path = absolute_path.clone();
    add.stats = None;
    CommitBuilder::default()
        .with_actions(vec![Action::Remove(remove), Action::Add(add)])
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Overwrite,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .unwrap();
    table.update_state().await.unwrap();
    let version = table.version().unwrap();
    let ctx = SessionContext::new();
    let (table, _) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![2], vec![222])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(table.version(), Some(version + 1));
    let actions = latest_dv_merge_actions(&table).await;
    let updated = actions
        .iter()
        .find_map(|action| match action {
            Action::Add(add) if add.path == absolute_path => Some(add),
            _ => None,
        })
        .unwrap();
    assert_eq!(updated.deletion_vector.as_ref().unwrap().cardinality, 1);
    let stats: serde_json::Value = serde_json::from_str(updated.stats.as_ref().unwrap()).unwrap();
    assert_eq!(stats["numRecords"], 3);
    assert_eq!(
        store.get(&data_path).await.unwrap().bytes().await.unwrap(),
        bytes
    );
    ctx.register_table("result", table.table_provider().await.unwrap())
        .unwrap();
    let result = ctx
        .sql("SELECT id, value FROM result ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 1  | 10    |",
            "| 2  | 222   |",
            "| 3  | 30    |",
            "+----+-------+",
        ],
        &result
    );
}

#[tokio::test]
async fn test_dv_merge_rejects_duplicate_active_physical_paths() {
    let mut table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2], vec![10, 20])])
        .await
        .unwrap();
    let add = latest_dv_merge_actions(&table)
        .await
        .into_iter()
        .find_map(|action| match action {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .unwrap();
    let files = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut actions = vec![Action::Remove(files[0].remove_action(true))];
    let store = table.log_store().object_store();
    for deleted_row in [0, 1] {
        let mut bytes = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
        let mut vector = KernelDeletionVector::new();
        vector.add_deleted_row_indexes([deleted_row]);
        let result = writer.write_deletion_vector(vector).unwrap();
        writer.finalize().unwrap();
        let path = Path::from(format!("deletion_vector_{deleted_row}.bin"));
        store.put(&path, bytes.into()).await.unwrap();
        let mut add = add.clone();
        add.deletion_vector = Some(deltalake_core::kernel::DeletionVectorDescriptor {
            storage_type: deltalake_core::kernel::StorageType::AbsolutePath,
            path_or_inline_dv: table
                .log_store()
                .root_url()
                .join(path.as_ref())
                .unwrap()
                .to_string(),
            offset: Some(result.offset),
            size_in_bytes: result.size_in_bytes,
            cardinality: result.cardinality,
        });
        let mut stats: serde_json::Value =
            serde_json::from_str(add.stats.as_ref().unwrap()).unwrap();
        stats["tightBounds"] = false.into();
        add.stats = Some(stats.to_string());
        actions.push(Action::Add(add));
    }
    CommitBuilder::default()
        .with_actions(actions)
        .build(
            Some(table.snapshot().unwrap()),
            table.log_store(),
            DeltaOperation::Write {
                mode: SaveMode::Overwrite,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .unwrap();
    table.update_state().await.unwrap();
    let active = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(active[0].path(), active[1].path());
    assert_ne!(
        active[0].deletion_vector_descriptor(),
        active[1].deletion_vector_descriptor()
    );
    let version = table.version().unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("before", table.table_provider().await.unwrap())
        .unwrap();
    let before = ctx
        .sql("SELECT id, value FROM before WHERE id = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(before.iter().any(|batch| batch.num_rows() > 0));
    let error = table
        .clone()
        .merge(
            ctx.read_batch(id_value_batch(vec![99], vec![100])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_not_matched_by_source_update(|update| {
            update
                .predicate(col("target.id").eq(lit(1)))
                .update("value", lit(100))
        })
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(error, DeltaTableError::Generic(ref message)
        if message.contains("multiple active logical files")));
    assert!(
        table
            .log_store()
            .read_commit_entry(version + 1)
            .await
            .unwrap()
            .is_none()
    );
    table.load().await.unwrap();
    assert_eq!(table.version(), Some(version));
}

#[rstest]
#[case::with_cdf(true)]
#[case::without_cdf(false)]
#[tokio::test]
async fn test_dv_merge_preserves_files_and_previous_deletions(#[case] cdf: bool) {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .with_configuration_property(
            TableProperty::EnableChangeDataFeed,
            Some(if cdf { "true" } else { "false" }),
        )
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2, 3, 4], vec![10, 20, 30, 40])])
        .with_writer_properties(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(2))
                .build(),
        )
        .await
        .unwrap();
    let originals = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(originals.len(), 1);
    let original = &originals[0];
    let store = table.log_store().object_store();
    let original_path = original.object_store_path();
    let original_bytes = store
        .get(&original_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let original_version = table.version().unwrap();
    let ctx = SessionContext::new();

    let (table, first_metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![2, 5], vec![200, 50]))
                .unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(table.version(), Some(original_version + 1));
    assert_eq!(first_metrics.num_target_rows_updated, 1);
    assert_eq!(first_metrics.num_target_rows_inserted, 1);
    assert_eq!(first_metrics.num_target_rows_copied, 0);
    assert_eq!(first_metrics.num_output_rows, 2);
    assert_eq!(first_metrics.num_target_files_added, 2);
    assert_eq!(first_metrics.num_target_files_removed, 1);
    let first_actions = latest_dv_merge_actions(&table).await;
    let first_descriptor = first_actions
        .iter()
        .find_map(|action| match action {
            Action::Add(add) if add.path == original.path() => add.deletion_vector.clone(),
            _ => None,
        })
        .unwrap();
    assert_eq!(first_descriptor.cardinality, 1);
    assert!(first_actions.iter().any(|action| {
        matches!(action, Action::Remove(remove) if remove.path == original.path() && remove.deletion_vector.is_none())
    }));
    assert_eq!(
        first_actions
            .iter()
            .any(|action| matches!(action, Action::Cdc(_))),
        cdf
    );

    let stale = table.clone();
    let (table, second_metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![3], vec![300])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(second_metrics.num_target_rows_copied, 0);
    assert_eq!(second_metrics.num_output_rows, 1);
    assert_eq!(second_metrics.num_target_files_added, 2);
    assert_eq!(second_metrics.num_target_files_removed, 1);
    let second_actions = latest_dv_merge_actions(&table).await;
    let second_descriptor = second_actions
        .iter()
        .find_map(|action| match action {
            Action::Add(add) if add.path == original.path() => {
                let stats: serde_json::Value =
                    serde_json::from_str(add.stats.as_ref().unwrap()).unwrap();
                assert_eq!(stats["numRecords"], 4);
                assert_eq!(stats["tightBounds"], false);
                assert_eq!(add.size, original.size());
                add.deletion_vector.clone()
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(second_descriptor.cardinality, 2);
    assert!(second_actions.iter().any(|action| {
        matches!(action, Action::Remove(remove) if remove.path == original.path() && remove.deletion_vector.as_ref() == Some(&first_descriptor))
    }));
    let kernel_dv: delta_kernel::actions::deletion_vector::DeletionVectorDescriptor =
        second_descriptor.try_into().unwrap();
    let deleted = kernel_dv
        .read(
            table.log_store().engine().storage_handler(),
            table.log_store().root_url(),
        )
        .unwrap();
    assert_eq!(deleted.iter().collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(
        store
            .get(&original_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        original_bytes
    );

    let reader = SessionContext::new();
    reader
        .register_table("result", table.table_provider().await.unwrap())
        .unwrap();
    let batches = reader
        .sql("SELECT id, value FROM result ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 1  | 10    |",
            "| 2  | 200   |",
            "| 3  | 300   |",
            "| 4  | 40    |",
            "| 5  | 50    |",
            "+----+-------+",
        ],
        &batches
    );
    if cdf {
        let changes = table
            .clone()
            .scan_cdf()
            .with_starting_version(original_version + 1)
            .build(&ctx.state(), None)
            .await
            .unwrap();
        let batches = datafusion::physical_plan::collect(changes, ctx.task_ctx())
            .await
            .unwrap();
        let provider =
            datafusion::datasource::MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();
        reader
            .register_table("changes", Arc::new(provider))
            .unwrap();
        let batches = reader
            .sql("SELECT id, value, _change_type FROM changes ORDER BY id, value")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        datafusion::assert_batches_eq!(
            &[
                "+----+-------+------------------+",
                "| id | value | _change_type     |",
                "+----+-------+------------------+",
                "| 2  | 20    | update_preimage  |",
                "| 2  | 200   | update_postimage |",
                "| 3  | 30    | update_preimage  |",
                "| 3  | 300   | update_postimage |",
                "| 5  | 50    | insert           |",
                "+----+-------+------------------+",
            ],
            &batches
        );
    }
    let conflict = stale
        .clone()
        .merge(
            ctx.read_batch(id_value_batch(vec![3], vec![999])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_)
        }
    ));
    assert_eq!(table.version(), Some(original_version + 2));
    let mut retry = stale;
    retry.load().await.unwrap();
    let (retry, _) = retry
        .merge(
            ctx.read_batch(id_value_batch(vec![3], vec![333])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(retry.version(), Some(original_version + 3));
    assert_eq!(
        store
            .get(&original_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        original_bytes
    );
}

#[rstest]
#[case::empty(false)]
#[case::fully_pruned(true)]
#[tokio::test]
async fn test_dv_merge_empty_or_fully_pruned_target(
    #[case] has_files: bool,
    #[values(false, true)] cdf: bool,
) {
    let mut table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .with_configuration_property(
            TableProperty::EnableChangeDataFeed,
            Some(if cdf { "true" } else { "false" }),
        )
        .await
        .unwrap();
    if has_files {
        table = table
            .write(vec![id_value_batch(vec![1, 2], vec![10, 20])])
            .await
            .unwrap();
    }
    let version = table.version().unwrap();
    let ctx = SessionContext::new();
    let (table, metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![99], vec![990])).unwrap(),
            col("target.id")
                .eq(col("source.id"))
                .and(col("target.id").eq(lit(99))),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(table.version(), Some(version + 1));
    assert_eq!(metrics.num_source_rows, 1);
    assert_eq!(metrics.num_target_rows_inserted, 1);
    assert_eq!(metrics.num_target_rows_updated, 0);
    assert_eq!(metrics.num_target_rows_copied, 0);
    assert_eq!(metrics.num_target_files_scanned, 0);
    assert_eq!(
        metrics.num_target_files_skipped_during_scan,
        usize::from(has_files)
    );
    let actions = latest_dv_merge_actions(&table).await;
    assert!(
        actions
            .iter()
            .all(|action| !matches!(action, Action::Remove(_)))
    );
    let adds: Vec<_> = actions
        .iter()
        .filter_map(|action| match action {
            Action::Add(add) => Some(add),
            _ => None,
        })
        .collect();
    assert_eq!(adds.len(), 1);
    assert!(adds[0].deletion_vector.is_none());
    ctx.register_table("result", table.table_provider().await.unwrap())
        .unwrap();
    let batches = ctx
        .sql("SELECT id, value FROM result ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut expected = vec!["+----+-------+", "| id | value |", "+----+-------+"];
    if has_files {
        expected.extend(["| 1  | 10    |", "| 2  | 20    |"]);
    }
    expected.extend(["| 99 | 990   |", "+----+-------+"]);
    datafusion::assert_batches_eq!(&expected, &batches);
}

#[tokio::test]
async fn test_dv_merge_delete_only_insert_only_and_noop() {
    let ctx = SessionContext::new();
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2], vec![10, 20])])
        .await
        .unwrap();
    let original_version = table.version().unwrap();
    let (table, metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![1], vec![10])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_delete(|delete| delete)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.num_target_rows_deleted, 1);
    assert_eq!(metrics.num_output_rows, 0);
    assert_eq!(metrics.num_target_rows_copied, 0);
    assert_eq!(metrics.num_target_files_added, 1);
    assert_eq!(metrics.num_target_files_removed, 1);
    let actions = latest_dv_merge_actions(&table).await;
    assert_eq!(
        actions
            .iter()
            .filter(|action| matches!(action, Action::Add(_)))
            .count(),
        1
    );
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, Action::Cdc(_)))
    );
    assert!(actions.iter().any(|action| matches!(action, Action::Add(add) if add.deletion_vector.as_ref().is_some_and(|dv| dv.cardinality == 1))));

    let (table, metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![3], vec![30])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.num_target_rows_inserted, 1);
    assert_eq!(metrics.num_target_rows_copied, 0);
    assert!(
        latest_dv_merge_actions(&table)
            .await
            .iter()
            .all(|action| !matches!(action, Action::Remove(_)))
    );
    let (table, metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![2], vec![20])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| {
            update
                .predicate(col("source.value").lt(lit(0)))
                .update("value", col("source.value"))
        })
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.num_target_rows_updated, 0);
    assert_eq!(metrics.num_output_rows, 0);
    assert_eq!(table.version(), Some(original_version + 2));

    let reader = SessionContext::new();
    reader
        .register_table("result", table.table_provider().await.unwrap())
        .unwrap();
    let batches = reader
        .sql("SELECT id, value FROM result ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 2  | 20    |",
            "| 3  | 30    |",
            "+----+-------+",
        ],
        &batches
    );
    let stale = table.clone();
    let (compacted, metrics) = table.optimize().await.unwrap();
    assert!(metrics.num_files_removed > 0);
    let conflict = stale
        .merge(
            ctx.read_batch(id_value_batch(vec![2], vec![222])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(_)
        }
    ));
    assert_eq!(compacted.version(), Some(original_version + 3));
}

#[tokio::test]
async fn test_dv_merge_unmatched_deletes_and_duplicate_source_validation() {
    let ctx = SessionContext::new();
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2, 3], vec![10, 20, 30])])
        .await
        .unwrap();
    let (table, metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![1], vec![10])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_not_matched_by_source_delete(|delete| delete)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.num_target_rows_deleted, 2);
    assert_eq!(metrics.num_output_rows, 0);
    assert!(latest_dv_merge_actions(&table).await.iter().any(|action| {
        matches!(action, Action::Add(add) if add.deletion_vector.as_ref().is_some_and(|dv| dv.cardinality == 2))
    }));
    let version = table.version().unwrap();
    let error = table
        .clone()
        .merge(
            ctx.read_batch(id_value_batch(vec![1, 1], vec![100, 200]))
                .unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple"));
    assert!(
        table
            .log_store()
            .read_commit_entry(version + 1)
            .await
            .unwrap()
            .is_none()
    );
    let reader = SessionContext::new();
    reader
        .register_table("result", table.table_provider().await.unwrap())
        .unwrap();
    let batches = reader
        .sql("SELECT id, value FROM result")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| 1  | 10    |",
            "+----+-------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_dv_merge_multiple_partitioned_files_with_small_batches() {
    let schema = get_arrow_schema();
    let dates = ["2026/11/07 a%?", "2026/11/08 b%?"];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "C", "D"])),
            Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            Arc::new(StringArray::from(vec![
                dates[0], dates[0], dates[1], dates[1],
            ])),
        ],
    )
    .unwrap();
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(get_delta_schema().fields().cloned())
        .with_partition_columns(["event_date"])
        .with_configuration_property(TableProperty::EnableDeletionVectors, Some("true"))
        .await
        .unwrap()
        .write(vec![batch])
        .await
        .unwrap();
    let originals = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(originals.len(), 2);
    let store = table.log_store().object_store();
    let mut original_bytes = Vec::new();
    for file in &originals {
        original_bytes.push(
            store
                .get(&file.object_store_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        );
    }
    let source = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["B", "D"])),
            Arc::new(arrow::array::Int32Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec![dates[0], dates[1]])),
        ],
    )
    .unwrap();
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(2),
    );
    let (table, metrics) = table
        .merge(
            ctx.read_batch(source).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.num_target_rows_updated, 2);
    assert_eq!(metrics.num_target_rows_copied, 0);
    let actions = latest_dv_merge_actions(&table).await;
    assert_eq!(
        actions
            .iter()
            .filter(|action| matches!(action, Action::Remove(_)))
            .count(),
        2
    );
    assert_eq!(actions.iter().filter(|action| {
        matches!(action, Action::Add(add) if add.deletion_vector.as_ref().is_some_and(|dv| dv.cardinality == 1))
    }).count(), 2);
    for (file, expected) in originals.iter().zip(original_bytes) {
        assert_eq!(
            store
                .get(&file.object_store_path())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            expected
        );
    }
    let reader = SessionContext::new();
    reader
        .register_table("result", table.table_provider().await.unwrap())
        .unwrap();
    let batches = reader
        .sql("SELECT id, value FROM result ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    datafusion::assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | value |",
            "+----+-------+",
            "| A  | 1     |",
            "| B  | 100   |",
            "| C  | 10    |",
            "| D  | 200   |",
            "+----+-------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_dv_merge_requires_enabled_table_and_keeps_default_rewrite() {
    let ctx = SessionContext::new();
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .await
        .unwrap()
        .write(vec![id_value_batch(vec![1, 2], vec![10, 20])])
        .await
        .unwrap();
    let error = table
        .clone()
        .merge(
            ctx.read_batch(id_value_batch(vec![1], vec![100])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_deletion_vectors(true)
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap_err();
    assert!(matches!(error, DeltaTableError::DeletionVectorsNotEnabled));
    let (table, metrics) = table
        .merge(
            ctx.read_batch(id_value_batch(vec![1], vec![100])).unwrap(),
            col("target.id").eq(col("source.id")),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| update.update("value", col("source.value")))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(metrics.num_target_rows_copied, 1);
    assert!(
        latest_dv_merge_actions(&table)
            .await
            .iter()
            .all(|action| { !matches!(action, Action::Add(add) if add.deletion_vector.is_some()) })
    );
}

fn scd_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("customer_id", DataType::Utf8, false),
        Field::new("metric_value", DataType::Float64, false),
        Field::new(
            "ingestion_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "valid_from_dt",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "valid_to_dt",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn scd_delta_schema() -> StructType {
    StructType::try_new(vec![
        StructField::new(
            "customer_id".to_string(),
            DeltaDataType::Primitive(PrimitiveType::String),
            false,
        ),
        StructField::new(
            "metric_value".to_string(),
            DeltaDataType::Primitive(PrimitiveType::Double),
            false,
        ),
        StructField::new(
            "ingestion_datetime".to_string(),
            DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
        StructField::new(
            "valid_from_dt".to_string(),
            DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
        StructField::new(
            "valid_to_dt".to_string(),
            DeltaDataType::Primitive(PrimitiveType::TimestampNtz),
            false,
        ),
    ])
    .unwrap()
}

fn scd_batch(
    row_count: i64,
    metric_values: Vec<f64>,
    ingestion_datetime: i64,
    valid_from_dt: i64,
    valid_to_dt: i64,
) -> RecordBatch {
    let row_count = usize::try_from(row_count).unwrap();
    assert_eq!(metric_values.len(), row_count);
    let customer_ids = (0..row_count)
        .map(|idx| format!("C-{idx}"))
        .collect::<Vec<_>>();

    RecordBatch::try_new(
        scd_arrow_schema(),
        vec![
            Arc::new(StringArray::from(customer_ids)),
            Arc::new(Float64Array::from(metric_values)),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..row_count).map(|_| Some(ingestion_datetime)),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..row_count).map(|_| Some(valid_from_dt)),
            )),
            Arc::new(TimestampMicrosecondArray::from_iter(
                (0..row_count).map(|_| Some(valid_to_dt)),
            )),
        ],
    )
    .unwrap()
}

fn qualified_col(relation: &str, name: &str) -> Expr {
    col(Column::new(Some(TableReference::bare(relation)), name))
}

enum TestStringArray<'a> {
    Utf8(&'a StringArray),
    Utf8View(&'a StringViewArray),
}

impl TestStringArray<'_> {
    fn value(&self, row: usize) -> &str {
        match self {
            Self::Utf8(array) => array.value(row),
            Self::Utf8View(array) => array.value(row),
        }
    }
}

fn string_column<'a>(batch: &'a RecordBatch, column: &str) -> TestStringArray<'a> {
    let array = batch.column_by_name(column).unwrap();
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        TestStringArray::Utf8(array)
    } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        TestStringArray::Utf8View(array)
    } else {
        panic!("unexpected {column} type: {:?}", array.data_type());
    }
}

async fn single_file_id_value_table(row_count: i64) -> DeltaTable {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .await
        .unwrap();

    let ids = (0..row_count).collect::<Vec<_>>();
    let values = vec![1_i64; row_count as usize];
    let table = table
        .write(vec![id_value_batch(ids, values)])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    table
}

async fn multi_file_id_value_table(file_count: usize, rows_per_file: i64) -> DeltaTable {
    let mut table = DeltaTable::new_in_memory()
        .create()
        .with_columns(id_value_delta_schema().fields().cloned())
        .await
        .unwrap();

    for file_idx in 0..file_count {
        let start = i64::try_from(file_idx).unwrap() * rows_per_file;
        let end = start + rows_per_file;
        table = table
            .write(vec![id_value_batch(
                (start..end).collect::<Vec<_>>(),
                vec![1_i64; rows_per_file as usize],
            )])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
    }

    assert_eq!(table.snapshot().unwrap().log_data().num_files(), file_count);
    table
}

async fn single_file_scd_table(row_count: i64) -> DeltaTable {
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(scd_delta_schema().fields().cloned())
        .await
        .unwrap();

    let table = table
        .write(vec![scd_batch(
            row_count,
            vec![100.0; row_count as usize],
            SCD_TS_V1_US,
            SCD_TS_V1_US,
            SCD_OPEN_END_US,
        )])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();

    assert_eq!(table.snapshot().unwrap().log_data().num_files(), 1);
    table
}

async fn assert_id_value_table_complete(
    table: &DeltaTable,
    row_count: i64,
    updated_id: Option<i64>,
    expected_updated_rows: i64,
) -> DeltaResult<()> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    let provider = table.table_provider().await.unwrap();
    ctx.register_table("target", provider).unwrap();

    let summary = ctx
        .sql(
            "SELECT COUNT(*) AS row_count, COUNT(DISTINCT id) AS distinct_ids, MIN(id) AS min_id, MAX(id) AS max_id, \
                    SUM(CASE WHEN value = 2 THEN 1 ELSE 0 END) AS updated_rows, \
                    SUM(CASE WHEN value = 1 THEN 1 ELSE 0 END) AS unchanged_rows \
             FROM target",
        )
        .await?
        .collect()
        .await?;
    let batch = &summary[0];
    assert_eq!(int64_value(batch, 0, 0), row_count);
    assert_eq!(int64_value(batch, 1, 0), row_count);
    assert_eq!(int64_value(batch, 2, 0), 0);
    assert_eq!(int64_value(batch, 3, 0), row_count - 1);
    assert_eq!(int64_value(batch, 4, 0), expected_updated_rows);
    assert_eq!(int64_value(batch, 5, 0), row_count - expected_updated_rows);

    if let Some(updated_id) = updated_id {
        let updated = ctx
            .sql(&format!("SELECT value FROM target WHERE id = {updated_id}"))
            .await?
            .collect()
            .await?;
        assert_eq!(int64_value(&updated[0], 0, 0), 2);
    }

    Ok(())
}

async fn assert_scd_table_complete(
    table: &DeltaTable,
    row_count: i64,
    expected_expired_rows: usize,
) -> DeltaResult<()> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    let provider = table.table_provider().await.unwrap();
    ctx.register_table("target", provider).unwrap();

    let batches = ctx
        .sql("SELECT customer_id, metric_value, valid_to_dt FROM target")
        .await?
        .collect()
        .await?;

    let mut customer_ids = HashSet::new();
    let mut row_total = 0usize;
    let mut expired_rows = 0usize;
    let mut current_rows = 0usize;
    let mut unchanged_metric_rows = 0usize;

    for batch in batches {
        row_total += batch.num_rows();
        let customer_id = string_column(&batch, "customer_id");
        let metric_value = batch
            .column_by_name("metric_value")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let valid_to = batch
            .column_by_name("valid_to_dt")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            customer_ids.insert(customer_id.value(row).to_string());
            assert_eq!(metric_value.value(row), 100.0);
            unchanged_metric_rows += 1;
            match valid_to.value(row) {
                SCD_TS_V2_US => expired_rows += 1,
                SCD_OPEN_END_US => current_rows += 1,
                other => panic!("unexpected valid_to_dt value: {other}"),
            }
        }
    }

    assert_eq!(row_total, row_count as usize);
    assert_eq!(customer_ids.len(), row_count as usize);
    assert_eq!(expired_rows, expected_expired_rows);
    assert_eq!(current_rows, row_count as usize - expected_expired_rows);
    assert_eq!(unchanged_metric_rows, row_count as usize);

    Ok(())
}

#[tokio::test]
async fn test_merge_large_single_file_unique_source_does_not_report_duplicate_match()
-> DeltaResult<()> {
    let table = single_file_id_value_table(25_000).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(
            (0..25_000).collect::<Vec<_>>(),
            vec![2_i64; 25_000],
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| update.update("value", col("source.value")))?
        .await?;

    assert_eq!(metrics.num_source_rows, 25_000);
    assert_eq!(metrics.num_target_rows_updated, 25_000);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, 25_000);
    assert_eq!(metrics.num_target_files_scanned, 1);

    assert_id_value_table_complete(&table, 25_000, Some(0), 25_000).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_noop_heavy_matched_update_with_inserts_does_not_rewrite_target_file()
-> DeltaResult<()> {
    let target_rows = 25_000;
    let inserted_rows = 1_000;
    let total_source_rows = target_rows + inserted_rows;
    let table = single_file_id_value_table(target_rows).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(
            (0..total_source_rows).collect::<Vec<_>>(),
            vec![1_i64; total_source_rows as usize],
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| {
            update
                .predicate(
                    qualified_col("target", "value").not_eq(qualified_col("source", "value")),
                )
                .update("value", qualified_col("source", "value"))
        })?
        .when_not_matched_insert(|insert| {
            insert
                .set("id", qualified_col("source", "id"))
                .set("value", qualified_col("source", "value"))
        })?
        .await?;

    assert_eq!(metrics.num_source_rows, total_source_rows as usize);
    assert_eq!(metrics.num_target_rows_updated, 0);
    assert_eq!(metrics.num_target_rows_inserted, inserted_rows as usize);
    assert_eq!(metrics.num_target_rows_deleted, 0);
    assert_eq!(metrics.num_target_rows_copied, 0);
    assert_eq!(metrics.num_output_rows, inserted_rows as usize);
    assert_eq!(metrics.num_target_files_scanned, 1);
    assert_eq!(metrics.num_target_files_removed, 0);

    assert_id_value_table_complete(&table, total_source_rows, None, 0).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_batch_boundary_update_does_not_drop_target_rows() -> DeltaResult<()> {
    let table = single_file_id_value_table(8_193).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(vec![8_192], vec![2]))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| update.update("value", col("source.value")))?
        .await?;

    assert_eq!(metrics.num_source_rows, 1);
    assert_eq!(metrics.num_target_rows_updated, 1);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, 8_193);
    assert_eq!(metrics.num_target_files_scanned, 1);

    assert_id_value_table_complete(&table, 8_193, Some(8_192), 1).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_multi_file_batch_boundary_unique_source_preserves_target_rows()
-> DeltaResult<()> {
    let file_count = 4;
    let rows_per_file = 8_193;
    let row_count = i64::try_from(file_count).unwrap() * rows_per_file;
    let table = multi_file_id_value_table(file_count, rows_per_file).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source = ctx
        .read_batch(id_value_batch(
            (0..row_count).collect::<Vec<_>>(),
            vec![2_i64; row_count as usize],
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(source, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| update.update("value", col("source.value")))?
        .await?;

    assert_eq!(metrics.num_source_rows, row_count as usize);
    assert_eq!(metrics.num_target_rows_updated, row_count as usize);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, row_count as usize);
    assert_eq!(metrics.num_target_files_scanned, file_count);

    assert_id_value_table_complete(&table, row_count, Some(0), row_count).await?;
    Ok(())
}

#[tokio::test]
async fn test_merge_scd2_conditional_update_unique_source_does_not_report_duplicate_match()
-> DeltaResult<()> {
    let row_count = 25_000;
    let table = single_file_scd_table(row_count).await;

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    let source_metric_values = (0..row_count)
        .map(|idx| if idx % 2 == 0 { 200.0 } else { 100.0 })
        .collect::<Vec<_>>();
    let source = ctx
        .read_batch(scd_batch(
            row_count,
            source_metric_values,
            SCD_TS_V2_US,
            SCD_TS_V2_US,
            SCD_OPEN_END_US,
        ))
        .unwrap();

    let (table, metrics) = table
        .merge(
            source,
            qualified_col("target", "customer_id")
                .eq(qualified_col("source", "customer_id"))
                .and(qualified_col("target", "valid_to_dt").eq(lit(
                    ScalarValue::TimestampMicrosecond(Some(SCD_OPEN_END_US), None),
                )))
                .and(qualified_col("target", "ingestion_datetime").lt(lit(
                    ScalarValue::TimestampMicrosecond(Some(SCD_TS_V2_US), None),
                ))),
        )
        .with_source_alias("source")
        .with_target_alias("target")
        .with_session_state(Arc::new(ctx.state()))
        .when_matched_update(|update| {
            update
                .update(
                    "valid_to_dt",
                    lit(ScalarValue::TimestampMicrosecond(Some(SCD_TS_V2_US), None)),
                )
                .predicate(
                    qualified_col("target", "metric_value")
                        .not_eq(qualified_col("source", "metric_value")),
                )
        })?
        .await?;

    assert_eq!(metrics.num_source_rows, 25_000);
    assert_eq!(metrics.num_target_rows_updated, 12_500);
    assert_eq!(metrics.num_target_rows_inserted, 0);
    assert_eq!(metrics.num_output_rows, 25_000);
    assert_eq!(metrics.num_target_files_scanned, 1);

    assert_scd_table_complete(&table, row_count, 12_500).await?;
    Ok(())
}

async fn merge(
    table: DeltaTable,
    df: DataFrame,
    predicate: Expr,
) -> DeltaResult<(DeltaTable, MergeMetrics)> {
    table
        .merge(df, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update
                .update("value", col("source.value"))
                .update("event_date", col("source.event_date"))
        })
        .unwrap()
        .when_not_matched_insert(|insert| {
            insert
                .set("id", col("source.id"))
                .set("value", col("source.value"))
                .set("event_date", col("source.event_date"))
        })
        .unwrap()
        .await
}

#[tokio::test]
async fn test_merge_concurrent_conflict() {
    // Overlapping id ranges -> Commit conflict
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, Some(vec!["event_date"])).await;
    let table_url = Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
    let (df1, _df2) = create_test_data();

    let expr = col("target.id").eq(col("source.id"));
    let (_table_ref1, _metrics) = merge(table_ref1, df1.clone(), expr.clone()).await.unwrap();
    let result = merge(table_ref2, df1, expr).await;

    assert!(matches!(
        result.as_ref().unwrap_err(),
        DeltaTableError::Transaction { .. }
    ));
    if let DeltaTableError::Transaction { source } = result.unwrap_err() {
        assert!(matches!(source, TransactionError::CommitConflict(_)));
    }
}

#[tokio::test]
async fn test_merge_different_range() {
    // No overlapping id ranges -> No conflict
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, Some(vec!["event_date"])).await;
    let table_url = Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
    let (df1, df2) = create_test_data();

    let expr = col("target.id").eq(col("source.id"));
    let (_table_ref1, _metrics) = merge(table_ref1, df1, expr.clone()).await.unwrap();
    let result = merge(table_ref2, df2, expr).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_merge_concurrent_different_partition() {
    // partition key in predicate -> Successful merge
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, Some(vec!["event_date"])).await;
    let table_url = url::Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
    let (df1, df2) = create_test_data();

    let expr = col("target.id")
        .eq(col("source.id"))
        .and(col("target.event_date").eq(col("source.event_date")));
    let (_table_ref1, _metrics) = merge(table_ref1, df1, expr.clone()).await.unwrap();
    let result = merge(table_ref2, df2, expr).await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_merge_concurrent_with_overlapping_files() {
    // predicate contains filter and files are overlapping -> Commit conflict
    let tmp_dir = tempfile::tempdir().unwrap();
    let table_uri = tmp_dir.path().to_str().to_owned().unwrap();

    let table_ref1 = create_table(table_uri, None).await;
    let table_url = Url::from_directory_path(table_uri).unwrap();
    let table_ref2 = open_table(table_url).await.unwrap();
    let (df1, _df2) = create_test_data();

    let expr = col("target.id").eq(col("source.id"));
    let (_table_ref1, _metrics) = merge(
        table_ref1,
        df1.clone(),
        expr.clone()
            .and(col(Column::from_qualified_name("target.event_date")).lt_eq(lit("2021-02-02"))),
    )
    .await
    .unwrap();
    let result = merge(
        table_ref2,
        df1,
        expr.and(col(Column::from_qualified_name("target.event_date")).eq(lit("2021-02-02"))),
    )
    .await;

    assert!(matches!(
        result.as_ref().unwrap_err(),
        DeltaTableError::Transaction { .. }
    ));
    if let DeltaTableError::Transaction { source } = result.unwrap_err() {
        assert!(matches!(source, TransactionError::CommitConflict(_)));
    }
}

/// Verify that the merge write projection preserves the physical Arrow types
/// produced by the target scan.
///
/// When `schema_force_view_types` is enabled (the default), the scan produces
/// `Utf8View`/`BinaryView` columns. The write projection must not cast these
/// back to `Utf8`/`Binary` (which would re-introduce Arrow's 2 GB offset limit).
///
/// When disabled, the legacy `Utf8`/`Binary` types must be preserved.
#[tokio::test]
async fn test_merge_respects_schema_force_view_types() -> DeltaResult<()> {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("content", DataType::Utf8, true),
        Field::new("payload", DataType::Binary, true),
    ]));

    let delta_schema = StructType::try_new(vec![
        StructField::new("id", DeltaDataType::Primitive(PrimitiveType::String), true),
        StructField::new(
            "content",
            DeltaDataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "payload",
            DeltaDataType::Primitive(PrimitiveType::Binary),
            true,
        ),
    ])
    .unwrap();

    let initial_batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B"])),
            Arc::new(arrow::array::StringArray::from(vec!["hello", "world"])),
            Arc::new(arrow::array::BinaryArray::from(vec![
                b"bin1".as_ref(),
                b"bin2".as_ref(),
            ])),
        ],
    )
    .unwrap();

    let source_batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["B", "C"])),
            Arc::new(arrow::array::StringArray::from(vec!["updated", "new"])),
            Arc::new(arrow::array::BinaryArray::from(vec![
                b"bin2_new".as_ref(),
                b"bin3".as_ref(),
            ])),
        ],
    )
    .unwrap();

    // -- view types ENABLED (default) -----------------------------------------
    {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .unwrap();
        let table = table
            .write(vec![initial_batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let source = ctx.read_batch(source_batch.clone()).unwrap();

        let (table, metrics) = table
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|update| {
                update
                    .update("content", col("source.content"))
                    .update("payload", col("source.payload"))
            })?
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", col("source.id"))
                    .set("content", col("source.content"))
                    .set("payload", col("source.payload"))
            })?
            .await?;

        assert_eq!(metrics.num_target_rows_updated, 1);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_output_rows, 3);

        // Read back and assert the physical Arrow types are Utf8View / BinaryView.
        let read_ctx = SessionContext::new();
        table.update_datafusion_session(&read_ctx.state()).unwrap();
        let provider = table.table_provider().await.unwrap();
        read_ctx.register_table("t", provider).unwrap();
        let batches = read_ctx
            .sql("SELECT * FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Utf8View,
            "expected Utf8View for 'id' when view types enabled"
        );
        assert_eq!(
            schema.field_with_name("content").unwrap().data_type(),
            &DataType::Utf8View,
            "expected Utf8View for 'content' when view types enabled"
        );
        assert_eq!(
            schema.field_with_name("payload").unwrap().data_type(),
            &DataType::BinaryView,
            "expected BinaryView for 'payload' when view types enabled"
        );
    }

    // -- view types DISABLED --------------------------------------------------
    {
        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(delta_schema.fields().cloned())
            .await
            .unwrap();
        let table = table
            .write(vec![initial_batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let source = ctx.read_batch(source_batch.clone()).unwrap();

        // Create a session with schema_force_view_types disabled
        let config: SessionConfig =
            deltalake_core::delta_datafusion::DeltaSessionConfig::default().into();
        let config = config.set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );
        let runtime_env = datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
            .build_arc()
            .unwrap();
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_runtime_env(runtime_env)
            .build();
        let session = Arc::new(state);

        let (table, metrics) = table
            .merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .with_session_state(session)
            .when_matched_update(|update| {
                update
                    .update("content", col("source.content"))
                    .update("payload", col("source.payload"))
            })?
            .when_not_matched_insert(|insert| {
                insert
                    .set("id", col("source.id"))
                    .set("content", col("source.content"))
                    .set("payload", col("source.payload"))
            })?
            .await?;

        assert_eq!(metrics.num_target_rows_updated, 1);
        assert_eq!(metrics.num_target_rows_inserted, 1);
        assert_eq!(metrics.num_target_rows_copied, 1);
        assert_eq!(metrics.num_output_rows, 3);

        // Read back with view types disabled and assert legacy Utf8 / Binary.
        let read_config = SessionConfig::default().set_bool(
            "datafusion.execution.parquet.schema_force_view_types",
            false,
        );
        let read_ctx = SessionContext::new_with_config(read_config);
        table.update_datafusion_session(&read_ctx.state()).unwrap();
        let provider = table
            .table_provider()
            .with_session(Arc::new(read_ctx.state()))
            .await
            .unwrap();
        read_ctx.register_table("t", provider).unwrap();
        let batches = read_ctx
            .sql("SELECT * FROM t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Utf8,
            "expected Utf8 for 'id' when view types disabled"
        );
        assert_eq!(
            schema.field_with_name("content").unwrap().data_type(),
            &DataType::Utf8,
            "expected Utf8 for 'content' when view types disabled"
        );
        assert_eq!(
            schema.field_with_name("payload").unwrap().data_type(),
            &DataType::Binary,
            "expected Binary for 'payload' when view types disabled"
        );
    }

    Ok(())
}
