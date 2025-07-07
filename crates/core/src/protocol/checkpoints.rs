//! Implementation for writing delta checkpoints.

use std::sync::{Arc, LazyLock};

use arrow::compute::filter_record_batch;
use arrow_array::{BooleanArray, RecordBatch};
use chrono::Utc;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::snapshot::LastCheckpointHint;
use delta_kernel::{FileMeta, Table};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{Error, ObjectStore};
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use regex::Regex;
use tokio::task::spawn_blocking;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::logstore::{LogStore, LogStoreExt};
use crate::{open_table_with_version, DeltaTable};
use crate::{DeltaResult, DeltaTableError};

/// Creates checkpoint for a given table version, table state and object store
pub(crate) async fn create_checkpoint_for(
    version: u64,
    log_store: &dyn LogStore,
    operation_id: Option<Uuid>,
) -> DeltaResult<()> {
    let table_root = if let Some(op_id) = operation_id {
        #[allow(deprecated)]
        log_store.transaction_url(op_id, &log_store.table_root_url())?
    } else {
        log_store.table_root_url()
    };
    let engine = log_store.engine(operation_id).await;

    let table = Table::try_from_uri(table_root)?;

    let task_engine = engine.clone();
    let snapshot = spawn_blocking(move || table.snapshot(task_engine.as_ref(), Some(version)))
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;
    let snapshot = Arc::new(snapshot);

    let cp_writer = snapshot.checkpoint()?;

    let cp_url = cp_writer.checkpoint_path()?;
    let cp_path = Path::from_url_path(cp_url.path())?;
    let mut cp_data = cp_writer.checkpoint_data(engine.as_ref())?;

    let (first_batch, mut cp_data) = spawn_blocking(move || {
        let Some(first_batch) = cp_data.next() else {
            return Err(DeltaTableError::Generic("No data".to_string()));
        };
        Ok((to_rb(first_batch?)?, cp_data))
    })
    .await
    .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

    let root_store = log_store.root_object_store(operation_id);
    let object_store_writer = ParquetObjectWriter::new(root_store.clone(), cp_path.clone());
    let mut writer = AsyncArrowWriter::try_new(object_store_writer, first_batch.schema(), None)?;
    writer.write(&first_batch).await?;

    let mut current_batch;
    loop {
        (current_batch, cp_data) = spawn_blocking(move || {
            let Some(first_batch) = cp_data.next() else {
                return Ok::<_, DeltaTableError>((None, cp_data));
            };
            Ok((Some(to_rb(first_batch?)?), cp_data))
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        let Some(batch) = current_batch else {
            break;
        };
        writer.write(&batch).await?;
    }

    let _pq_meta = writer.close().await?;
    let file_meta = root_store.head(&cp_path).await?;
    let file_meta = FileMeta {
        location: cp_url,
        size: file_meta.size,
        last_modified: file_meta.last_modified.timestamp_millis(),
    };

    spawn_blocking(move || cp_writer.finalize(engine.as_ref(), &file_meta, cp_data))
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

    Ok(())
}

fn to_rb(data: FilteredEngineData) -> DeltaResult<RecordBatch> {
    let engine_data = ArrowEngineData::try_from_engine_data(data.data)?;
    let predicate = BooleanArray::from(data.selection_vector);
    let batch = filter_record_batch(engine_data.record_batch(), &predicate)?;
    Ok(batch)
}

/// Creates checkpoint at current table version
pub async fn create_checkpoint(table: &DeltaTable, operation_id: Option<Uuid>) -> DeltaResult<()> {
    let snapshot = table.snapshot()?;
    create_checkpoint_for(
        snapshot.version() as u64,
        table.log_store.as_ref(),
        operation_id,
    )
    .await?;
    Ok(())
}

/// Delete expires log files before given version from table. The table log retention is based on
/// the `logRetentionDuration` property of the Delta Table, 30 days by default.
pub async fn cleanup_metadata(
    table: &DeltaTable,
    operation_id: Option<Uuid>,
) -> DeltaResult<usize> {
    let snapshot = table.snapshot()?;
    let log_retention_timestamp = Utc::now().timestamp_millis()
        - snapshot.table_config().log_retention_duration().as_millis() as i64;
    cleanup_expired_logs_for(
        snapshot.version(),
        table.log_store.as_ref(),
        log_retention_timestamp,
        operation_id,
    )
    .await
}

/// Loads table from given `table_uri` at given `version` and creates checkpoint for it.
/// The `cleanup` param decides whether to run metadata cleanup of obsolete logs.
/// If it's empty then the table's `enableExpiredLogCleanup` is used.
pub async fn create_checkpoint_from_table_uri_and_cleanup(
    table_uri: &str,
    version: i64,
    cleanup: Option<bool>,
    operation_id: Option<Uuid>,
) -> DeltaResult<()> {
    let table = open_table_with_version(table_uri, version).await?;
    let snapshot = table.snapshot()?;
    create_checkpoint_for(version as u64, table.log_store.as_ref(), operation_id).await?;

    let enable_expired_log_cleanup =
        cleanup.unwrap_or_else(|| snapshot.table_config().enable_expired_log_cleanup());

    if snapshot.version() >= 0 && enable_expired_log_cleanup {
        let deleted_log_num = cleanup_metadata(&table, operation_id).await?;
        debug!("Deleted {deleted_log_num:?} log files.");
    }

    Ok(())
}

/// Deletes all delta log commits that are older than the cutoff time
/// and less than the specified version.
pub async fn cleanup_expired_logs_for(
    until_version: i64,
    log_store: &dyn LogStore,
    cutoff_timestamp: i64,
    operation_id: Option<Uuid>,
) -> DeltaResult<usize> {
    static DELTA_LOG_REGEX: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"_delta_log/(\d{20})\.(json|checkpoint|json.tmp).*$").unwrap()
    });

    let object_store = log_store.object_store(None);
    let log_path = log_store.log_path();
    let maybe_last_checkpoint = read_last_checkpoint(&object_store, log_path).await?;

    let Some(last_checkpoint) = maybe_last_checkpoint else {
        return Ok(0);
    };

    let until_version = i64::min(until_version, last_checkpoint.version as i64);

    // Feed a stream of candidate deletion files directly into the delete_stream
    // function to try to improve the speed of cleanup and reduce the need for
    // intermediate memory.
    let object_store = log_store.object_store(operation_id);
    let deleted = object_store
        .delete_stream(
            object_store
                .list(Some(log_store.log_path()))
                // This predicate function will filter out any locations that don't
                // match the given timestamp range
                .filter_map(|meta: Result<crate::ObjectMeta, _>| async move {
                    if meta.is_err() {
                        error!("Error received while cleaning up expired logs: {meta:?}");
                        return None;
                    }
                    let meta = meta.unwrap();
                    let ts = meta.last_modified.timestamp_millis();

                    match DELTA_LOG_REGEX.captures(meta.location.as_ref()) {
                        Some(captures) => {
                            let log_ver_str = captures.get(1).unwrap().as_str();
                            let log_ver: i64 = log_ver_str.parse().unwrap();
                            if log_ver < until_version && ts <= cutoff_timestamp {
                                // This location is ready to be deleted
                                Some(Ok(meta.location))
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                })
                .boxed(),
        )
        .try_collect::<Vec<_>>()
        .await?;

    debug!("Deleted {} expired logs", deleted.len());
    Ok(deleted.len())
}

/// Try reading the `_last_checkpoint` file.
///
/// Note that we typically want to ignore a missing/invalid `_last_checkpoint` file without failing
/// the read. Thus, the semantics of this function are to return `None` if the file is not found or
/// is invalid JSON. Unexpected/unrecoverable errors are returned as `Err` case and are assumed to
/// cause failure.
async fn read_last_checkpoint(
    storage: &dyn ObjectStore,
    log_path: &Path,
) -> DeltaResult<Option<LastCheckpointHint>> {
    const LAST_CHECKPOINT_FILE_NAME: &str = "_last_checkpoint";
    let file_path = log_path.child(LAST_CHECKPOINT_FILE_NAME);
    let maybe_data = storage.get(&file_path).await;
    let data = match maybe_data {
        Ok(data) => data.bytes().await?,
        Err(Error::NotFound { .. }) => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(serde_json::from_slice(&data)
        .inspect_err(|e| warn!("invalid _last_checkpoint JSON: {e}"))
        .ok())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, StructBuilder};
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use arrow_schema::Schema as ArrowSchema;
    use chrono::Duration;
    use object_store::path::Path;

    use super::*;
    use crate::kernel::transaction::{CommitBuilder, TableReference};
    use crate::kernel::Action;
    use crate::operations::DeltaOps;
    use crate::writer::test_utils::get_delta_schema;
    use crate::DeltaResult;

    #[tokio::test]
    async fn test_create_checkpoint_for() {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(crate::protocol::SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_schema().unwrap(), &table_schema);
        let res = create_checkpoint_for(0, table.log_store.as_ref(), None).await;
        assert!(res.is_ok());

        // Look at the "files" and verify that the _last_checkpoint has the right version
        let log_path = Path::from("_delta_log");
        let store = table.log_store().object_store(None);
        let last_checkpoint = read_last_checkpoint(store.as_ref(), &log_path)
            .await
            .expect("Failed to get the _last_checkpoint")
            .expect("Expected checkpoint hint");
        assert_eq!(last_checkpoint.version, 0);
    }

    /// This test validates that a checkpoint can be written and re-read with the minimum viable
    /// Metadata. There was a bug which didn't handle the optionality of createdTime.
    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_create_checkpoint_with_metadata() {
        use crate::kernel::new_metadata;

        let table_schema = get_delta_schema();

        let mut table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(crate::protocol::SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_schema().unwrap(), &table_schema);

        let part_cols: Vec<String> = vec![];
        let metadata =
            new_metadata(&table_schema, part_cols, std::iter::empty::<(&str, &str)>()).unwrap();
        let actions = vec![Action::Metadata(metadata)];

        let epoch_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;

        let operation = crate::protocol::DeltaOperation::StreamingUpdate {
            output_mode: crate::protocol::OutputMode::Append,
            query_id: "test".into(),
            epoch_id,
        };
        let finalized_commit = CommitBuilder::default()
            .with_actions(actions)
            .build(
                table.state.as_ref().map(|f| f as &dyn TableReference),
                table.log_store(),
                operation,
            )
            .await
            .unwrap();

        assert_eq!(
            1,
            finalized_commit.version(),
            "Expected the commit to create table version 1"
        );
        assert_eq!(
            0, finalized_commit.metrics.num_retries,
            "Expected no retries"
        );
        assert_eq!(
            0, finalized_commit.metrics.num_log_files_cleaned_up,
            "Expected no log files cleaned up"
        );
        assert!(
            !finalized_commit.metrics.new_checkpoint_created,
            "Expected checkpoint created."
        );
        table.load().await.expect("Failed to reload table");
        assert_eq!(
            table.version(),
            Some(1),
            "The loaded version of the table is not up to date"
        );

        let res = create_checkpoint_for(
            table.version().unwrap() as u64,
            table.log_store.as_ref(),
            None,
        )
        .await;
        assert!(res.is_ok());

        // Look at the "files" and verify that the _last_checkpoint has the right version
        let log_path = Path::from("_delta_log");
        let store = table.log_store().object_store(None);
        let last_checkpoint = read_last_checkpoint(store.as_ref(), &log_path)
            .await
            .expect("Failed to get the _last_checkpoint")
            .expect("Expected checkpoint hint");
        assert_eq!(last_checkpoint.version, 1);

        // If the regression exists, this will fail
        table.load().await.expect("Failed to reload the table, this likely means that the optional createdTime was not actually optional");
        assert_eq!(
            Some(1),
            table.version(),
            "The reloaded table doesn't have the right version"
        );
    }

    #[tokio::test]
    async fn test_create_checkpoint_for_invalid_version() {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(crate::protocol::SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.get_schema().unwrap(), &table_schema);
        match create_checkpoint_for(1, table.log_store.as_ref(), None).await {
            Ok(_) => {
                /*
                 * If a checkpoint is allowed to be created here, it will use the passed in
                 * version, but _last_checkpoint is generated from the table state will point to a
                 * version 0 checkpoint.
                 * E.g.
                 *
                 * Path { raw: "_delta_log/00000000000000000000.json" }
                 * Path { raw: "_delta_log/00000000000000000001.checkpoint.parquet" }
                 * Path { raw: "_delta_log/_last_checkpoint" }
                 *
                 */
                panic!(
                    "We should not allow creating a checkpoint for a version which doesn't exist!"
                );
            }
            Err(_) => { /* We should expect an error in the "right" case */ }
        }
    }

    #[cfg(feature = "datafusion")]
    async fn setup_table() -> DeltaTable {
        use arrow_schema::{DataType, Field};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Utf8,
            false,
        )]));

        let data =
            vec![Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])) as ArrayRef];
        let batches = vec![RecordBatch::try_new(schema.clone(), data).unwrap()];

        let table = DeltaOps::new_in_memory()
            .write(batches.clone())
            .await
            .unwrap();

        DeltaOps(table)
            .write(batches)
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap()
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_cleanup_no_checkpoints() {
        // Test that metadata clean up does not corrupt the table when no checkpoints exist
        let table = setup_table().await;

        let log_retention_timestamp = (Utc::now().timestamp_millis()
            + Duration::days(31).num_milliseconds())
            - table
                .snapshot()
                .unwrap()
                .table_config()
                .log_retention_duration()
                .as_millis() as i64;
        let count = cleanup_expired_logs_for(
            table.version().unwrap(),
            table.log_store().as_ref(),
            log_retention_timestamp,
            None,
        )
        .await
        .unwrap();
        assert_eq!(count, 0);
        println!("{count:?}");

        let path = Path::from("_delta_log/00000000000000000000.json");
        let res = table.log_store().object_store(None).get(&path).await;
        assert!(res.is_ok());
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_cleanup_with_checkpoints() {
        let table = setup_table().await;
        create_checkpoint(&table, None).await.unwrap();

        let log_retention_timestamp = (Utc::now().timestamp_millis()
            + Duration::days(32).num_milliseconds())
            - table
                .snapshot()
                .unwrap()
                .table_config()
                .log_retention_duration()
                .as_millis() as i64;
        let count = cleanup_expired_logs_for(
            table.version().unwrap(),
            table.log_store().as_ref(),
            log_retention_timestamp,
            None,
        )
        .await
        .unwrap();
        assert_eq!(count, 1);

        let log_store = table.log_store();

        let path = log_store.log_path().child("00000000000000000000.json");
        let res = table.log_store().object_store(None).get(&path).await;
        assert!(res.is_err());

        let path = log_store
            .log_path()
            .child("00000000000000000001.checkpoint.parquet");
        let res = table.log_store().object_store(None).get(&path).await;
        assert!(res.is_ok());

        let path = log_store.log_path().child("00000000000000000001.json");
        let res = table.log_store().object_store(None).get(&path).await;
        assert!(res.is_ok());
    }

    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_struct_with_single_list_field() {
        // you need another column otherwise the entire stats struct is empty
        // which also fails parquet write during checkpoint
        let other_column_array: ArrayRef = Arc::new(Int32Array::from(vec![1]));

        let mut list_item_builder = Int32Builder::new();
        list_item_builder.append_value(1);

        let mut list_in_struct_builder = ListBuilder::new(list_item_builder);
        list_in_struct_builder.append(true);

        let mut struct_builder = StructBuilder::new(
            vec![arrow_schema::Field::new(
                "list_in_struct",
                arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    arrow_schema::DataType::Int32,
                    true,
                ))),
                true,
            )],
            vec![Box::new(list_in_struct_builder)],
        );
        struct_builder.append(true);

        let struct_with_list_array: ArrayRef = Arc::new(struct_builder.finish());
        let batch = RecordBatch::try_from_iter(vec![
            ("other_column", other_column_array),
            ("struct_with_list", struct_with_list_array),
        ])
        .unwrap();
        let table = DeltaOps::new_in_memory().write(vec![batch]).await.unwrap();

        create_checkpoint(&table, None).await.unwrap();
    }

    #[ignore = "This test is only useful if the batch size has been made small"]
    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_checkpoint_large_table() -> DeltaResult<()> {
        use crate::writer::test_utils::get_arrow_schema;

        let table_schema = get_delta_schema();
        let temp_dir = tempfile::tempdir()?;
        let table_path = temp_dir.path().to_str().unwrap();
        let mut table = DeltaOps::try_from_uri(&table_path)
            .await?
            .create()
            .with_columns(table_schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        let count = 20;

        for _ in 0..count {
            table.load().await?;
            let batch = RecordBatch::try_new(
                Arc::clone(&get_arrow_schema(&None)),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "C"])),
                    Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                    Arc::new(arrow::array::StringArray::from(vec![
                        "2021-02-02",
                        "2021-02-03",
                        "2021-02-02",
                        "2021-02-04",
                    ])),
                ],
            )
            .unwrap();
            let _ = DeltaOps(table.clone()).write(vec![batch]).await?;
        }

        table.load().await?;
        assert_eq!(
            table.version().unwrap(),
            count,
            "Expected {count} transactions"
        );
        let pre_checkpoint_actions = table.snapshot()?.file_actions()?;

        let before = table.version();
        let res = create_checkpoint(&table, None).await;
        assert!(res.is_ok(), "Failed to create the checkpoint! {res:#?}");

        let table = crate::open_table(&table_path).await?;
        assert_eq!(
            before,
            table.version(),
            "Why on earth did a checkpoint creata version?"
        );

        let post_checkpoint_actions = table.snapshot()?.file_actions()?;

        assert_eq!(
            pre_checkpoint_actions.len(),
            post_checkpoint_actions.len(),
            "The number of actions read from the table after checkpointing is wrong!"
        );
        Ok(())
    }

    /// <https://github.com/delta-io/delta-rs/issues/3030>
    #[cfg(feature = "datafusion")]
    #[tokio::test]
    async fn test_create_checkpoint_overwrite() -> DeltaResult<()> {
        use crate::protocol::SaveMode;
        use crate::writer::test_utils::datafusion::get_data_sorted;
        use crate::writer::test_utils::get_arrow_schema;
        use datafusion::assert_batches_sorted_eq;

        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp_dir.path()).unwrap();

        let batch = RecordBatch::try_new(
            Arc::clone(&get_arrow_schema(&None)),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["C"])),
                Arc::new(arrow::array::Int32Array::from(vec![30])),
                Arc::new(arrow::array::StringArray::from(vec!["2021-02-03"])),
            ],
        )
        .unwrap();

        let mut table = DeltaOps::try_from_uri(tmp_path.as_os_str().to_str().unwrap())
            .await?
            .write(vec![batch])
            .await?;
        table.load().await?;
        assert_eq!(table.version(), Some(0));

        create_checkpoint(&table, None).await?;

        let batch = RecordBatch::try_new(
            Arc::clone(&get_arrow_schema(&None)),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0])),
                Arc::new(arrow::array::StringArray::from(vec!["2021-02-02"])),
            ],
        )
        .unwrap();

        let table = DeltaOps::try_from_uri(tmp_path.as_os_str().to_str().unwrap())
            .await?
            .write(vec![batch])
            .with_save_mode(SaveMode::Overwrite)
            .await?;
        assert_eq!(table.version(), Some(1));

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "+----+-------+------------+",
        ];
        let actual = get_data_sorted(&table, "id,value,modified").await;
        assert_batches_sorted_eq!(&expected, &actual);
        Ok(())
    }
}
