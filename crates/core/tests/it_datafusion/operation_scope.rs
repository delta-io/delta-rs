//! Every writing operation runs inside exactly one operation scope.
//!
//! These tests drive each builder against the in-memory isolating store of
//! [`deltalake_core::test_utils::isolating_store`] and check the scope lifecycle: one scope per operation,
//! sibling scopes for post-commit work, every write below the scope prefix, and an abort on
//! every failure path.
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use chrono::Duration;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use delta_kernel::schema::MetadataValue;
use std::future::{Future, IntoFuture};

use deltalake_core::TableProperty;
use deltalake_core::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake_core::kernel::transaction::CommitProperties;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField, TableFeatures};
use deltalake_core::logstore::LogStoreRef;
use deltalake_core::operations::convert_to_delta::ConvertToDeltaBuilder;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::operations::update_table_metadata::TableMetadataUpdate;
use deltalake_core::protocol::SaveMode;
use deltalake_core::protocol::log_compaction::compact_logs;
use deltalake_core::test_utils::isolating_store::{IsolatingLogStore, ScopeEvent};
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableConfig};

fn schema() -> Vec<StructField> {
    vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false),
        StructField::new("value", DataType::Primitive(PrimitiveType::String), true),
    ]
}

fn batch(ids: &[i32]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowDataType::Int32, false),
        Field::new("value", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("v{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn create_table(
    store: &Arc<IsolatingLogStore>,
    properties: Vec<(TableProperty, &str)>,
) -> DeltaTable {
    let log_store: LogStoreRef = store.clone();
    let mut builder = CreateBuilder::new()
        .with_log_store(log_store)
        .with_columns(schema());
    for (key, value) in properties {
        builder = builder.with_configuration_property(key, Some(value));
    }
    builder.await.unwrap()
}

/// A table with two data files, at version 2.
async fn table_with_data(store: &Arc<IsolatingLogStore>) -> DeltaTable {
    let table = create_table(store, vec![]).await;
    let table = table.write(vec![batch(&[1, 2, 3])]).await.unwrap();
    let table = table.write(vec![batch(&[4, 5, 6])]).await.unwrap();
    assert_eq!(table.version(), Some(2));
    store.reset();
    table
}

/// Check that the recorded events describe exactly one finished operation scope, with sibling
/// scopes nested inside it and no aborts, and that every write stayed below the scope prefix.
fn assert_single_operation(name: &str, store: &IsolatingLogStore) -> Vec<ScopeEvent> {
    let events = store.events();
    let Some(ScopeEvent::Begin(outer)) = events.first().cloned() else {
        panic!("{name}: no scope was opened: {events:?}");
    };
    assert!(
        matches!(events.last(), Some(ScopeEvent::Finish { scope, .. }) if *scope == outer),
        "{name}: the operation scope must finish last: {events:?}"
    );
    let begins = events
        .iter()
        .filter(|e| matches!(e, ScopeEvent::Begin(_)))
        .count();
    let finishes = events
        .iter()
        .filter(|e| matches!(e, ScopeEvent::Finish { .. }))
        .count();
    assert_eq!(
        begins, finishes,
        "{name}: every scope must finish: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ScopeEvent::Begin(s) if *s == outer))
            .count(),
        1,
        "{name}: exactly one operation scope: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(e, ScopeEvent::Abort(_))),
        "{name}: no scope may abort: {events:?}"
    );
    let writes = store.take_recorded_writes();
    assert!(
        writes.iter().all(|p| p.as_ref().starts_with("scopes/")),
        "{name}: a write reached the table outside a scope: {writes:?}"
    );
    events
}

/// Run `op` on `table` and check that it ran inside one operation scope.
async fn check_scoped<F, Fut>(
    name: &str,
    store: &Arc<IsolatingLogStore>,
    table: DeltaTable,
    op: F,
) -> DeltaTable
where
    F: FnOnce(DeltaTable) -> Fut,
    Fut: Future<Output = DeltaResult<DeltaTable>>,
{
    store.reset();
    let before = table.version();
    let table = op(table)
        .await
        .unwrap_or_else(|e| panic!("{name} failed: {e}"));
    let log_store = table.log_store();
    assert!(
        log_store.name() == "IsolatingLogStore"
            && log_store.write_root_url() == *log_store.root_url(),
        "{name}: the returned table must use the parent store, not the scoped store"
    );
    assert_single_operation(name, store);
    assert!(table.version() >= before, "{name}: version went backwards");
    table
}

fn dirty_flags(events: &[ScopeEvent]) -> Vec<bool> {
    events
        .iter()
        .filter_map(|e| match e {
            ScopeEvent::Finish { dirty, .. } => Some(*dirty),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn create_and_write_run_in_one_scope_each_with_a_post_commit_sibling() {
    let store = IsolatingLogStore::new();
    let table = create_table(&store, vec![]).await;
    let events = assert_single_operation("create", &store);
    assert_eq!(
        events,
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Commit {
                scope: 1,
                version: 0
            },
            ScopeEvent::Finish {
                scope: 1,
                dirty: false
            },
        ]
    );

    let table = check_scoped("write", &store, table, |t| {
        t.write(vec![batch(&[1, 2, 3])]).into_future()
    })
    .await;
    assert_eq!(table.version(), Some(1));
    let events = store.events();
    // The commit publishes the data files. Expired-log cleanup runs in a sibling scope that
    // writes nothing, so both scopes finish clean.
    assert_eq!(
        events,
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Commit {
                scope: 1,
                version: 1
            },
            ScopeEvent::Begin(2),
            ScopeEvent::Finish {
                scope: 2,
                dirty: false
            },
            ScopeEvent::Finish {
                scope: 1,
                dirty: false
            },
        ]
    );
    let objects = store.table_objects().await;
    assert!(objects.iter().any(|o| o.ends_with(".parquet")));
    assert!(objects.contains("_delta_log/00000000000000000001.json"));
}

#[tokio::test]
async fn data_operations_run_in_one_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;

    let table = check_scoped("write overwrite", &store, table, |t| {
        t.write(vec![batch(&[7, 8])])
            .with_save_mode(SaveMode::Overwrite)
            .into_future()
    })
    .await;
    let table = check_scoped("delete", &store, table, |t| async move {
        t.delete()
            .with_predicate(col("id").eq(lit(7)))
            .await
            .map(|(t, _)| t)
    })
    .await;
    let table = check_scoped("update", &store, table, |t| async move {
        t.update()
            .with_predicate(col("id").eq(lit(8)))
            .with_update("value", lit("updated"))
            .await
            .map(|(t, _)| t)
    })
    .await;
    let table = check_scoped("merge", &store, table, |t| async move {
        let source = SessionContext::new().read_batch(batch(&[8, 9]))?;
        t.merge(source, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .when_matched_update(|u| u.update("value", col("source.value")))?
            .when_not_matched_insert(|i| {
                i.set("id", col("source.id"))
                    .set("value", col("source.value"))
            })?
            .await
            .map(|(t, _)| t)
    })
    .await;
    // A second file gives optimize something to compact.
    let table = check_scoped("write append", &store, table, |t| {
        t.write(vec![batch(&[20, 21])]).into_future()
    })
    .await;
    let table = check_scoped("optimize", &store, table, |t| async move {
        t.optimize()
            .with_target_size(NonZeroU64::new(10_000_000).unwrap())
            .await
            .map(|(t, metrics)| {
                assert_eq!(
                    metrics.num_files_removed, 2,
                    "optimize compacted both files"
                );
                t
            })
    })
    .await;
    let table = check_scoped("restore", &store, table, |t| async move {
        t.restore().with_version_to_restore(2).await.map(|(t, _)| t)
    })
    .await;
    assert_eq!(table.version(), Some(9));
}

#[tokio::test]
async fn vacuum_runs_both_commits_and_the_deletes_in_one_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;
    let table = table
        .write(vec![batch(&[7])])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
    let files_before = store.table_objects().await;

    let table = check_scoped("vacuum", &store, table, |t| async move {
        t.vacuum()
            .with_retention_period(Duration::hours(0))
            .with_enforce_retention_duration(false)
            .await
            .map(|(t, _)| t)
    })
    .await;
    let events = store.events();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, ScopeEvent::Commit { scope: 1, .. }))
            .count(),
        2,
        "vacuum start and end commit in the operation scope: {events:?}"
    );
    assert_eq!(table.version(), Some(5));
    let files_after = store.table_objects().await;
    assert!(
        files_before
            .iter()
            .filter(|f| f.ends_with(".parquet"))
            .count()
            > files_after
                .iter()
                .filter(|f| f.ends_with(".parquet"))
                .count(),
        "vacuum deletes were published"
    );
}

#[tokio::test]
async fn empty_vacuum_opens_no_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;
    let (table, metrics) = table
        .vacuum()
        .with_retention_period(Duration::hours(0))
        .with_enforce_retention_duration(false)
        .await
        .unwrap();
    assert!(metrics.files_deleted.is_empty());
    assert_eq!(table.version(), Some(2));
    assert!(store.events().is_empty(), "{:?}", store.events());
}

#[tokio::test]
async fn filesystem_check_runs_in_one_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;
    let missing = store
        .table_objects()
        .await
        .into_iter()
        .find(|o| o.ends_with(".parquet"))
        .unwrap();
    store.delete_from_table(&missing).await;

    let table = check_scoped("filesystem_check", &store, table, |t| async move {
        t.filesystem_check().await.map(|(t, _)| t)
    })
    .await;
    assert_eq!(table.version(), Some(3));
}

#[tokio::test]
async fn metadata_operations_run_in_one_clean_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;

    let table = check_scoped("add_columns", &store, table, |t| {
        t.add_columns()
            .with_fields(vec![StructField::new(
                "extra",
                DataType::Primitive(PrimitiveType::String),
                true,
            )])
            .into_future()
    })
    .await;
    assert_eq!(dirty_flags(&store.events()), vec![false, false]);

    let table = check_scoped("add_constraint", &store, table, |t| {
        t.add_constraint()
            .with_constraint("id_positive", "id > 0")
            .into_future()
    })
    .await;
    let table = check_scoped("drop_constraints", &store, table, |t| {
        t.drop_constraints()
            .with_constraint("id_positive")
            .into_future()
    })
    .await;
    let table = check_scoped("set_tbl_properties", &store, table, |t| {
        t.set_tbl_properties()
            .with_properties(HashMap::from([(
                "delta.appendOnly".to_string(),
                "false".to_string(),
            )]))
            .into_future()
    })
    .await;
    let table = check_scoped("update_field_metadata", &store, table, |t| {
        t.update_field_metadata()
            .with_field_name("value")
            .with_metadata(HashMap::from([(
                "comment".to_string(),
                MetadataValue::String("a value".to_string()),
            )]))
            .into_future()
    })
    .await;
    let table = check_scoped("update_table_metadata", &store, table, |t| {
        t.update_table_metadata()
            .with_update(TableMetadataUpdate {
                name: Some("scoped".to_string()),
                description: None,
            })
            .into_future()
    })
    .await;
    let table = check_scoped("drop_column_not_null", &store, table, |t| {
        t.drop_column_not_null().with_column("id").into_future()
    })
    .await;
    let table = check_scoped("add_feature", &store, table, |t| {
        t.add_feature()
            .with_feature(TableFeatures::DeletionVectors)
            .with_allow_protocol_versions_increase(true)
            .into_future()
    })
    .await;
    assert_eq!(dirty_flags(&store.events()), vec![false, false]);
    assert_eq!(table.version(), Some(10));
}

#[tokio::test]
async fn generate_publishes_manifests_through_a_dirty_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;
    let table = check_scoped("generate", &store, table, |t| t.generate().into_future()).await;
    assert_eq!(
        store.events(),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Finish {
                scope: 1,
                dirty: true
            }
        ]
    );
    assert_eq!(table.version(), Some(2));
    assert!(
        store
            .table_objects()
            .await
            .iter()
            .any(|o| o.starts_with("_symlink_format_manifest/")),
        "the manifest was published"
    );
}

#[tokio::test]
async fn convert_to_delta_creates_the_table_in_one_scope() {
    let store = IsolatingLogStore::new_at("parquet_table");
    let mut buffer = Vec::new();
    {
        let data = batch(&[1, 2]);
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, data.schema(), None).unwrap();
        writer.write(&data).unwrap();
        writer.close().unwrap();
    }
    store.put_in_table("part-00000.parquet", buffer).await;
    store.reset();

    let log_store: LogStoreRef = store.clone();
    let table = ConvertToDeltaBuilder::new()
        .with_log_store(log_store)
        .await
        .unwrap();
    assert_eq!(table.version(), Some(0));
    assert_eq!(
        assert_single_operation("convert_to_delta", &store),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Commit {
                scope: 1,
                version: 0
            },
            ScopeEvent::Finish {
                scope: 1,
                dirty: false
            },
        ]
    );
}

#[tokio::test]
async fn datafusion_insert_into_runs_in_one_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    ctx.sql("INSERT INTO t VALUES (10, 'ten'), (11, 'eleven')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let events = assert_single_operation("INSERT INTO", &store);
    assert_eq!(events[0], ScopeEvent::Begin(1));
    assert_eq!(
        events[1],
        ScopeEvent::Commit {
            scope: 1,
            version: 3
        }
    );

    let mut reloaded = DeltaTable::new(store.clone(), DeltaTableConfig::default());
    reloaded.load().await.unwrap();
    assert_eq!(reloaded.version(), Some(3));
}

#[tokio::test]
async fn public_checkpoint_functions_own_a_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;

    create_checkpoint(&table).await.unwrap();
    assert_eq!(
        assert_single_operation("create_checkpoint", &store),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Finish {
                scope: 1,
                dirty: true
            }
        ]
    );
    assert!(
        store
            .table_objects()
            .await
            .contains("_delta_log/00000000000000000002.checkpoint.parquet")
    );

    store.reset();
    compact_logs(&table, 0, 2).await.unwrap();
    assert_eq!(
        assert_single_operation("compact_logs", &store),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Finish {
                scope: 1,
                dirty: true
            }
        ]
    );
    assert!(
        store
            .table_objects()
            .await
            .contains("_delta_log/00000000000000000000.00000000000000000002.compacted.json"),
        "{:?}",
        store.table_objects().await
    );

    store.reset();
    let deleted = cleanup_metadata(&table).await.unwrap();
    assert_eq!(deleted, 0);
    assert_eq!(
        assert_single_operation("cleanup_metadata", &store),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Finish {
                scope: 1,
                dirty: false
            }
        ]
    );
}

#[tokio::test]
async fn constraint_violation_aborts_the_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;
    let table = table
        .add_constraint()
        .with_constraint("id_positive", "id > 0")
        .await
        .unwrap();
    store.reset();

    let err = table.write(vec![batch(&[-1])]).await.unwrap_err();
    assert!(err.to_string().contains("failed validation"), "{err}");
    let events = store.events();
    assert_eq!(events.first(), Some(&ScopeEvent::Begin(1)));
    assert_eq!(events.last(), Some(&ScopeEvent::Abort(1)));
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ScopeEvent::Commit { .. }))
    );
    assert!(
        !store
            .table_objects()
            .await
            .contains("_delta_log/00000000000000000004.json"),
        "nothing was published"
    );
}

#[tokio::test]
async fn concurrent_commit_with_no_retries_aborts_the_scope() {
    let store = IsolatingLogStore::new();
    table_with_data(&store).await;
    let mut writer_a = DeltaTable::new(store.clone(), DeltaTableConfig::default());
    writer_a.load().await.unwrap();
    let mut writer_b = DeltaTable::new(store.clone(), DeltaTableConfig::default());
    writer_b.load().await.unwrap();

    let writer_a = writer_a.write(vec![batch(&[7])]).await.unwrap();
    assert_eq!(writer_a.version(), Some(3));
    store.reset();

    let err = writer_b
        .write(vec![batch(&[8])])
        .with_commit_properties(CommitProperties::default().with_max_retries(0))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Failed to commit transaction"),
        "{err}"
    );
    let events = store.events();
    assert_eq!(events.first(), Some(&ScopeEvent::Begin(1)));
    assert_eq!(events.last(), Some(&ScopeEvent::Abort(1)));
}

#[tokio::test]
async fn two_writers_commit_distinct_versions() {
    let store = IsolatingLogStore::new();
    table_with_data(&store).await;
    let mut writer_a = DeltaTable::new(store.clone(), DeltaTableConfig::default());
    writer_a.load().await.unwrap();
    let mut writer_b = DeltaTable::new(store.clone(), DeltaTableConfig::default());
    writer_b.load().await.unwrap();

    let writer_a = writer_a.write(vec![batch(&[7])]).await.unwrap();
    let writer_b = writer_b.write(vec![batch(&[8])]).await.unwrap();
    assert_eq!(writer_a.version(), Some(3));
    assert_eq!(writer_b.version(), Some(4));
    assert!(
        !store
            .events()
            .iter()
            .any(|e| matches!(e, ScopeEvent::Abort(_)))
    );
}

#[tokio::test]
async fn dropping_a_running_write_aborts_the_scope() {
    let store = IsolatingLogStore::new();
    let table = table_with_data(&store).await;

    store.stall_writes(true);
    let outcome = tokio::time::timeout(
        StdDuration::from_millis(200),
        table.write(vec![batch(&[7])]).into_future(),
    )
    .await;
    assert!(outcome.is_err(), "the write must still be stalled");
    store.stall_writes(false);
    for _ in 0..20 {
        tokio::task::yield_now().await;
        tokio::time::sleep(StdDuration::from_millis(5)).await;
    }

    let events = store.events();
    assert_eq!(events.first(), Some(&ScopeEvent::Begin(1)), "{events:?}");
    assert!(events.contains(&ScopeEvent::Abort(1)), "{events:?}");
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, ScopeEvent::Finish { .. })),
        "{events:?}"
    );
}

#[tokio::test]
async fn failed_post_commit_checkpoint_aborts_its_sibling_scope_and_keeps_the_commit() {
    let store = IsolatingLogStore::new();
    let table = create_table(&store, vec![(TableProperty::CheckpointInterval, "1")]).await;
    store.reset();
    // Scope 1 is the write, scope 2 the sibling that writes the checkpoint.
    store.fail_finish_of_scope(2);

    let err = table.write(vec![batch(&[1])]).await.unwrap_err();
    assert!(err.to_string().contains("injected failure"), "{err}");
    assert_eq!(
        store.events(),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Commit {
                scope: 1,
                version: 1
            },
            ScopeEvent::Begin(2),
            ScopeEvent::Finish {
                scope: 2,
                dirty: true
            },
            ScopeEvent::Abort(2),
            ScopeEvent::Abort(1),
        ]
    );

    // The commit itself is durable; only the checkpoint is missing.
    let mut reloaded = DeltaTable::new(store.clone(), DeltaTableConfig::default());
    reloaded.load().await.unwrap();
    assert_eq!(reloaded.version(), Some(1));
    assert!(
        !store
            .table_objects()
            .await
            .contains("_delta_log/00000000000000000001.checkpoint.parquet")
    );
}

#[tokio::test]
async fn successful_post_commit_checkpoint_is_published_by_the_sibling_scope() {
    let store = IsolatingLogStore::new();
    let table = create_table(&store, vec![(TableProperty::CheckpointInterval, "1")]).await;
    let table = check_scoped("write with checkpoint", &store, table, |t| {
        t.write(vec![batch(&[1])]).into_future()
    })
    .await;
    assert_eq!(table.version(), Some(1));
    assert_eq!(
        store.events(),
        vec![
            ScopeEvent::Begin(1),
            ScopeEvent::Commit {
                scope: 1,
                version: 1
            },
            ScopeEvent::Begin(2),
            ScopeEvent::Finish {
                scope: 2,
                dirty: true
            },
            ScopeEvent::Finish {
                scope: 1,
                dirty: false
            },
        ]
    );
    assert!(
        store
            .table_objects()
            .await
            .contains("_delta_log/00000000000000000001.checkpoint.parquet")
    );
}
