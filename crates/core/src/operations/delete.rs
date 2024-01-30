//! Delete records from a Delta Table that statisfy a predicate
//!
//! When a predicate is not provided then all records are deleted from the Delta
//! Table. Otherwise a scan of the Delta table is performed to mark any files
//! that contain records that satisfy the predicate. Once files are determined
//! they are rewritten without the records.
//!
//!
//! Predicates MUST be deterministic otherwise undefined behaviour may occur during the
//! scanning and rewriting phase.
//!
//! # Example
//! ```rust ignore
//! let table = open_table("../path/to/table")?;
//! let (table, metrics) = DeleteBuilder::new(table.object_store(), table.state)
//!     .with_predicate(col("col1").eq(lit(1)))
//!     .await?;
//! ````

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::logstore::LogStoreRef;
use datafusion::execution::context::{SessionContext, SessionState};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_common::scalar::ScalarValue;
use datafusion_common::DFSchema;
use futures::future::BoxFuture;
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use serde_json::Value;

use super::datafusion_utils::Expression;
use super::transaction::PROTOCOL;
use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{find_files, register_store, DeltaScanBuilder, DeltaSessionContext};
use crate::errors::DeltaResult;
use crate::kernel::{Action, Add, Remove};
use crate::operations::transaction::commit;
use crate::operations::write::write_execution_plan;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::DeltaTable;

/// Delete Records from the Delta Table.
/// See this module's documentation for more information
pub struct DeleteBuilder {
    /// Which records to delete
    predicate: Option<Expression>,
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Properties passed to underlying parquet writer for when files are rewritten
    writer_properties: Option<WriterProperties>,
    /// Additional metadata to be added to commit
    app_metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Default, Debug, Serialize)]
/// Metrics for the Delete Operation
pub struct DeleteMetrics {
    /// Number of files added
    pub num_added_files: usize,
    /// Number of files removed
    pub num_removed_files: usize,
    /// Number of rows removed
    pub num_deleted_rows: Option<usize>,
    /// Number of rows copied in the process of deleting files
    pub num_copied_rows: Option<usize>,
    /// Time taken to execute the entire operation
    pub execution_time_ms: u128,
    /// Time taken to scan the file for matches
    pub scan_time_ms: u128,
    /// Time taken to rewrite the matched files
    pub rewrite_time_ms: u128,
}

impl DeleteBuilder {
    /// Create a new [`DeleteBuilder`]
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            predicate: None,
            snapshot,
            log_store,
            state: None,
            app_metadata: None,
            writer_properties: None,
        }
    }

    /// A predicate that determines if a record is deleted
    pub fn with_predicate<E: Into<Expression>>(mut self, predicate: E) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_metadata(
        mut self,
        metadata: impl IntoIterator<Item = (String, serde_json::Value)>,
    ) -> Self {
        self.app_metadata = Some(HashMap::from_iter(metadata));
        self
    }

    /// Writer properties passed to parquet writer for when files are rewritten
    pub fn with_writer_properties(mut self, writer_properties: WriterProperties) -> Self {
        self.writer_properties = Some(writer_properties);
        self
    }
}

async fn excute_non_empty_expr(
    snapshot: &DeltaTableState,
    log_store: LogStoreRef,
    state: &SessionState,
    expression: &Expr,
    metrics: &mut DeleteMetrics,
    rewrite: &[Add],
    writer_properties: Option<WriterProperties>,
) -> DeltaResult<Vec<Add>> {
    // For each identified file perform a parquet scan + filter + limit (1) + count.
    // If returned count is not zero then append the file to be rewritten and removed from the log. Otherwise do nothing to the file.

    let input_schema = snapshot.input_schema()?;
    let input_dfschema: DFSchema = input_schema.clone().as_ref().clone().try_into()?;

    let table_partition_cols = snapshot.metadata().partition_columns.clone();

    let scan = DeltaScanBuilder::new(snapshot, log_store.clone(), state)
        .with_files(rewrite)
        .build()
        .await?;
    let scan = Arc::new(scan);

    // Apply the negation of the filter and rewrite files
    let negated_expression = Expr::Not(Box::new(Expr::IsTrue(Box::new(expression.clone()))));

    let predicate_expr = create_physical_expr(
        &negated_expression,
        &input_dfschema,
        state.execution_props(),
    )?;
    let filter: Arc<dyn ExecutionPlan> =
        Arc::new(FilterExec::try_new(predicate_expr, scan.clone())?);

    let add_actions = write_execution_plan(
        Some(snapshot),
        state.clone(),
        filter.clone(),
        table_partition_cols.clone(),
        log_store.object_store(),
        Some(snapshot.table_config().target_file_size() as usize),
        None,
        writer_properties,
        false,
        false,
    )
    .await?;

    let read_records = scan.parquet_scan.metrics().and_then(|m| m.output_rows());
    let filter_records = filter.metrics().and_then(|m| m.output_rows());
    metrics.num_copied_rows = filter_records;
    metrics.num_deleted_rows = read_records
        .zip(filter_records)
        .map(|(read, filter)| read - filter);

    Ok(add_actions)
}

async fn execute(
    predicate: Option<Expr>,
    log_store: LogStoreRef,
    snapshot: &DeltaTableState,
    state: SessionState,
    writer_properties: Option<WriterProperties>,
    app_metadata: Option<HashMap<String, Value>>,
) -> DeltaResult<((Vec<Action>, i64, Option<DeltaOperation>), DeleteMetrics)> {
    let exec_start = Instant::now();
    let mut metrics = DeleteMetrics::default();

    let scan_start = Instant::now();
    let candidates = find_files(snapshot, log_store.clone(), &state, predicate.clone()).await?;
    metrics.scan_time_ms = Instant::now().duration_since(scan_start).as_micros();

    let predicate = predicate.unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(true))));

    let add = if candidates.partition_scan {
        Vec::new()
    } else {
        let write_start = Instant::now();
        let add = excute_non_empty_expr(
            snapshot,
            log_store.clone(),
            &state,
            &predicate,
            &mut metrics,
            &candidates.candidates,
            writer_properties,
        )
        .await?;
        metrics.rewrite_time_ms = Instant::now().duration_since(write_start).as_millis();
        add
    };
    let remove = candidates.candidates;

    let deletion_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut actions: Vec<Action> = add.into_iter().map(Action::Add).collect();
    let mut version = snapshot.version();
    metrics.num_removed_files = remove.len();
    metrics.num_added_files = actions.len();

    for action in remove {
        actions.push(Action::Remove(Remove {
            path: action.path,
            deletion_timestamp: Some(deletion_timestamp),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(action.partition_values),
            size: Some(action.size),
            deletion_vector: action.deletion_vector,
            tags: None,
            base_row_id: action.base_row_id,
            default_row_commit_version: action.default_row_commit_version,
        }))
    }

    metrics.execution_time_ms = Instant::now().duration_since(exec_start).as_micros();

    let mut app_metadata = match app_metadata {
        Some(meta) => meta,
        None => HashMap::new(),
    };

    app_metadata.insert("readVersion".to_owned(), snapshot.version().into());

    if let Ok(map) = serde_json::to_value(&metrics) {
        app_metadata.insert("operationMetrics".to_owned(), map);
    }

    // Do not make a commit when there are zero updates to the state
    let operation = DeltaOperation::Delete {
        predicate: Some(fmt_expr_to_sql(&predicate)?),
    };
    if !actions.is_empty() {
        version = commit(
            log_store.as_ref(),
            &actions,
            operation.clone(),
            Some(snapshot),
            Some(app_metadata),
        )
        .await?;
    }
    let op = (!actions.is_empty()).then_some(operation);
    Ok(((actions, version, op), metrics))
}

impl std::future::IntoFuture for DeleteBuilder {
    type Output = DeltaResult<(DeltaTable, DeleteMetrics)>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mut this = self;

        Box::pin(async move {
            PROTOCOL.check_append_only(&this.snapshot)?;

            PROTOCOL.can_write_to(&this.snapshot)?;

            let state = this.state.unwrap_or_else(|| {
                let session: SessionContext = DeltaSessionContext::default().into();

                // If a user provides their own their DF state then they must register the store themselves
                register_store(this.log_store.clone(), session.runtime_env());

                session.state()
            });

            let predicate = match this.predicate {
                Some(predicate) => match predicate {
                    Expression::DataFusion(expr) => Some(expr),
                    Expression::String(s) => {
                        Some(this.snapshot.parse_predicate_expression(s, &state)?)
                    }
                },
                None => None,
            };

            let ((actions, version, operation), metrics) = execute(
                predicate,
                this.log_store.clone(),
                &this.snapshot,
                state,
                this.writer_properties,
                this.app_metadata,
            )
            .await?;

            if let Some(op) = &operation {
                this.snapshot.merge(actions, op, version)?;
            }

            let table = DeltaTable::new_with_state(this.log_store, this.snapshot);
            Ok((table, metrics))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::operations::DeltaOps;
    use crate::protocol::*;
    use crate::writer::test_utils::datafusion::get_data;
    use crate::writer::test_utils::datafusion::write_batch;
    use crate::writer::test_utils::{
        get_arrow_schema, get_delta_schema, get_record_batch, setup_table_with_configuration,
    };
    use crate::DeltaConfigKey;
    use crate::DeltaTable;
    use arrow::array::Int32Array;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use arrow_array::StructArray;
    use arrow_buffer::NullBuffer;
    use arrow_schema::Fields;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::*;
    use serde_json::json;
    use std::sync::Arc;

    async fn setup_table(partitions: Option<Vec<&str>>) -> DeltaTable {
        let table_schema = get_delta_schema();

        let table = DeltaOps::new_in_memory()
            .create()
            .with_columns(table_schema.fields().clone())
            .with_partition_columns(partitions.unwrap_or_default())
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        table
    }

    #[tokio::test]
    async fn test_delete_when_delta_table_is_append_only() {
        let table = setup_table_with_configuration(DeltaConfigKey::AppendOnly, Some("true")).await;
        let batch = get_record_batch(None, false);
        // append some data
        let table = write_batch(table, batch).await;
        // delete
        let _err = DeltaOps(table)
            .delete()
            .await
            .expect_err("Remove action is included when Delta table is append-only. Should error");
    }

    #[tokio::test]
    async fn test_delete_default() {
        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();
        // write some data
        let table = DeltaOps(table)
            .write(vec![batch.clone()])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let (table, metrics) = DeltaOps(table).delete().await.unwrap();

        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 0);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let _extra_info = last_commit.info.clone();
        // assert_eq!(
        //     extra_info["operationMetrics"],
        //     serde_json::to_value(&metrics).unwrap()
        // );

        // rewrite is not required
        assert_eq!(metrics.rewrite_time_ms, 0);

        // Deletes with no changes to state must not commit
        let (table, metrics) = DeltaOps(table).delete().await.unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 0);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);
    }

    #[tokio::test]
    async fn test_delete_on_nonpartition_column() {
        // Delete based on a nonpartition column
        // Only rewrite files that match the predicate
        // Test data designed to force a scan of the underlying data

        let schema = get_arrow_schema(&None);
        let table = setup_table(None).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 1);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                    "2021-02-02",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(lit(1)))
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_files_count(), 2);

        assert_eq!(metrics.num_added_files, 1);
        assert_eq!(metrics.num_removed_files, 1);
        assert!(metrics.scan_time_ms > 0);
        assert_eq!(metrics.num_deleted_rows, Some(1));
        assert_eq!(metrics.num_copied_rows, Some(3));

        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        let parameters = last_commit.operation_parameters.clone().unwrap();
        assert_eq!(parameters["predicate"], json!("value = 1"));

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| A  | 100   | 2021-02-02 |",
            "| B  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_null() {
        // Demonstrate deletion of null

        async fn prepare_table() -> DeltaTable {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                arrow::datatypes::DataType::Int32,
                true,
            )]));

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(vec![
                    Some(0),
                    None,
                    Some(2),
                    None,
                    Some(4),
                ]))],
            )
            .unwrap();

            DeltaOps::new_in_memory().write(vec![batch]).await.unwrap()
        }

        // Validate behaviour of greater than
        let table = prepare_table().await;
        let (table, _) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").gt(lit(2)))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 0     |",
            "| 2     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate behaviour of less than
        let table = prepare_table().await;
        let (table, _) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").lt(lit(2)))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "|       |",
            "|       |",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // Validate behaviour of less plus not null
        let table = prepare_table().await;
        let (table, _) = DeltaOps(table)
            .delete()
            .with_predicate(col("value").lt(lit(2)).or(col("value").is_null()))
            .await
            .unwrap();

        let expected = vec![
            "+-------+",
            "| value |",
            "+-------+",
            "| 2     |",
            "| 4     |",
            "+-------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_on_partition_column() {
        // Perform a delete where the predicate only contains partition columns

        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 20, 10, 100])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2021-02-03",
                    "2021-02-02",
                    "2021-02-03",
                ])),
            ],
        )
        .unwrap();

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 2);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(col("modified").eq(lit("2021-02-03")))
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 1);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, None);
        assert_eq!(metrics.num_copied_rows, None);
        assert!(metrics.scan_time_ms > 0);
        assert_eq!(metrics.rewrite_time_ms, 0);

        let expected = vec![
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "+----+-------+------------+",
        ];

        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_on_mixed_columns() {
        // Test predicates that contain non-partition and partition column
        let schema = get_arrow_schema(&None);
        let table = setup_table(Some(["modified"].to_vec())).await;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["A", "B", "A", "A"])),
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

        // write some data
        let table = DeltaOps(table)
            .write(vec![batch])
            .with_save_mode(SaveMode::Append)
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_files_count(), 3);

        let (table, metrics) = DeltaOps(table)
            .delete()
            .with_predicate(
                col("modified")
                    .eq(lit("2021-02-04"))
                    .and(col("value").eq(lit(100))),
            )
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_files_count(), 2);

        assert_eq!(metrics.num_added_files, 0);
        assert_eq!(metrics.num_removed_files, 1);
        assert_eq!(metrics.num_deleted_rows, Some(1));
        assert_eq!(metrics.num_copied_rows, Some(0));
        assert!(metrics.scan_time_ms > 0);

        let expected = [
            "+----+-------+------------+",
            "| id | value | modified   |",
            "+----+-------+------------+",
            "| A  | 0     | 2021-02-02 |",
            "| A  | 10    | 2021-02-02 |",
            "| B  | 20    | 2021-02-03 |",
            "+----+-------+------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_delete_nested() {
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        // Test Delete with a predicate that references struct fields
        // See #2019
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(
                "props",
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Utf8, true)])),
                true,
            ),
        ]));

        let struct_array = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Utf8, true)]),
            vec![Arc::new(arrow::array::StringArray::from(vec![
                Some("2021-02-01"),
                Some("2021-02-02"),
                None,
                None,
            ])) as ArrayRef],
            Some(NullBuffer::from_iter(vec![true, true, true, false])),
        );

        let data = vec![
            Arc::new(arrow::array::StringArray::from(vec!["A", "B", "C", "D"])) as ArrayRef,
            Arc::new(struct_array) as ArrayRef,
        ];
        let batches = vec![RecordBatch::try_new(schema.clone(), data).unwrap()];

        let table = DeltaOps::new_in_memory().write(batches).await.unwrap();

        let (table, _metrics) = DeltaOps(table)
            .delete()
            .with_predicate("props['a'] = '2021-02-02'")
            .await
            .unwrap();

        let expected = [
            "+----+-----------------+",
            "| id | props           |",
            "+----+-----------------+",
            "| A  | {a: 2021-02-01} |",
            "| C  | {a: }           |",
            "| D  |                 |",
            "+----+-----------------+",
        ];
        let actual = get_data(&table).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn test_failure_nondeterministic_query() {
        // Deletion requires a deterministic predicate

        let table = setup_table(None).await;

        let res = DeltaOps(table)
            .delete()
            .with_predicate(col("value").eq(cast(
                random() * lit(20.0),
                arrow::datatypes::DataType::Int32,
            )))
            .await;
        assert!(res.is_err());
    }
}
