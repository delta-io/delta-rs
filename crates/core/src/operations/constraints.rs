//! Add a check constraint to a table

use std::sync::Arc;

use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::prelude::SessionContext;
use datafusion_common::ToDFSchema;
use datafusion_physical_plan::ExecutionPlan;
use futures::future::BoxFuture;
use futures::StreamExt;

use crate::delta_datafusion::expr::fmt_expr_to_sql;
use crate::delta_datafusion::{
    register_store, DeltaDataChecker, DeltaScanBuilder, DeltaSessionContext,
};
use crate::kernel::{Protocol, WriterFeatures};
use crate::logstore::LogStoreRef;
use crate::operations::datafusion_utils::Expression;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::table::Constraint;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

use super::datafusion_utils::into_expr;
use super::transaction::{CommitBuilder, CommitProperties};

/// Build a constraint to add to a table
pub struct ConstraintBuilder {
    /// A snapshot of the table's state
    snapshot: DeltaTableState,
    /// Name of the constraint
    name: Option<String>,
    /// Constraint expression
    expr: Option<Expression>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    state: Option<SessionState>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
}

impl super::Operation<()> for ConstraintBuilder {}

impl ConstraintBuilder {
    /// Create a new builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            name: None,
            expr: None,
            snapshot,
            log_store,
            state: None,
            commit_properties: CommitProperties::default(),
        }
    }

    /// Specify the constraint to be added
    pub fn with_constraint<S: Into<String>, E: Into<Expression>>(
        mut self,
        name: S,
        expression: E,
    ) -> Self {
        self.name = Some(name.into());
        self.expr = Some(expression.into());
        self
    }

    /// Specify the datafusion session context
    pub fn with_session_state(mut self, state: SessionState) -> Self {
        self.state = Some(state);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }
}

impl std::future::IntoFuture for ConstraintBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            if !this.snapshot.load_config().require_files {
                return Err(DeltaTableError::NotInitializedWithFiles(
                    "ADD CONSTRAINTS".into(),
                ));
            }

            let name = match this.name {
                Some(v) => v,
                None => return Err(DeltaTableError::Generic("No name provided".to_string())),
            };

            let expr = this
                .expr
                .ok_or_else(|| DeltaTableError::Generic("No Expresion provided".to_string()))?;

            let mut metadata = this.snapshot.metadata().clone();
            let configuration_key = format!("delta.constraints.{}", name);

            if metadata.configuration.contains_key(&configuration_key) {
                return Err(DeltaTableError::Generic(format!(
                    "Constraint with name: {} already exists",
                    name
                )));
            }

            let state = this.state.unwrap_or_else(|| {
                let session: SessionContext = DeltaSessionContext::default().into();
                register_store(this.log_store.clone(), session.runtime_env());
                session.state()
            });

            let scan = DeltaScanBuilder::new(&this.snapshot, this.log_store.clone(), &state)
                .build()
                .await?;

            let schema = scan.schema().to_dfschema()?;
            let expr = into_expr(expr, &schema, &state)?;
            let expr_str = fmt_expr_to_sql(&expr)?;

            // Checker built here with the one time constraint to check.
            let checker =
                DeltaDataChecker::new_with_constraints(vec![Constraint::new("*", &expr_str)]);

            let plan: Arc<dyn ExecutionPlan> = Arc::new(scan);
            let mut tasks = vec![];
            for p in 0..plan.properties().output_partitioning().partition_count() {
                let inner_plan = plan.clone();
                let inner_checker = checker.clone();
                let task_ctx = Arc::new(TaskContext::from(&state));
                let mut record_stream: SendableRecordBatchStream =
                    inner_plan.execute(p, task_ctx)?;
                let handle: tokio::task::JoinHandle<DeltaResult<()>> =
                    tokio::task::spawn(async move {
                        while let Some(maybe_batch) = record_stream.next().await {
                            let batch = maybe_batch?;
                            inner_checker.check_batch(&batch).await?;
                        }
                        Ok(())
                    });
                tasks.push(handle);
            }
            futures::future::join_all(tasks)
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|err| DeltaTableError::Generic(err.to_string()))?
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;

            // We have validated the table passes it's constraints, now to add the constraint to
            // the table.

            metadata.configuration.insert(
                format!("delta.constraints.{}", name),
                Some(expr_str.clone()),
            );

            let old_protocol = this.snapshot.protocol();
            let protocol = Protocol {
                min_reader_version: if old_protocol.min_reader_version > 1 {
                    old_protocol.min_reader_version
                } else {
                    1
                },
                min_writer_version: if old_protocol.min_writer_version > 3 {
                    old_protocol.min_writer_version
                } else {
                    3
                },
                reader_features: old_protocol.reader_features.clone(),
                writer_features: if old_protocol.min_writer_version < 7 {
                    old_protocol.writer_features.clone()
                } else {
                    let current_features = old_protocol.writer_features.clone();
                    if let Some(mut features) = current_features {
                        features.insert(WriterFeatures::CheckConstraints);
                        Some(features)
                    } else {
                        current_features
                    }
                },
            };

            let operation = DeltaOperation::AddConstraint {
                name: name.clone(),
                expr: expr_str.clone(),
            };

            let actions = vec![metadata.into(), protocol.into()];

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .build(Some(&this.snapshot), this.log_store.clone(), operation)
                .await?;

            Ok(DeltaTable::new_with_state(
                this.log_store,
                commit.snapshot(),
            ))
        })
    }
}

#[cfg(feature = "datafusion")]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use datafusion_expr::{col, lit};

    use crate::writer::test_utils::{create_bare_table, get_arrow_schema, get_record_batch};
    use crate::{DeltaOps, DeltaResult, DeltaTable};

    fn get_constraint(table: &DeltaTable, name: &str) -> String {
        table
            .metadata()
            .unwrap()
            .configuration
            .get(name)
            .unwrap()
            .clone()
            .unwrap()
    }

    async fn get_constraint_op_params(table: &mut DeltaTable) -> String {
        let commit_info = table.history(None).await.unwrap();
        let last_commit = &commit_info[0];
        last_commit
            .operation_parameters
            .as_ref()
            .unwrap()
            .get("expr")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned()
    }

    #[tokio::test]
    async fn add_constraint_with_invalid_data() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let table = DeltaOps(write);

        let constraint = table
            .add_constraint()
            .with_constraint("id", "value > 5")
            .await;
        assert!(constraint.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn add_valid_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let table = DeltaOps(write);

        let mut table = table
            .add_constraint()
            .with_constraint("id", "value <    1000")
            .await?;
        let version = table.version();
        assert_eq!(version, 1);

        let expected_expr = "value < 1000";
        assert_eq!(get_constraint_op_params(&mut table).await, expected_expr);
        assert_eq!(
            get_constraint(&table, "delta.constraints.id"),
            expected_expr
        );
        Ok(())
    }

    #[tokio::test]
    async fn add_constraint_datafusion() -> DeltaResult<()> {
        // Add constraint by providing a datafusion expression.
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let table = DeltaOps(write);

        let mut table = table
            .add_constraint()
            .with_constraint("valid_values", col("value").lt(lit(1000)))
            .await?;
        let version = table.version();
        assert_eq!(version, 1);

        let expected_expr = "value < 1000";
        assert_eq!(get_constraint_op_params(&mut table).await, expected_expr);
        assert_eq!(
            get_constraint(&table, "delta.constraints.valid_values"),
            expected_expr
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_constraint_case_sensitive() -> DeltaResult<()> {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("Id", ArrowDataType::Utf8, true),
            Field::new("vAlue", ArrowDataType::Int32, true),
            Field::new("mOdifieD", ArrowDataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema.clone()),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::StringArray::from(vec![
                    "2021-02-02",
                    "2023-07-04",
                    "2023-07-04",
                ])),
            ],
        )
        .unwrap();

        let table = DeltaOps::new_in_memory().write(vec![batch]).await.unwrap();

        let mut table = DeltaOps(table)
            .add_constraint()
            .with_constraint("valid_values", "vAlue < 1000")
            .await?;
        let version = table.version();
        assert_eq!(version, 1);

        let expected_expr = "vAlue < 1000";
        assert_eq!(get_constraint_op_params(&mut table).await, expected_expr);
        assert_eq!(
            get_constraint(&table, "delta.constraints.valid_values"),
            expected_expr
        );

        Ok(())
    }

    #[tokio::test]
    async fn add_conflicting_named_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let table = DeltaOps(write);

        let new_table = table
            .add_constraint()
            .with_constraint("id", "value < 60")
            .await?;

        let new_table = DeltaOps(new_table);
        let second_constraint = new_table
            .add_constraint()
            .with_constraint("id", "value < 10")
            .await;
        assert!(second_constraint.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn write_data_that_violates_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;

        let table = DeltaOps(write)
            .add_constraint()
            .with_constraint("id", "value > 0")
            .await?;
        let table = DeltaOps(table);
        let invalid_values: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(Int32Array::from(vec![-10])),
            Arc::new(StringArray::from(vec!["2021-02-02"])),
        ];
        let batch = RecordBatch::try_new(get_arrow_schema(&None), invalid_values)?;
        let err = table.write(vec![batch]).await;
        assert!(err.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn write_data_that_does_not_violate_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let write = DeltaOps(create_bare_table())
            .write(vec![batch.clone()])
            .await?;
        let table = DeltaOps(write);

        let err = table.write(vec![batch]).await;

        assert!(err.is_ok());
        Ok(())
    }
}
