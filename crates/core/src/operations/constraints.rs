//! Add a check constraint to a table

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::physical_plan::execute_stream;
use delta_kernel::table_features::TableFeature;
use futures::StreamExt as _;
use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::delta_datafusion::{
    DataValidationExec, DeltaScanNext, DeltaSessionExt, Expression, constraints_to_exprs,
    create_session, expr::fmt_expr_to_sql, into_expr,
};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{
    EagerSnapshot, MetadataExt, ProtocolExt as _, ProtocolInner, resolve_snapshot,
};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::Constraint;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use std::collections::HashMap;

/// Build a constraint to add to a table
pub struct ConstraintBuilder {
    /// A snapshot of the table's state
    snapshot: Option<EagerSnapshot>,
    /// Hashmap containing an name of the constraint and expression
    check_constraints: HashMap<String, Expression>,
    /// Delta object store for handling data files
    log_store: LogStoreRef,
    /// Datafusion session state relevant for executing the input plan
    session: Option<Arc<dyn Session>>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for ConstraintBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl ConstraintBuilder {
    /// Create a new builder
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            check_constraints: Default::default(),
            snapshot,
            log_store,
            session: None,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Specify the constraint to be added
    pub fn with_constraint<S: Into<String>, E: Into<Expression>>(
        mut self,
        name: S,
        expression: E,
    ) -> Self {
        self.check_constraints
            .insert(name.into(), expression.into());
        self
    }

    /// Specify multiple constraints to be added
    pub fn with_constraints<S: Into<String>, E: Into<Expression>>(
        mut self,
        constraints: HashMap<S, E>,
    ) -> Self {
        self.check_constraints.extend(
            constraints
                .into_iter()
                .map(|(name, expr)| (name.into(), expr.into())),
        );
        self
    }

    /// The Datafusion session state to use
    pub fn with_session_state(mut self, session: Arc<dyn Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }
}

impl std::future::IntoFuture for ConstraintBuilder {
    type Output = DeltaResult<DeltaTable>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let snapshot =
                resolve_snapshot(&this.log_store, this.snapshot.clone(), true, None).await?;

            let operation_id = this.get_operation_id();
            this.pre_execute(operation_id).await?;

            if this.check_constraints.is_empty() {
                return Err(DeltaTableError::Generic(
                    "No check constraint (Name and Expression) provided".to_string(),
                ));
            }

            let mut metadata = snapshot.metadata().clone();

            let configuration_key_mapper: HashMap<String, String> = HashMap::from_iter(
                this.check_constraints
                    .keys()
                    .map(|name| (name.clone(), format!("delta.constraints.{name}"))),
            );

            // Hold all the conflicted constraints
            let preexisting_constraints =
                configuration_key_mapper
                    .iter()
                    .filter(|(_, configuration_key)| {
                        metadata
                            .configuration()
                            .contains_key(configuration_key.as_str())
                    });

            let session = this
                .session
                .unwrap_or_else(|| Arc::new(create_session().into_inner().state()));
            session
                .as_ref()
                .ensure_object_store_registered(this.log_store.as_ref(), Some(operation_id))?;

            let proivider = DeltaScanNext::builder()
                .with_eager_snapshot(snapshot.clone())
                .await?;
            let schema = proivider.schema().to_dfschema()?;

            // Create an Hashmap of the name to the processed expression
            let mut constraints_sql_mapper = HashMap::with_capacity(this.check_constraints.len());
            for (name, _) in configuration_key_mapper.iter() {
                let converted_expr = into_expr(
                    this.check_constraints[name].clone(),
                    &schema,
                    session.as_ref(),
                )?;
                let constraint_sql = fmt_expr_to_sql(&converted_expr)?;
                constraints_sql_mapper.insert(name, constraint_sql);
            }

            for (name, configuration_key) in preexisting_constraints {
                // when the expression is different in the conflicted constraint --> error out due not knowing how to resolve it
                if !metadata.configuration()[configuration_key].eq(&constraints_sql_mapper[name]) {
                    return Err(DeltaTableError::Generic(format!(
                        "Cannot add constraint '{name}': a constraint with this name already exists with a different expression. Existing: '{}', New: '{}'",
                        metadata.configuration()[configuration_key],
                        constraints_sql_mapper[name]
                    )));
                }
                tracing::warn!(
                    "Skipping constraint '{name}': identical constraint already exists with expression '{}'",
                    constraints_sql_mapper[name]
                );
            }
            let constraints_checker: Vec<Constraint> = constraints_sql_mapper
                .values()
                .map(|sql| Constraint::new("*", sql))
                .collect();

            let plan = DataValidationExec::try_new_with_predicates(
                session.as_ref(),
                proivider.scan(session.as_ref(), None, &[], None).await?,
                constraints_to_exprs(session.as_ref(), &schema, &constraints_checker)?,
            )?;

            // We must not just try to collect the plan here, because that would load
            // everything into memory. Instead we stream the results and discard them.
            let mut result_stream = execute_stream(plan, session.task_ctx())?;
            while let Some(maybe_batch) = result_stream.next().await {
                // No need to do anything with the data, if we get data back it means
                // the constraints are satisfied. We do want to propagate any errors though.
                let _result = maybe_batch?;
            }

            // We have validated the table passes it's constraints, now to add the constraint to
            // the table.
            for (name, configuration_key) in configuration_key_mapper.iter() {
                metadata = metadata.add_config_key(
                    configuration_key.to_string(),
                    constraints_sql_mapper[&name].clone(),
                )?;
            }

            let old_protocol = snapshot.protocol();
            let protocol = ProtocolInner {
                min_reader_version: if old_protocol.min_reader_version() > 1 {
                    old_protocol.min_reader_version()
                } else {
                    1
                },
                min_writer_version: if old_protocol.min_writer_version() > 3 {
                    old_protocol.min_writer_version()
                } else {
                    3
                },
                reader_features: old_protocol.reader_features_set(),
                writer_features: if old_protocol.min_writer_version() < 7 {
                    old_protocol.writer_features_set()
                } else {
                    let current_features = old_protocol.writer_features_set();
                    if let Some(mut features) = current_features {
                        features.insert(TableFeature::CheckConstraints);
                        Some(features)
                    } else {
                        current_features
                    }
                },
            }
            .as_kernel();
            // Put all the constraint into one commit
            let operation = DeltaOperation::AddConstraint {
                constraints: constraints_sql_mapper
                    .into_iter()
                    .map(|(name, sql)| Constraint::new(name, &sql))
                    .collect(),
            };

            let actions = vec![metadata.into(), protocol.into()];

            let commit = CommitBuilder::from(this.commit_properties)
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(this.custom_execute_handler.clone())
                .build(Some(&snapshot), this.log_store.clone(), operation)
                .await?;

            if let Some(handler) = this.custom_execute_handler {
                handler.post_execute(&this.log_store, operation_id).await?;
            }

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
    use datafusion::logical_expr::{col, lit};
    use std::collections::HashMap;

    use crate::table::config::TablePropertiesExt as _;
    use crate::writer::test_utils::{create_bare_table, get_arrow_schema, get_record_batch};
    use crate::{DeltaResult, DeltaTable};

    fn get_constraint(table: &DeltaTable, name: &str) -> String {
        table
            .snapshot()
            .unwrap()
            .metadata()
            .configuration()
            .get(name)
            .cloned()
            .unwrap()
    }

    async fn get_constraint_op_params(table: &mut DeltaTable) -> HashMap<String, String> {
        let last_commit = table.last_commit().await.unwrap();
        let constraints_str = last_commit
            .operation_parameters
            .as_ref()
            .unwrap()
            .get("constraints")
            .unwrap()
            .as_str()
            .unwrap();

        let constraints: serde_json::Value = serde_json::from_str(constraints_str).unwrap();
        constraints
            .as_array()
            .unwrap()
            .iter()
            .map(|value| {
                let name = value.get("name").unwrap().as_str().unwrap().to_owned();
                let expr = value.get("expr").unwrap().as_str().unwrap().to_owned();
                (name, expr)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_get_constraints_with_correct_names() -> DeltaResult<()> {
        // The key of a constraint is allowed to be custom
        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#check-constraints
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let constraint = table
            .add_constraint()
            .with_constraint("my_custom_constraint", "value < 100")
            .await;
        assert!(constraint.is_ok());
        let constraints = constraint
            .unwrap()
            .state
            .unwrap()
            .table_config()
            .get_constraints();
        assert!(constraints.len() == 1);
        assert_eq!(constraints[0].name, "my_custom_constraint");
        Ok(())
    }

    #[tokio::test]
    async fn test_add_constraint_with_invalid_data() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let constraint = table
            .add_constraint()
            .with_constraint("id", "value > 5")
            .await;
        assert!(constraint.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_add_valid_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let mut table = table
            .add_constraint()
            .with_constraint("id", "value <    1000")
            .await?;
        let version = table.version();
        assert_eq!(version, Some(1));

        let expected_expr = vec!["value < 1000"];
        assert_eq!(
            get_constraint_op_params(&mut table)
                .await
                .into_values()
                .collect::<Vec<String>>(),
            expected_expr
        );
        assert_eq!(
            get_constraint(&table, "delta.constraints.id"),
            expected_expr[0]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_add_valid_multiple_constraints() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let constraints = HashMap::from([("id", "value <    1000"), ("id2", "value <    20")]);

        let mut table = table.add_constraint().with_constraints(constraints).await?;
        let version = table.version();
        assert_eq!(version, Some(1));

        let expected_exprs = HashMap::from([
            ("id".to_string(), "value < 1000".to_string()),
            ("id2".to_string(), "value < 20".to_string()),
        ]);
        assert_eq!(get_constraint_op_params(&mut table).await, expected_exprs);
        assert_eq!(
            get_constraint(&table, "delta.constraints.id"),
            expected_exprs["id"]
        );
        assert_eq!(
            get_constraint(&table, "delta.constraints.id2"),
            expected_exprs["id2"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_add_constraint_datafusion() -> DeltaResult<()> {
        // Add constraint by providing a datafusion expression.
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let mut table = table
            .add_constraint()
            .with_constraint("valid_values", col("value").lt(lit(1000)))
            .await?;
        let version = table.version();
        assert_eq!(version, Some(1));

        let expected_expr = vec!["value < 1000"];
        assert_eq!(
            get_constraint_op_params(&mut table)
                .await
                .into_values()
                .collect::<Vec<String>>(),
            expected_expr
        );
        assert_eq!(
            get_constraint(&table, "delta.constraints.valid_values"),
            expected_expr[0]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_constraint_case_sensitive() -> DeltaResult<()> {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("Id", ArrowDataType::Utf8, true),
            Field::new("vAlue", ArrowDataType::Int32, true), // spellchecker:disable-line
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

        let table = DeltaTable::new_in_memory()
            .write(vec![batch])
            .await
            .unwrap();

        let mut table = table
            .add_constraint()
            .with_constraint("valid_values", "vAlue < 1000") // spellchecker:disable-line
            .await?;
        let version = table.version();
        assert_eq!(version, Some(1));

        let expected_expr = vec!["\"vAlue\" < 1000"]; // spellchecker:disable-line
        assert_eq!(
            get_constraint_op_params(&mut table)
                .await
                .into_values()
                .collect::<Vec<String>>(),
            expected_expr
        );
        assert_eq!(
            get_constraint(&table, "delta.constraints.valid_values"),
            expected_expr[0]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_add_conflicting_named_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let new_table = table
            .add_constraint()
            .with_constraint("id", "value < 60")
            .await?;

        let second_constraint = new_table
            .add_constraint()
            .with_constraint("id", "value < 10")
            .await;
        assert!(second_constraint.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_data_that_violates_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let table = table
            .add_constraint()
            .with_constraint("id", "value > 0")
            .await?;
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
    async fn test_write_data_that_violates_multiple_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let table = table
            .add_constraint()
            .with_constraints(HashMap::from([
                ("id", "value > 0"),
                ("custom_cons", "value < 30"),
            ]))
            .await?;
        let invalid_values: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(Int32Array::from(vec![-10])),
            Arc::new(StringArray::from(vec!["2021-02-02"])),
        ];
        let invalid_values_2: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(vec!["B"])),
            Arc::new(Int32Array::from(vec![30])),
            Arc::new(StringArray::from(vec!["2021-02-02"])),
        ];
        let batch = RecordBatch::try_new(get_arrow_schema(&None), invalid_values)?;
        let batch2 = RecordBatch::try_new(get_arrow_schema(&None), invalid_values_2)?;
        let err = table.write(vec![batch, batch2]).await;
        assert!(err.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_write_data_that_does_not_violate_constraint() -> DeltaResult<()> {
        let batch = get_record_batch(None, false);
        let table = create_bare_table().write(vec![batch.clone()]).await?;

        let err = table.write(vec![batch]).await;

        assert!(err.is_ok());
        Ok(())
    }
}
