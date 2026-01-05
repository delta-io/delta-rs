use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::display::array_value_to_string;
use datafusion::catalog::{MemTable, TableProvider as _};
use datafusion::prelude::SessionContext;
use itertools::Itertools as _;

use crate::delta_datafusion::DeltaSessionContext;
use crate::kernel::EagerSnapshot;
use crate::table::config::TablePropertiesExt as _;
use crate::table::{Constraint, GeneratedColumn};
use crate::{DataCheck, DeltaTableError, Invariant, StructTypeExt as _};

/// Responsible for checking batches of data conform to table's invariants, constraints and nullability.
#[derive(Clone, Default)]
pub(crate) struct DeltaDataChecker {
    constraints: Vec<Constraint>,
    invariants: Vec<Invariant>,
    generated_columns: Vec<GeneratedColumn>,
    non_nullable_columns: Vec<String>,
    ctx: SessionContext,
}

impl DeltaDataChecker {
    /// Create a new [`DeltaDataChecker`] with no invariants or constraints
    pub fn empty() -> Self {
        Self {
            invariants: vec![],
            constraints: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new [`DeltaDataChecker`] with a specified set of invariants
    pub fn new_with_invariants(invariants: Vec<Invariant>) -> Self {
        Self {
            invariants,
            constraints: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new [`DeltaDataChecker`] with a specified set of constraints
    pub fn new_with_constraints(constraints: Vec<Constraint>) -> Self {
        Self {
            constraints,
            invariants: vec![],
            generated_columns: vec![],
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Create a new [`DeltaDataChecker`] with a specified set of generated columns
    pub fn new_with_generated_columns(generated_columns: Vec<GeneratedColumn>) -> Self {
        Self {
            constraints: vec![],
            invariants: vec![],
            generated_columns,
            non_nullable_columns: vec![],
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Specify the Datafusion context
    pub fn with_session_context(mut self, context: SessionContext) -> Self {
        self.ctx = context;
        self
    }

    /// Add the specified set of constraints to the current DeltaDataChecker's constraints
    pub fn with_extra_constraints(mut self, constraints: Vec<Constraint>) -> Self {
        self.constraints.extend(constraints);
        self
    }

    /// Create a new DeltaDataChecker
    pub fn new(snapshot: &EagerSnapshot) -> Self {
        let invariants = snapshot.schema().get_invariants().unwrap_or_default();
        let generated_columns = snapshot
            .schema()
            .get_generated_columns()
            .unwrap_or_default();
        let constraints = snapshot.table_properties().get_constraints();
        let non_nullable_columns = snapshot
            .schema()
            .fields()
            .filter_map(|f| {
                if !f.is_nullable() {
                    Some(f.name().clone())
                } else {
                    None
                }
            })
            .collect_vec();
        Self {
            invariants,
            constraints,
            generated_columns,
            non_nullable_columns,
            ctx: DeltaSessionContext::default().into(),
        }
    }

    /// Check that a record batch conforms to table's invariants.
    ///
    /// If it does not, it will return [DeltaTableError::InvalidData] with a list
    /// of values that violated each invariant.
    pub async fn check_batch(&self, record_batch: &RecordBatch) -> Result<(), DeltaTableError> {
        self.check_nullability(record_batch)?;
        self.enforce_checks(record_batch, &self.invariants).await?;
        self.enforce_checks(record_batch, &self.constraints).await?;
        self.enforce_checks(record_batch, &self.generated_columns)
            .await
    }

    /// Return true if all the nullability checks are valid
    fn check_nullability(&self, record_batch: &RecordBatch) -> Result<bool, DeltaTableError> {
        let mut violations = Vec::with_capacity(self.non_nullable_columns.len());
        for col in self.non_nullable_columns.iter() {
            if let Some(arr) = record_batch.column_by_name(col) {
                if arr.null_count() > 0 {
                    violations.push(format!(
                        "Non-nullable column violation for {col}, found {} null values",
                        arr.null_count()
                    ));
                }
            } else {
                violations.push(format!(
                    "Non-nullable column violation for {col}, not found in batch!"
                ));
            }
        }
        if !violations.is_empty() {
            Err(DeltaTableError::InvalidData { violations })
        } else {
            Ok(true)
        }
    }

    async fn enforce_checks<C: DataCheck>(
        &self,
        record_batch: &RecordBatch,
        checks: &[C],
    ) -> Result<(), DeltaTableError> {
        if checks.is_empty() {
            return Ok(());
        }
        let table = MemTable::try_new(record_batch.schema(), vec![vec![record_batch.clone()]])?;
        table.schema();
        // Use a random table name to avoid clashes when running multiple parallel tasks, e.g. when using a partitioned table
        let table_name: String = uuid::Uuid::new_v4().to_string();
        self.ctx.register_table(&table_name, Arc::new(table))?;

        let mut violations: Vec<String> = Vec::with_capacity(checks.len());

        for check in checks {
            if check.get_name().contains('.') {
                return Err(DeltaTableError::Generic(
                    "delta constraints for nested columns are not supported at the moment."
                        .to_string(),
                ));
            }

            let field_to_select = if check.as_any().is::<Constraint>() {
                "*"
            } else {
                check.get_name()
            };
            let sql = format!(
                "SELECT {} FROM `{table_name}` WHERE NOT ({}) LIMIT 1",
                field_to_select,
                check.get_expression()
            );

            let dfs: Vec<RecordBatch> = self.ctx.sql(&sql).await?.collect().await?;
            if !dfs.is_empty() && dfs[0].num_rows() > 0 {
                let value: String = dfs[0]
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c, 0).unwrap_or(String::from("null")))
                    .join(", ");

                let msg = format!(
                    "Check or Invariant ({}) violated by value in row: [{value}]",
                    check.get_expression(),
                );
                violations.push(msg);
            }
        }

        self.ctx.deregister_table(&table_name)?;
        if !violations.is_empty() {
            Err(DeltaTableError::InvalidData { violations })
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, StructArray};
    use arrow_schema::{DataType, Field, Schema};

    use crate::test_utils::TestResult;

    use super::*;

    #[tokio::test]
    async fn test_enforce_invariants() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )
        .unwrap();
        // Empty invariants is okay
        let invariants: Vec<Invariant> = vec![];
        assert!(
            DeltaDataChecker::new_with_invariants(invariants)
                .check_batch(&batch)
                .await
                .is_ok()
        );

        // Valid invariants return Ok(())
        let invariants = vec![
            Invariant::new("a", "a is not null"),
            Invariant::new("b", "b < 1000"),
        ];
        assert!(
            DeltaDataChecker::new_with_invariants(invariants)
                .check_batch(&batch)
                .await
                .is_ok()
        );

        // Violated invariants returns an error with list of violations
        let invariants = vec![
            Invariant::new("a", "a is null"),
            Invariant::new("b", "b < 100"),
        ];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant invariants return a different error
        let invariants = vec![Invariant::new("c", "c > 2000")];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());

        // Nested invariants are unsupported
        let struct_fields = schema.fields().clone();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Struct(struct_fields),
            false,
        )]));
        let inner = Arc::new(StructArray::from(batch));
        let batch = RecordBatch::try_new(schema, vec![inner]).unwrap();

        let invariants = vec![Invariant::new("x.b", "x.b < 1000")];
        let result = DeltaDataChecker::new_with_invariants(invariants)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::Generic { .. })));
    }

    #[tokio::test]
    async fn test_enforce_constraints() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )
        .unwrap();
        // Empty constraints is okay
        let constraints: Vec<Constraint> = vec![];
        assert!(
            DeltaDataChecker::new_with_constraints(constraints)
                .check_batch(&batch)
                .await
                .is_ok()
        );

        // Valid invariants return Ok(())
        let constraints = vec![
            Constraint::new("custom_a", "a is not null"),
            Constraint::new("custom_b", "b < 1000"),
        ];
        assert!(
            DeltaDataChecker::new_with_constraints(constraints)
                .check_batch(&batch)
                .await
                .is_ok()
        );

        // Violated invariants returns an error with list of violations
        let constraints = vec![
            Constraint::new("custom_a", "a is null"),
            Constraint::new("custom_B", "b < 100"),
        ];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant constraints return a different error
        let constraints = vec![Constraint::new("custom_c", "c > 2000")];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
    }

    /// Ensure that constraints when there are spaces in the field name still work
    ///
    /// See <https://github.com/delta-io/delta-rs/pull/3374>
    #[tokio::test]
    async fn test_constraints_with_spacey_fields() -> TestResult {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b bop", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b bop", "c", "d",
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 10, 10, 100])),
            ],
        )?;

        // Valid invariants return Ok(())
        let constraints = vec![
            Constraint::new("custom a", "a is not null"),
            Constraint::new("custom_b", "`b bop` < 1000"),
        ];
        assert!(
            DeltaDataChecker::new_with_constraints(constraints)
                .check_batch(&batch)
                .await
                .is_ok()
        );

        // Violated invariants returns an error with list of violations
        let constraints = vec![
            Constraint::new("custom_a", "a is null"),
            Constraint::new("custom_B", "\"b bop\" < 100"),
        ];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        assert!(matches!(result, Err(DeltaTableError::InvalidData { .. })));
        if let Err(DeltaTableError::InvalidData { violations }) = result {
            assert_eq!(violations.len(), 2);
        }

        // Irrelevant constraints return a different error
        let constraints = vec![Constraint::new("custom_c", "c > 2000")];
        let result = DeltaDataChecker::new_with_constraints(constraints)
            .check_batch(&batch)
            .await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_check_nullability() -> TestResult {
        use arrow::array::StringArray;

        let data_checker = DeltaDataChecker {
            non_nullable_columns: vec!["zed".to_string(), "yap".to_string()],
            ..Default::default()
        };

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let nulls: Arc<dyn Array> = Arc::new(StringArray::new_null(1));
        let batch = RecordBatch::try_from_iter(vec![("a", arr), ("zed", nulls)]).unwrap();

        let result = data_checker.check_nullability(&batch);
        assert!(
            result.is_err(),
            "The result should have errored! {result:?}"
        );

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter(vec![("zed", arr)]).unwrap();
        let result = data_checker.check_nullability(&batch);
        assert!(
            result.is_err(),
            "The result should have errored! {result:?}"
        );

        let arr: Arc<dyn Array> = Arc::new(StringArray::from(vec!["s"]));
        let batch = RecordBatch::try_from_iter(vec![("zed", arr.clone()), ("yap", arr)]).unwrap();
        let _ = data_checker.check_nullability(&batch)?;

        Ok(())
    }
}
