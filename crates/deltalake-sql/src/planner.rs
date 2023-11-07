use std::sync::Arc;

use datafusion_common::{OwnedTableReference, Result as DFResult};
use datafusion_expr::logical_plan::{Extension, LogicalPlan};
use datafusion_sql::planner::{
    object_name_to_table_reference, ContextProvider, IdentNormalizer, ParserOptions, SqlToRel,
};
use datafusion_sql::sqlparser::ast::ObjectName;

use crate::logical_plan::{DeltaStatement, DescribeFiles, Vacuum};
use crate::parser::{DescribeStatement, Statement, VacuumStatement};

/// Delta SQL query planner
pub struct DeltaSqlToRel<'a, S: ContextProvider> {
    pub(crate) context_provider: &'a S,
    pub(crate) options: ParserOptions,
    pub(crate) _normalizer: IdentNormalizer,
}

impl<'a, S: ContextProvider> DeltaSqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        Self::new_with_options(schema_provider, ParserOptions::default())
    }

    /// Create a new query planner
    pub fn new_with_options(schema_provider: &'a S, options: ParserOptions) -> Self {
        let normalize = options.enable_ident_normalization;
        DeltaSqlToRel {
            context_provider: schema_provider,
            options,
            _normalizer: IdentNormalizer::new(normalize),
        }
    }

    /// Generate a logical plan from an Delta SQL statement
    pub fn statement_to_plan(&self, statement: Statement) -> DFResult<LogicalPlan> {
        match statement {
            Statement::Datafusion(s) => {
                let planner = SqlToRel::new_with_options(
                    self.context_provider,
                    ParserOptions {
                        parse_float_as_decimal: self.options.parse_float_as_decimal,
                        enable_ident_normalization: self.options.enable_ident_normalization,
                    },
                );
                planner.statement_to_plan(s)
            }
            Statement::Describe(describe) => self.describe_to_plan(describe),
            Statement::Vacuum(vacuum) => self.vacuum_to_plan(vacuum),
            _ => todo!(),
        }
    }

    fn vacuum_to_plan(&self, vacuum: VacuumStatement) -> DFResult<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(vacuum.table)?;
        let plan = DeltaStatement::Vacuum(Vacuum::new(
            table_ref.to_owned_reference(),
            vacuum.retention_hours,
            vacuum.dry_run,
        ));
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(plan),
        }))
    }

    fn describe_to_plan(&self, describe: DescribeStatement) -> DFResult<LogicalPlan> {
        let table_ref = self.object_name_to_table_reference(describe.table)?;
        let plan =
            DeltaStatement::DescribeFiles(DescribeFiles::new(table_ref.to_owned_reference()));
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(plan),
        }))
    }

    pub(crate) fn object_name_to_table_reference(
        &self,
        object_name: ObjectName,
    ) -> DFResult<OwnedTableReference> {
        object_name_to_table_reference(object_name, self.options.enable_ident_normalization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::assert_plan_eq;

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::DataFusionError;

    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource};
    use datafusion_sql::TableReference;

    use crate::parser::DeltaParser;

    struct TestSchemaProvider {
        options: ConfigOptions,
        tables: HashMap<String, Arc<dyn TableSource>>,
    }

    impl TestSchemaProvider {
        pub fn new() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "table1".to_string(),
                create_table_source(vec![Field::new(
                    "column1".to_string(),
                    DataType::Utf8,
                    false,
                )]),
            );

            Self {
                options: Default::default(),
                tables,
            }
        }
    }

    impl ContextProvider for TestSchemaProvider {
        fn get_table_provider(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
            self.get_table_source(name)
        }

        fn get_table_source(&self, name: TableReference) -> DFResult<Arc<dyn TableSource>> {
            match self.tables.get(name.table()) {
                Some(table) => Ok(table.clone()),
                _ => Err(DataFusionError::Plan(format!(
                    "Table not found: {}",
                    name.table()
                ))),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<datafusion_expr::WindowUDF>> {
            None
        }
    }

    fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(
            Schema::new_with_metadata(fields, HashMap::new()),
        )))
    }

    fn test_statement(sql: &str, expected_lines: &[&str]) {
        let cp = TestSchemaProvider::new();
        let planner = DeltaSqlToRel::new(&cp);
        let mut stmts = DeltaParser::parse_sql(sql).unwrap();
        let plan = planner
            .statement_to_plan(stmts.pop_front().unwrap())
            .unwrap();
        assert_plan_eq(&plan, expected_lines)
    }

    #[test]
    fn test_planner() {
        test_statement(
            "SELECT * FROM table1",
            &["Projection: table1.column1", "  TableScan: table1"],
        );

        test_statement("VACUUM table1", &["Vacuum: table1 dry_run=false"]);
        test_statement("VACUUM table1 DRY RUN", &["Vacuum: table1 dry_run=true"]);
        test_statement(
            "VACUUM table1 RETAIN 1234 HOURS",
            &["Vacuum: table1 retention_hours=1234 dry_run=false"],
        );
    }
}
