//! Logical Operations for DataFusion

use std::collections::HashSet;

use datafusion_common::{
    Column, DataFusionError, Result, SchemaError, unqualified_field_not_found,
};
use datafusion_expr::logical_plan::LogicalPlanBuilder;
use datafusion_expr::utils::find_window_exprs;
use datafusion_expr::{
    LogicalPlan, UserDefinedLogicalNodeCore, col, expr::Expr, select_expr::SelectExpr,
};

// Metric Observer is used to update DataFusion metrics from a record batch.
// See MetricObserverExec for the physical implementation

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd)]
pub(crate) struct MetricObserver {
    // id is preserved during conversion to physical node
    pub id: String,
    pub input: LogicalPlan,
    pub enable_pushdown: bool,
}

impl UserDefinedLogicalNodeCore for MetricObserver {
    // Predicate push down is not supported for this node. Try to limit usage
    // near the end of plan.
    fn name(&self) -> &str {
        "MetricObserver"
    }

    fn inputs(&self) -> Vec<&datafusion_expr::LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion_common::DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        vec![]
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        if self.enable_pushdown {
            HashSet::new()
        } else {
            self.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        }
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MetricObserver id={}", self.id)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion_expr::Expr>,
        inputs: Vec<datafusion_expr::LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        Ok(MetricObserver {
            id: self.id.clone(),
            input: inputs[0].clone(),
            enable_pushdown: self.enable_pushdown,
        })
    }
}

pub trait LogicalPlanBuilderExt: Sized {
    fn select_exprs(
        self,
        expr_list: impl IntoIterator<Item = impl Into<SelectExpr>>,
    ) -> Result<Self>;
    fn select_columns(self, columns: &[&str]) -> Result<Self>;
    fn drop_columns(self, columns: &[&str]) -> Result<Self>;
    fn with_column(
        self,
        name: &str,
        expr: Expr,
        projection_requires_validation: bool,
    ) -> Result<Self>;
    fn with_column_renamed(self, old_column: Column, new_name: &str) -> Result<Self>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn select_exprs(
        self,
        expr_list: impl IntoIterator<Item = impl Into<SelectExpr>>,
    ) -> Result<Self> {
        let expr_list: Vec<SelectExpr> =
            expr_list.into_iter().map(|e| e.into()).collect::<Vec<_>>();

        let expressions = expr_list.iter().filter_map(|e| match e {
            SelectExpr::Expression(expr) => Some(expr),
            _ => None,
        });

        let window_func_exprs = find_window_exprs(expressions);
        let plan = if window_func_exprs.is_empty() {
            self.plan().clone()
        } else {
            LogicalPlanBuilder::window_plan(self.plan().clone(), window_func_exprs)?
        };

        let project_plan = LogicalPlanBuilder::from(plan).project(expr_list)?.build()?;
        Ok(Self::new(project_plan))
    }

    fn select_columns(self, columns: &[&str]) -> Result<Self> {
        let schema = self.schema();
        let fields = columns
            .iter()
            .map(|name| {
                let fields = schema.qualified_fields_with_unqualified_name(name);
                if fields.is_empty() {
                    Err(unqualified_field_not_found(name, schema))
                } else {
                    Ok(fields)
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = fields
            .into_iter()
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        self.select_exprs(expr)
    }

    fn drop_columns(self, columns: &[&str]) -> Result<Self> {
        let schema = self.schema();
        let fields_to_drop = columns
            .iter()
            .flat_map(|name| schema.qualified_fields_with_unqualified_name(name))
            .collect::<Vec<_>>();
        let expr: Vec<Expr> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, _)| schema.qualified_field(idx))
            .filter(|(qualifier, f)| !fields_to_drop.contains(&(*qualifier, f)))
            .map(|(qualifier, field)| Expr::Column(Column::from((qualifier, field))))
            .collect();
        self.select_exprs(expr)
    }

    fn with_column(
        self,
        name: &str,
        expr: Expr,
        projection_requires_validation: bool,
    ) -> Result<Self> {
        let window_func_exprs = find_window_exprs([&expr]);

        let original_names: HashSet<String> = self
            .schema()
            .iter()
            .map(|(_, f)| f.name().clone())
            .collect();

        let plan = if window_func_exprs.is_empty() {
            self.plan().clone()
        } else {
            LogicalPlanBuilder::window_plan(self.plan().clone(), window_func_exprs)?
        };

        let new_column = expr.alias(name);
        let mut col_exists = false;

        let mut fields: Vec<(Expr, bool)> = plan
            .schema()
            .iter()
            .filter_map(|(qualifier, field)| {
                if !original_names.contains(field.name()) {
                    return None;
                }

                if field.name() == name {
                    col_exists = true;
                    Some((new_column.clone(), true))
                } else {
                    let e = col(Column::from((qualifier, field)));
                    Some((e, projection_requires_validation))
                }
            })
            .collect();

        if !col_exists {
            fields.push((new_column, true));
        }

        let project_plan = LogicalPlanBuilder::from(plan)
            .project_with_validation(fields)?
            .build()?;

        Ok(Self::new(project_plan))
    }

    fn with_column_renamed(self, old_column: Column, new_name: &str) -> Result<Self> {
        let plan = self.plan();
        let schema = plan.schema();
        let (qualifier_rename, field_rename) = match schema.qualified_field_from_column(&old_column)
        {
            Ok(qualifier_and_field) => qualifier_and_field,
            Err(DataFusionError::SchemaError(e, _))
                if matches!(*e, SchemaError::FieldNotFound { .. }) =>
            {
                return Ok(Self::new(plan.clone()));
            }
            Err(err) => return Err(err),
        };
        let projection = schema
            .iter()
            .map(|(qualifier, field)| {
                if qualifier.eq(&qualifier_rename) && **field == *field_rename {
                    (
                        col(Column::from((qualifier, field)))
                            .alias_qualified(qualifier.cloned(), new_name),
                        false,
                    )
                } else {
                    (col(Column::from((qualifier, field))), false)
                }
            })
            .collect::<Vec<_>>();
        let project_plan = LogicalPlanBuilder::from(plan.clone())
            .project_with_validation(projection)?
            .build()?;
        Ok(Self::new(project_plan))
    }
}
