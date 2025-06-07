use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::execution::SessionState;
use datafusion_common::{exec_datafusion_err, Column, DFSchema, Result as DataFusionResult};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::limit::GlobalLimitExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::ExecutionPlan;

use crate::operations::table_changes::TableChangesBuilder;
use crate::DeltaResult;
use crate::DeltaTableError;

fn session_state_from_session(session: &dyn Session) -> DataFusionResult<&SessionState> {
    session
        .as_any()
        .downcast_ref::<SessionState>()
        .ok_or_else(|| exec_datafusion_err!("Failed to downcast Session to SessionState"))
}

#[derive(Debug)]
pub struct DeltaCdfTableProvider {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl DeltaCdfTableProvider {
    /// Build a DeltaCDFTableProvider
    pub async fn try_new(cdf_builder: TableChangesBuilder) -> DeltaResult<Self> {
        let plan: Arc<dyn ExecutionPlan> = cdf_builder.build().await?;
        let schema = plan.schema();
        Ok(DeltaCdfTableProvider { plan, schema })
    }
}

#[async_trait]
impl TableProvider for DeltaCdfTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        session_state_from_session(session)?;
        let schema: DFSchema = self.schema().try_into()?;

        let mut plan = if let Some(filter_expr) = conjunction(filters.iter().cloned()) {
            let physical_expr = session.create_physical_expr(filter_expr, &schema)?;
            Arc::new(FilterExec::try_new(physical_expr, self.plan.clone())?)
        } else {
            self.plan.clone()
        };

        let df_schema: DFSchema = plan.schema().try_into()?;

        if let Some(projection) = projection {
            let current_projection = (0..plan.schema().fields().len()).collect::<Vec<usize>>();
            if projection != &current_projection {
                let fields: DeltaResult<Vec<(Arc<dyn PhysicalExpr>, String)>> = projection
                    .iter()
                    .map(|i| {
                        let (table_ref, field) = df_schema.qualified_field(*i);
                        session
                            .create_physical_expr(
                                Expr::Column(Column::from((table_ref, field))),
                                &df_schema,
                            )
                            .map(|expr| (expr, field.name().clone()))
                            .map_err(DeltaTableError::from)
                    })
                    .collect();
                let fields = fields?;
                plan = Arc::new(ProjectionExec::try_new(fields, plan)?);
            }
        }

        if let Some(limit) = limit {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(limit)))
        };
        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filter
            .iter()
            .map(|_| TableProviderFilterPushDown::Exact) // maybe exact
            .collect())
    }
}
