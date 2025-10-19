use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DFSchema, Result as DataFusionResult};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::{
    delta_datafusion::DataFusionMixins, operations::load_cdf::CdfLoadBuilder, DeltaResult,
    DeltaTableError,
};

use super::ADD_PARTITION_SCHEMA;

#[derive(Debug)]
pub struct DeltaCdfTableProvider {
    cdf_builder: CdfLoadBuilder,
    schema: SchemaRef,
}

impl DeltaCdfTableProvider {
    /// Build a DeltaCDFTableProvider
    pub fn try_new(cdf_builder: CdfLoadBuilder) -> DeltaResult<Self> {
        let mut fields = cdf_builder
            .snapshot
            .as_ref()
            .ok_or(DeltaTableError::generic(
                "expected initialized snapshot for DeltaCdfTableProvider",
            ))?
            .input_schema()
            .fields()
            .to_vec();
        for f in ADD_PARTITION_SCHEMA.clone() {
            fields.push(f.into());
        }
        Ok(DeltaCdfTableProvider {
            cdf_builder,
            schema: Schema::new(fields).into(),
        })
    }
}

#[async_trait::async_trait]
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
        let schema: DFSchema = self.schema().try_into()?;

        let mut plan = if let Some(filter_expr) = conjunction(filters.iter().cloned()) {
            let physical_expr = session.create_physical_expr(filter_expr, &schema)?;
            let plan = self
                .cdf_builder
                .build(session, Some(&physical_expr))
                .await?;
            Arc::new(FilterExec::try_new(physical_expr, plan)?)
        } else {
            self.cdf_builder.build(session, None).await?
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
