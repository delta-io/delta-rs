use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use deltalake::datafusion::catalog::{Session, TableProvider};
use deltalake::datafusion::common::{Column, DFSchema, Result as DataFusionResult};
use deltalake::datafusion::datasource::TableType;
use deltalake::datafusion::execution::object_store::ObjectStoreUrl;
use deltalake::datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown};
use deltalake::datafusion::prelude::Expr;
use deltalake::delta_datafusion::DeltaScanNext;
use deltalake::logstore::object_store::DynObjectStore;
use deltalake::{DeltaResult, DeltaTableError, datafusion};
use parking_lot::RwLock;
use tokio::runtime::Handle;

#[derive(Debug)]
pub(crate) struct LazyTableProvider {
    schema: Arc<ArrowSchema>,
    batches: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
}

impl LazyTableProvider {
    /// Build a DeltaTableProvider
    pub fn try_new(
        schema: Arc<ArrowSchema>,
        batches: Vec<Arc<RwLock<dyn LazyBatchGenerator>>>,
    ) -> DeltaResult<Self> {
        Ok(LazyTableProvider { schema, batches })
    }
}

#[async_trait::async_trait]
impl TableProvider for LazyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut plan: Arc<dyn ExecutionPlan> = Arc::new(LazyMemoryExec::try_new(
            self.schema(),
            self.batches.clone(),
        )?);

        let df_schema: DFSchema = plan.schema().try_into()?;

        if let Some(filter_expr) = conjunction(filters.iter().cloned()) {
            let physical_expr =
                create_physical_expr(&filter_expr, &df_schema, &ExecutionProps::new())?;
            plan = Arc::new(FilterExec::try_new(physical_expr, plan)?);
        }

        if let Some(projection) = projection {
            let current_projection = (0..plan.schema().fields().len()).collect::<Vec<usize>>();
            if projection != &current_projection {
                let execution_props = &ExecutionProps::new();
                let fields: DeltaResult<Vec<(Arc<dyn PhysicalExpr>, String)>> = projection
                    .iter()
                    .map(|i| {
                        let (table_ref, field) = df_schema.qualified_field(*i);
                        create_physical_expr(
                            &Expr::Column(Column::from((table_ref, field))),
                            &df_schema,
                            execution_props,
                        )
                        .map(|expr| (expr, field.name().clone()))
                        .map_err(DeltaTableError::from)
                    })
                    .collect();
                plan = Arc::new(ProjectionExec::try_new(fields?, plan)?);
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
        Ok(vec![TableProviderFilterPushDown::Inexact; filter.len()])
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct TokioDeltaScan {
    inner: DeltaScanNext,
    handle: Handle,
    object_store_url: Option<ObjectStoreUrl>,
    object_store: Option<Arc<DynObjectStore>>,
}

impl TokioDeltaScan {
    pub fn new(inner: DeltaScanNext, handle: Handle) -> Self {
        Self {
            inner,
            handle,
            object_store_url: None,
            object_store: None,
        }
    }

    pub fn with_object_store(
        mut self,
        object_store_url: ObjectStoreUrl,
        object_store: Arc<DynObjectStore>,
    ) -> Self {
        self.object_store_url = Some(object_store_url);
        self.object_store = Some(object_store);
        self
    }
}

#[async_trait::async_trait]
impl TableProvider for TokioDeltaScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if let (Some(url), Some(store)) = (&self.object_store_url, &self.object_store) {
            session
                .runtime_env()
                .register_object_store(url.as_ref(), store.clone());
        }

        let inner = &self.inner;

        self.handle
            .block_on(async { inner.scan(session, projection, filters, limit).await })
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::physical_plan::memory::LazyBatchGenerator;
    use deltalake::arrow::array::{Int32Array, StringArray};
    use deltalake::arrow::record_batch::RecordBatch;
    use deltalake::datafusion::common::Result as DataFusionResult;
    use deltalake::datafusion::prelude::SessionContext;
    use parking_lot::RwLock;

    // Import the LazyTableProvider
    use crate::datafusion::LazyTableProvider;

    // A dummy LazyBatchGenerator implementation for testing
    #[derive(Debug)]
    struct TestBatchGenerator {
        data: Vec<RecordBatch>,
        current_index: usize,
    }

    impl std::fmt::Display for TestBatchGenerator {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "TestBatchGenerator")
        }
    }

    impl TestBatchGenerator {
        fn new(data: Vec<RecordBatch>) -> Self {
            Self {
                data,
                current_index: 0,
            }
        }

        // Helper to create a test batch generator with sample data
        fn create_test_generator(schema: Arc<ArrowSchema>) -> Arc<RwLock<dyn LazyBatchGenerator>> {
            // Create sample data
            let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Carol", "Dave", "Eve"]);

            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
                    .unwrap();

            Arc::new(RwLock::new(TestBatchGenerator::new(vec![batch])))
        }
    }

    impl LazyBatchGenerator for TestBatchGenerator {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn generate_next_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
            if self.current_index < self.data.len() {
                let batch = self.data[self.current_index].clone();
                self.current_index += 1;
                Ok(Some(batch))
            } else {
                Ok(None)
            }
        }

        fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
            // DataFusion may need to restart a stream from the beginning (e.g. recursive CTEs).
            Arc::new(RwLock::new(TestBatchGenerator::new(self.data.clone())))
        }
    }

    #[tokio::test]
    async fn test_lazy_table_provider_basic() {
        // Create a schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a test generator
        let generator = TestBatchGenerator::create_test_generator(schema.clone());

        // Create the LazyTableProvider
        let provider = LazyTableProvider::try_new(schema.clone(), vec![generator]).unwrap();

        // Check that the schema matches
        assert_eq!(provider.schema(), schema);

        // Create a session context
        let ctx = SessionContext::new();
        let session = ctx.state();

        // Test basic scan without projections or filters
        let plan = provider.scan(&session, None, &[], None).await.unwrap();
        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().field(0).name(), "id");
        assert_eq!(plan.schema().field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_lazy_table_provider_with_projection() {
        // Create a schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a test generator
        let generator = TestBatchGenerator::create_test_generator(schema.clone());

        // Create the LazyTableProvider
        let provider = LazyTableProvider::try_new(schema, vec![generator]).unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        let session = ctx.state();

        // Test scanning with projection (only select the id column)
        let projection = Some(vec![0]);
        let plan = provider
            .scan(&session, projection.as_ref(), &[], None)
            .await
            .unwrap();

        // Verify the plan schema only includes the projected column
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_lazy_table_provider_with_filter() {
        // Create a schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a test generator
        let generator = TestBatchGenerator::create_test_generator(schema.clone());

        // Create the LazyTableProvider
        let provider = LazyTableProvider::try_new(schema, vec![generator]).unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        let session = ctx.state();

        // Test scanning with filter (id > 2)
        let filter = vec![col("id").gt(lit(2))];
        let plan = provider.scan(&session, None, &filter, None).await.unwrap();

        // The scan method should add a FilterExec to the plan
        // We can verify this by checking the plan's children
        assert!(!plan.children().is_empty());
    }

    #[tokio::test]
    async fn test_lazy_table_provider_with_limit() {
        // Create a schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a test generator
        let generator = TestBatchGenerator::create_test_generator(schema.clone());

        // Create the LazyTableProvider
        let provider = LazyTableProvider::try_new(schema, vec![generator]).unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        let session = ctx.state();

        // Test scanning with limit
        let limit = Some(3);
        let plan = provider.scan(&session, None, &[], limit).await.unwrap();

        // The plan should include a LimitExec
        // We can verify this by checking that the plan type is correct
        assert!(plan.as_any().downcast_ref::<GlobalLimitExec>().is_some());
    }

    #[tokio::test]
    async fn test_lazy_table_provider_combined() {
        // Create a schema
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create a test generator
        let generator = TestBatchGenerator::create_test_generator(schema.clone());

        // Create the LazyTableProvider
        let provider = LazyTableProvider::try_new(schema, vec![generator]).unwrap();

        // Create a session context
        let ctx = SessionContext::new();
        let session = ctx.state();

        // Test scanning with projection, filter, and limit combined
        let projection = Some(vec![0]); // Only id column
        let filter = vec![col("id").gt(lit(2))]; // id > 2
        let limit = Some(2); // Return only 2 rows

        let plan = provider
            .scan(&session, projection.as_ref(), &filter, limit)
            .await
            .unwrap();

        // Verify the plan schema only includes the projected column
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "id");

        // The resulting plan should have a chain of operations:
        // GlobalLimitExec -> ProjectionExec -> FilterExec -> LazyMemoryExec
        assert!(plan.as_any().downcast_ref::<GlobalLimitExec>().is_some());
    }
}
