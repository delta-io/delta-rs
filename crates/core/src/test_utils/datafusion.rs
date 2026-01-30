use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session as DataFusionSession;
use datafusion::common::{DFSchema, DataFusionError, ScalarValue};
use datafusion::config::TableOptions;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, Expr, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility, WindowUDF,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;

pub(crate) fn make_test_scalar_udf(name: &'static str) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(TestScalarUdf { name }))
}

#[derive(Debug, Hash, PartialEq, Eq)]
struct TestScalarUdf {
    name: &'static str,
}

impl ScalarUDFImpl for TestScalarUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| Signature {
            type_signature: TypeSignature::VariadicAny,
            volatility: Volatility::Immutable,
            parameter_names: Some(vec![]),
        });
        &SIGNATURE
    }

    fn return_type(
        &self,
        _arg_types: &[arrow_schema::DataType],
    ) -> DataFusionResult<arrow_schema::DataType> {
        Ok(arrow_schema::DataType::Int32)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(1))))
    }
}

pub(crate) struct WrapperSession {
    inner: SessionState,
    planning_error: Option<&'static str>,
}

impl WrapperSession {
    pub(crate) fn new(inner: SessionState) -> Self {
        Self {
            inner,
            planning_error: None,
        }
    }

    pub(crate) fn new_with_planning_error(inner: SessionState, message: &'static str) -> Self {
        Self {
            inner,
            planning_error: Some(message),
        }
    }
}

#[async_trait]
impl DataFusionSession for WrapperSession {
    fn session_id(&self) -> &str {
        DataFusionSession::session_id(&self.inner)
    }

    fn config(&self) -> &SessionConfig {
        DataFusionSession::config(&self.inner)
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if let Some(message) = self.planning_error {
            return Err(DataFusionError::Plan(message.to_string()));
        }
        DataFusionSession::create_physical_plan(&self.inner, logical_plan).await
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if let Some(message) = self.planning_error {
            return Err(DataFusionError::Plan(message.to_string()));
        }
        DataFusionSession::create_physical_expr(&self.inner, expr, df_schema)
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        DataFusionSession::scalar_functions(&self.inner)
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        DataFusionSession::aggregate_functions(&self.inner)
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        DataFusionSession::window_functions(&self.inner)
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        DataFusionSession::runtime_env(&self.inner)
    }

    fn execution_props(&self) -> &ExecutionProps {
        DataFusionSession::execution_props(&self.inner)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_options(&self) -> &TableOptions {
        DataFusionSession::table_options(&self.inner)
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        DataFusionSession::table_options_mut(&mut self.inner)
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        DataFusionSession::task_ctx(&self.inner)
    }
}
