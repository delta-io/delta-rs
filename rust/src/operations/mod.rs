//! High level operations API to interact with Delta tables
//!
//! At the heart of the high level operations APIs is the [`DeltaOps`] struct,
//! which consumes a [`DeltaTable`] and exposes methods to attain builders for
//! several high level operations. The specific builder structs allow fine-tuning
//! the operations' behaviors and will return an updated table potentially in conjunction
//! with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.

use self::create::CreateBuilder;
use self::filesystem_check::FileSystemCheckBuilder;
use self::vacuum::VacuumBuilder;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::builder::DeltaTableBuilder;
use crate::DeltaTable;

pub mod create;
pub mod filesystem_check;
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod optimize;
pub mod restore;
pub mod transaction;
pub mod vacuum;

#[cfg(feature = "datafusion")]
use self::{
    datafusion_utils::Expression, delete::DeleteBuilder, load::LoadBuilder, merge::MergeBuilder,
    update::UpdateBuilder, write::WriteBuilder,
};
#[cfg(feature = "datafusion")]
pub use ::datafusion::physical_plan::common::collect as collect_sendable_stream;
#[cfg(feature = "datafusion")]
use arrow::record_batch::RecordBatch;
#[cfg(all(feature = "arrow", feature = "parquet"))]
use optimize::OptimizeBuilder;
use restore::RestoreBuilder;

#[cfg(feature = "datafusion")]
pub mod delete;
#[cfg(feature = "datafusion")]
mod load;
#[cfg(feature = "datafusion")]
pub mod merge;
#[cfg(feature = "datafusion")]
pub mod update;
#[cfg(feature = "datafusion")]
pub mod write;
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod writer;

/// Maximum supported writer version
pub const MAX_SUPPORTED_WRITER_VERSION: i32 = 1;
/// Maximum supported reader version
pub const MAX_SUPPORTED_READER_VERSION: i32 = 1;

/// High level interface for executing commands against a DeltaTable
pub struct DeltaOps(pub DeltaTable);

impl DeltaOps {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given uri.
    ///
    /// ```
    /// use deltalake::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    /// };
    /// ```
    pub async fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri).build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a new [`DeltaOps`] instance, backed by an un-initialized in memory table
    ///
    /// Using this will not persist any changes beyond the lifetime of the table object.
    /// The main purpose of in-memory tables is for use in testing.
    ///
    /// ```
    /// use deltalake::DeltaOps;
    ///
    /// let ops = DeltaOps::new_in_memory();
    /// ```
    #[must_use]
    pub fn new_in_memory() -> Self {
        DeltaTableBuilder::from_uri("memory://")
            .build()
            .unwrap()
            .into()
    }

    /// Create a new Delta table
    ///
    /// ```
    /// use deltalake::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    ///     let table = ops.create().with_table_name("my_table").await.unwrap();
    ///     assert_eq!(table.version(), 0);
    /// };
    /// ```
    #[must_use]
    pub fn create(self) -> CreateBuilder {
        CreateBuilder::default().with_object_store(self.0.object_store())
    }

    /// Load data from a DeltaTable
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn load(self) -> LoadBuilder {
        LoadBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::new(self.0.object_store(), self.0.state).with_input_batches(batches)
    }

    /// Vacuum stale files from delta table
    #[must_use]
    pub fn vacuum(self) -> VacuumBuilder {
        VacuumBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Audit active files with files present on the filesystem
    #[must_use]
    pub fn filesystem_check(self) -> FileSystemCheckBuilder {
        FileSystemCheckBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Audit active files with files present on the filesystem
    #[cfg(all(feature = "arrow", feature = "parquet"))]
    #[must_use]
    pub fn optimize<'a>(self) -> OptimizeBuilder<'a> {
        OptimizeBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Delete data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn delete(self) -> DeleteBuilder {
        DeleteBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn update(self) -> UpdateBuilder {
        UpdateBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Restore delta table to a specified version or datetime
    #[must_use]
    pub fn restore(self) -> RestoreBuilder {
        RestoreBuilder::new(self.0.object_store(), self.0.state)
    }

    /// Update data from Delta table
    #[cfg(feature = "datafusion")]
    #[must_use]
    pub fn merge<E: Into<Expression>>(
        self,
        source: datafusion::prelude::DataFrame,
        predicate: E,
    ) -> MergeBuilder {
        MergeBuilder::new(
            self.0.object_store(),
            self.0.state,
            predicate.into(),
            source,
        )
    }
}

impl From<DeltaTable> for DeltaOps {
    fn from(table: DeltaTable) -> Self {
        Self(table)
    }
}

impl From<DeltaOps> for DeltaTable {
    fn from(ops: DeltaOps) -> Self {
        ops.0
    }
}

impl AsRef<DeltaTable> for DeltaOps {
    fn as_ref(&self) -> &DeltaTable {
        &self.0
    }
}

#[cfg(feature = "datafusion")]
mod datafusion_utils {
    use std::sync::Arc;

    use arrow_schema::SchemaRef;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::Result as DataFusionResult;
    use datafusion::execution::context::SessionState;
    use datafusion::physical_plan::DisplayAs;
    use datafusion::physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
    };
    use datafusion_common::DFSchema;
    use datafusion_expr::Expr;
    use futures::{Stream, StreamExt};

    use crate::{delta_datafusion::expr::parse_predicate_expression, DeltaResult};

    /// Used to represent user input of either a Datafusion expression or string expression
    pub enum Expression {
        /// Datafusion Expression
        DataFusion(Expr),
        /// String Expression
        String(String),
    }

    impl From<Expr> for Expression {
        fn from(val: Expr) -> Self {
            Expression::DataFusion(val)
        }
    }

    impl From<&str> for Expression {
        fn from(val: &str) -> Self {
            Expression::String(val.to_string())
        }
    }
    impl From<String> for Expression {
        fn from(val: String) -> Self {
            Expression::String(val)
        }
    }

    pub(crate) fn into_expr(
        expr: Expression,
        schema: &DFSchema,
        df_state: &SessionState,
    ) -> DeltaResult<Expr> {
        match expr {
            Expression::DataFusion(expr) => Ok(expr),
            Expression::String(s) => parse_predicate_expression(schema, s, df_state),
        }
    }

    pub(crate) fn maybe_into_expr(
        expr: Option<Expression>,
        schema: &DFSchema,
        df_state: &SessionState,
    ) -> DeltaResult<Option<Expr>> {
        Ok(match expr {
            Some(predicate) => Some(into_expr(predicate, schema, df_state)?),
            None => None,
        })
    }

    pub(crate) type MetricObserverFunction = fn(&RecordBatch, &ExecutionPlanMetricsSet) -> ();

    pub(crate) struct MetricObserverExec {
        parent: Arc<dyn ExecutionPlan>,
        metrics: ExecutionPlanMetricsSet,
        update: MetricObserverFunction,
    }

    impl MetricObserverExec {
        pub fn new(parent: Arc<dyn ExecutionPlan>, f: MetricObserverFunction) -> Self {
            MetricObserverExec {
                parent,
                metrics: ExecutionPlanMetricsSet::new(),
                update: f,
            }
        }
    }

    impl std::fmt::Debug for MetricObserverExec {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MergeStatsExec")
                .field("parent", &self.parent)
                .field("metrics", &self.metrics)
                .finish()
        }
    }

    impl DisplayAs for MetricObserverExec {
        fn fmt_as(
            &self,
            _: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "MetricObserverExec")
        }
    }

    impl ExecutionPlan for MetricObserverExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> arrow_schema::SchemaRef {
            self.parent.schema()
        }

        fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
            self.parent.output_partitioning()
        }

        fn output_ordering(&self) -> Option<&[datafusion_physical_expr::PhysicalSortExpr]> {
            self.parent.output_ordering()
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![self.parent.clone()]
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<datafusion::execution::context::TaskContext>,
        ) -> datafusion_common::Result<datafusion::physical_plan::SendableRecordBatchStream>
        {
            let res = self.parent.execute(partition, context)?;
            Ok(Box::pin(MetricObserverStream {
                schema: self.schema(),
                input: res,
                metrics: self.metrics.clone(),
                update: self.update,
            }))
        }

        fn statistics(&self) -> datafusion_common::Statistics {
            self.parent.statistics()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            ExecutionPlan::with_new_children(self.parent.clone(), children)
        }

        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
    }

    struct MetricObserverStream {
        schema: SchemaRef,
        input: SendableRecordBatchStream,
        metrics: ExecutionPlanMetricsSet,
        update: MetricObserverFunction,
    }

    impl Stream for MetricObserverStream {
        type Item = DataFusionResult<RecordBatch>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.input.poll_next_unpin(cx).map(|x| match x {
                Some(Ok(batch)) => {
                    (self.update)(&batch, &self.metrics);
                    Some(Ok(batch))
                }
                other => other,
            })
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.input.size_hint()
        }
    }

    impl RecordBatchStream for MetricObserverStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }
}
