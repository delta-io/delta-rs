use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion_physical_expr::{Partitioning, PhysicalSortExpr};
use lazy_static::lazy_static;

/// Change type column name
pub const CHANGE_TYPE_COL: &str = "_change_type";
/// Commit version column name
pub const COMMIT_VERSION_COL: &str = "_commit_version";
/// Commit Timestamp column name
pub const COMMIT_TIMESTAMP_COL: &str = "_commit_timestamp";

lazy_static! {
    pub static ref CDC_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        )
    ];
    pub static ref ADD_PARTITION_SCHEMA: Vec<Field> = vec![
        Field::new(CHANGE_TYPE_COL, DataType::Utf8, true),
        Field::new(COMMIT_VERSION_COL, DataType::Int64, true),
        Field::new(
            COMMIT_TIMESTAMP_COL,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true
        ),
    ];
}

/// Physical execution of a scan
#[derive(Debug)]
pub struct DeltaCdfScan {
    plan: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl DeltaCdfScan {
    /// Creates a new scan
    pub fn new(plan: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        Self { plan, schema }
    }
}

impl DisplayAs for DeltaCdfScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ExecutionPlan for DeltaCdfScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.plan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.plan.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.plan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.plan.clone().with_new_children(_children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.plan.execute(partition, context)
    }
}

// #[cfg(test)]
// mod tests {
//     use arrow_array::RecordBatch;
//     use arrow_cast::pretty::print_batches;
//
//     use crate::DeltaOps;
//     use crate::operations::collect_sendable_stream;
//     use crate::writer::test_utils::TestResult;
//
//     use super::DeltaCdfScan;
//
//     /**
//     This is the output from a cdf read on the test table using spark. It's the reference point I used to work from.
//
//     ```scala
//         val table = spark.read
//         .format("delta")
//         .option("readChangeFeed", "true")
//         .option("startingVersion", 0)
//         .load("./cdf-table")
//       table.show(truncate = false, numRows = 100)
//     ```
//
//     +---+------+----------+----------------+---------------+-----------------------+
//     |id |name  |birthday  |_change_type    |_commit_version|_commit_timestamp      |
//     +---+------+----------+----------------+---------------+-----------------------+
//     |7  |Dennis|2023-12-29|update_postimage|2              |2023-12-29 16:41:33.843|
//     |5  |Emily |2023-12-29|update_postimage|2              |2023-12-29 16:41:33.843|
//     |7  |Dennis|2023-12-24|update_preimage |2              |2023-12-29 16:41:33.843|
//     |5  |Emily |2023-12-24|update_preimage |2              |2023-12-29 16:41:33.843|
//     |6  |Carl  |2023-12-29|update_postimage|2              |2023-12-29 16:41:33.843|
//     |3  |Dave  |2023-12-22|update_postimage|1              |2023-12-22 12:10:21.688|
//     |4  |Kate  |2023-12-22|update_postimage|1              |2023-12-22 12:10:21.688|
//     |6  |Carl  |2023-12-24|update_preimage |2              |2023-12-29 16:41:33.843|
//     |2  |Bob   |2023-12-22|update_postimage|1              |2023-12-22 12:10:21.688|
//     |3  |Dave  |2023-12-23|update_preimage |1              |2023-12-22 12:10:21.688|
//     |4  |Kate  |2023-12-23|update_preimage |1              |2023-12-22 12:10:21.688|
//     |2  |Bob   |2023-12-23|update_preimage |1              |2023-12-22 12:10:21.688|
//     |7  |Dennis|2023-12-29|delete          |3              |2024-01-06 11:44:59.624|
//     |8  |Claire|2023-12-25|insert          |0              |2023-12-22 12:10:18.922|
//     |7  |Dennis|2023-12-24|insert          |0              |2023-12-22 12:10:18.922|
//     |1  |Steve |2023-12-22|insert          |0              |2023-12-22 12:10:18.922|
//     |5  |Emily |2023-12-24|insert          |0              |2023-12-22 12:10:18.922|
//     |3  |Dave  |2023-12-23|insert          |0              |2023-12-22 12:10:18.922|
//     |4  |Kate  |2023-12-23|insert          |0              |2023-12-22 12:10:18.922|
//     |10 |Borb  |2023-12-25|insert          |0              |2023-12-22 12:10:18.922|
//     |6  |Carl  |2023-12-24|insert          |0              |2023-12-22 12:10:18.922|
//     |2  |Bob   |2023-12-23|insert          |0              |2023-12-22 12:10:18.922|
//     |9  |Ada   |2023-12-25|insert          |0              |2023-12-22 12:10:18.922|
//     +---+------+----------+----------------+---------------+-----------------------+
//     **/
//
//     // #[tokio::test]
//     // async fn test_load_local() -> TestResult {
//     //     // I checked in a pre-built table from spark, once the writing side is finished I will circle back to make these
//     //     // tests self-contained and not rely on a physical table with them
//     //     let _table = DeltaOps::try_from_uri("../test/tests/data/cdf-table").await?.0.clone();
//     //     let metadata = _table.metadata()?;
//     //     let state = _table.clone().state.unwrap().clone();
//     //     let schema = state.arrow_schema()?;
//     //     let partition_cols = metadata.partition_columns.clone();
//     //
//     //     let scan = DeltaCdfScan::new(_table.log_store.clone(), 0, schema, partition_cols);
//     //
//     //     let results = scan.scan().await?;
//     //     let data: Vec<RecordBatch> = collect_sendable_stream(results).await?;
//     //     print_batches(&data)?;
//     //
//     //     Ok(())
//     // }
// }
