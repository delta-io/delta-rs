//! Datafusion integration for Delta Table
//!
//! Example:
//!
//! ```rust
//! use std::sync::Arc;
//! use datafusion::execution::context::ExecutionContext;
//!
//! async {
//!   let mut ctx = ExecutionContext::new();
//!   let table = deltalake::open_table("./tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{combine_filters, Expr};
use datafusion::physical_plan::parquet::{ParquetExec, ParquetPartition, RowGroupPredicateBuilder};
use datafusion::physical_plan::ExecutionPlan;

use crate::delta;
use crate::schema;

impl TableProvider for delta::DeltaTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(
            <ArrowSchema as TryFrom<&schema::Schema>>::try_from(
                delta::DeltaTable::schema(&self).unwrap(),
            )
            .unwrap(),
        )
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = <ArrowSchema as TryFrom<&schema::Schema>>::try_from(
            delta::DeltaTable::schema(&self).unwrap(),
        )?;
        let filenames = self.get_file_uris();

        let partitions = filenames
            .into_iter()
            .zip(self.get_stats())
            .map(|(fname, stats)| {
                let statistics = if let Ok(Some(statistics)) = stats {
                    Statistics {
                        num_rows: Some(statistics.num_records as usize),
                        total_byte_size: None,
                        // TODO map column statistics
                        column_statistics: None,
                    }
                } else {
                    Statistics::default()
                };

                Ok(ParquetPartition::new(vec![fname], statistics))
            })
            .collect::<datafusion::error::Result<_>>()?;

        let predicate_builder = combine_filters(filters).and_then(|predicate_expr| {
            RowGroupPredicateBuilder::try_new(&predicate_expr, schema.clone()).ok()
        });

        Ok(Arc::new(ParquetExec::new(
            partitions,
            schema,
            projection.clone(),
            predicate_builder,
            batch_size,
            limit,
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn statistics(&self) -> Statistics {
        self.get_stats()
            .into_iter()
            .fold(
                Some(Statistics {
                    num_rows: Some(0),
                    total_byte_size: None,
                    column_statistics: None,
                }),
                |acc, stats| {
                    let acc = acc?;
                    let new_stats = stats.unwrap_or(None)?;
                    Some(Statistics {
                        num_rows: acc
                            .num_rows
                            .map(|rows| rows + new_stats.num_records as usize),
                        total_byte_size: None,
                        column_statistics: None, // TODO: add column statistics
                    })
                },
            )
            .unwrap_or_default()
    }
}
