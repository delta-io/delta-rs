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
use std::fs::File;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{combine_filters, Expr};
use datafusion::physical_plan::parquet::{ParquetExec, ParquetPartition, RowGroupPredicateBuilder};
use datafusion::physical_plan::ExecutionPlan;
use parquet::arrow::ParquetFileArrowReader;
use parquet::file::reader::SerializedFileReader;

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
            .map(|fname| {
                let mut num_rows = 0;
                let mut total_byte_size = 0;

                let file = File::open(&fname)?;
                let file_reader = Arc::new(SerializedFileReader::new(file)?);
                let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
                let meta_data = arrow_reader.get_metadata();
                // collect all the unique schemas in this data set
                for i in 0..meta_data.num_row_groups() {
                    let row_group_meta = meta_data.row_group(i);
                    num_rows += row_group_meta.num_rows();
                    total_byte_size += row_group_meta.total_byte_size();
                }
                let statistics = Statistics {
                    num_rows: Some(num_rows as usize),
                    total_byte_size: Some(total_byte_size as usize),
                    column_statistics: None,
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
        // TODO: proxy delta table stats after https://github.com/delta-io/delta.rs/issues/45 has
        // been completed
        Statistics::default()
    }
}
