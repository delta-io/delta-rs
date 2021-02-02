use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::datasource::Statistics;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::delta;
use crate::schema;

impl TableProvider for delta::DeltaTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(<ArrowSchema as From<&schema::Schema>>::from(
            delta::DeltaTable::schema(&self).unwrap(),
        ))
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema =
            <ArrowSchema as From<&schema::Schema>>::from(delta::DeltaTable::schema(&self).unwrap());
        let filenames = self.get_file_paths();

        Ok(Arc::new(ParquetExec::new(
            filenames,
            schema,
            projection.clone(),
            batch_size,
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
