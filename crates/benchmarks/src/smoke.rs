use std::sync::Arc;

use deltalake_core::arrow;
use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake_core::protocol::SaveMode;
use deltalake_core::{DeltaOps, DeltaResult, DeltaTableError};
use url::Url;

#[derive(Debug, Clone)]
pub struct SmokeParams {
    pub rows: usize,
}

pub async fn run_smoke_once(table_url: &Url, params: &SmokeParams) -> DeltaResult<()> {
    if params.rows > i32::MAX as usize {
        return Err(DeltaTableError::generic(
            "smoke benchmark supports at most i32::MAX rows",
        ));
    }

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Utf8, false),
    ]));

    let ids: Vec<i32> = (0..params.rows).map(|i| i as i32).collect();
    let values: Vec<String> = ids.iter().map(|id| format!("value_{id}")).collect();

    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int32Array::from(ids)),
            Arc::new(arrow::array::StringArray::from(values)),
        ],
    )?;

    let table = DeltaOps::try_from_uri(table_url.clone())
        .await?
        .write(vec![batch])
        .with_save_mode(SaveMode::Overwrite)
        .await?;

    let snapshot = table.snapshot()?.snapshot().clone();
    let config = DeltaScanConfigBuilder::new().build(&snapshot)?;
    let provider = DeltaTableProvider::try_new(snapshot, table.log_store(), config)?;

    let ctx = SessionContext::new();
    ctx.register_table("smoke", Arc::new(provider))?;

    let df = ctx.sql("SELECT id, value FROM smoke ORDER BY id").await?;
    let batches = df.collect().await?;

    let mut total_rows = 0usize;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .ok_or_else(|| DeltaTableError::generic("unexpected column type for id"))?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or_else(|| DeltaTableError::generic("unexpected column type for value"))?;

        for i in 0..batch.num_rows() {
            let id = ids.value(i) as usize;
            let expected_value = format!("value_{id}");
            if values.value(i) != expected_value {
                return Err(DeltaTableError::generic(
                    "unexpected value returned from smoke table",
                ));
            }
        }
        total_rows += batch.num_rows();
    }

    if total_rows != params.rows {
        return Err(DeltaTableError::generic(format!(
            "expected {} rows, found {} in smoke table",
            params.rows, total_rows
        )));
    }

    Ok(())
}
