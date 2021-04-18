#[cfg(feature = "datafusion-ext")]
mod datafusion {
    use std::sync::Arc;

    use arrow::array::*;
    use datafusion::error::Result;
    use datafusion::execution::context::ExecutionContext;

    #[tokio::test]
    async fn test_datafusion_simple_query() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let table = deltalake::open_table("./tests/data/simple_table")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT id FROM demo WHERE id > 5")?
            .collect()
            .await?;

        assert_eq!(batches.len(), 2);

        assert_eq!(
            batches[0].column(0).as_ref(),
            Arc::new(Int64Array::from(vec![7])).as_ref(),
        );

        assert_eq!(
            batches[1].column(0).as_ref(),
            Arc::new(Int64Array::from(vec![9])).as_ref(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_date_column() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let table = deltalake::open_table("./tests/data/delta-0.8.0-date")
            .await
            .unwrap();
        ctx.register_table("dates", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT date from dates WHERE dayOfYear = 2")?
            .collect()
            .await?;

        assert_eq!(
            batches[0].column(0).as_ref(),
            Arc::new(Date32Array::from(vec![18629])).as_ref(),
        );

        Ok(())
    }
}
