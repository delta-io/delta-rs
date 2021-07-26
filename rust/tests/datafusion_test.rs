#[cfg(feature = "datafusion-ext")]
mod datafusion {
    use std::sync::Arc;

    use arrow::array::*;
    use datafusion::datasource::TableProvider;
    use datafusion::error::Result;
    use datafusion::execution::context::ExecutionContext;
    use datafusion::scalar::ScalarValue;

    #[tokio::test]
    async fn test_datafusion_simple_query() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let table = deltalake::open_table("./tests/data/simple_table")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT id FROM demo WHERE id > 5 ORDER BY id ASC")?
            .collect()
            .await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int64Array::from(vec![7, 9])).as_ref(),
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

    #[tokio::test]
    async fn test_datafusion_stats() -> Result<()> {
        let table = deltalake::open_table("./tests/data/delta-0.8.0")
            .await
            .unwrap();
        let statistics = table.statistics();

        assert_eq!(statistics.num_rows, Some(4),);

        assert_eq!(statistics.total_byte_size, Some(440 + 440));

        assert_eq!(
            statistics
                .column_statistics
                .clone()
                .unwrap()
                .iter()
                .map(|x| x.null_count)
                .collect::<Vec<Option<usize>>>(),
            vec![Some(0)],
        );

        let mut ctx = ExecutionContext::new();
        ctx.register_table("test_table", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT max(value), min(value) FROM test_table")?
            .collect()
            .await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![4])).as_ref(),
        );

        assert_eq!(
            batch.column(1).as_ref(),
            Arc::new(Int32Array::from(vec![0])).as_ref(),
        );

        assert_eq!(
            statistics
                .column_statistics
                .clone()
                .unwrap()
                .iter()
                .map(|x| x.max_value.as_ref())
                .collect::<Vec<Option<&ScalarValue>>>(),
            vec![Some(&ScalarValue::from(4 as i32))],
        );

        assert_eq!(
            statistics
                .column_statistics
                .clone()
                .unwrap()
                .iter()
                .map(|x| x.min_value.as_ref())
                .collect::<Vec<Option<&ScalarValue>>>(),
            vec![Some(&ScalarValue::from(0 as i32))],
        );

        Ok(())
    }
}
