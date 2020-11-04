#[cfg(feature = "datafusion-ext")]
mod datafusion {
    extern crate arrow;
    extern crate datafusion;
    extern crate deltalake;

    use self::arrow::array::UInt64Array;
    use self::datafusion::error::Result;
    use self::datafusion::execution::context::ExecutionContext;

    #[tokio::test]
    async fn test_datafusion_simple_query() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let table = deltalake::open_table("./tests/data/simple_table").unwrap();
        ctx.register_table("demo", Box::new(table));

        let plan = ctx.create_logical_plan("SELECT id FROM demo WHERE id > 5")?;
        let plan = ctx.optimize(&plan)?;
        let plan = ctx.create_physical_plan(&plan)?;
        let results = ctx.collect(plan).await?;

        let results = results
            .iter()
            .filter(|batch| batch.num_rows() > 0)
            .flat_map(|batch| {
                UInt64Array::from(batch.column(0).data())
                    .value_slice(0, 1)
                    .iter()
                    .map(|v| *v)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<u64>>();
        assert_eq!(results, vec![7u64, 9u64]);

        Ok(())
    }
}
