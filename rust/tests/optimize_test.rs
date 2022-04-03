#[cfg(feature = "datafusion-ext")]
mod optiize {
    extern crate deltalake;

    use arrow::array::Int64Array;
    use datafusion::prelude::ExecutionContext;
    use deltalake::{open_table, optimize::Optimize, DeltaTable, DeltaTableConfig};
    use fs_extra::dir;
    use std::{env, error::Error, matches, process::Command, sync::Arc};

    #[tokio::test]
    async fn test_optimize_no_partitions() -> Result<(), Box<dyn Error>> {
        let tmp_dir = tempdir::TempDir::new("optimize_test")?;
        let path = tmp_dir.path();
        let mut opt = dir::CopyOptions::new();
        dir::copy("./tests/data/simple_table/", path, &opt)?;
        let p = path.join("simple_table").to_owned();

        let mut dt = open_table(p.to_str().unwrap()).await?;
        let version = dt.version;

        let optimize = Optimize::default();
        let metrics = optimize.execute(&mut dt).await?;

        println!("{:?}", metrics);
        //Validate the data matches
        assert_eq!(version + 1, dt.version);

        let mut ctx = ExecutionContext::new();
        let table = deltalake::open_table(p.to_str().unwrap()).await.unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT id FROM demo ORDER BY id ASC")
            .await?
            .collect()
            .await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int64Array::from(vec![5, 7, 9])).as_ref(),
        );

        Ok(())
    }
}
