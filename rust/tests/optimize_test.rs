#[cfg(feature = "datafusion-ext")]
mod optiize {
    extern crate deltalake;

    use arrow::array::{Int64Array, Int32Array};
    use datafusion::prelude::ExecutionContext;
    use deltalake::{open_table, optimize::Optimize, PartitionFilter};
    use fs_extra::dir;
    use std::{error::Error, sync::Arc};

    #[tokio::test]
    async fn test_optimize_no_partitions() -> Result<(), Box<dyn Error>> {
        let tmp_dir = tempdir::TempDir::new("optimize_test")?;
        let path = tmp_dir.path();
        let opt = dir::CopyOptions::new();
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

    #[tokio::test]
    async fn test_optimize_with_partitions() -> Result<(), Box<dyn Error>> {
        let tmp_dir = tempdir::TempDir::new("optimize_test")?;
        let path = tmp_dir.path();
        let opt = dir::CopyOptions::new();
        dir::copy("./tests/data/optimize_partitions/", path, &opt)?;
        let p = path.join("optimize_partitions").to_owned();

        let mut dt = open_table(p.to_str().unwrap()).await?;
        let version = dt.version;
        let mut filter = vec![];
        filter.push(PartitionFilter::try_from(("event_year", "=", "2022"))?);
        filter.push(PartitionFilter::try_from(("event_month", "=", "04"))?);

        let optimize = Optimize::default()._where(&filter);
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
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])).as_ref(),
        );

        Ok(())
    }
}
