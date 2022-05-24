mod optimize {
    extern crate deltalake;

    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field},
        record_batch::RecordBatch,
    };
    use deltalake::{
        action, get_backend_for_uri_with_options, open_table,
        optimize::Optimize,
        writer::{DeltaWriter, RecordBatchWriter},
        DeltaTableConfig, DeltaTableMetaData, PartitionFilter,
    };
    use deltalake::{DeltaTable, Schema, SchemaDataType, SchemaField};
    use fs_extra::dir;
    use rand::prelude::*;
    use serde_json::{Map, Value};
    use std::{collections::HashMap, error::Error, sync::Arc};
    use tempdir::TempDir;

    #[cfg(feature = "datafusion-ext")]
    #[tokio::test]
    async fn test_optimize_no_partitions() -> Result<(), Box<dyn Error>> {
        use datafusion::prelude::SessionContext;
        let tmp_dir = tempdir::TempDir::new("optimize_test")?;
        let path = tmp_dir.path();
        let opt = dir::CopyOptions::new();
        dir::copy("./tests/data/simple_table/", path, &opt)?;
        let p = path.join("simple_table").to_owned();

        let mut dt = open_table(p.to_str().unwrap()).await?;
        let version = dt.version;

        let optimize = Optimize::default();
        let _metrics = optimize.execute(&mut dt).await?;

        assert_eq!(version + 1, dt.version);

        let ctx = SessionContext::new();
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

    #[cfg(feature = "datafusion-ext")]
    #[tokio::test]
    async fn test_optimize_with_partitions() -> Result<(), Box<dyn Error>> {
        use datafusion::prelude::SessionContext;
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

        let optimize = Optimize::default().filter(&filter);
        let _metrics = optimize.execute(&mut dt).await?;

        assert_eq!(version + 1, dt.version);

        let ctx = SessionContext::new();
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

    struct Context {
        pub tmp_dir: TempDir,
        pub table: DeltaTable,
    }

    async fn setup_test() -> Result<Context, Box<dyn Error>> {
        let schema = Schema::new(vec![
            SchemaField::new(
                "x".to_owned(),
                SchemaDataType::primitive("integer".to_owned()),
                false,
                HashMap::new(),
            ),
            SchemaField::new(
                "y".to_owned(),
                SchemaDataType::primitive("integer".to_owned()),
                false,
                HashMap::new(),
            ),
            SchemaField::new(
                "date".to_owned(),
                SchemaDataType::primitive("string".to_owned()),
                false,
                HashMap::new(),
            ),
        ]);

        let table_meta = DeltaTableMetaData::new(
            Some("perf_table".to_owned()),
            Some("Used to measure performance for various operations".to_owned()),
            None,
            schema.clone(),
            vec!["date".to_owned()],
            HashMap::new(),
        );

        let tmp_dir = tempdir::TempDir::new("perf_table").unwrap();
        let p = tmp_dir.path().to_str().to_owned().unwrap();

        let backend = get_backend_for_uri_with_options(&p, HashMap::new())?;
        let mut dt = DeltaTable::new(&p, backend, DeltaTableConfig::default())?;
        let mut commit_info = Map::<String, Value>::new();

        let protocol = action::Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        let _res = dt
            .create(table_meta.clone(), protocol.clone(), None, None)
            .await?;

        Ok(Context { tmp_dir, table: dt })
    }

    fn generate_random_batch(rows: usize) -> Result<RecordBatch, Box<dyn Error>> {
        let mut x_vec: Vec<i32> = Vec::with_capacity(rows);
        let mut y_vec: Vec<i32> = Vec::with_capacity(rows);
        let mut date_vec = Vec::with_capacity(rows);
        let mut rng = rand::thread_rng();

        for _ in 0..rows {
            x_vec.push(rng.gen());
            y_vec.push(rng.gen());
            date_vec.push("2022-05-22".to_owned());
        }

        let x_array = Int32Array::from(x_vec);
        let y_array = Int32Array::from(y_vec);
        let date_array = StringArray::from(date_vec);

        Ok(RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Int32, false),
                Field::new("date", DataType::Utf8, false),
            ])),
            vec![Arc::new(x_array), Arc::new(y_array), Arc::new(date_array)],
        )?)
    }

    fn records_for_size(size: usize) -> usize {
        //12 bytes to account of overhead
        size / 12
    }

    #[tokio::test]
    ///Validate that bin packing is idempotent.
    async fn test_idempotent() -> Result<(), Box<dyn Error>> {
        //TODO: Compression makes it hard to get the target file size...
        //Maybe just commit files with a known size
        let context = setup_test().await?;
        let mut dt = context.table;
        let mut writer = RecordBatchWriter::for_table(&dt, HashMap::new())?;

        writer
            .write(generate_random_batch(records_for_size(7_000_000))?)
            .await?;
        writer.flush_and_commit(&mut dt).await?;

        writer
            .write(generate_random_batch(records_for_size(9_000_000))?)
            .await?;
        writer.flush_and_commit(&mut dt).await?;

        writer
            .write(generate_random_batch(records_for_size(2_000_000))?)
            .await?;
        writer.flush_and_commit(&mut dt).await?;

        let mut filter = vec![];
        filter.push(PartitionFilter::try_from(("date", "=", "2022-05-22"))?);

        let optimize = Optimize::default().filter(&filter).target_size(10_000_000);
        let metrics = optimize.execute(&mut dt).await?;
        assert_eq!(metrics.num_files_added, 1);
        assert_eq!(metrics.num_files_removed, 2);

        let metrics = optimize.execute(&mut dt).await?;

        assert_eq!(metrics.num_files_added, 0);

        Ok(())
    }
}
