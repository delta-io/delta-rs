mod optimize {
    extern crate deltalake;

    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field},
        record_batch::RecordBatch,
    };
    use deltalake::optimize::{MetricDetails, Metrics};
    use deltalake::writer::DeltaWriterError;
    use deltalake::{
        action, get_backend_for_uri_with_options,
        optimize::Optimize,
        writer::{DeltaWriter, RecordBatchWriter},
        DeltaTableConfig, DeltaTableMetaData, PartitionFilter,
    };
    use deltalake::{DeltaTable, Schema, SchemaDataType, SchemaField};
    use rand::prelude::*;
    use serde_json::{Map, Value};
    use std::{collections::HashMap, error::Error, sync::Arc};
    use tempdir::TempDir;
    struct Context {
        pub tmp_dir: TempDir,
        pub table: DeltaTable,
    }

    async fn setup_test(partitioned: bool) -> Result<Context, Box<dyn Error>> {
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

        let p = if partitioned {
            vec!["date".to_owned()]
        } else {
            vec![]
        };

        let table_meta = DeltaTableMetaData::new(
            Some("opt_table".to_owned()),
            Some("Table for optimize tests".to_owned()),
            None,
            schema.clone(),
            p,
            HashMap::new(),
        );

        let tmp_dir = tempdir::TempDir::new("opt_table").unwrap();
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

    fn generate_random_batch<T: Into<String>>(
        rows: usize,
        partition: T,
    ) -> Result<RecordBatch, Box<dyn Error>> {
        let mut x_vec: Vec<i32> = Vec::with_capacity(rows);
        let mut y_vec: Vec<i32> = Vec::with_capacity(rows);
        let mut date_vec = Vec::with_capacity(rows);
        let mut rng = rand::thread_rng();
        let s = partition.into();

        for _ in 0..rows {
            x_vec.push(rng.gen());
            y_vec.push(rng.gen());
            date_vec.push(s.clone());
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

    fn tuples_to_batch<T: Into<String>>(
        tuples: Vec<(i32, i32)>,
        partition: T,
    ) -> Result<RecordBatch, Box<dyn Error>> {
        let mut x_vec: Vec<i32> = Vec::new();
        let mut y_vec: Vec<i32> = Vec::new();
        let mut date_vec = Vec::new();
        let s = partition.into();

        for t in tuples {
            x_vec.push(t.0);
            y_vec.push(t.1);
            date_vec.push(s.clone());
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
    async fn test_optimize_non_partitioned_table() -> Result<(), Box<dyn Error>> {
        let context = setup_test(false).await?;
        let mut dt = context.table;
        let mut writer = RecordBatchWriter::for_table(&dt, HashMap::new())?;

        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(2, 1), (2, 3), (2, 3)], "2022-05-23")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(3, 1), (3, 3), (3, 3)], "2022-05-22")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(4, 1), (4, 3), (4, 3)], "2022-05-23")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            generate_random_batch(records_for_size(4_000_000), "2022-05-22")?,
        )
        .await?;

        let version = dt.version;
        assert_eq!(dt.get_active_add_actions().len(), 5);

        let optimize = Optimize::default().target_size(2_000_00);
        let metrics = optimize.execute(&mut dt).await?;

        assert_eq!(version + 1, dt.version);
        assert_eq!(metrics.num_files_added, 1);
        assert_eq!(metrics.num_files_removed, 4);
        assert_eq!(metrics.total_considered_files, 5);
        assert_eq!(metrics.partitions_optimized, 1);
        assert_eq!(dt.get_active_add_actions().len(), 2);

        Ok(())
    }

    async fn write(
        writer: &mut RecordBatchWriter,
        mut table: &mut DeltaTable,
        batch: RecordBatch,
    ) -> Result<(), DeltaWriterError> {
        writer.write(batch).await?;
        writer.flush_and_commit(&mut table).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_optimize_with_partitions() -> Result<(), Box<dyn Error>> {
        let context = setup_test(true).await?;
        let mut dt = context.table;
        let mut writer = RecordBatchWriter::for_table(&dt, HashMap::new())?;

        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(1, 2), (1, 3), (1, 4)], "2022-05-22")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(2, 1), (2, 3), (2, 3)], "2022-05-23")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(3, 1), (3, 3), (3, 3)], "2022-05-22")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            tuples_to_batch(vec![(4, 1), (4, 3), (4, 3)], "2022-05-23")?,
        )
        .await?;

        let version = dt.version;
        let mut filter = vec![];
        filter.push(PartitionFilter::try_from(("date", "=", "2022-05-22"))?);

        let optimize = Optimize::default().filter(&filter);
        let metrics = optimize.execute(&mut dt).await?;

        assert_eq!(version + 1, dt.version);
        assert_eq!(metrics.num_files_added, 1);
        assert_eq!(metrics.num_files_removed, 2);
        assert_eq!(dt.get_active_add_actions().len(), 3);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    ///Validate that bin packing is idempotent.
    async fn test_idempotent() -> Result<(), Box<dyn Error>> {
        //TODO: Compression makes it hard to get the target file size...
        //Maybe just commit files with a known size
        let context = setup_test(true).await?;
        let mut dt = context.table;
        let mut writer = RecordBatchWriter::for_table(&dt, HashMap::new())?;

        write(
            &mut writer,
            &mut dt,
            generate_random_batch(records_for_size(6_000_000), "2022-05-22")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            generate_random_batch(records_for_size(9_000_000), "2022-05-22")?,
        )
        .await?;
        write(
            &mut writer,
            &mut dt,
            generate_random_batch(records_for_size(2_000_000), "2022-05-22")?,
        )
        .await?;

        let mut filter = vec![];
        filter.push(PartitionFilter::try_from(("date", "=", "2022-05-22"))?);

        let optimize = Optimize::default().filter(&filter).target_size(10_000_000);
        let metrics = optimize.execute(&mut dt).await?;
        assert_eq!(metrics.num_files_added, 1);
        assert_eq!(metrics.num_files_removed, 2);

        let metrics = optimize.execute(&mut dt).await?;

        assert_eq!(metrics.num_files_added, 0);
        assert_eq!(metrics.num_files_removed, 0);

        Ok(())
    }

    #[tokio::test]
    /// Validate Metrics when no files are optimized
    async fn test_idempotent_metrics() -> Result<(), Box<dyn Error>> {
        let context = setup_test(true).await?;
        let mut dt = context.table;
        let mut writer = RecordBatchWriter::for_table(&dt, HashMap::new())?;

        write(
            &mut writer,
            &mut dt,
            generate_random_batch(records_for_size(1_000_000), "2022-05-22")?,
        )
        .await?;

        let optimize = Optimize::default().target_size(10_000_000);
        let metrics = optimize.execute(&mut dt).await?;

        let mut expected_metric_details = MetricDetails::default();
        expected_metric_details.min = 0;
        expected_metric_details.max = 0;
        expected_metric_details.avg = 0.0;
        expected_metric_details.total_files = 0;
        expected_metric_details.total_size = 0;

        let mut expected = Metrics::default();
        expected.num_files_added = 0;
        expected.num_files_removed = 0;
        expected.partitions_optimized = 0;
        expected.num_batches = 0;
        expected.total_considered_files = 1;
        expected.total_files_skipped = 1;
        expected.preserve_insertion_order = true;
        expected.files_added = expected_metric_details.clone();
        expected.files_removed = expected_metric_details.clone();

        assert_eq!(expected, metrics);
        Ok(())
    }
}
