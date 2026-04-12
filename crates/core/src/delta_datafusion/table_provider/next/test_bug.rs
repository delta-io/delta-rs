use crate::delta_datafusion::DeltaScanConfig;
use crate::delta_datafusion::session::create_session;
use crate::test_utils::{TestResult, open_fs_path};
use arrow::array::{Date32Array, TimestampMillisecondArray};
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::{BinaryDictionaryBuilder, StringDictionaryBuilder};
use arrow_array::types::UInt16Type;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::dml::InsertOp;
use std::sync::Arc;

fn multi_partitioned_override_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "letter",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Utf8),
            ),
            true,
        ),
        ArrowField::new("date", ArrowDataType::Date32, true),
        ArrowField::new(
            "data",
            ArrowDataType::Dictionary(
                Box::new(ArrowDataType::UInt16),
                Box::new(ArrowDataType::Binary),
            ),
            true,
        ),
        ArrowField::new(
            "number",
            ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ]))
}

mod provider_schema_override {
    use super::*;

    async fn provider_for_partitioned_table() -> TestResult<(
        crate::DeltaTable,
        Arc<crate::delta_datafusion::table_provider::next::DeltaScan>,
    )> {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await.unwrap();

        let provider = crate::delta_datafusion::table_provider::next::DeltaScan::new(
            table.snapshot().unwrap().snapshot().clone(),
            DeltaScanConfig::default().with_schema(multi_partitioned_override_schema()),
        )?
        .with_log_store(table.log_store());

        Ok((table, Arc::new(provider)))
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_scan() -> TestResult {
        let (_table, provider) = provider_for_partitioned_table().await?;

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider).unwrap();

        let df = ctx.sql("SELECT number FROM test_table").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(
            batches[0].schema().fields()[0].data_type(),
            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None)
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_filter() -> TestResult {
        let (_table, provider) = provider_for_partitioned_table().await?;

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider).unwrap();

        let df = ctx
            .sql("SELECT number FROM test_table WHERE number < '2020-01-01T00:00:00Z'")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert_eq!(
            batches[0].schema().fields()[0].data_type(),
            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            "Filter output schema does not match logical schema"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_filter_aggregate() -> TestResult {
        let (_table, provider) = provider_for_partitioned_table().await?;

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider).unwrap();
        let query = "SELECT count(1), max(number) fake_ts FROM test_table WHERE letter != 'a' and number < '2020-01-01T00:00:00Z'";
        let df = ctx.sql(query).await.unwrap();
        let batches = df.collect().await.unwrap();
        datafusion::assert_batches_eq!(
            [
                "+-----------------+-------------------------+",
                "| count(Int64(1)) | fake_ts                 |",
                "+-----------------+-------------------------+",
                "| 2               | 1970-01-01T00:00:00.007 |",
                "+-----------------+-------------------------+",
            ],
            &batches
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_delta_scan_config_schema_override_insert() -> TestResult {
        use crate::operations::create::CreateBuilder;
        use delta_kernel::schema::{DataType, PrimitiveType, StructField, StructType};

        let (_partitioned_table, provider) = provider_for_partitioned_table().await?;
        let logical_schema = provider.schema();

        let table_dir = tempfile::tempdir()?;
        let table = CreateBuilder::new()
            .with_location(table_dir.path().to_str().unwrap())
            .with_columns(
                StructType::try_new(vec![
                    StructField::new(
                        "letter",
                        DataType::Primitive(PrimitiveType::String),
                        true,
                    ),
                    StructField::new("date", DataType::DATE, true),
                    StructField::new(
                        "data",
                        DataType::Primitive(PrimitiveType::Binary),
                        true,
                    ),
                    StructField::new(
                        "number",
                        DataType::Primitive(PrimitiveType::Long),
                        true,
                    ),
                ])?
                .fields()
                .cloned(),
            )
            .await?;

        let provider = Arc::new(
            crate::delta_datafusion::table_provider::next::DeltaScan::new(
                table.snapshot().unwrap().snapshot().clone(),
                DeltaScanConfig::default().with_schema(multi_partitioned_override_schema()),
            )?
            .with_log_store(table.log_store()),
        );

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider.clone()).unwrap();
        let state = ctx.state();

        let mut dict_builder = StringDictionaryBuilder::<UInt16Type>::new();
        dict_builder.append("a").unwrap();
        let mut bin_builder = BinaryDictionaryBuilder::<UInt16Type>::new();
        bin_builder.append(b"hello").unwrap();

        let batch = RecordBatch::try_new(
            logical_schema.clone(),
            vec![
                Arc::new(dict_builder.finish()),
                Arc::new(Date32Array::from(vec![0])),
                Arc::new(bin_builder.finish()),
                Arc::new(TimestampMillisecondArray::from(vec![2000])),
            ],
        )
        .unwrap();

        let mem_table = MemTable::try_new(logical_schema.clone(), vec![vec![batch]]).unwrap();
        let input = mem_table.scan(&state, None, &[], None).await.unwrap();

        let write_plan = provider
            .insert_into(&state, input, InsertOp::Append)
            .await
            .unwrap();

        let _ = datafusion::physical_plan::collect_partitioned(write_plan, ctx.task_ctx())
            .await
            .unwrap();

        Ok(())
    }
}

mod file_column_projection {
    use super::*;

    #[tokio::test]
    async fn test_delta_scan_config_file_column_projection() -> TestResult {
        let mut table =
            open_fs_path("../../dat/v0.0.3/reader_tests/generated/multi_partitioned/delta");
        table.load().await.unwrap();
        let provider = Arc::new(
            crate::delta_datafusion::table_provider::next::DeltaScan::new(
                table.snapshot().unwrap().snapshot().clone(),
                DeltaScanConfig::default()
                    .with_schema(multi_partitioned_override_schema())
                    .with_file_column_name("_file"),
            )?
            .with_log_store(table.log_store()),
        );

        let ctx = create_session().into_inner();
        ctx.register_table("test_table", provider).unwrap();

        let df = ctx
            .sql("SELECT * EXCEPT (_file) FROM test_table ORDER BY date")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        let all_fields = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            batches[0].schema().fields().len(),
            4,
            "Expected exactly 4 fields after projection, but got: {:?}",
            all_fields
        );
        assert!(
            !all_fields.contains(&"_file".to_string()),
            "The output must not contain _file column"
        );

        let df_agg = ctx
            .sql("SELECT date, substr(_file, 0, 9) as _file, count FROM (SELECT date, _file, count(1) as count FROM test_table GROUP BY date, _file)")
            .await
            .unwrap();
        let batches_agg = df_agg.collect().await.unwrap();

        let agg_fields = batches_agg[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            batches_agg[0].schema().fields().len(),
            3,
            "Expected exactly 3 fields after aggregation projection, but got: {:?}",
            agg_fields
        );
        assert!(
            agg_fields.contains(&"_file".to_string()),
            "The output must contain _file column"
        );

        let df_file = ctx.sql("SELECT data, _file FROM test_table").await.unwrap();
        let batches_file = df_file.collect().await.unwrap();

        let file_fields = batches_file[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            batches_file[0].schema().fields().len(),
            2,
            "Expected exactly 2 fields after projection, but got: {:?}",
            file_fields
        );
        assert!(file_fields.contains(&"_file".to_string()));

        Ok(())
    }
}
