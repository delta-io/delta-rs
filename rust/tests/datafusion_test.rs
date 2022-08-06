#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[cfg(feature = "datafusion-ext")]
mod datafusion {
    use std::sync::Arc;

    use arrow::array::*;
    use datafusion::error::Result;
    use datafusion::execution::context::SessionContext;
    use datafusion::scalar::ScalarValue;

    #[tokio::test]
    async fn test_datafusion_simple_query() -> Result<()> {
        let ctx = SessionContext::new();
        let table = deltalake::open_table("./tests/data/simple_table")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT id FROM demo WHERE id > 5 ORDER BY id ASC")
            .await?
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
    async fn test_datafusion_simple_query_partitioned() -> Result<()> {
        let ctx = SessionContext::new();
        let table = deltalake::open_table("./tests/data/delta-0.8.0-partitioned")
            .await
            .unwrap();
        ctx.register_table("demo", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT CAST( day as int ) FROM demo WHERE CAST( year as int ) > 2020 ORDER BY CAST( day as int ) ASC")
            .await?
            .collect()
            .await?;

        let batch = &batches[0];

        assert_eq!(
            batch.column(0).as_ref(),
            Arc::new(Int32Array::from(vec![4, 5, 20, 20])).as_ref(),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_date_column() -> Result<()> {
        let ctx = SessionContext::new();
        let table = deltalake::open_table("./tests/data/delta-0.8.0-date")
            .await
            .unwrap();
        ctx.register_table("dates", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT date from dates WHERE \"dayOfYear\" = 2")
            .await?
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
        let statistics = table.datafusion_table_statistics();

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

        let ctx = SessionContext::new();
        ctx.register_table("test_table", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT max(value), min(value) FROM test_table")
            .await?
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

    #[cfg(feature = "s3")]
    mod s3 {
        use super::*;
        use crate::s3_common::setup;
        use deltalake::s3_storage_options;
        use deltalake::storage;
        use dynamodb_lock::dynamo_lock_options;
        use maplit::hashmap;
        use serial_test::serial;

        #[tokio::test]
        #[serial]
        async fn test_datafusion_simple_query() -> Result<()> {
            setup();

            // Use the manual options API so we have some basic integrationcoverage.
            let table_uri = "s3://deltars/simple";
            let storage = storage::get_backend_for_uri_with_options(
                table_uri,
                hashmap! {
                    s3_storage_options::AWS_REGION.to_string() => "us-east-2".to_string(),
                    dynamo_lock_options::DYNAMO_LOCK_OWNER_NAME.to_string() => "s3::deltars/simple".to_string(),
                },
            )
            .unwrap();
            let mut table = deltalake::DeltaTable::new(
                table_uri,
                storage,
                deltalake::DeltaTableConfig::default(),
            )
            .unwrap();
            table.load().await.unwrap();

            let ctx = SessionContext::new();
            ctx.register_table("demo", Arc::new(table))?;

            let batches = ctx
                .sql("SELECT id FROM demo WHERE id > 5 ORDER BY id ASC")
                .await?
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
    }
}
