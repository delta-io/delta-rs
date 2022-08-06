#[cfg(feature = "s3")]
#[allow(dead_code)]
mod s3_common;

#[cfg(feature = "datafusion-ext")]
mod datafusion {
    use std::{collections::HashSet, sync::Arc};

    use arrow::{
        array::*,
        datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };
    use datafusion::datasource::TableProvider;
    use datafusion::error::Result;
    use datafusion::execution::context::{SessionContext, TaskContext};
    use datafusion::logical_expr::Expr;
    use datafusion::logical_plan::Column;
    use datafusion::physical_plan::{coalesce_partitions::CoalescePartitionsExec, ExecutionPlan};
    use datafusion::scalar::ScalarValue;
    use deltalake::{action::SaveMode, operations::DeltaCommands, DeltaTableMetaData};
    use std::collections::HashMap;

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

    #[tokio::test]
    async fn test_files_scanned() -> Result<()> {
        let table_dir = tempfile::tempdir().unwrap();
        let table_path = table_dir.path();
        let table_uri = table_path.to_str().unwrap().to_string();

        let mut commands = DeltaCommands::try_from_uri(table_uri.clone())
            .await
            .unwrap();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("string", ArrowDataType::Utf8, true),
        ]));

        let table_schema = arrow_schema.clone().try_into()?;
        let metadata =
            DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());

        let _ = commands
            .create(metadata.clone(), SaveMode::Ignore)
            .await
            .unwrap();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
            Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
        ];
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;
        let _ = commands
            .write(vec![batch], SaveMode::Overwrite, None)
            .await
            .unwrap();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
            Arc::new(StringArray::from(vec![Some("hello"), Some("world")])),
        ];
        let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;
        let _ = commands
            .write(vec![batch], SaveMode::Append, None)
            .await
            .unwrap();

        let table = Arc::new(deltalake::open_table(&table_uri).await?);
        assert_eq!(table.version(), 2);

        let ctx = SessionContext::new();
        let plan = table.scan(&ctx.state(), &None, &[], None).await?;
        let plan = CoalescePartitionsExec::new(plan.clone());

        let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
        let stream = plan.execute(0, task_ctx)?;
        let _result = datafusion::physical_plan::common::collect(stream).await?;

        let files_scanned = plan.children()[0]
            .metrics()
            .unwrap()
            .clone()
            .iter()
            .cloned()
            .flat_map(|m| m.labels().to_vec())
            .collect::<HashSet<_>>();

        assert!(files_scanned.len() == 2);

        let filter = Expr::gt(
            Expr::Column(Column::from_name("id")),
            Expr::Literal(ScalarValue::Int32(Some(5))),
        );

        let plan = table.scan(&ctx.state(), &None, &[filter], None).await?;
        let plan = CoalescePartitionsExec::new(plan.clone());
        let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
        let stream = plan.execute(0, task_ctx)?;
        let _result = datafusion::physical_plan::common::collect(stream).await?;

        let files_scanned = plan.children()[0]
            .metrics()
            .unwrap()
            .clone()
            .iter()
            .cloned()
            .flat_map(|m| m.labels().to_vec())
            .collect::<HashSet<_>>();

        assert!(files_scanned.len() == 1);

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
