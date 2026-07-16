// Test basic operations on a table that uses the nanosecond-timestamps feature.
//
// Two scenarios are covered:
//   * `timestamp_nanos` columns (Arrow `Timestamp(Nanosecond, Some("UTC"))`),
//   * `timestamp_nanos_ntz` columns (Arrow `Timestamp(Nanosecond, None)`).

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::{RecordBatch, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::create_session;
use deltalake_core::{DeltaResult, DeltaTable};
use rstest::rstest;
use std::sync::Arc;

const START_TIMESTAMP_NS: i64 = 1_704_067_200 * 1_000_000_000; // 2024-01-01 00:00:00 UTC
const DAY_NS: i64 = 60 * 60 * 24 * 1_000_000_000;

fn timestamp_array(values: Vec<i64>, timezone: Option<&str>) -> Arc<TimestampNanosecondArray> {
    let array = TimestampNanosecondArray::from(values);
    let array = match timezone {
        Some(tz) => array.with_timezone(tz),
        None => array,
    };
    Arc::new(array)
}

async fn setup_table(timezone: Option<&str>) -> DeltaResult<(DeltaTable, Arc<Schema>)> {
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Nanosecond, timezone.map(Arc::from)),
        true,
    )]));

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![timestamp_array(
            vec![
                START_TIMESTAMP_NS,
                START_TIMESTAMP_NS + DAY_NS,
                START_TIMESTAMP_NS + 2 * DAY_NS,
            ],
            timezone,
        )],
    )?;

    let table: DeltaTable = DeltaTable::new_in_memory().write(vec![batch]).await?;

    Ok((table, arrow_schema))
}

async fn get_data(table: DeltaTable) -> Vec<RecordBatch> {
    let ctx: SessionContext = create_session().into();
    table.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("test", table.table_provider().await.unwrap())
        .unwrap();
    ctx.sql("select * from test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

#[rstest]
#[case::tz(Some("UTC"))]
#[case::ntz(None)]
#[tokio::test]
async fn read_timestamp_nanos_table(#[case] timezone: Option<&str>) -> DeltaResult<()> {
    let (table, _) = setup_table(timezone).await?;
    let batches = get_data(table).await;

    let schema = batches[0].schema();
    let timestamp_field = schema.field_with_name("timestamp")?;
    assert_eq!(
        timestamp_field.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, timezone.map(Arc::from)),
    );

    let mut values: Vec<i64> = Vec::new();
    for batch in batches {
        let nanos_column = batch.column(0).as_primitive::<TimestampNanosecondType>();
        for i in 0..batch.num_rows() {
            values.push(nanos_column.value(i));
        }
    }
    values.sort();
    assert_eq!(
        values,
        vec![
            START_TIMESTAMP_NS,
            START_TIMESTAMP_NS + DAY_NS,
            START_TIMESTAMP_NS + 2 * DAY_NS,
        ]
    );

    Ok(())
}

#[rstest]
#[case::tz(Some("UTC"))]
#[case::ntz(None)]
#[tokio::test]
async fn append_timestamp_nanos(#[case] timezone: Option<&str>) -> DeltaResult<()> {
    let (table, arrow_schema) = setup_table(timezone).await?;

    let new_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![timestamp_array(
            vec![
                START_TIMESTAMP_NS + 3 * DAY_NS,
                START_TIMESTAMP_NS + 4 * DAY_NS,
            ],
            timezone,
        )],
    )?;
    let table = table.write(vec![new_batch]).await?;
    assert_eq!(table.version(), Some(1));

    Ok(())
}

#[rstest]
#[case::tz(Some("UTC"))]
#[case::ntz(None)]
#[tokio::test]
async fn delete_with_timestamp_nanos_predicate(#[case] timezone: Option<&str>) -> DeltaResult<()> {
    let (table, _) = setup_table(timezone).await?;

    let predicate = match timezone {
        Some(_) => "timestamp >= '2024-01-02T00:00:00Z'",
        None => "timestamp >= '2024-01-02T00:00:00'",
    };

    let (table, metrics) = table.delete().with_predicate(predicate).await?;
    assert_eq!(table.version(), Some(1));
    assert!(metrics.num_deleted_rows.is_some_and(|r| r == 2));

    Ok(())
}

#[rstest]
#[case::tz(Some("UTC"))]
#[case::ntz(None)]
#[tokio::test]
async fn update_with_timestamp_nanos(#[case] timezone: Option<&str>) -> DeltaResult<()> {
    let (table, _) = setup_table(timezone).await?;

    let new_timestamp_ns: i64 = START_TIMESTAMP_NS + 10 * DAY_NS + 123_456_789;

    let update_value = match timezone {
        Some(_) => "'2024-01-11T00:00:00.123456789Z'",
        None => "'2024-01-11T00:00:00.123456789'",
    };

    let predicate = match timezone {
        Some(_) => "timestamp >= '2024-01-02T00:00:00Z'",
        None => "timestamp >= '2024-01-02T00:00:00'",
    };

    let (table, metrics) = table
        .update()
        .with_predicate(predicate)
        .with_update("timestamp", update_value)
        .await?;
    assert_eq!(table.version(), Some(1));
    assert_eq!(metrics.num_updated_rows, 2);

    let batches = get_data(table).await;
    let mut values: Vec<i64> = Vec::new();
    for batch in batches {
        let nanos_column = batch.column(0).as_primitive::<TimestampNanosecondType>();
        for i in 0..batch.num_rows() {
            values.push(nanos_column.value(i));
        }
    }
    values.sort();
    assert_eq!(
        values,
        vec![START_TIMESTAMP_NS, new_timestamp_ns, new_timestamp_ns]
    );

    Ok(())
}

#[rstest]
#[case::tz(Some("UTC"))]
#[case::ntz(None)]
#[tokio::test]
async fn merge_with_timestamp_nanos(#[case] timezone: Option<&str>) -> DeltaResult<()> {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit};

    let (table, arrow_schema) = setup_table(timezone).await?;

    let matched_update_ns: i64 = START_TIMESTAMP_NS + 14 * DAY_NS + 987_654_321;
    let inserted_ns: i64 = START_TIMESTAMP_NS + 4 * DAY_NS + 123_456_789;

    // Source row 1 matches a target row, row 2 does not.
    let source_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![timestamp_array(
            vec![START_TIMESTAMP_NS + DAY_NS, inserted_ns],
            timezone,
        )],
    )?;

    let ctx: SessionContext = create_session().into();
    let source = ctx.read_batch(source_batch)?;

    let (table, metrics) = table
        .merge(source, col("target.timestamp").eq(col("source.timestamp")))
        .with_source_alias("source")
        .with_target_alias("target")
        .when_matched_update(|update| {
            update.update(
                "timestamp",
                lit(ScalarValue::TimestampNanosecond(
                    Some(matched_update_ns),
                    timezone.map(Arc::from),
                )),
            )
        })?
        .when_not_matched_insert(|insert| insert.set("timestamp", col("source.timestamp")))?
        .await?;

    assert_eq!(table.version(), Some(1));
    assert_eq!(metrics.num_target_rows_updated, 1);
    assert_eq!(metrics.num_target_rows_inserted, 1);

    let batches = get_data(table).await;
    let mut values: Vec<i64> = Vec::new();
    for batch in batches {
        let nanos_column = batch.column(0).as_primitive::<TimestampNanosecondType>();
        for i in 0..batch.num_rows() {
            values.push(nanos_column.value(i));
        }
    }
    values.sort();
    assert_eq!(
        values,
        vec![
            START_TIMESTAMP_NS,
            START_TIMESTAMP_NS + 2 * DAY_NS,
            inserted_ns,
            matched_update_ns,
        ]
    );

    Ok(())
}
