// Tests handling of min/max statistics for timestamp columns,
// which must be truncated to millisecond precision according to the Delta Protocol.

use arrow_array::{ArrayRef, RecordBatch, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::prelude::SessionContext;
use deltalake_core::delta_datafusion::create_session;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError};
use futures::TryStreamExt;
use std::sync::Arc;

const START_TIMESTAMP_US: i64 = 1_704_067_200 * 1_000_000; // 2024-01-01 00:00:00 UTC
const START_TIMESTAMP_NS: i64 = START_TIMESTAMP_US * 1_000;
const DAY_US: i64 = 60 * 60 * 24 * 1_000_000;
const DAY_NS: i64 = DAY_US * 1_000;

fn timestamp_nanos_array(values: Vec<i64>, timezone: Option<&str>) -> ArrayRef {
    let array = TimestampNanosecondArray::from(values);
    let array = match timezone {
        Some(tz) => array.with_timezone(tz),
        None => array,
    };
    Arc::new(array)
}

fn timestamp_micros_array(values: Vec<i64>, timezone: Option<&str>) -> ArrayRef {
    let array = TimestampMicrosecondArray::from(values);
    let array = match timezone {
        Some(tz) => array.with_timezone(tz),
        None => array,
    };
    Arc::new(array)
}

fn get_timestamp_statistic(
    statistics: Option<delta_kernel::expressions::Scalar>,
    timezone: Option<&str>,
) -> DeltaResult<i64> {
    use delta_kernel::expressions::Scalar;

    let statistics =
        statistics.ok_or_else(|| DeltaTableError::generic("missing timestamp statistics"))?;
    match statistics {
        Scalar::Struct(data) => match (&data.values()[0], timezone) {
            #[cfg(feature = "nanosecond-timestamps")]
            (Scalar::TimestampNanos(v), Some(_)) => Ok(*v),
            #[cfg(feature = "nanosecond-timestamps")]
            (Scalar::TimestampNanosNtz(v), None) => Ok(*v),
            (Scalar::Timestamp(v), Some(_)) => Ok(*v),
            (Scalar::TimestampNtz(v), None) => Ok(*v),
            (other, _) => Err(DeltaTableError::generic(format!(
                "Unexpected scalar for timestamp column: {other:?}"
            ))),
        },
        other => Err(DeltaTableError::generic(format!(
            "Expected struct statistics, got {other:?}"
        ))),
    }
}

async fn count_where(ctx: &SessionContext, predicate: &str) -> DeltaResult<usize> {
    let batches = ctx
        .sql(&format!("select * from test where {predicate}"))
        .await?
        .collect()
        .await?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

#[tokio::test]
async fn stats_truncation_micros() -> DeltaResult<()> {
    stats_truncation_test(Some("UTC"), false).await
}

#[tokio::test]
async fn stats_truncation_micros_ntz() -> DeltaResult<()> {
    stats_truncation_test(None, false).await
}

#[cfg(feature = "nanosecond-timestamps")]
#[tokio::test]
async fn stats_truncation_nanos() -> DeltaResult<()> {
    stats_truncation_test(Some("UTC"), true).await
}

#[cfg(feature = "nanosecond-timestamps")]
#[tokio::test]
async fn stats_truncation_nanos_ntz() -> DeltaResult<()> {
    stats_truncation_test(None, true).await
}

async fn stats_truncation_test(timezone: Option<&str>, nanos: bool) -> DeltaResult<()> {
    let unit = if nanos {
        TimeUnit::Nanosecond
    } else {
        TimeUnit::Microsecond
    };
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "timestamp",
        DataType::Timestamp(unit, timezone.map(Arc::from)),
        true,
    )]));

    let (start, day_count) = if nanos {
        (START_TIMESTAMP_NS, DAY_NS)
    } else {
        (START_TIMESTAMP_US, DAY_US)
    };

    let make_array = |values: Vec<i64>, timezone: Option<&str>| {
        if nanos {
            timestamp_nanos_array(values, timezone)
        } else {
            timestamp_micros_array(values, timezone)
        }
    };

    let batch0 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![make_array(
            vec![
                start,
                start + 123,
                start + if nanos { 123456789 } else { 123456 },
            ],
            timezone,
        )],
    )?;

    let table: DeltaTable = DeltaTable::new_in_memory().write(vec![batch0]).await?;

    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![make_array(
            vec![
                start + day_count,
                start + day_count + 123,
                start + day_count + if nanos { 123456 } else { 456 },
            ],
            timezone,
        )],
    )?;
    let table = table.write(vec![batch1]).await?;
    assert_eq!(table.version(), Some(1));

    let actions = table
        .get_active_add_actions_by_partitions(&[])
        .try_collect::<Vec<_>>()
        .await?;

    assert_eq!(actions.len(), 2);
    let mut batch0_seen = false;
    let mut batch1_seen = false;
    for action in actions {
        let min = get_timestamp_statistic(action.min_values(), timezone)?;
        let max = get_timestamp_statistic(action.max_values(), timezone)?;

        if min == start {
            batch0_seen = true;
            // The maximum is truncated down to have a `.123` millisecond
            // component when written to JSON stats, then rounded back up by a
            // millisecond on read so it remains a valid upper bound.
            let expected_max = if nanos {
                start + 124_000_000
            } else {
                start + 124_000
            };
            assert_eq!(max, expected_max);
        } else if min == start + day_count {
            batch1_seen = true;
            // The maximum truncates to value with no millisecond component
            // and is rounded up by a millisecond.
            let expected_max = if nanos {
                start + day_count + 1_000_000
            } else {
                start + day_count + 1_000
            };
            assert_eq!(max, expected_max);
        } else {
            return Err(DeltaTableError::generic(format!(
                "Unexpected min value for an add action: {min}"
            )));
        }
    }
    assert!(batch0_seen && batch1_seen);

    // Verify that no rows are incorrectly skipped due to statistics truncation.
    let day0 = match timezone {
        Some(_) => "'2024-01-01T00:00:00Z'",
        None => "'2024-01-01T00:00:00'",
    };
    let day1 = match timezone {
        Some(_) => "'2024-01-02T00:00:00Z'",
        None => "'2024-01-02T00:00:00'",
    };

    let ctx: SessionContext = create_session().into();
    table.update_datafusion_session(&ctx.state())?;
    ctx.register_table("test", table.table_provider().await?)?;

    // Excludes the first row of `batch0`
    assert_eq!(count_where(&ctx, &format!("timestamp > {day0}")).await?, 5);
    // Excludes all of `batch0` and the first row of `batch1`
    assert_eq!(count_where(&ctx, &format!("timestamp > {day1}")).await?, 2);
    // Includes all of `batch0` and the first row of `batch1`
    assert_eq!(count_where(&ctx, &format!("timestamp <= {day1}")).await?, 4);

    Ok(())
}
