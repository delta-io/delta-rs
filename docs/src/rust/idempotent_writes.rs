use std::sync::Arc;
use url::Url;

use deltalake::arrow::array::{Float64Array, Int64Array, RecordBatch};
use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use deltalake::kernel::Transaction;
use deltalake::kernel::transaction::{CommitConflictError, CommitProperties, TransactionError};
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableError};

// --8<-- [start:idempotent_append]
const APP_ID: &str = "orders-loader";

/// Append `batch` unless this batch, or a later one, was already committed.
async fn append_batch(
    table: DeltaTable,
    batch_id: i64,
    batch: RecordBatch,
) -> Result<(DeltaTable, bool), DeltaTableError> {
    let last = table
        .snapshot()?
        .transaction_version(table.log_store().as_ref(), APP_ID)
        .await?;
    if last.is_some_and(|version| batch_id <= version) {
        return Ok((table, false));
    }
    // The write is validated against the snapshot that was used for the check.
    let table = table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .with_commit_properties(
            CommitProperties::default()
                .with_application_transaction(Transaction::new(APP_ID, batch_id)),
        )
        .await?;
    Ok((table, true))
}
// --8<-- [end:idempotent_append]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = ArrowSchema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
        ],
    )?;

    let table = deltalake::open_table(Url::from_directory_path("/tmp/orders").unwrap()).await?;
    let (table, written) = append_batch(table, 1, batch.clone()).await?;
    assert!(written);
    // Retrying the same batch is a no-op.
    let (mut table, written) = append_batch(table, 1, batch.clone()).await?;
    assert!(!written);

    // --8<-- [start:recover_from_conflict]
    let result = table
        .clone()
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Append)
        .with_commit_properties(
            CommitProperties::default().with_application_transaction(Transaction::new(APP_ID, 2)),
        )
        .await;
    match result {
        Ok(committed) => table = committed,
        Err(DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(CommitConflictError::ConcurrentTransaction),
        }) => {
            // Another writer committed a transaction for APP_ID after our snapshot
            // was taken. Refresh and check whether it already covered this batch.
            table.update_incremental(None).await?;
            let last = table
                .snapshot()?
                .transaction_version(table.log_store().as_ref(), APP_ID)
                .await?;
            if last.is_none_or(|version| version < 2) {
                return Err("batch 2 is still missing, retry the write".into());
            }
        }
        Err(err) => return Err(err.into()),
    }
    // --8<-- [end:recover_from_conflict]

    println!("table is at version {:?}", table.version());
    Ok(())
}
