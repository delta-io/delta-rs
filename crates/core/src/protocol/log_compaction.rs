//! Implementation for writing delta log compaction.

use delta_kernel::snapshot::Snapshot as KernelSnapshot;

use object_store::ObjectStore;
use object_store::path::Path;

use uuid::Uuid;

use crate::kernel::{Snapshot, spawn_blocking_with_span};
use crate::logstore::LogStore;
use crate::protocol::to_rb;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use arrow_json::LineDelimitedWriter;
use object_store::MultipartUpload;

#[tracing::instrument(skip(log_store, snapshot), fields(operation = "log_compaction", start_version = start_version, end_version = end_version, table_uri = %log_store.root_url()))]
pub(crate) async fn compact_logs_for(
    start_version: u64,
    end_version: u64,
    log_store: &dyn LogStore,
    operation_id: Option<Uuid>,
    snapshot: &Snapshot,
) -> DeltaResult<()> {
    let engine = log_store.engine(operation_id);

    let task_engine = engine.clone();

    if start_version >= end_version {
        return Err(DeltaTableError::Generic(format!(
            "Invalid version range: end_version {end_version} must be greater than start_version {start_version}"
        )));
    }
    let mut inner_snapshot = snapshot.inner.clone();

    if end_version > inner_snapshot.version() {
        inner_snapshot = spawn_blocking_with_span(move || {
            KernelSnapshot::builder_from(inner_snapshot)
                .at_version(end_version)
                .build(task_engine.as_ref())
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;
    }

    let mut lc_writer = inner_snapshot.log_compaction_writer(start_version, end_version)?;

    let lc_url = lc_writer.compaction_path();
    let lc_path = Path::from_url_path(lc_url.path())?;

    let mut lc_data = lc_writer.compaction_data(engine.as_ref())?;

    let root_store = log_store.root_object_store(operation_id);

    let mut upload = root_store.put_multipart(&lc_path).await?;
    let mut buffer = Vec::with_capacity(8 * 1024 * 1024);

    loop {
        let (current_batch, lc_data_next) = spawn_blocking_with_span(move || {
            let Some(first_batch) = lc_data.next() else {
                return Ok::<_, DeltaTableError>((None, lc_data));
            };
            Ok((Some(to_rb(first_batch?)?), lc_data))
        })
        .await
        .map_err(|e| DeltaTableError::Generic(e.to_string()))??;

        lc_data = lc_data_next;

        let Some(batch) = current_batch else {
            break;
        };

        let mut writer = LineDelimitedWriter::new(&mut buffer);
        writer.write(&batch)?;
        writer.finish()?;

        if buffer.len() >= 5 * 1024 * 1024 {
            upload.put_part(std::mem::take(&mut buffer).into()).await?;
        }
    }

    if !buffer.is_empty() {
        upload.put_part(buffer.into()).await?;
    }

    upload.complete().await?;

    Ok(())
}

/// Creates a log compaction file for a specified version range
pub async fn compact_logs(
    table: &DeltaTable,
    start_version: u64,
    end_version: u64,
    operation_id: Option<Uuid>,
) -> DeltaResult<()> {
    let snapshot = table.snapshot()?.snapshot().snapshot();
    let log_store = table.log_store();
    compact_logs_for(
        start_version,
        end_version,
        log_store.as_ref(),
        operation_id,
        snapshot,
    )
    .await?;
    Ok(())
}
