//! Implementation for writing delta log compaction.

use delta_kernel::snapshot::Snapshot as KernelSnapshot;

use object_store::ObjectStoreExt as _;
use object_store::path::Path;

use crate::kernel::{Snapshot, spawn_blocking_with_span};
use crate::logstore::{LogStore, with_operation};
use crate::protocol::to_rb;
use crate::{DeltaResult, DeltaTable, DeltaTableError};
use arrow_json::LineDelimitedWriter;

/// Write the compacted commit for `start_version..=end_version`.
///
/// The compacted file is written table-relative through `log_store.object_store()`, so inside an
/// operation scope it lands on the isolated write target and is published with the scope.
#[tracing::instrument(skip(log_store, snapshot), fields(operation = "log_compaction", start_version = start_version, end_version = end_version, table_uri = %log_store.root_url()))]
pub(crate) async fn compact_logs_for(
    start_version: u64,
    end_version: u64,
    log_store: &dyn LogStore,
    snapshot: &Snapshot,
) -> DeltaResult<()> {
    let engine = log_store.engine();

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

    // The kernel resolves the compaction path against the table root it was built for. Only the
    // file name is used here, so that the file is written relative to the write target.
    let lc_url = lc_writer.compaction_path();
    let file_name = lc_url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            DeltaTableError::Generic(format!("Invalid log compaction path: {lc_url}"))
        })?;
    let lc_path = Path::from_iter([log_store.log_path().as_ref(), file_name]);

    let mut lc_data = lc_writer.compaction_data(engine.as_ref())?;

    let store = log_store.object_store();

    let mut upload = store.put_multipart(&lc_path).await?;
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
///
/// The compaction runs inside its own operation scope, so on an isolating backend such as LakeFS
/// the compacted file lands on a transaction branch that is merged when the file is complete.
pub async fn compact_logs(
    table: &DeltaTable,
    start_version: u64,
    end_version: u64,
) -> DeltaResult<()> {
    let snapshot = table.snapshot()?.snapshot().snapshot().clone();
    with_operation(&table.log_store(), |log_store| async move {
        compact_logs_for(start_version, end_version, log_store.as_ref(), &snapshot).await
    })
    .await
}
