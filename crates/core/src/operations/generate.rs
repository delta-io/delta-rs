//!
//! The generate supports the fairly simple "GENERATE" operation which produces a
//! [symlink_format_manifest](https://docs.delta.io/delta-utility/#generate-a-manifest-file) file
//! when needed for an external engine such as Presto or BigQuery.
//!
use bytes::{BufMut, BytesMut};
use futures::future::BoxFuture;
use std::sync::Arc;

use object_store::path::Path;
use tracing::log::*;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::{resolve_snapshot, EagerSnapshot};
use crate::logstore::object_store::PutPayload;
use crate::logstore::LogStoreRef;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable};

/// Simple builder to generate the manifest
#[derive(Clone)]
pub struct GenerateBuilder {
    /// A snapshot of the table state to be generated
    snapshot: Option<EagerSnapshot>,
    log_store: LogStoreRef,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl GenerateBuilder {
    /// Create a new [GenerateBuilder]
    ///
    /// Currently only one mode is supported: [GenerateMode::SymlinkFormatManifest]
    pub(crate) fn new(log_store: LogStoreRef, snapshot: Option<EagerSnapshot>) -> Self {
        Self {
            snapshot,
            log_store,
            custom_execute_handler: None,
        }
    }
}

impl super::Operation for GenerateBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl std::future::IntoFuture for GenerateBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;
        Box::pin(async move {
            let snapshot = resolve_snapshot(this.log_store(), this.snapshot.clone(), true).await?;
            let mut payload = BytesMut::new();

            for add in this
                .snapshot
                .clone()
                .expect("A GenerateBuilder with no snapshot is not a valid state!")
                .log_data()
                .into_iter()
            {
                let uri = this.log_store().to_uri(&add.object_store_path());
                trace!("Prepare {uri} for the symlink_format_manifest");
                payload.put(uri.as_bytes());
                payload.put_u8(b'\n');
            }
            debug!("Generate manifest {} bytes prepared", payload.len());
            let payload = PutPayload::from(payload.freeze());
            this.log_store()
                .object_store(None)
                .put(&Path::from("_symlink_format_manifest/manifest"), payload)
                .await?;
            Ok(DeltaTable::new_with_state(
                this.log_store().clone(),
                DeltaTableState::new(snapshot),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::kernel::schema::{DataType, PrimitiveType};
    use crate::kernel::Action;
    use crate::DeltaOps;

    use futures::StreamExt;

    #[tokio::test]
    async fn test_generate() -> DeltaResult<()> {
        use crate::kernel::Add;
        let actions = vec![Action::Add(Add {
            path: "some-files.parquet".into(),
            ..Default::default()
        })];
        let table = DeltaOps::new_in_memory()
            .create()
            .with_column("id", DataType::Primitive(PrimitiveType::Long), true, None)
            .with_actions(actions)
            .await?;

        let generate = GenerateBuilder::new(table.log_store(), table.state.map(|s| s.snapshot));
        let table = generate.await?;

        let store = table.log_store().object_store(None);
        let mut stream = store.list(None);
        let mut found = false;
        while let Some(meta) = stream.next().await.transpose().unwrap() {
            // Printing out the files so the failed assertion below will include the actual
            // contents of the table's prefix in the log
            println!("Name: {}, size: {}", meta.location, meta.size);
            if meta.location == Path::from("_symlink_format_manifest/manifest") {
                found = true;
                break;
            }
        }
        assert!(found, "The _symlink_format_manifest/manifest was not found in the Delta table's object store prefix");
        Ok(())
    }
}
