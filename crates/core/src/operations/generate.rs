//!
//! The generate supports the fairly simple "GENERATE" operation which produces a
//! [symlink_format_manifest](https://docs.delta.io/delta-utility/#generate-a-manifest-file) file
//! when needed for an external engine such as Presto or BigQuery.
//!
//! The "symlink_format_manifest" is not something that has been well documented, but for
//! enon-partitioned tables this will generate a `_symlink_format_manifest/manifest` file next to
//! the `_delta_log`, for example:
//!
//! ```ignore
//! COVID-19_NYT
//! ├── _delta_log
//! │   ├── 00000000000000000000.crc
//! │   └── 00000000000000000000.json
//! ├── part-00000-a496f40c-e091-413a-85f9-b1b69d4b3b4e-c000.snappy.parquet
//! ├── part-00001-9d9d980b-c500-4f0b-bb96-771a515fbccc-c000.snappy.parquet
//! ├── part-00002-8826af84-73bd-49a6-a4b9-e39ffed9c15a-c000.snappy.parquet
//! ├── part-00003-539aff30-2349-4b0d-9726-c18630c6ad90-c000.snappy.parquet
//! ├── part-00004-1bb9c3e3-c5b0-4d60-8420-23261f58a5eb-c000.snappy.parquet
//! ├── part-00005-4d47f8ff-94db-4d32-806c-781a1cf123d2-c000.snappy.parquet
//! ├── part-00006-d0ec7722-b30c-4e1c-92cd-b4fe8d3bb954-c000.snappy.parquet
//! ├── part-00007-4582392f-9fc2-41b0-ba97-a74b3afc8239-c000.snappy.parquet
//! └── _symlink_format_manifest
//!     └── manifest
//! ```
//!
//! For partitioned tables, a `manifest` file will be generated inside a hive-style partitioned
//! tree structure, e.g.:
//!
//! ```ignore
//! delta-0.8.0-partitioned
//! ├── _delta_log
//! │   └── 00000000000000000000.json
//! ├── _symlink_format_manifest
//! │   ├── year=2020
//! │   │   ├── month=1
//! │   │   │   └── day=1
//! │   │   │       └── manifest
//! │   │   └── month=2
//! │   │       ├── day=3
//! │   │       │   └── manifest
//! │   │       └── day=5
//! │   │           └── manifest
//! │   └── year=2021
//! │       ├── month=12
//! │       │   ├── day=20
//! │       │   │   └── manifest
//! │       │   └── day=4
//! │       │       └── manifest
//! │       └── month=4
//! │           └── day=5
//! │               └── manifest
//! ├── year=2020
//! │   ├── month=1
//! │   │   └── day=1
//! │   │       └── part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet
//! │   └── month=2
//! │       ├── day=3
//! │       │   └── part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet
//! │       └── day=5
//! │           └── part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet
//! └── year=2021
//!     ├── month=12
//!     │   ├── day=20
//!     │   │   └── part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet
//!     │   └── day=4
//!     │       └── part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet
//!     └── month=4
//!         └── day=5
//!             └── part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet
//! ```
use bytes::{BufMut, BytesMut};
use futures::StreamExt as _;
use futures::future::BoxFuture;
use std::collections::HashMap;
use std::sync::Arc;

use object_store::path::{Path, PathPart};
use tracing::log::*;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::{EagerSnapshot, resolve_snapshot};
use crate::logstore::LogStoreRef;
use crate::logstore::object_store::PutPayload;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

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
            let snapshot =
                resolve_snapshot(this.log_store(), this.snapshot.clone(), true, None).await?;
            let mut payloads = HashMap::new();
            let manifest_part = PathPart::parse("manifest").expect("This is not possible");

            let mut file_stream = snapshot.file_views(&this.log_store, None);
            while let Some(add) = file_stream.next().await {
                let add = add?;
                let path = add.object_store_path();
                // The output_path is more or less the tree structure as the original file, just
                // inside the _symlink_format_manifest directory. This makes it easier to avoid
                // messing with partition values on the action
                let output_path = Path::from_iter(
                    std::iter::once(PathPart::parse("_symlink_format_manifest").map_err(|e| {
                        DeltaTableError::GenericError {
                            source: Box::new(e),
                        }
                    })?)
                    .chain(path.parts().filter(|p| path.filename() != Some(p.as_ref())))
                    .chain(std::iter::once(manifest_part.clone())),
                );
                trace!("Computed output path for add action: {output_path:?}");
                if !payloads.contains_key(&output_path) {
                    payloads.insert(output_path.clone(), BytesMut::new());
                }

                if let Some(payload) = payloads.get_mut(&output_path) {
                    let uri = this.log_store().to_uri(&path);
                    trace!("Prepare {uri} for the symlink_format_manifest");
                    payload.put(uri.as_bytes());
                    payload.put_u8(b'\n');
                }
            }
            debug!("Total of {} manifest files prepared", payloads.len());
            for (path, payload) in payloads.drain() {
                debug!(
                    "Generated manifest for {:?} is {} bytes",
                    path,
                    payload.len()
                );
                let payload = PutPayload::from(payload.freeze());
                this.log_store()
                    .object_store(None)
                    .put(&path, payload)
                    .await?;
            }
            Ok(DeltaTable::new_with_state(
                this.log_store().clone(),
                DeltaTableState::new(snapshot.clone()),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;

    use crate::DeltaTable;
    use crate::kernel::schema::{DataType, PrimitiveType};
    use crate::kernel::{Action, Add};

    #[tokio::test]
    async fn test_generate() -> DeltaResult<()> {
        let actions = vec![Action::Add(Add {
            path: "some-files.parquet".into(),
            ..Default::default()
        })];
        let table = DeltaTable::new_in_memory()
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
        assert!(
            found,
            "The _symlink_format_manifest/manifest was not found in the Delta table's object store prefix"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_generate_with_partitions() -> DeltaResult<()> {
        use crate::kernel::Add;
        let actions = vec![Action::Add(Add {
            path: "locale=us/some-files.parquet".into(),
            partition_values: HashMap::from([("locale".to_string(), Some("us".to_string()))]),
            ..Default::default()
        })];
        let table = DeltaTable::new_in_memory()
            .create()
            .with_column("id", DataType::Primitive(PrimitiveType::Long), true, None)
            .with_column(
                "locale",
                DataType::Primitive(PrimitiveType::String),
                true,
                None,
            )
            .with_partition_columns(vec!["locale"])
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
            if meta.location == Path::from("_symlink_format_manifest/locale=us/manifest") {
                found = true;
                break;
            }
            assert_ne!(
                meta.location,
                Path::from("_symlink_format_manifest/manifest"),
                "The 'root' manifest file is not expected in a partitioned table!"
            );
        }
        assert!(
            found,
            "The _symlink_format_manifest/manifest was not found in the Delta table's object store prefix"
        );
        Ok(())
    }
}
