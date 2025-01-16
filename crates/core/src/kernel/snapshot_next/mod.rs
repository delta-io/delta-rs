//! Snapshot of a Delta Table at a specific version.
//!
use std::sync::Arc;

use arrow_array::RecordBatch;
use delta_kernel::actions::visitors::SetTransactionMap;
use delta_kernel::actions::{Add, Metadata, Protocol, SetTransaction};
use delta_kernel::expressions::{Scalar, StructData};
use delta_kernel::schema::Schema;
use delta_kernel::table_properties::TableProperties;
use delta_kernel::Version;
use iterators::{AddIterator, AddView, AddViewItem};
use url::Url;

use crate::{DeltaResult, DeltaTableError};

pub use eager::EagerSnapshot;
pub use lazy::LazySnapshot;

mod cache;
mod eager;
mod iterators;
mod lazy;

// TODO: avoid repetitive parsing of json stats

#[derive(thiserror::Error, Debug)]
enum SnapshotError {
    #[error("Tried accessing file data at snapshot initialized with no files.")]
    FilesNotInitialized,
}

impl From<SnapshotError> for DeltaTableError {
    fn from(e: SnapshotError) -> Self {
        match &e {
            SnapshotError::FilesNotInitialized => DeltaTableError::generic(e),
        }
    }
}

/// Helper trait to extract individual values from a `StructData`.
pub trait StructDataExt {
    fn get(&self, key: &str) -> Option<&Scalar>;
}

impl StructDataExt for StructData {
    fn get(&self, key: &str) -> Option<&Scalar> {
        self.fields()
            .iter()
            .zip(self.values().iter())
            .find(|(k, _)| k.name() == key)
            .map(|(_, v)| v)
    }
}

pub trait Snapshot {
    /// Location where the Delta Table (metadata) is stored.
    fn table_root(&self) -> &Url;

    /// Version of this `Snapshot` in the table.
    fn version(&self) -> Version;

    /// Table [`Schema`] at this `Snapshot`s version.
    fn schema(&self) -> &Schema;

    /// Table [`Metadata`] at this `Snapshot`s version.
    fn metadata(&self) -> &Metadata;

    /// Table [`Protocol`] at this `Snapshot`s version.
    fn protocol(&self) -> &Protocol;

    /// Get the [`TableProperties`] for this [`Snapshot`].
    fn table_properties(&self) -> &TableProperties;

    fn files(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>>;

    fn files_view(
        &self,
    ) -> DeltaResult<impl Iterator<Item = DeltaResult<impl Iterator<Item = AddViewItem>>>> {
        Ok(self.files()?.map(|r| r.and_then(|b| AddView::try_new(b))))
    }

    fn tombstones(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>>;

    /// Scan the Delta Log to obtain the latest transaction for all applications
    ///
    /// This method requires a full scan of the log to find all transactions.
    /// When a specific application id is requested, it is much more efficient to use
    /// [`application_transaction`](Self::application_transaction) instead.
    fn application_transactions(&self) -> DeltaResult<SetTransactionMap>;

    /// Scan the Delta Log for the latest transaction entry for a specific application.
    ///
    /// Initiates a log scan, but terminates as soon as the transaction
    /// for the given application is found.
    fn application_transaction(
        &self,
        app_id: impl AsRef<str>,
    ) -> DeltaResult<Option<SetTransaction>>;
}

impl<T: Snapshot> Snapshot for Arc<T> {
    fn table_root(&self) -> &Url {
        self.as_ref().table_root()
    }

    fn version(&self) -> Version {
        self.as_ref().version()
    }

    fn schema(&self) -> &Schema {
        self.as_ref().schema()
    }

    fn metadata(&self) -> &Metadata {
        self.as_ref().metadata()
    }

    fn protocol(&self) -> &Protocol {
        self.as_ref().protocol()
    }

    fn table_properties(&self) -> &TableProperties {
        self.as_ref().table_properties()
    }

    fn files(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        self.as_ref().files()
    }

    fn tombstones(&self) -> DeltaResult<impl Iterator<Item = DeltaResult<RecordBatch>>> {
        self.as_ref().tombstones()
    }

    fn application_transactions(&self) -> DeltaResult<SetTransactionMap> {
        self.as_ref().application_transactions()
    }

    fn application_transaction(
        &self,
        app_id: impl AsRef<str>,
    ) -> DeltaResult<Option<SetTransaction>> {
        self.as_ref().application_transaction(app_id)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    pub(super) fn get_dat_dir() -> PathBuf {
        let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut rep_root = d
            .parent()
            .and_then(|p| p.parent())
            .expect("valid directory")
            .to_path_buf();
        rep_root.push("dat/out/reader_tests/generated");
        rep_root
    }
}
