//! High level operations API to interact with Delta tables
//!
//! At the heart of the high level operations APIs is the [`DeltaOps`] struct,
//! which consumes a [`DeltaTable`] and exposes methods to attain builders for
//! several high level operations. The specific builder structs allow fine-tuning
//! the operations' behaviors and will return an updated table potentially in conjunction
//! with a [data stream][datafusion::physical_plan::SendableRecordBatchStream],
//! if the operation returns data as well.

use self::create::CreateBuilder;
use crate::builder::DeltaTableBuilder;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

pub mod create;
pub mod transaction;

#[cfg(feature = "datafusion-ext")]
use self::{load::LoadBuilder, write::WriteBuilder};
#[cfg(feature = "datafusion-ext")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "datafusion-ext")]
pub use datafusion::physical_plan::common::collect as collect_sendable_stream;

#[cfg(feature = "datafusion-ext")]
mod load;
#[cfg(feature = "datafusion-ext")]
pub mod write;
// TODO the writer module does not actually depend on datafusion,
// eventually we should consolidate with the record batch writer
#[cfg(feature = "datafusion-ext")]
mod writer;

/// Maximum supported writer version
pub const MAX_SUPPORTED_WRITER_VERSION: i32 = 1;
/// Maximum supported reader version
pub const MAX_SUPPORTED_READER_VERSION: i32 = 1;

/// High level interface for executing commands against a DeltaTable
pub struct DeltaOps(pub DeltaTable);

impl DeltaOps {
    /// Create a new [`DeltaOps`] instance, operating on [`DeltaTable`] at given uri.
    ///
    /// ```
    /// use deltalake::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    /// };
    /// ```
    pub async fn try_from_uri(uri: impl AsRef<str>) -> DeltaResult<Self> {
        let mut table = DeltaTableBuilder::from_uri(uri).build()?;
        // We allow for uninitialized locations, since we may want to create the table
        match table.load().await {
            Ok(_) => Ok(table.into()),
            Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
            Err(err) => Err(err),
        }
    }

    /// Create a new [`DeltaOps`] instance, backed by an un-initialized in memory table
    ///
    /// Using this will not persist any changes beyond the lifetime of the table object.
    /// The main purpose of in-memory tables is for use in testing.
    ///
    /// ```
    /// use deltalake::DeltaOps;
    ///
    /// let ops = DeltaOps::new_in_memory();
    /// ```
    #[must_use]
    pub fn new_in_memory() -> Self {
        DeltaTableBuilder::from_uri("memory://")
            .build()
            .unwrap()
            .into()
    }

    /// Create a new Delta table
    ///
    /// ```
    /// use deltalake::DeltaOps;
    ///
    /// async {
    ///     let ops = DeltaOps::try_from_uri("memory://").await.unwrap();
    ///     let table = ops.create().with_table_name("my_table").await.unwrap();
    ///     assert_eq!(table.version(), 0);
    /// };
    /// ```
    #[must_use]
    pub fn create(self) -> CreateBuilder {
        CreateBuilder::default().with_object_store(self.0.object_store())
    }

    /// Load data from a DeltaTable
    #[cfg(feature = "datafusion-ext")]
    #[must_use]
    pub fn load(self) -> LoadBuilder {
        LoadBuilder::default().with_object_store(self.0.object_store())
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion-ext")]
    #[must_use]
    pub fn write(self, batches: impl IntoIterator<Item = RecordBatch>) -> WriteBuilder {
        WriteBuilder::default()
            .with_input_batches(batches)
            .with_object_store(self.0.object_store())
    }
}

impl From<DeltaTable> for DeltaOps {
    fn from(table: DeltaTable) -> Self {
        Self(table)
    }
}

impl From<DeltaOps> for DeltaTable {
    fn from(ops: DeltaOps) -> Self {
        ops.0
    }
}

impl AsRef<DeltaTable> for DeltaOps {
    fn as_ref(&self) -> &DeltaTable {
        &self.0
    }
}
