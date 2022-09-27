//! High level delta commands that can be executed against a delta table

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
pub struct DeltaOps(DeltaTable);

impl DeltaOps {
    /// load table from uri
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
    pub fn create(self) -> CreateBuilder {
        CreateBuilder::default().with_object_store(self.0.object_store())
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion-ext")]
    pub fn load(self) -> LoadBuilder {
        LoadBuilder::default().with_object_store(self.0.object_store())
    }

    /// Write data to Delta table
    #[cfg(feature = "datafusion-ext")]
    pub fn write(self, batches: Vec<RecordBatch>) -> WriteBuilder {
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
