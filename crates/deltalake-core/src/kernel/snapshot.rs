//! Snapshot of a Delta table.

use crate::kernel::error::DeltaResult;
use crate::kernel::{Add, Metadata, Protocol, StructType};
use crate::table::config::TableConfig;

/// A snapshot of a Delta table at a given version.
pub trait Snapshot: std::fmt::Display + Send + Sync + std::fmt::Debug + 'static {
    /// The version of the table at this [`Snapshot`].
    fn version(&self) -> i64;

    /// Table [`Schema`](crate::kernel::schema::StructType) at this [`Snapshot`]'s version.
    fn schema(&self) -> Option<&StructType>;

    /// Table [`Metadata`] at this [`Snapshot`]'s version.
    fn metadata(&self) -> DeltaResult<Metadata>;

    /// Table [`Protocol`] at this [`Snapshot`]'s version.
    fn protocol(&self) -> DeltaResult<Protocol>;

    /// Iterator over the [`Add`] actions at this [`Snapshot`]'s version.
    fn files(&self) -> DeltaResult<Box<dyn Iterator<Item = Add> + Send + '_>>;

    /// Well known table [configuration](crate::table::config::TableConfig).
    fn table_config(&self) -> TableConfig<'_>;
}
