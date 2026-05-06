use deltalake::kernel::TableFeatures as KernelTableFeatures;
use pyo3::pyclass;

/// High level table features
#[pyclass(eq, eq_int, from_py_object)]
#[derive(Clone, PartialEq)]
pub enum TableFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
    #[cfg(feature = "nanosecond-timestamps")]
    /// nanosecond-resolution timestamps
    TimestampNanos,
    /// timestamps without timezone support
    TimestampWithoutTimezone,
    /// version 2 of checkpointing
    V2Checkpoint,
    /// Append Only Tables
    AppendOnly,
    /// Table invariants
    Invariants,
    /// Check constraints on columns
    CheckConstraints,
    /// CDF on a table
    ChangeDataFeed,
    /// Columns with generated values
    GeneratedColumns,
    /// ID Columns
    IdentityColumns,
    /// Row tracking on tables
    RowTracking,
    /// domain specific metadata
    DomainMetadata,
    /// Iceberg compatibility support
    IcebergCompatV1,
    /// Variant type support
    VariantType,
    /// Preview variant type support
    VariantTypePreview,
}

impl From<TableFeatures> for KernelTableFeatures {
    fn from(value: TableFeatures) -> Self {
        match value {
            TableFeatures::ColumnMapping => KernelTableFeatures::ColumnMapping,
            TableFeatures::DeletionVectors => KernelTableFeatures::DeletionVectors,
            #[cfg(feature = "nanosecond-timestamps")]
            TableFeatures::TimestampNanos => KernelTableFeatures::TimestampNanos,
            TableFeatures::TimestampWithoutTimezone => {
                KernelTableFeatures::TimestampWithoutTimezone
            }
            TableFeatures::V2Checkpoint => KernelTableFeatures::V2Checkpoint,
            TableFeatures::AppendOnly => KernelTableFeatures::AppendOnly,
            TableFeatures::Invariants => KernelTableFeatures::Invariants,
            TableFeatures::CheckConstraints => KernelTableFeatures::CheckConstraints,
            TableFeatures::ChangeDataFeed => KernelTableFeatures::ChangeDataFeed,
            TableFeatures::GeneratedColumns => KernelTableFeatures::GeneratedColumns,
            TableFeatures::IdentityColumns => KernelTableFeatures::IdentityColumns,
            TableFeatures::RowTracking => KernelTableFeatures::RowTracking,
            TableFeatures::DomainMetadata => KernelTableFeatures::DomainMetadata,
            TableFeatures::IcebergCompatV1 => KernelTableFeatures::IcebergCompatV1,
            TableFeatures::VariantType => KernelTableFeatures::VariantType,
            TableFeatures::VariantTypePreview => KernelTableFeatures::VariantTypePreview,
        }
    }
}
