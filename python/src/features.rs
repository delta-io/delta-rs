use deltalake::kernel::TableFeatures;
use pyo3::pyclass;

/// High level table features
#[pyclass]
#[derive(Clone)]
pub enum PyTableFeatures {
    /// Mapping of one column to another
    ColumnMapping,
    /// Deletion vectors for merge, update, delete
    DeletionVectors,
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
}

impl From<PyTableFeatures> for TableFeatures {
    fn from(value: PyTableFeatures) -> Self {
        match value {
            PyTableFeatures::ColumnMapping => TableFeatures::ColumnMapping,
            PyTableFeatures::DeletionVectors => TableFeatures::DeletionVectors,
            PyTableFeatures::TimestampWithoutTimezone => TableFeatures::TimestampWithoutTimezone,
            PyTableFeatures::V2Checkpoint => TableFeatures::V2Checkpoint,
            PyTableFeatures::AppendOnly => TableFeatures::AppendOnly,
            PyTableFeatures::Invariants => TableFeatures::Invariants,
            PyTableFeatures::CheckConstraints => TableFeatures::CheckConstraints,
            PyTableFeatures::ChangeDataFeed => TableFeatures::ChangeDataFeed,
            PyTableFeatures::GeneratedColumns => TableFeatures::GeneratedColumns,
            PyTableFeatures::IdentityColumns => TableFeatures::IdentityColumns,
            PyTableFeatures::RowTracking => TableFeatures::RowTracking,
            PyTableFeatures::DomainMetadata => TableFeatures::DomainMetadata,
            PyTableFeatures::IcebergCompatV1 => TableFeatures::IcebergCompatV1,
        }
    }
}
