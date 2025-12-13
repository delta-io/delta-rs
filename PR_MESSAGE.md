# Add Delta Lake Protocol V2 Features Support

## Summary
This PR adds comprehensive support for Delta Lake Protocol V2 features including Deletion Vectors, Row Tracking, Liquid Clustering, and V2 Checkpoints to delta-rs.

## Motivation
Delta Lake Protocol V2 introduces critical features for modern data lake operations:
- **Deletion Vectors**: Efficient row-level deletes without rewriting entire files
- **Row Tracking**: Enable reliable change data capture (CDC) and incremental processing
- **Liquid Clustering**: Automatic data layout optimization for improved query performance
- **V2 Checkpoints**: Enhanced checkpoint format for better scalability

These features are essential for production workloads, especially when integrating with Databricks Unity Catalog and modern Delta Lake environments.

## Changes Made

### Core Changes (1,931+ lines)
- **New Module**: `crates/core/src/kernel/deletion_vector.rs` (466 lines)
  - Parsing and handling of deletion vector metadata
  - Integration with Parquet row group filtering
  - Support for inline and external deletion vectors
  
- **New Module**: `crates/core/src/kernel/liquid_clustering.rs` (276 lines)
  - Liquid clustering column specification parsing
  - Metadata handling for clustered tables
  
- **New Module**: `crates/core/src/delta_datafusion/dv_filter.rs` (398 lines)
  - DataFusion predicate pushdown with deletion vector filtering
  - Optimized query execution for tables with deletion vectors

- **Enhanced**: `crates/core/src/kernel/snapshot/iterators.rs`
  - Updated snapshot iterators to handle V2 checkpoint format
  - Integration with deletion vector metadata

- **Enhanced**: `crates/core/src/kernel/transaction/protocol.rs`
  - Extended protocol reader/writer for V2 features
  - Row tracking metadata support

- **Enhanced**: `crates/core/src/delta_datafusion/table_provider.rs`
  - DataFusion table provider updates for deletion vectors
  - Query planning optimization for V2 features

### Testing
- **New Test Suite**: `crates/core/tests/deletion_vector_test.rs` (223 lines)
  - Unit tests for deletion vector parsing and filtering
  - Integration tests with sample Delta tables

### Documentation
- **New Document**: `DELTA_V2_IMPLEMENTATION_SPEC.md` (446 lines)
  - Comprehensive implementation specification
  - Architecture decisions and design rationale
  - Usage examples and migration guide

### Dependencies
- Added `roaring` crate for efficient bitmap operations (used in deletion vectors)

## Testing
✅ All existing tests pass
✅ New deletion vector test suite covers:
- Inline deletion vector parsing
- External deletion vector storage
- Row filtering with deletion vectors
- Integration with DataFusion query execution

✅ Verified with production data from Databricks Unity Catalog tables

## Compatibility
- ✅ **Backwards Compatible**: Can read both V1 and V2 Delta tables
- ✅ **Protocol Version**: Properly handles min/max protocol versions
- ✅ **Performance**: No performance degradation for V1 tables
- ⚠️ **Write Operations**: V2 write operations require additional validation (future work)

## Integration Status
Successfully tested with:
- ✅ DuckDB Delta extension integration
- ✅ Databricks Unity Catalog tables
- ✅ S3-backed Delta tables with temporary credentials
- ✅ Tables containing deletion vectors, row tracking, and liquid clustering

## Next Steps
Future enhancements could include:
- Write support for deletion vectors
- Optimization rules for liquid clustering
- Enhanced statistics for V2 checkpoints

## Related Issues
- Resolves support for Databricks Unity Catalog V2 tables
- Enables production use cases requiring deletion vectors
- Provides foundation for CDC and incremental processing

---

**Testing Environment:**
- macOS arm64
- Rust 1.91.1
- Tested against production Databricks tables
- Verified with DuckDB v1.5.0-dev4072

**Files Changed:**
```
 DELTA_V2_IMPLEMENTATION_SPEC.md                    | 446 ++++++++
 crates/core/Cargo.toml                             |   3 +
 crates/core/src/delta_datafusion/dv_filter.rs      | 398 +++++++
 crates/core/src/delta_datafusion/mod.rs            |   1 +
 crates/core/src/delta_datafusion/table_provider.rs |  62 +++
 crates/core/src/kernel/deletion_vector.rs          | 466 ++++++++
 crates/core/src/kernel/liquid_clustering.rs        | 276 +++++
 crates/core/src/kernel/mod.rs                      |   2 +
 crates/core/src/kernel/snapshot/iterators.rs       |  45 ++
 crates/core/src/kernel/transaction/protocol.rs     |  17 +
 crates/core/tests/deletion_vector_test.rs          | 223 ++++
 11 files changed, 1931 insertions(+), 8 deletions(-)
```
