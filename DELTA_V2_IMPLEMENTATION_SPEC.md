# Delta Lake V2 Implementation Specification

## Executive Summary

This document provides a detailed technical specification for implementing Delta Lake V2 features in delta-rs, including:
- **Deletion Vectors (DV)** - Row-level deletes without file rewrites
- **V2 Checkpoints** - UUID-based checkpoint files with sidecars ✅ (Already Working)
- **Row Tracking** - Base row ID and commit version tracking
- **Liquid Clustering** - File clustering for query optimization

---

## Current State Analysis

### What's Already Working

| Feature | Status | Notes |
|---------|--------|-------|
| V2 UUID Checkpoints | ✅ Working | `test_v2_checkpoint_json` passes |
| Checkpoint Sidecars | ✅ Working | Parquet sidecars parsed correctly |
| `v2Checkpoint` Reader Feature | ✅ Working | Properly recognized |
| DV Metadata Parsing | ✅ Working | `DeletionVectorDescriptor` exists |
| DV Filtering | ❌ **Not Implemented** | Metadata parsed but not applied |
| Row Tracking Columns | ❌ **Not Implemented** | Fields exist but hardcoded to `None` |
| Liquid Clustering Pruning | ❌ **Not Implemented** | Metadata parsed but not used |

### Key Gap: Deletion Vector Filtering

The critical missing piece is that deletion vectors are **parsed but not applied** during reads:

```rust
// Current code in table_provider.rs:195-204
deletion_vector: None,  // ← HARDCODED TO NONE - DVs IGNORED!
```

---

## Architecture Overview

### Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        DeltaTable                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐     ┌──────────────────┐                  │
│  │   Transaction    │     │    Snapshot      │                  │
│  │      Log         │────▶│   (EagerSnapshot)│                  │
│  └──────────────────┘     └────────┬─────────┘                  │
│                                    │                            │
│           ┌────────────────────────┼────────────────────────┐   │
│           │                        │                        │   │
│           ▼                        ▼                        ▼   │
│   ┌───────────────┐    ┌───────────────────┐    ┌───────────┐   │
│   │   Protocol    │    │    Add Actions    │    │  Metadata │   │
│   │ (V2 features) │    │ (with DVs)        │    │           │   │
│   └───────────────┘    └─────────┬─────────┘    └───────────┘   │
│                                  │                              │
│                                  ▼                              │
│                        ┌─────────────────────┐                  │
│                        │ DeletionVector      │ ◀── NEW MODULE   │
│                        │ Descriptor          │                  │
│                        └─────────┬───────────┘                  │
│                                  │                              │
│                                  ▼                              │
│                        ┌─────────────────────┐                  │
│                        │ DeletionVectorLoader│ ◀── NEW          │
│                        │ (RoaringBitmap)     │                  │
│                        └─────────┬───────────┘                  │
│                                  │                              │
└──────────────────────────────────┼──────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                    DataFusion Integration                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐     ┌──────────────────┐                   │
│  │  DeltaScan       │────▶│  ParquetSource   │                   │
│  │  (file pruning)  │     │  (row reading)   │                   │
│  └──────────────────┘     └────────┬─────────┘                   │
│                                    │                             │
│                                    ▼                             │
│                           ┌────────────────────┐                 │
│                           │ DeletionVectorFilter│ ◀── NEW        │
│                           │ ExecutionPlan       │                │
│                           └────────┬────────────┘                │
│                                    │                             │
│                                    ▼                             │
│                           ┌────────────────────┐                 │
│                           │  RecordBatch       │                 │
│                           │  (filtered rows)   │                 │
│                           └────────────────────┘                 │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Detailed Implementation Plan

### Epic 1: Deletion Vector Read Support

#### Task DV-1: RoaringBitmap Integration

**File:** `crates/core/Cargo.toml`

Add dependencies:
```toml
[dependencies]
roaring = "0.10"
# z85 is not available as a crate; use custom base85 decoder
```

**File:** `crates/core/src/kernel/deletion_vector.rs`

Replace placeholder with actual RoaringBitmap:

```rust
use roaring::RoaringBitmap;

impl DeletionVector {
    pub fn from_roaring_bytes(bytes: Bytes, cardinality: u64) -> DeltaResult<Self> {
        let bitmap = RoaringBitmap::deserialize_from(&bytes[..])
            .map_err(|e| DeltaTableError::Generic(format!("Failed to deserialize DV: {}", e)))?;
        
        Ok(Self {
            deleted_rows: DeletedRowSet::Bitmap(bitmap),
            cardinality,
        })
    }
}
```

#### Task DV-2: DataFusion Filter Integration

**File:** `crates/core/src/delta_datafusion/dv_filter.rs` (NEW)

Create a custom `ExecutionPlan` that wraps ParquetSource and applies DV filtering:

```rust
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use std::sync::Arc;

/// Execution plan that filters rows based on deletion vectors
#[derive(Debug)]
pub struct DeletionVectorFilterExec {
    /// The underlying parquet scan
    input: Arc<dyn ExecutionPlan>,
    /// Map from file path to deletion vector
    deletion_vectors: HashMap<String, DeletionVector>,
    /// Schema of the output
    schema: SchemaRef,
}

impl ExecutionPlan for DeletionVectorFilterExec {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        
        // Wrap stream to apply DV filtering
        Ok(Box::pin(DeletionVectorFilterStream::new(
            input_stream,
            self.deletion_vectors.clone(),
        )))
    }
}
```

#### Task DV-3: Table Provider Integration

**File:** `crates/core/src/delta_datafusion/table_provider.rs`

Modify `DeltaScanBuilder::build()` to:

1. Pass deletion vectors through to scan config:
```rust
// In add_action() method, change from:
deletion_vector: None,
// To:
deletion_vector: add.deletion_vector.clone(),
```

2. Create DV filter execution plan:
```rust
impl DeltaScanBuilder {
    pub async fn build(self) -> DeltaResult<DeltaScan> {
        // ... existing code ...
        
        // Load deletion vectors for files that have them
        let dv_loader = DeletionVectorLoader::new(
            table_root.clone(),
            self.log_store.object_store(None),
        );
        
        let mut deletion_vectors = HashMap::new();
        for file in &files {
            if let Some(ref dv_desc) = file.deletion_vector {
                let dv = dv_loader.load(dv_desc).await?;
                deletion_vectors.insert(file.path.clone(), dv);
            }
        }
        
        // Wrap parquet scan with DV filter if needed
        let scan = if !deletion_vectors.is_empty() {
            Arc::new(DeletionVectorFilterExec::new(parquet_scan, deletion_vectors))
        } else {
            parquet_scan
        };
        
        // ... rest of method ...
    }
}
```

---

### Epic 2: Row Tracking Support

#### Task RT-1: Extract Row Tracking Fields

**File:** `crates/core/src/kernel/snapshot/iterators.rs`

Modify `FileView::add_action()` to extract row tracking fields:

```rust
impl FileView<'_> {
    pub(crate) fn add_action(&self) -> Add {
        Add {
            path: self.path().to_string(),
            partition_values: self.partition_values_map(),
            size: self.size(),
            modification_time: self.modification_time(),
            data_change: true,
            stats: self.stats(),
            tags: None,
            deletion_vector: self.deletion_vector().map(|dv| dv.descriptor()),
            base_row_id: self.base_row_id(),           // ← Extract from kernel
            default_row_commit_version: self.default_row_commit_version(), // ← Extract
            clustering_provider: self.clustering_provider(),
        }
    }
    
    /// Extract base_row_id from kernel data
    fn base_row_id(&self) -> Option<i64> {
        self.files
            .column_by_name("baseRowId")?
            .as_primitive::<Int64Type>()
            .value(self.index)
            .into()
    }
    
    /// Extract default_row_commit_version from kernel data  
    fn default_row_commit_version(&self) -> Option<i64> {
        self.files
            .column_by_name("defaultRowCommitVersion")?
            .as_primitive::<Int64Type>()
            .value(self.index)
            .into()
    }
}
```

#### Task RT-2: Expose as Virtual Columns

**File:** `crates/core/src/delta_datafusion/table_provider.rs`

Add support for virtual row tracking columns:

```rust
const ROW_ID_COLUMN: &str = "_row_id";
const ROW_COMMIT_VERSION_COLUMN: &str = "_row_commit_version";

impl DeltaScanConfig {
    /// Include row tracking columns in output
    pub include_row_tracking: bool,
}
```

---

### Epic 3: Liquid Clustering Optimization

#### Task LC-1: Parse Clustering Metadata

**File:** `crates/core/src/kernel/models/clustering.rs` (NEW)

```rust
/// Liquid clustering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusteringMetadata {
    /// The clustering columns
    pub clustering_columns: Vec<String>,
    /// Domain metadata for clustering
    pub domain_metadata: Option<serde_json::Value>,
}

impl ClusteringMetadata {
    /// Parse from table properties and domain metadata
    pub fn from_table_config(
        properties: &TableProperties,
        domain_metadata: Option<&serde_json::Value>,
    ) -> Option<Self> {
        // Extract clustering info
    }
}
```

#### Task LC-2: Use Clustering for File Pruning

**File:** `crates/core/src/delta_datafusion/table_provider.rs`

Modify file pruning to leverage clustering:

```rust
impl DeltaScanBuilder {
    fn prune_files_with_clustering(
        &self,
        files: Vec<Add>,
        predicate: &Expr,
        clustering: &ClusteringMetadata,
    ) -> Vec<Add> {
        // Sort files by clustering columns
        // Use clustering stats for more aggressive pruning
    }
}
```

---

## Testing Strategy

### Unit Tests

**File:** `crates/core/src/kernel/deletion_vector.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inline_dv_decoding() {
        // Test base85 decoding
    }

    #[test]
    fn test_uuid_path_reconstruction() {
        // Test UUID to path conversion
    }

    #[tokio::test]
    async fn test_load_uuid_relative_dv() {
        // Test loading from file
    }
}
```

### Integration Tests

**File:** `crates/core/tests/deletion_vector_test.rs` (NEW)

```rust
use deltalake_core::{open_table, DeltaOps};
use deltalake_core::test_utils::TestTables;

#[tokio::test]
async fn test_read_table_with_deletion_vectors() {
    // Use table-with-dv-small test data
    let table = open_table(TestTables::WithDvSmall.as_path()).await.unwrap();
    
    // Original table has 10 rows, 2 deleted
    let df = table.scan(None, None).await.unwrap();
    let batches = df.collect().await.unwrap();
    
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 8, "Expected 8 rows after DV filtering");
}

#[tokio::test]
async fn test_dv_cardinality_matches() {
    let table = open_table(TestTables::WithDvSmall.as_path()).await.unwrap();
    
    // Check that DV cardinality is reported correctly
    let files = table.snapshot().unwrap().log_data();
    for file in files.iter() {
        if let Some(dv) = &file.add_action().deletion_vector {
            assert_eq!(dv.cardinality, 2);
        }
    }
}
```

---

## File Inventory

### New Files to Create

| Path | Purpose |
|------|---------|
| `crates/core/src/kernel/deletion_vector.rs` | DV loading and filtering |
| `crates/core/src/delta_datafusion/dv_filter.rs` | DataFusion DV filter exec |
| `crates/core/src/kernel/models/clustering.rs` | Clustering metadata |
| `crates/core/tests/deletion_vector_test.rs` | DV integration tests |

### Files to Modify

| Path | Changes |
|------|---------|
| `crates/core/Cargo.toml` | Add `roaring` dependency |
| `crates/core/src/kernel/mod.rs` | Export `deletion_vector` module |
| `crates/core/src/kernel/snapshot/iterators.rs` | Extract row tracking fields |
| `crates/core/src/delta_datafusion/mod.rs` | Export DV filter |
| `crates/core/src/delta_datafusion/table_provider.rs` | Integrate DV filtering |

---

## Estimated Effort

| Task | Complexity | Time (Hours) | Dependencies |
|------|------------|--------------|--------------|
| DV-1: RoaringBitmap | ⭐⭐ | 4-8 | None |
| DV-2: DataFusion Filter | ⭐⭐⭐⭐ | 16-24 | DV-1 |
| DV-3: Table Provider | ⭐⭐⭐ | 8-16 | DV-2 |
| RT-1: Extract Fields | ⭐⭐ | 4-8 | None |
| RT-2: Virtual Columns | ⭐⭐⭐ | 8-16 | RT-1 |
| LC-1: Parse Clustering | ⭐⭐ | 4-8 | None |
| LC-2: Use for Pruning | ⭐⭐⭐⭐ | 16-24 | LC-1 |

**Total Estimate:** 60-104 hours (2-4 weeks for 1 engineer)

---

## References

- [Delta Protocol Spec - Deletion Vectors](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#deletion-vectors)
- [Delta Protocol Spec - V2 Checkpoints](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#v2-spec)
- [Delta Protocol Spec - Row Tracking](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#row-tracking)
- [RoaringBitmap Format](https://roaringbitmap.org/about/)

