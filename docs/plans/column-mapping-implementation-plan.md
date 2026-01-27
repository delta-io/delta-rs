# Column Mapping Mode Support for delta-rs

## Overview

Enable support for Delta Lake column mapping mode (`name` and `id`) in delta-rs, allowing reading tables where logical column names differ from physical Parquet column names.

## Status: Read Support Complete ✅

Column mapping read support is now fully functional through the delta_kernel integration.

## Background

**Column Mapping Mode** decouples logical column names (user-visible) from physical column names (in Parquet files):
- **`name` mode**: Uses `delta.columnMapping.physicalName` field metadata
- **`id` mode**: Uses Parquet `field_id` via `delta.columnMapping.id` field metadata

## Implementation Summary

### Completed ✅

#### Phase 0: Setup
- [x] Add upstream remote and fetch latest changes
- [x] Merge upstream main (delta_kernel 0.19.0) into working branch
- [x] Verify build works

#### Read Support (Already Implemented in Upstream)
The "next" scan implementation in `delta_datafusion/table_provider/next/` already handles column mapping correctly:

- [x] `parse_partitions()` in `scan_row.rs` uses `field.physical_name(column_mapping_mode)` to map partition values
- [x] `rewrite_expression()` in `scan/plan.rs` transforms predicates from logical to physical names
- [x] `DeltaScanExec` handles statistics with physical column names
- [x] Schema transformation via `schema.make_physical(column_mapping_mode)` from delta_kernel

#### Testing
- [x] Rust integration tests (`crates/core/tests/integration_column_mapping.rs`)
  - test_read_table_with_column_mapping
  - test_datafusion_query_with_column_mapping
  - test_partition_filter_with_column_mapping
  - test_statistics_with_column_mapping
  - test_projection_with_column_mapping
  - test_physical_name_access
  - test_full_table_scan_with_column_mapping

- [x] Python tests (`python/tests/test_column_mapping.py`)
  - test_load_table_with_column_mapping
  - test_schema_has_logical_names
  - test_read_to_pyarrow
  - test_read_to_pandas
  - test_filter_on_partition_column
  - test_metadata
  - test_datafusion_select_all
  - test_datafusion_select_with_quotes

#### Python Bindings
- [x] Enable reader version 2 (required for column mapping)
- [x] Add `columnMapping` to supported reader features
- [x] Remove explicit block on column mapping in `to_pyarrow_dataset()`

### Future Work (Write Support)

> **Note:** Write support requires additional changes and is not yet implemented.
> The `ColumnMapping` feature is currently commented out in `protocol.rs` ProtocolChecker.

- [ ] Enable `TableFeature::ColumnMapping` in `kernel/transaction/protocol.rs`
- [ ] Update `WriterConfig` to accept column mapping mode
- [ ] Transform Arrow schema fields to physical names before writing Parquet
- [ ] Transform partition values to physical names in `Add` actions
- [ ] Verify statistics are correct

---

## Key Files

| File | Description |
|------|-------------|
| `crates/core/src/kernel/snapshot/iterators/scan_row.rs` | Partition value parsing with physical names |
| `crates/core/src/delta_datafusion/table_provider/next/scan/plan.rs` | Predicate rewriting for physical names |
| `crates/core/src/delta_datafusion/table_provider/next/scan/exec.rs` | Statistics handling with column mapping |
| `crates/core/src/kernel/arrow/engine_ext.rs` | `make_physical()` and stats schema utilities |
| `python/deltalake/table.py` | Python protocol feature support |

---

## Test Data

Existing test table: `crates/test/tests/data/table_with_column_mapping/`

Schema with column mapping metadata:
```json
{
  "name": "Company Very Short",
  "metadata": {
    "delta.columnMapping.id": 1,
    "delta.columnMapping.physicalName": "col-173b4db9-b5ad-427f-9e75-516aae37fbbb"
  }
}
```

Partition values in transaction log use physical names:
```json
{"partitionValues": {"col-173b4db9-b5ad-427f-9e75-516aae37fbbb": "BMS"}}
```

---

## Verification

### Rust
```bash
cargo test -p deltalake-core --test integration_column_mapping --features datafusion
```

### Python
```bash
cd python
source .venv/bin/activate
pytest tests/test_column_mapping.py -v
```

### Manual Verification
```python
import deltalake
dt = deltalake.DeltaTable("crates/test/tests/data/table_with_column_mapping")
print(dt.schema())  # Shows logical column names with metadata
print(dt.to_pandas())  # Data with logical column names

from datafusion import SessionContext
ctx = SessionContext()
ctx.register_table("test", dt.to_pyarrow_dataset())
result = ctx.sql('SELECT "Company Very Short", "Super Name" FROM test').collect()
print(result)
```

---

## Backward Compatibility

- `ColumnMappingMode::None` preserves existing behavior exactly
- No breaking API changes
- All existing tests continue to pass
