# Optimized sort of Delta tables

The problem we want to solve is that we write to Delta tables such that Parquet
data is sorted in a specific order.
When reading back the data in a Rust client with Datafusion,
we often want to sort data in the same order it was written.
Currently, we always need to do a global sort because DataFusion doesn't know that the
files are already sorted and that this can be optimized.

Datafusion (source at ../../datafusion/datafusion) contains tooling to help with this,
which is used by its ListingTable, but this isn't used by delta-rs's TableProvider.

DataFusion 54 has two independent mechanisms for avoiding a full sort:

1. **Plan-time file grouping**: `ListingTable` declares a `file_sort_order`
   which becomes the `FileScanConfig`'s `output_ordering`. The ordering is
   validated against file min/max statistics (each file group must be
   internally non-overlapping in sort order), and the
   `split_file_groups_by_statistics` config option regroups files by
   statistics at plan time. `EnforceSorting` then replaces the `SortExec`
   with a `SortPreservingMergeExec` over the ordered groups.
2. **`PushdownSort` optimizer rule** (enabled by default): asks the data
   source to reorganize itself for the query's required ordering via
   `ExecutionPlan::try_pushdown_sort`. `FileScanConfig::try_pushdown_sort`
   re-sorts and regroups files by statistics per query and, if the declared
   `output_ordering` then validates, the `SortExec` is removed entirely
   (`Exact`); otherwise the scan is still optimized (row-group reordering)
   and the sort kept (`Inexact`).

We will target the **PushdownSort route** as the primary mechanism: it is on
by default, driven by the query's actual ORDER BY, and doesn't require eager
regrouping of files at plan time. Note that the source must still *declare*
its file sort order (via `FileScanConfig::output_ordering`) for the `Exact`
(sort-eliminated) outcome — statistics alone only prove files don't overlap,
not that rows within a file are sorted. Because `DeltaScanExec` is a custom
plan node, the `PushdownSort` rule stops at it unless we implement
`try_pushdown_sort` on it, delegating to the inner `DataSourceExec`.

**Not being done for now (writer side)**: delta-rs never writes Parquet
`sorting_columns` metadata itself, and this plan does not add a writer-side
option for it. Callers can already opt in manually via
`WriteBuilder::with_writer_properties` with `set_sorting_columns(...)`, which
is what the tests do. A first-class write-time sort-order declaration (set
`sorting_columns`, keep data sorted per file) is a natural follow-up but out
of scope here; until then, inference (step 4) only helps for tables written
by engines that set the metadata.

## Current rough plan outline

1. Verify behaviour with plain Datafusion + Parquet — **done**

Add an integration test that writes Hive partitioned Parquet files
where each write is ordered by "timestamp", and writes
to the same partition also have non-overlapping time ranges.
Also set the Parquet "sorting_columns" metadata correctly.

Query the ListingTable with Datafusion with a sort by timestamp.
Compare queries with and without split_groups_by_statistics
set and check that we can test that the optimized path is taken
that avoids a full sort.

Implemented in `crates/core/tests/it_datafusion/sort_order.rs`. Verified:

- `split_file_groups_by_statistics` + declared `file_sort_order` →
  `SortPreservingMergeExec` directly over the scan, no `SortExec`.
- `PushdownSort` alone (split disabled) also eliminates the sort.
- With both disabled, a full `SortExec` is required.
- With no declared sort order, the ordering is inferred from the Parquet
  `sorting_columns` metadata (requires statistics collection).

Gotcha: `ListingOptions::new` defaults to one target partition and no
statistics collection; session config must be applied explicitly.

2. Add end-to-end DeltaLake test

Add an integration test that writes partitioned Parquet files to
a delta-rs table where each write is ordered by "timestamp", and writes
to the same partition also have non-overlapping time ranges.
Set `sorting_columns` by passing custom `WriterProperties` (see writer-side
note above).

Query the DeltaTable with Datafusion and add a check that the
optimized sort push down is used. This check should fail initially,
but correctly sorted data should be returned.

3. Configure sorting in TableProviderBuilder

Add a "with_file_sort_order" option on the TableProviderBuilder.
This will define the sort order used by all Parquet files.

Plumb this through so the PushdownSort optimization can take advantage of it
and the test from step 2 passes:

- Store the sort order on the scan configuration and set it as the
  `output_ordering` on the `FileScanConfigBuilder` in `get_read_plan`
  (`crates/core/src/delta_datafusion/table_provider/next/scan/mod.rs`).
- Implement `try_pushdown_sort` on `DeltaScanExec`, delegating to its input
  (the `DataSourceExec`) and rebuilding itself around the optimized input.
  Sort expressions must be remapped between the DeltaScanExec output schema
  and the inner parquet scan schema (which includes the synthetic `file_id`
  column and excludes Delta partition columns).
- Advertise the resulting ordering in `DeltaScanExec`'s `PlanProperties`
  (currently built with no orderings) so `EnforceSorting` can also use it.

Scope: only sort orders over regular (non-partition) columns are supported.
Delta partition columns are injected above the parquet scan by kernel
transforms in `DeltaScanExec`, so they cannot appear in the file-level
`output_ordering`.

### Things to clarify during step 3

- `DeltaScanExec` has `maintains_input_order` commented out with a TODO
  ("setting this will fail certain tests, but why"). Understand the failure
  before advertising orderings — don't just re-enable it.
- Kernel transforms (partition value injection, column mapping, deletion
  vector filtering) should all be row-order-preserving; confirm this holds
  for every transform the scan can produce.
- File groups are currently chunked by file-id dictionary cardinality
  (65536 files per group), i.e. effectively one big group. Check whether
  `FileScanConfig::try_pushdown_sort`'s per-query regrouping is sufficient,
  or whether we need to build groups differently when a sort order is
  declared. Also check the interaction with `repartition_file_scans`
  (byte-range splitting), which must not split files when ordering matters.
- Are the Delta log (Add action) statistics that delta-rs attaches to
  `PartitionedFile`s sufficient for `MinMaxStatistics`? The sort column must
  have min/max stats (by default Delta only collects stats for the first 32
  columns).
- `DeltaScanConfig` is `Serialize`/`Deserialize` (used by the proto codec),
  so if the sort order lives there it needs a serializable representation
  (column names + ASC/DESC + NULLS FIRST/LAST), not physical expressions.
- Multi-object-store tables produce a `UnionExec` of per-store scans, which
  destroys ordering. This can stay unsupported, but must degrade gracefully
  (no false ordering advertised) rather than error.
- LIMIT / TopK interaction: `PushdownSort` pushes fetch into the scan on the
  `Exact` path — make sure `DeltaScanExec::with_fetch`/limit handling is
  consistent with that.

4. Infer file sort order

Add an "with_inferred_file_sort_order(bool)" method to TableProviderBuilder.
This should read the Parquet sorting columns from Parquet metadata at scan
time to infer the sort order, reusing DataFusion's
`ordering_from_parquet_metadata` and per-file `PartitionedFile::ordering` +
common-prefix derivation where possible.

### Things to clarify during step 4

- Cost: delta-rs deliberately reads statistics from the Delta log, not
  Parquet footers. Inference adds one footer read per file per scan on the
  gated path. Consider caching, or documenting that users should prefer
  `with_file_sort_order` for large tables once the order is known.
- DataFusion's `ordering_from_parquet_metadata` only reads the *first* row
  group's `sorting_columns` and the table ordering is the common prefix
  across files — confirm these semantics are acceptable.
