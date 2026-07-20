# Optimized sort of Delta tables

The problem we want to solve is that we write to Delta tables such that Parquet
data is sorted in a specific order.
When reading back the data in a Rust client with Datafusion,
we often want to sort data in the same order it was written.
Currently, we always need to do a global sort because DataFusion doesn't know that the
files are already sorted and that this can be optimized.

Datafusion (source at ../../datafusion/datafusion) contains tooling to help with this,
which is used by its ListingTable, but this isn't used by delta-rs's TableProvider.
sort_pushdown.rs in Datafusion describes the Datafusion optimizations involved.

## Current rough plan outline

1. Verify behaviour with plain Datafusion + Parquet

Add an integration test that writes Hive partitioned Parquet files
where each write is ordered by "timestamp", and writes
to the same partition also have non-overlapping time ranges.
Also set the Parquet "sorting_columns" metadata correctly.

Query the ListingTable with Datafusion with a sort by timestamp. 
Compare queries with and without split_groups_by_statistics
set and check that we can test that the optimized path is taken
that avoids a full sort. 

2. Add end-to-end DeltaLake test

Add an integration test that writes partitioned Parquet files to
a delta-rs where each write is ordered by "timestamp", and writes
to the same partition also have non-overlapping time ranges.

Query the DeltaTable with Datafusion and add a check that the
optimized sort push down is used. This check should fail initially,
but correctly sorted data should be returned.

3. Configure sorting in TableProviderBuilder

Add a "with_file_sort_order" option on the TableProviderBuilder.
This will define the sort order used by all Parquet files.
Later we might look at whether we can infer this from Parquet sorting_columns
metadata.

Plumb this through to Datafusion such that the sort pushdown
optimization can take advantage of it and the test from step 2 passes.

4. Infer file sort order

Add an "with_inferred_file_sort_order(bool)" method to TableProviderBuilder.
This should read ths Parquet sorting columns from Parquet metadata at scan
time to infer the sort order.
