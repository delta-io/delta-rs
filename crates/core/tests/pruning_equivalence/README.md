# SQL fixtures for data-skipping correctness

These files borrow DuckDB's SQLLogicTest `.test` format. The Rust test runs the SQL with DataFusion. It creates a temporary on-disk Delta table named `actual` for each fixture. Other tables, including the independent copy of the input, live in memory. The runner reopens `actual` after a write or DELETE so a subsequent query reads the committed snapshot.

## Run the fixtures

From the workspace root, run the passing fixtures with

```sh
cargo test -p deltalake-core --features datafusion --test pruning_equivalence fixtures::suite::sql_fixtures -- --exact --nocapture
```

Replay a single file by setting `DELTA_FIXTURE` to its exact path relative to this directory.

```sh
DELTA_FIXTURE=select_delete.test cargo test -p deltalake-core --features datafusion --test pruning_equivalence fixtures::suite::sql_fixtures -- --exact --nocapture
```

Files under `known/` reproduce current engine defects. Run them on demand with the command below. It runs each selected file and returns a test failure for any mismatch. The same environment variable selects one file in this group.

```sh
cargo test -p deltalake-core --features datafusion --test pruning_equivalence fixtures::suite::known_sql_fixtures -- --exact --ignored --nocapture
```

## Write a fixture

Separate records by blank lines. Use `statement ok` for setup and mutation, including `CREATE TABLE actual AS SELECT ...`, `INSERT INTO actual ...` and `DELETE FROM actual ...`. Use `statement error` to expect a SQL error, with an optional `----` followed by a substring of the message.

A `query` record accepts read-only SQL. Specify one of `I`, `R` or `T` per output column. These letters check the width of the result. Write literal rows after `----`, separating cells with tabs and using `NULL` for a null and `(empty)` for an empty string. An unlabelled query with no rows after `----` asserts an empty result.

For a comparison against an in-memory table, use the same label on two queries, as in `query II rowsort survivors`. Each query needs a `----` separator. Compare complete rows, including duplicate rows. Use `rowsort` for order-independent comparisons or `nosort` with an explicit SQL `ORDER BY` for ordered results. For DELETE, select the expected survivors from the original rows using `WHERE (predicate) IS NOT TRUE` so a NULL predicate keeps the row. Keep SELECT and DELETE in separate files when both operations reproduce a defect.

## Assert that a file was skipped

Before the first record, `# delta: partition-by p,q` sets the writer's partition columns and `# delta: stats on` or `# delta: stats off` controls indexed column statistics. A leading SQL comment in a query can assert the number of files scanned out of the total. For example, in `layout_stats.test` two separate writes store IDs 11 and 99.

```sql
query I
-- delta-scan: 1/2
SELECT id FROM actual WHERE id = 11;
----
11
```

The Rust runner checks the scan metric and the returned rows. DuckDB ignores the Delta-specific comments, so its own SQLLogicTest runner can check the SQL and expected rows in the same file.
