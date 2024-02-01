# Delta Lake Best Practices

This page outlines Delta Lake best practices.

You should consider several factors to optimize the performance of your Delta tables and minimize costs.

The Delta Lake best practices depend on your data ingestion into the Delta table and query patterns.  You must understand your data and how users run queries to best leverage Delta Lake.

## Compacting small files

Delta tables work best when the files are “right-sized”.  Files that are too small create I/O overhead.  Files that are too large limit the impact of file skipping (a critical query optimization).

Delta tables can accumulate a lot of small files, especially if you’re frequently writing small amounts of data.  If your table has many small files, you should run a small compaction operation to consolidate all the tiny files into “right-sized” files.

It’s generally best for files in a Delta table to be between 100MB and 1GB, but that can vary based on the overall size of the table and the query patterns.

Delta Lake makes it easy to [compact the small files](https://delta-io.github.io/delta-rs/usage/optimize/small-file-compaction-with-optimize/).

## Optimizing table layout

You can colocate similar data in the same files to make file skipping more effective.  Delta Lake supports [Z Ordering](https://delta-io.github.io/delta-rs/usage/optimize/delta-lake-z-order/), which can colocate similar data in the same files.

Z Ordering can yield impressive performance gains for low-cardinality columns but also works well for high-cardinality columns.  This is an advantage compared to Hive-style partitioning, which is only suitable for low-cardinality columns.

You must analyze the most common query patterns and Z Order your dataset based on the columns allowing the most file skipping.  The ability to colocate data in the same files decreases when you add more Z Order columns.

Let’s look at Hive-style partitioning, another way to colocate data in the same files.  You can also use Hive-style partitioning in conjunction with Z Ordering.

## Partitioning datasets

You can partition your Delta tables, which separates the data by one or more partition keys into separate folders.  Partitioning can be an excellent performance optimization (when you filter on the partition key) and is a good way to sidestep concurrency conflict issues.

Hive-style partitioning also has some significant downsides.

* It’s only suitable for low-cardinality columns.
* It can create many small files, especially if you use the wrong partition key or frequently update the Delta table.
* It can cause some queries that don’t rely on the partition key to run slower (because of the excessive number of small files).  A large number of small files is problematic for I/O throughput.

Hive-style partitioning can be a great data management tactic and a fantastic option for many Delta tables.  Beware of the downsides before partitioning your tables.

You can use Hive-style partitioning in conjunction with Z Ordering.  You can partition a table by one column and Z Order by another.  They’re different tactics that aim to help you skip more files and run queries faster.

Let’s look at some of the built-in Delta features that help maintain the integrity of your tables.

## Use appropriate quality controls

Delta Lake supports schema enforcement and column constraints to protect the integrity of your data.

Delta Lake enabled schema enforcement by default, so you can only append data to an existing table with the same exact schema.  You can bypass schema enforcement by enabling schema evolution, which allows you to append mismatched schemas to a table.

You should only enable schema evolution when you want to allow the schema of your table to change.  You should not enable schema evolution if you don’t want this flexibility.  Schema enforcement is a good default setting.

Column-level constraints prevent you from appending data that fail SQL predicates.  For example, you may add a constraint that requires all the values in the `age` column of a table to be positive.

You should add column constraints to your table whenever you want a column only to include values that satisfy a predicate.

No data is appended when you apply a constraint and a row check fails.  For example, if you try to append 100 rows of data to a table and one row has a failing check, then no data is added.

When you have column constraints, it’s often a good idea to append the failing data to a “quarantine table” and the passing data to the main Delta table.  Or you can filter out the failing rows and just append the passing rows.  Keeping a history of the failing rows in a quarantine table is helpful for debugging.

See here to learn more about [Delta Lake constraints](https://delta-io.github.io/delta-rs/usage/constraints/).

## Best practices for DML operations

DML operations like deleting, updating, and merging write existing data in new files and mark existing files for deletion in the transaction log.  Rewriting data files is expensive, so you want to minimize the number of rewritten files when you run DML operations.

Delta Lake supports a table feature called deletion vectors that implements DML transactions more efficiently under the hood.  Enabling deletion vectors is usually the best way to make DML operations run faster.  Note: delta-rs doesn’t support deletion vectors yet.

You should periodically purge deletion vectors because they can accumulate and slow subsequent read operations.  Once you enable the feature, you must purge the deletion vectors in your table with an appropriate cadence.

## Use vacuum to save storage costs

Delta Lake supports transactions, which necessitates keeping old versions of data in storage, even the files marked for removal in the transactions log.

Keeping old versions of Delta tables in storage is often desirable because it allows for versioned data, time travel, and rolling back tables to a previous state.

If you don’t want to leverage older versions of a table, then you should remove the legacy files from storage with the vacuum command.  Vacuum will remove all files older than the table retention period and marked for removal in the transaction log.

You only need to vacuum when you perform operations that mark files for removal in the transaction log.  An append-only table doesn’t create legacy files that need to be vacuumed.

Create a good vacuum strategy for your tables to minimize your storage costs.

## Delta Lake best practices to minimize costs

Delta Lake helps you minimize costs in many ways:

* It's a free, open source format (based on Parquet). It's not a proprietary format that you need to pay for.
* Delta tables store column-level min/max values in the transaction log, allowing file skipping.
* Delta tables can be optimized (small file compaction, Z Ordering, etc.), so your queries run faster. When your queries run faster, then you pay less on compute.
* Deletion vectors let you perform DML operations (delete, update, merge) much faster. If your delete operation runs 100x faster, then you pay 100x less compute.
* It's easy to remove legacy files from storage with VACUUM, which minimizes storage costs.

You should understand your organization’s query patterns and use these features to minimize the overall cost.  You need to assess tradeoffs.  For example, Z Ordering is a computation that costs money, but it can save you lots of money in the long run if all your subsequent queries run a lot faster and use less compute.

## Collect metadata stats on columns used for file skipping

Delta tables don’t always store each column's min/max values.  Some Delta Lake implementations only store min/max values for the first 32 columns in the table, for example.

Delta Lake can only apply file-skipping when it has min/max values for the relevant columns stored in the transaction log.  Suppose you’re running a filtering operation on `col_a,` for example.  Delta Lake can only apply file skipping when the transaction log stores `col_a` min/max metadata.

Ensure the transaction log stores metadata stats for all the columns that benefit from file skipping.

## Don’t collect column metadata when it’s unnecessary

It takes some time to compute column statistics when writing files, and it isn’t worth the effort if you cannot use the column for file skipping.

Suppose you have a table column containing a long string of arbitrary text.  It’s unlikely that this column would ever provide any data-skipping benefits.  So, you can just avoid the overhead of collecting the statistics for this particular column.

## Additional reading

Delta Lake relies on transactions, and you should check out [this page to learn more](https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-acid-transactions/).

Many Delta Lake performance benefits rely on [file skipping](https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-file-skipping/), which you should understand well to get the most out of Delta.

## Conclusion

Delta Lake is a powerful technology that makes your data pipelines more reliable, saves money, and makes everyday data processing tasks easy.

You need to learn how Delta Lake works at a high level to leverage Delta's power fully.  You will not be able to leverage Delta Lake’s full performance potential if your table has improperly sized files or if you’re not colocating data in the same files to maximize data skipping, for example.

Luckily, there are only a few details that are important to learn.  You don’t need to know the implementation details - just the essential high-level concepts.
