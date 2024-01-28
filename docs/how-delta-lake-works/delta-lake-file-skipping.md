# Delta Lake File Skipping

Delta tables store file-level metadata information, which allows for a powerful optimization called file skipping.

This page explains how Delta Lake implements file skipping, how to optimize your tables to maximize file skipping, and the benefits of file skipping.

Let’s start by looking at the file-level metadata in Delta tables.

## Delta Lake file metadata

Delta Lake stores metadata about each file's min/max values in the table.  Query engines can skip entire files when they don’t contain data that’s relevant to the query.

Suppose you have a Delta table with data stored in two files and has the following metadata.

```
filename	min_name	max_name	min_age	max_age
fileA		alice		joy		    12		46	
fileB		allan		linda		34		78
```

Suppose you want to run the following query: `select * from the_table where age &lt; 20`.

The engine only needs to read `fileA` to execute this query.  `fileB` has a `min_age` of 34, so we know there aren’t any rows of data with an `age` less than 20.

The benefit of file skipping depends on the query and the data layout of the Delta table.  Some queries cannot take advantage of any file skipping.  Here’s an example query that does not benefit from file skipping: `select * from the_table group by age`.

Let’s recreate this example with Polars to drive the point home.

Start by writing out one file of data:

```python
import polars as pl
from deltalake import DeltaTable

df = pl.DataFrame({"name": ["alice", "cat", "joy"], "age": [12, 35, 46]})
df.write_delta("tmp/a_table")
```

Now, write out another file of data:

```python
df = pl.DataFrame({"name": ["allan", "brian", "linda"], "age": [34, 35, 78]})
df.write_delta("tmp/a_table", mode="append")
```

Here are the contents of the Delta table:

```
tmp/a_table
├── 0-7d414a88-a634-4c2f-9c5b-c29b6ee5f524-0.parquet
├── 1-0617ef60-b17b-46a5-9b0f-c7dda1b73eee-0.parquet
└── _delta_log
    ├── 00000000000000000000.json
    └── 00000000000000000001.json
```

Now run a query to fetch all the records where the age is less than 20:

```python
pl.scan_delta("tmp/a_table").filter(pl.col("age") < 20).collect()
```

```
+-------+-----+
| name  | age |
| ---   | --- |
| str   | i64 |
+=============+
| alice | 12  |
+-------+-----+
```

Polars can use the Delta table metadata to skip the file that does not contain data relevant to the query.

## How Delta Lake implements file skipping

Here’s how engines execute queries on Delta tables:

* Start by reading the transaction log to get the file paths, file sizes, and min/max value for each column
* Parse the query and push down the predicates to skip files
* Read the minimal subset of the files needed for the query

Some file formats don’t allow for file skipping.  For example, CSV files don’t have file-level metadata, so query engines can’t read a minimal subset of the data.  The query engine has to check all the files, even if they don’t contain any relevant data.

When data is in Parquet files, the query engine can open up all the files, read the footers, build the file-level metadata, and perform file skipping.  Fetching metadata in each file is slower than grabbing the pre-built file-level metadata from the transaction log.

Now, let’s see how to structure your tables to allow for more file skipping.

## File skipping for different file sizes

Delta tables store data in files.  Smaller files allow for more file skipping compared to bigger files.

However, an excessive number of small files isn’t good because it creates I/O overhead and slows down queries.

Your Delta tables should have files that are “right-sized”.  For a table with 150 GB of data, 5 GB files would probably be too large, and 10 KB files would be too small.  It’s generally best to store data in files that are between 100 MB and 1 GB.

Delta Lake has [an optimize function](https://delta-io.github.io/delta-rs/usage/optimize/small-file-compaction-with-optimize/) that performs small file compaction, so you don’t need to program this logic yourself.

Now, let's investigate how to store data in files to maximize the file skipping opportunities.

## How to maximize file skipping

You can maximize file-skipping by colocating similar data in the same files.

Suppose you have a table with test scores and frequently run queries that filter based on the `test_score` column.

```
filename	min_test_score	max_test_score
fileA		45			    100
fileB		65			    98
fileC		50			    96
```

Suppose you want to run the following query: `select * from exams where test_score > 90`.

This query cannot skip files, given the current organization of the data.  You can rearrange the data to colocate similar test scores in the same files to allow for file skipping.  Here’s the new layout: 

```
filename	min_test_score	max_test_score
fileD		45			    70
fileE		55		    	80
fileF		78			    100
```

The query (`select * from exams where test_score > 90`) can skip two of the three files with the new Delta table layout.  The query engine only has to read `fileF` for this query.

Now, let’s look at how file skipping works with string values.

## How file skipping works with strings

File skipping is also effective when filtering on string values.

Suppose you have a table with `person_name` and `country` columns.  There are millions of rows of data.  Here are the first three rows of data:

```
person_name	country
person1		angola
person2		china
person3		mexico
```

The Delta table contains three files with the following metadata:

```
filename	min_country	max_country
fileA		albania		mali
fileB		libia		paraguay
fileC		oman		zimbabwe
```

Suppose you want to run the following query: `select * from some_people where country = 'austria'`.

You only need to read the data in `fileA` to run this query.  The `min_country` value for `fileB` and `fileC` are greater than “austria”, so we know those files don’t contain any data relevant to the query.

File skipping can also be a robust optimization for string values.  Now, let’s see how file skipping works for partitioned tables.

## File skipping for partitioned tables

You can partition Delta tables for file skipping as well.  Suppose we have the same data as in the previous section, but the table is partitioned by `country`.

Here’s the Delta table:

```
filename	partition
fileA		albania
fileB		libia
fileC		oman
fileD		jamaica
fileE		albania
fileF		oman
```

Suppose you want to run the following query on this partitioned table: `select * from some_partitioned_table where country = 'albania'`.

You only need to read `fileA` and `fileE` to execute this query.  Delta Lake provides the file-level partition metadata in the transaction log so that this query will run quickly.

## Conclusion

Delta Lake allows for file skipping, which is a powerful performance optimization.

Delta Lake also provides built-in utilities to colocate data in the same files like partitioning, Z Ordering, and compaction to improve file skipping.

Delta Lake users need to know how to assess the tradeoffs of these techniques to optimize file skipping.  Users also need to understand the most frequent query patterns of their tables to best allow for file maximal file skipping.
