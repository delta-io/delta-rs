# Delta Lake Arrow Integrations

Delta Lake tables can be exposed as Arrow tables and Arrow datasets, which allows for interoperability with a variety of query engines.

This page shows you how to convert Delta tables to Arrow data structures and teaches you the difference between Arrow tables and Arrow datasets.

## Delta Lake to Arrow Dataset

Delta tables can easily be exposed as Arrow datasets.  This makes it easy for any query engine that can read Arrow datasets to read a Delta table.

Let's take a look at the h2o groupby dataset that contains 9 columns of data.  Here are three representative rows of data:

```
+-------+-------+--------------+-------+-------+--------+------+------+---------+
| id1   | id2   | id3          |   id4 |   id5 |    id6 |   v1 |   v2 |      v3 |
|-------+-------+--------------+-------+-------+--------+------+------+---------|
| id016 | id046 | id0000109363 |    88 |    13 | 146094 |    4 |    6 | 18.8377 |
| id039 | id087 | id0000466766 |    14 |    30 | 111330 |    4 |   14 | 46.7973 |
| id047 | id098 | id0000307804 |    85 |    23 | 187639 |    3 |    5 | 47.5773 |
+-------+-------+--------------+-------+-------+--------+------+------+---------+
```

Here's how to expose the Delta table as a PyArrow dataset and run a query with DuckDB:

```python
import duckdb
from deltalake import DeltaTable

table = DeltaTable("delta/G1_1e9_1e2_0_0")
dataset = table.to_pyarrow_dataset()
quack = duckdb.arrow(dataset)
quack.filter("id1 = 'id016' and v2 > 10")
```

Here's the result:

```
┌─────────┬─────────┬──────────────┬───────┬───────┬─────────┬───────┬───────┬───────────┐
│   id1   │   id2   │     id3      │  id4  │  id5  │   id6   │  v1   │  v2   │    v3     │
│ varchar │ varchar │   varchar    │ int32 │ int32 │  int32  │ int32 │ int32 │  double   │
├─────────┼─────────┼──────────────┼───────┼───────┼─────────┼───────┼───────┼───────────┤
│ id016   │ id054   │ id0002309114 │    62 │    95 │ 7180859 │     4 │    13 │  7.750173 │
│ id016   │ id044   │ id0003968533 │    63 │    98 │ 2356363 │     4 │    14 │  3.942417 │
│ id016   │ id034   │ id0001082839 │    58 │    73 │ 8039808 │     5 │    12 │ 76.820135 │
├─────────┴─────────┴──────────────┴───────┴───────┴─────────┴───────┴───────┴───────────┤
│ ? rows (>9999 rows, 3 shown)                                                 9 columns │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Arrow datasets allow for the predicates to get pushed down to the query engine, so the query is executed quickly.

## Delta Lake to Arrow Table

You can also run the same query with DuckDB on an Arrow table:

```python
quack = duckdb.arrow(table.to_pyarrow_table())
quack.filter("id1 = 'id016' and v2 > 10")
```

This returns the same result, but it runs slower.

## Difference between Arrow Dataset and Arrow Table

Arrow Datasets are lazy and allow for full predicate pushdown unlike Arrow tables which are eagerly loaded into memory.

The previous DuckDB queries were run on a 1 billion row dataset that's roughly 50 GB when stored as an uncompressed CSV file.  Here are the runtimes when the data is stored in a Delta table and the queries are executed on a 2021 Macbook M1 with 64 GB of RAM:

* Arrow table: 17.1 seconds
* Arrow dataset: 0.01 seconds

The query runs much faster on an Arrow dataset because the predicates can be pushed down to the query engine and lots of data can be skipped.

Arrow tables are eagerly materialized in memory and don't allow for the same amount of data skipping.

## Multiple query engines can query Arrow Datasets

Other query engines like DataFusion can also query Arrow datasets, see the following example:

```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_dataset("my_dataset", table.to_pyarrow_dataset())
ctx.sql("select * from my_dataset where v2 > 5")
```

Here's the result:

```
+-------+-------+--------------+-----+-----+--------+----+----+-----------+
| id1   | id2   | id3          | id4 | id5 | id6    | v1 | v2 | v3        |
+-------+-------+--------------+-----+-----+--------+----+----+-----------+
| id082 | id049 | id0000022715 | 97  | 55  | 756924 | 2  | 11 | 74.161136 |
| id053 | id052 | id0000113549 | 19  | 56  | 139048 | 1  | 10 | 95.178444 |
| id090 | id043 | id0000637409 | 94  | 50  | 12448  | 3  | 12 | 60.21896  |
+-------+-------+--------------+-----+-----+--------+----+----+-----------+
```

Any query engine that's capable of reading an Arrow table/dataset can read a Delta table.

## Conclusion

Delta tables can easily be exposed as Arrow tables/datasets.

Therefore any query engine that can read an Arrow table/dataset can also read a Delta table.

Arrow datasets allow for more predicates to be pushed down to the query engine, so they can perform better performance than Arrow tables.
