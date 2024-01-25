# Delta Lake small file compaction with optimize

This post shows you how to perform small file compaction with using the `optimize` method.  This was added to the `DeltaTable` class in version 0.9.0.  This command rearranges the small files into larger files which will reduce the number of files and speed up queries.

This is very helpful for workloads that append frequently. For example, if you have a table that is appended to every 10 minutes, after a year you will have 52,560 files in the table. If the table is partitioned by another dimension, you will have 52,560 files per partition; with just 100 unique values that's millions of files. By running `optimize` periodically, you can reduce the number of files in the table to a more manageable number.

Typically, you will run optimize less frequently than you append data. If possible, you might run optimize once you know you have finished writing to a particular partition. For example, on a table partitioned by date, you might append data every 10 minutes, but only run optimize once a day at the end of the day. This will ensure you don't need to compact the same data twice.

This section will also teach you about how to use `vacuum` to physically remove files from storage that are no longer needed.  You’ll often want vacuum after running optimize to remove the small files from storage once they’ve been compacted into larger files.

Let’s start with an example to explain these key concepts.  All the code covered in this post is stored in [this notebook](https://github.com/delta-io/delta-examples/blob/master/notebooks/python-deltalake/deltalake_0_9_0.ipynb) in case you’d like to follow along.

## Create a Delta table with small files

Let’s start by creating a Delta table with a lot of small files so we can demonstrate the usefulness of the `optimize` command.

Start by writing a function that generates on thousand rows of random data given a timestamp.

```python
def record_observations(date: datetime) -> pa.Table:
    """Pulls data for a certain datetime"""
    nrows = 1000
    return pa.table(
        {
            "date": pa.array([date.date()] * nrows),
            "timestamp": pa.array([date] * nrows),
            "value": pc.random(nrows),
        }
    )
```

Let’s run this function and observe the output:

```python
record_observations(datetime(2021, 1, 1, 12)).to_pandas()

	date				timestamp	value
0	2021-01-01	2021-01-01 12:00:00	0.3186397383362023
1	2021-01-01	2021-01-01 12:00:00	0.04253766974259088
2	2021-01-01	2021-01-01 12:00:00	0.9355682965171573
…
999	2021-01-01	2021-01-01 12:00:00	0.23207037062879843
```

Let’s write 100 hours worth of data to the Delta table.

```python
# Every hour starting at midnight on 2021-01-01
hours_iter = (datetime(2021, 1, 1) + timedelta(hours=i) for i in itertools.count())

# Write 100 hours worth of data
for timestamp in itertools.islice(hours_iter, 100):
    write_deltalake(
        "observation_data",
        record_observations(timestamp),
        partition_by=["date"],
        mode="append",
    )
```

This data was appended to the Delta table in 100 separate transactions, so the table will contain 100 transaction log entries and 100 data files.  You can see the number of files with the `files()` method.

```python
dt = DeltaTable("observation_data")
len(dt.files()) # 100
```

Here’s how the files are persisted in storage.

```
observation_data
├── _delta_log
│   ├── 00000000000000000000.json
│   ├── …
│   └── 00000000000000000099.json
├── date=2021-01-01
│   ├── 0-cfe227c6-edd9-4369-a1b0-db4559a2e693-0.parquet
│   ├── …
│   ├── 23-a4ace29e-e73e-40a1-81d3-0f5dc13093de-0.parquet
├── date=2021-01-02
│   ├── 24-9698b456-66eb-4075-8732-fe56d81edb60-0.parquet
│   ├── …
│   └── 47-d3fce527-e018-4c02-8acd-a649f6f523d2-0.parquet
├── date=2021-01-03
│   ├── 48-fd90a7fa-5a14-42ed-9f59-9fe48d87899d-0.parquet
│   ├── …
│   └── 71-5f143ade-8ae2-4854-bdc5-61154175665f-0.parquet
├── date=2021-01-04
│   ├── 72-477c10fe-dc09-4087-80f0-56006e4a7911-0.parquet
│   ├── …
│   └── 95-1c92cbce-8af4-4fe4-9c11-832245cf4d40-0.parquet
└── date=2021-01-05
    ├── 96-1b878ee5-25fd-431a-bc3e-6dcacc96b470-0.parquet
    ├── …
    └── 99-9650ed63-c195-433d-a86b-9469088c14ba-0.parquet
```

Each of these Parquet files are tiny - they’re only 10 KB.  Let’s see how to compact these tiny files into larger files, which is more efficient for data queries.

## Compact small files in the Delta table with optimize

Let’s run the optimize command to compact the existing small files into larger files:

```python
dt = DeltaTable("observation_data")

dt.optimize()
```

Here’s the output of the command:

```python
{'numFilesAdded': 5,
 'numFilesRemoved': 100,
 'filesAdded': {'min': 39000,
  'max': 238282,
  'avg': 198425.6,
  'totalFiles': 5,
  'totalSize': 992128},
 'filesRemoved': {'min': 10244,
  'max': 10244,
  'avg': 10244.0,
  'totalFiles': 100,
  'totalSize': 1024400},
 'partitionsOptimized': 5,
 'numBatches': 1,
 'totalConsideredFiles': 100,
 'totalFilesSkipped': 0,
 'preserveInsertionOrder': True}
```

The optimize operation has added 5 new files and marked 100 exisitng files for removal (this is also known as “tombstoning” files).  It has compacted the 100 tiny files into 5 larger files.

Let’s append some more data to the Delta table and see how we can selectively run optimize on the new data that’s added.

## Handling incremental updates with optimize

Let’s append another 24 hours of data to the Delta table:

```python
for timestamp in itertools.islice(hours_iter, 24):
    write_deltalake(
        dt,
        record_observations(timestamp),
        partition_by=["date"],
        mode="append",
    )
```

We can use `get_add_actions()` to introspect the table state. We can see that `2021-01-06` has only a few hours of data so far, so we don't want to optimize that yet. But `2021-01-05` has all 24 hours of data, so it's ready to be optimized.

```python
dt.get_add_actions(flatten=True).to_pandas()[
    "partition.date"
].value_counts().sort_index()

2021-01-01     1
2021-01-02     1
2021-01-03     1
2021-01-04     1
2021-01-05    21
2021-01-06     4
```

To optimize a single partition, you can pass in a `partition_filters` argument speficying which partitions to optimize.

```python
dt.optimize(partition_filters=[("date", "=", "2021-01-05")])

{'numFilesAdded': 1,
 'numFilesRemoved': 21,
 'filesAdded': {'min': 238282,
  'max': 238282,
  'avg': 238282.0,
  'totalFiles': 1,
  'totalSize': 238282},
 'filesRemoved': {'min': 10244,
  'max': 39000,
  'avg': 11613.333333333334,
  'totalFiles': 21,
  'totalSize': 243880},
 'partitionsOptimized': 1,
 'numBatches': 1,
 'totalConsideredFiles': 21,
 'totalFilesSkipped': 0,
 'preserveInsertionOrder': True}
```

This optimize operation tombstones 21 small data files and adds one file with all the existing data properly condensed.  Let’s take a look a portion of the `_delta_log/00000000000000000125.json` file, which is the transaction log entry that corresponds with this incremental optimize command.

```python
{
  "remove": {
    "path": "date=2021-01-05/part-00000-41178aab-2491-488f-943d-8f03867295ee-c000.snappy.parquet",
    "deletionTimestamp": 1683465499480,
    "dataChange": false,
    "extendedFileMetadata": null,
    "partitionValues": {
      "date": "2021-01-05"
    },
    "size": 39000,
    "tags": null
  }
}

{
  "remove": {
    "path": "date=2021-01-05/101-79ae6fc9-c0cc-49ec-bb94-9aba879ac949-0.parquet",
    "deletionTimestamp": 1683465499481,
    "dataChange": false,
    "extendedFileMetadata": null,
    "partitionValues": {
      "date": "2021-01-05"
    },
    "size": 10244,
    "tags": null
  }
}

…

{
  "add": {
    "path": "date=2021-01-05/part-00000-4b020a40-c836-4a11-851f-4691370c9f3a-c000.snappy.parquet",
    "size": 238282,
    "partitionValues": {
      "date": "2021-01-05"
    },
    "modificationTime": 1683465499493,
    "dataChange": false,
    "stats": "{\"numRecords\":24000,\"minValues\":{\"value\":0.00005581532256615507,\"timestamp\":\"2021-01-05T00:00:00.000Z\"},\"maxValues\":{\"timestamp\":\"2021-01-05T23:00:00.000Z\",\"value\":0.9999911402868216},\"nullCount\":{\"timestamp\":0,\"value\":0}}",
    "tags": null
  }
}
```

The trasaction log indicates that many files have been tombstoned and one file is added, as expected.

The Delta Lake optimize command “removes” data by marking the data files as removed in the transaction log.  The optimize command doesn’t physically delete the Parquet file from storage.  Optimize performs a “logical remove” not a “physical remove”.

Delta Lake uses logical operations so you can time travel back to earlier versions of your data.  You can vacuum your Delta table to physically remove Parquet files from storage if you don’t need to time travel and don’t want to pay to store the tombstoned files.

## Vacuuming after optimizing

The vacuum command deletes all files from storage that are marked for removal in the transaction log and older than the retention period which is 7 days by default.

It’s normally a good idea to have a retention period of at least 7 days.  For purposes of this example, we will set the retention period to zero, just so you can see how the files get removed from storage.  Adjusting the retention period in this manner isn’t recommended for production use cases.

Let’s run the vacuum command:

```python
dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
```

The command returns a list of all the files that are removed from storage:

```python
['date=2021-01-02/39-a98680f2-0e0e-4f26-a491-18b183f9eb05-0.parquet',
 'date=2021-01-02/41-e96bc8bb-c571-484c-b534-e897424fb7da-0.parquet',
 …
 'date=2021-01-01/0-cfe227c6-edd9-4369-a1b0-db4559a2e693-0.parquet',
 'date=2021-01-01/18-ded53418-172b-4e40-bf2e-7c8142e71bd1-0.parquet']
```

Let’s look at the content of the Delta table now that all the really small files have been removed from storage:

```
observation_data
├── _delta_log
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   ├── …
│   ├── 00000000000000000124.json
│   └── 00000000000000000125.json
├── date=2021-01-01
│   └── part-00000-31e3df5a-8bbe-425c-b85d-77794f922837-c000.snappy.parquet
├── date=2021-01-02
│   └── part-00000-8af07878-b179-49ce-a900-d58595ffb60a-c000.snappy.parquet
├── date=2021-01-03
│   └── part-00000-5e980864-b32f-4686-a58d-a75fae455c1e-c000.snappy.parquet
├── date=2021-01-04
│   └── part-00000-1e82d23b-084d-47e3-9790-d68289c39837-c000.snappy.parquet
├── date=2021-01-05
│   └── part-00000-4b020a40-c836-4a11-851f-4691370c9f3a-c000.snappy.parquet
└── date=2021-01-06
    ├── 121-0ecb5d70-4a28-4cd4-b2d2-89ee2285eaaa-0.parquet
    ├── 122-6b2d2758-9154-4392-b287-fe371ee507ec-0.parquet
    ├── 123-551d318f-4968-441f-83fc-89f98cd15daf-0.parquet
    └── 124-287309d3-662e-449d-b4da-2e67b7cc0557-0.parquet
```

All the partitions only contain a single file now, except for the `date=2021-01-06` partition that has not been compacted yet.

An entire partition won’t necessarily get compacted to a single data file when optimize is run.  Each partition has data files that are condensed to the target file size.

## What causes the small file problem?

Delta tables can accumulate small files for a variety of reasons:

* User error: users can accidentally write files that are too small.  Users should sometimes repartition in memory before writing to disk to avoid appending files that are too small.
* Frequent appends: systems that append more often tend to append more smaller files.  A pipeline that appends every minute will generally generate ten times as many small files compared to a system that appends every ten minutes.
* Appending to partitioned data lakes with high cardinality columns can also cause small files.  If you append every hour to a table that’s partitioned on a column with 1,000 distinct values, then every append could create 1,000 new files.  Partitioning by date avoids this problem because the data isn’t split up across partitions in this manner.  

## Conclusion

This page showed you how to create a Delta table with many small files, compact the small files into larger files with optimize, and remove the tombstoned files from storage with vacuum.

You also learned about how to incrementally optimize partitioned Delta tables, so you only compact newly added data.

An excessive number of small files slows down Delta table queries, so periodic compaction is important.  Make sure to properly maintain your Delta tables, so performance does not degrade over time.
