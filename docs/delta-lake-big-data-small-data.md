# Delta Lake for big data and small data

Delta Lake is an excellent storage format for big data and small data.

This post explains why Delta Lake is suitable for massive datasets and why many of these features that are great, even for tiny tables.  Delta Lake is fine for a table with less than 1 GB of data or hundreds of petabytes of data.

Let’s start by discussing the features that are great for small data.

## Delta Lake for small data tables

Delta Lake has many features that are useful for small datasets:

* [Reliable transactions](https://delta-io.github.io/delta-rs/how-delta-lake-works/delta-lake-acid-transactions/)
* Better performance via file skipping
* DML operations to make deletes, updates, and merges easy and performant
* Features like schema enforcement and constraints to enforce data quality
* Versioned data & time travel

All of these features are great for large and small tables.

Delta Lake DML operations are ACID transactions, so they either finish entirely or don’t finish at all.  Delta tables don’t require any downtime while DML operations are running.  The Delta Lake user experience is better than a data lake that doesn’t support transactions and has downtime while running DML operations.

The Delta Lake API also makes it easy to run DML operations.  You can delete a line of code from a Delta table with a single line of code.  Writing code to delete rows from CSV files is more challenging, especially if you want to implement this operation efficiently.

Delta Lake has built-in checks to retain the integrity of your tables.  For example, Delta tables have schema enforcement and prevent you from appending DataFrames with mismatched schema from the existing table.  Delta Lake also lets you add constraints that only allow appending specific values to a column.  Data quality is also essential for small tables!

Delta Lake splits data into multiple files with file-level metadata in the transaction log, so query engines can sometimes skip data.  Data skipping can be a huge performance benefit, depending on how much data can be ignored by the query engine.  

As previously mentioned, Delta tables record all DML operations as transactions.  Recording operations as transactions means that existing data isn’t mutated.  So Delta Lake provides versioned data and time travel out of the box.  Versioning data is better because it allows you to roll back mistakes and compare the state of the table at different points in time.

Delta Lake has many useful features for small data tables.  Let’s look at how Delta Lake is scalable for massive datasets.

## Delta Lake for large data tables

Delta Lake is designed to be scalable and can handle tables with terabytes or petabytes of data.

[See here](https://www.databricks.com/dataaisummit/session/flink-delta-driving-real-time-pipelines-doordash/) for an example of an organization ingesting 220 TB of data into a Delta table daily.

Delta tables store data in Parquet files, and cloud object stores allow engines to write any number of files.  Delta tables store metadata information in the transaction log as JSON files, which are periodically compacted into Parquet files, so an arbitrarily large amount of Delta table metadata can also be stored.

Delta Lake transactions and concurrency protection maintain the integrity of tables, even for large write operations or long-running computations.

It’s well known that Delta tables are scalable, even for the most enormous tables.

## Small data operations on large tables

Delta Lake is flexible and allows you to use “small data engines,” even for large tables, depending on the computation.

Suppose you have a Delta table containing 10 TB of data and a pipeline that appends 0.5 GB of data to the table every hour.  You don’t need a big data query engine to append a small amount of data.  You can set up this job to run the Delta table append with a small data engine like pandas or Polars.

Delta tables are flexible and interoperable with many technologies so that you can use the right tool for each data processing job.  This allows you to design pipelines how you’d like and minimize costs.

## When Delta Lake isn’t needed

You don’t need Delta Lake for a small dataset that never changes and can be stored in a single Parquet file.

Suppose you have a 0.5 GB dataset in a Parquet file that never needs to be updated.  You can just keep that data in a Parquet table.  Reading the metadata from the Parquet footer of a single file isn’t expensive.  You won’t be taking advantage of Delta Lake's features like transactions, convenient DML operations, or versioned data.

But in most cases, it’s best to use Delta Lake because its features protect the integrity of your tables and make your life easier.

## Conclusion

Delta Lake is well known for being scalable to huge tables but is also an excellent technology for small tables.

Delta Lake is a lightweight technology, so there is little overhead.  Writing the metadata file after performing a transaction is fast.  It’s a minuscule cost, considering the benefits you receive.

Many reasons that make Delta Lake better than data lakes for large tables also apply to small tables!
