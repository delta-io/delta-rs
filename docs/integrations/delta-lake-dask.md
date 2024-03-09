# Using Delta Lake with Dask

Delta Lake is a great storage format for Dask analyses. This page explains why and how to use Delta Lake with Dask.

You will learn how to read Delta Lakes into Dask DataFrames, how to query Delta tables with Dask, and the unique advantages Delta Lake offers the Dask community.

Here are some of the benefits that Delta Lake provides Dask users:
- better performance with file skipping
- enhanced file skipping via Z Ordering
- ACID transactions for reliable writes
- easy time-travel functionality

> ❗️ dask-deltatable currently works with deltalake<=13.0. See https://github.com/dask-contrib/dask-deltatable/issues/65

## Install Dask-Deltatable

To use Delta Lake with Dask, first install the library using

```
pip install dask-deltatable
```


## Reading Delta Tables into a Dask DataFrame

You can read data stored in a Delta Lake into a Dask DataFrame using `dask-deltatable.read_deltalake`. 

Let's read in a toy dataset to see what we can do with Delta Lake and Dask. You can access the data stored as a Delta Lake [on Github](https://github.com/rrpelgrim/delta-examples/tree/master/data)

```
import dask_deltatable as ddt

# read delta table into Dask DataFrame
delta_path = "path/to/data/people_countries_delta_dask"
ddf = ddt.read_deltalake(delta_path)

```

Dask is a library for efficient distributed computing and works with lazy evaluation. Function calls to `dask.dataframe` build a task graph in the background. To trigger computation, call `.compute()`:

```
> ddf.compute()

|    | first_name   | last_name   | country   | continent   |
|---:|:-------------|:------------|:----------|:------------|
|  0 | Ernesto      | Guevara     | Argentina | NaN         |
|  0 | Bruce        | Lee         | China     | Asia        |
|  1 | Jack         | Ma          | China     | Asia        |
|  0 | Wolfgang     | Manche      | Germany   | NaN         |
|  1 | Soraya       | Jala        | Germany   | NaN         |
```


You can read in specific versions of Delta tables by specifying a `version` number or a timestamp:

```
# with specific version
ddf = ddt.read_deltalake(delta_path, version=3)

# with specific datetime
ddt.read_deltalake(delta_path, datetime="2018-12-19T16:39:57-08:00")
```

`dask-deltatable` also supports reading from remote sources like S3 with:

```
ddt.read_deltalake("s3://bucket_name/delta_path", version=3)
```

> To read data from remote sources you'll need to make sure the credentials are properly configured in environment variables or config files. Refer to your cloud provider documentation to configure these.

## What can I do with a Dask Deltatable?

Reading a Delta Lake in with `dask-deltatable` returns a regular Dask DataFrame. You can perform [all the regular Dask operations](https://docs.dask.org/en/stable/dataframe.html) on this DataFrame.

Let's take a look at the first few rows:

```
> ddf.head(n=3)

|    | first_name   | last_name   | country   |   continent |
|---:|:-------------|:------------|:----------|------------:|
|  0 | Ernesto      | Guevara     | Argentina |         nan |
```

`dask.dataframe.head()` shows you the first rows of the first partition in the dataframe. In this case, the first partition only has 1 row.

This is because the Delta Lake has been partitioned by country:

```
> !ls ../../data/people_countries_delta_dask
_delta_log        country=Argentina country=China     country=Germany
```

`dask-deltatable` neatly reads in the partitioned Delta Lake into corresponding Dask DataFrame partitions:

```
> # see number of partitions
> ddf.npartitions
3
```

You can inspect a single partition using `dask.dataframe.get_partition()`:

```
> ddf.get_partition(n=1).compute()

|    | first_name   | last_name   | country   | continent   |
|---:|:-------------|:------------|:----------|:------------|
|  0 | Bruce        | Lee         | China     | Asia        |
|  1 | Jack         | Ma          | China     | Asia        |
```

## Perform Dask Operations

Let's perform some basic computations over the Delta Lake data that's now stored in our Dask DataFrame. 

Suppose you want to group the dataset by the `country` column:

```
> ddf.groupby(['country']).count().compute()

| country   |   first_name |   last_name |   continent |
|:----------|-------------:|------------:|------------:|
| Argentina |            1 |           1 |           1 |
| China     |            2 |           2 |           2 |
| Germany   |            2 |           2 |           2 |
```

Dask executes this `groupby` operation in parallel across all available cores. 

## Map Functions over Partitions

You can also use Dask's `map_partitions` method to map a custom Python function over all the partitions. 

Let's write a function that will replace the missing `continent` values with the right continent names.

```
# define custom python function

# get na_string
df = ddf.get_partition(0).compute()
na_string = df.iloc[0].continent
na_string

# define function
def replace_proper(partition, na_string):
    if [partition.country == "Argentina"]:
        partition.loc[partition.country=="Argentina"] = partition.loc[partition.country=="Argentina"].replace(na_string, "South America")
    if [partition.country == "Germany"]:
        partition.loc[partition.country=="Germany"] = partition.loc[partition.country=="Germany"].replace(na_string, "Europe")
    else:
        pass
    return partition        
```

Now map this over all partitions in the Dask DataFrame:

```
# define metadata and map function over partitions
> meta = dict(ddf.dtypes)
> ddf3 = ddf.map_partitions(replace_proper, na_string, meta=meta)
> ddf3.compute()

|    | first_name   | last_name   | country   | continent     |
|---:|:-------------|:------------|:----------|:--------------|
|  0 | Ernesto      | Guevara     | Argentina | South America |
|  0 | Bruce        | Lee         | China     | Asia          |
|  1 | Jack         | Ma          | China     | Asia          |
|  0 | Wolfgang     | Manche      | Germany   | Europe        |
|  1 | Soraya       | Jala        | Germany   | Europe        |
```

## Write to Delta Lake
After doing your data processing in Dask, you can write the data back out to Delta Lake using `to_deltalake`:

```
ddt.to_deltalake(ddf, "tmp/test_write")
```

## Contribute to `dask-deltalake`
To contribute, go to the [`dask-deltalake` Github repository](https://github.com/rrpelgrim/dask-deltatable).